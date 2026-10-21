// Package ramzinex is the fake ramzinex (exchange 4): the Centrifugo WebSocket at
// wss://websocket.ramzinex.com/websocket, channel orderbook:{numeric market id}, and the REST
// order book NiFi's RAMZINEX group calls on a snapshot request. The protocol (connect,
// subscribe, ping) is package centrifugo, with ramzinex's differences (seen live 2026-10-21):
// no "version" in the connect reply, a number-like epoch, any channel namespace acked, and
// 10 s to answer a ping.
//
// Every push is the WHOLE book: {"buys":[…],"sells":[…]}, both sides price-DESCENDING (the
// best ask is the last sell), each level [price, quantity, price × quantity, false, null,
// a small int, epoch ms]. Real ramzinex computes the notional with floats (1373.3777799999998);
// so does this. The meaning of the small int is not known; it is 8…90 as live, derived from
// the level (so a level keeps it between pushes). The per-level time is now minus up to 25
// minutes, also derived from the level, as live times spread over the last minutes.
// The REST answer is {"data":{buys,sells},"status":0}; lpa-core never sends ramzinex a
// snapshot request today, and its parser has no REST branch.
package ramzinex

import (
	"bytes"
	"hash/fnv"
	"net/http"
	"strconv"
	"strings"
	"time"

	"exchange-simulator/internal/domain"
	"exchange-simulator/internal/exchange"
	"exchange-simulator/internal/exchange/centrifugo"
)

// ChannelPrefix + market is the channel of a market's book.
const ChannelPrefix = "orderbook:"

// PongTimeout is how long real ramzinex waits for {} after its {} ping.
const PongTimeout = 10 * time.Second

// restSuffix ends the REST path: {REST.Path}{market}/buys_sells.
const restSuffix = "/buys_sells"

// Options mirrors the RAMZINEX_* settings in .env.
type Options struct {
	ID          int64
	WS          domain.Endpoint
	REST        domain.Endpoint // Path ends in "/": the market follows it
	PingEvery   time.Duration   // 0 = 25 s
	PongTimeout time.Duration   // 0 = PongTimeout
	TimeNow     func() time.Time
}

// Exchange is the fake ramzinex.
type Exchange struct {
	*centrifugo.Protocol
	opts Options
}

// New builds the fake ramzinex.
func New(opts Options) *Exchange {
	if opts.PongTimeout == 0 {
		opts.PongTimeout = PongTimeout
	}
	if opts.TimeNow == nil {
		opts.TimeNow = time.Now
	}
	return &Exchange{Protocol: centrifugo.New(centrifugo.Options{
		Prefix: ChannelPrefix, Epoch: strconv.FormatInt(time.Now().Unix(), 10), AnyNamespace: true,
		PingEvery: opts.PingEvery, PongTimeout: opts.PongTimeout,
	}), opts: opts}
}

var (
	_ exchange.Exchange    = (*Exchange)(nil)
	_ exchange.Heartbeater = (*Exchange)(nil)
)

func (e *Exchange) Name() string                  { return "ramzinex" }
func (e *Exchange) ID() int64                     { return e.opts.ID }
func (e *Exchange) WS() domain.Endpoint           { return e.opts.WS }
func (e *Exchange) REST() (domain.Endpoint, bool) { return e.opts.REST, true }

// Snapshot is the whole book at the current offset.
func (e *Exchange) Snapshot(market string, b *domain.Book, now time.Time) []byte {
	return e.Push(market, b.Seq(), sides(b, b.Depth(), now))
}

// Update is the whole book too: a ramzinex push never carries only the changed levels.
func (e *Exchange) Update(market string, b *domain.Book, _, _ []domain.Level, _ int64, now time.Time) []byte {
	return e.Push(market, b.Seq(), sides(b, b.Depth(), now))
}

// ServeREST is GET {REST.Path}{market}/buys_sells.
func (e *Exchange) ServeREST(w http.ResponseWriter, r *http.Request, books exchange.Books) {
	rest, ok := strings.CutPrefix(r.URL.Path, e.opts.REST.Path)
	market, ok2 := strings.CutSuffix(rest, restSuffix)
	if !ok || !ok2 || market == "" || strings.Contains(market, "/") {
		http.NotFound(w, r)
		return
	}
	asks, bids, _ := books.Top(market, 1<<30)
	var out bytes.Buffer
	out.WriteString(`{"data":`)
	out.Write(encode(asks, bids, e.opts.TimeNow()))
	out.WriteString(`,"status":0}`)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(out.Bytes())
}

func sides(b *domain.Book, n int, now time.Time) []byte {
	asks, bids := b.Top(n)
	return encode(asks, bids, now)
}

// encode writes {"buys":[…],"sells":[…]}, both sides price-descending.
func encode(asks, bids []domain.Level, now time.Time) []byte {
	var w bytes.Buffer
	w.WriteString(`{"buys":[`)
	for i, l := range bids { // best first = descending
		if i > 0 {
			w.WriteByte(',')
		}
		writeLevel(&w, l, now)
	}
	w.WriteString(`],"sells":[`)
	for i := len(asks) - 1; i >= 0; i-- { // best first is ascending: reversed
		if i < len(asks)-1 {
			w.WriteByte(',')
		}
		writeLevel(&w, asks[i], now)
	}
	w.WriteString(`]}`)
	return w.Bytes()
}

// writeLevel writes [price,quantity,notional,false,null,n,ms].
func writeLevel(w *bytes.Buffer, l domain.Level, now time.Time) {
	p, q := trim(l.Price), trim(l.Qty)
	pf, _ := strconv.ParseFloat(p, 64)
	qf, _ := strconv.ParseFloat(q, 64)
	h := fnv.New32a()
	_, _ = h.Write([]byte(p + ":" + q))
	sum := h.Sum32()
	w.WriteByte('[')
	w.WriteString(p + "," + q + ",")
	w.WriteString(strconv.FormatFloat(pf*qf, 'f', -1, 64))
	w.WriteString(",false,null,")
	w.WriteString(strconv.Itoa(8 + int(sum%83)))
	w.WriteByte(',')
	w.WriteString(strconv.FormatInt(now.UnixMilli()-int64(sum%1_500_000), 10))
	w.WriteByte(']')
}

// trim drops trailing zeros: ramzinex writes JSON numbers (84048, 0.000042).
func trim(s string) string {
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	return s
}
