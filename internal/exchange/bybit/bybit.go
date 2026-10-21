// Package bybit is the fake bybit (exchange 6): the v5 public spot WebSocket at
// wss://stream.bybit.com/v5/public/spot, topic orderbook.50.{symbol}, and the REST order book
// NiFi's BYBIT group calls (through the proxy) on a snapshot request. As real bybit behaves
// (captured 2026-10-21):
//
//   - {"op":"subscribe","args":["orderbook.50.BTCUSDT",…]} gets ONE ack,
//     {"success":true,"ret_msg":"subscribe","conn_id":…,"op":"subscribe"} (req_id echoed), then
//     each new topic's snapshot. A topic already subscribed on the connection gets nothing.
//     More than 10 args: "args size >10" and nothing is subscribed. An unsubscribe is acked,
//     subscribed or not.
//   - Then deltas: only the changed levels, an unchanged side as []; u moves by exactly one;
//     seq is a counter shared across topics that moves by a few (4…30 seen).
//   - {"op":"ping"} → pong reply; text that is not JSON (even "ping") → "Invalid JSON!"; an
//     unknown op → no reply.
//   - REST {"retCode":0,"retMsg":"OK","result":{s,a,b,ts,u,seq,cts},"retExtInfo":{},"time":…};
//     result.u belongs to another stream (the 1000-level one), so it is not the WS u.
//
// Not copied: an unknown symbol is "Invalid symbol" / "Not supported symbols" on real bybit;
// here every market exists. Real deltas also delete (qty "0") and add levels; the book here
// only changes quantities.
package bybit

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"hash/fnv"
	mrand "math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"exchange-simulator/internal/domain"
	"exchange-simulator/internal/exchange"
)

// TopicPrefix + symbol is a market's topic.
const TopicPrefix = "orderbook.50."

// MaxArgs is the most topics one subscribe may carry.
const MaxArgs = 10

// seqBase puts seq near real bybit's values (115874171723 seen).
const seqBase = 115_000_000_000

// Options mirrors the BYBIT_* settings in .env.
type Options struct {
	ID      int64
	WS      domain.Endpoint
	REST    domain.Endpoint
	TimeNow func() time.Time // nil = time.Now; for tests
}

// Exchange is the fake bybit.
type Exchange struct{ opts Options }

// New builds the fake bybit.
func New(opts Options) *Exchange {
	if opts.TimeNow == nil {
		opts.TimeNow = time.Now
	}
	return &Exchange{opts: opts}
}

var (
	_ exchange.Exchange  = (*Exchange)(nil)
	_ exchange.Connector = (*Exchange)(nil)
)

func (e *Exchange) Name() string                  { return "bybit" }
func (e *Exchange) ID() int64                     { return e.opts.ID }
func (e *Exchange) WS() domain.Endpoint           { return e.opts.WS }
func (e *Exchange) REST() (domain.Endpoint, bool) { return e.opts.REST, true }
func (e *Exchange) SeqStep(*mrand.Rand) int64     { return 1 } // u moves by exactly one
func (e *Exchange) Envelope(frame []byte, id string, simulation int) []byte {
	return exchange.PrependIDAndSimulation(frame, id, simulation)
}

// seq is bybit's cross-topic counter for the frame with this u: always rising, by 5…29.
func (e *Exchange) seq(u int64) int64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(strconv.FormatInt(u, 10)))
	return seqBase + 17*u + int64(h.Sum32()%13)
}

// Snapshot is the whole book at u.
func (e *Exchange) Snapshot(market string, b *domain.Book, now time.Time) []byte {
	asks, bids := b.Top(b.Depth())
	return e.frame("snapshot", market, asks, bids, b.Seq(), now)
}

// Update is a delta of the changed levels.
func (e *Exchange) Update(market string, b *domain.Book, asks, bids []domain.Level, _ int64, now time.Time) []byte {
	return e.frame("delta", market, asks, bids, b.Seq(), now)
}

func (e *Exchange) frame(kind, market string, asks, bids []domain.Level, u int64, now time.Time) []byte {
	sym, _ := json.Marshal(market)
	topic, _ := json.Marshal(TopicPrefix + market)
	cts := now.UnixMilli()
	var w bytes.Buffer
	w.WriteString(`{"topic":`)
	w.Write(topic)
	// ts (gateway) comes a few ms after cts (matching engine): 1…8 ms seen.
	w.WriteString(`,"ts":` + strconv.FormatInt(cts+3, 10) + `,"type":"` + kind + `","data":{"s":`)
	w.Write(sym)
	w.WriteString(`,"b":`)
	writeLevels(&w, bids)
	w.WriteString(`,"a":`)
	writeLevels(&w, asks)
	w.WriteString(`,"u":` + strconv.FormatInt(u, 10) + `,"seq":` + strconv.FormatInt(e.seq(u), 10) +
		`},"cts":` + strconv.FormatInt(cts, 10) + `}`)
	return w.Bytes()
}

// writeLevels writes [["price","qty"],…], quantities without trailing zeros; never null.
func writeLevels(w *bytes.Buffer, side []domain.Level) {
	w.WriteByte('[')
	for i, l := range side {
		if i > 0 {
			w.WriteByte(',')
		}
		w.WriteString(`["` + l.Price + `","` + trim(l.Qty) + `"]`)
	}
	w.WriteByte(']')
}

func trim(s string) string {
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	return s
}

type request struct {
	Op    string   `json:"op"`
	Args  []string `json:"args"`
	ReqID string   `json:"req_id"`
}

// reply is bybit's answer, keys in its order. json.Marshal escapes '>' as >, as bybit does.
type reply struct {
	Success bool   `json:"success"`
	RetMsg  string `json:"ret_msg"`
	ConnID  string `json:"conn_id"`
	ReqID   string `json:"req_id,omitempty"`
	Op      string `json:"op"`
}

// Connect returns the connection's handler: it keeps the conn_id and the topics subscribed.
func (e *Exchange) Connect(s exchange.Session) func(msg []byte) {
	connID := newConnID()
	subscribed := map[string]bool{}
	return func(msg []byte) { e.handle(s, connID, subscribed, msg) }
}

// OnMessage is a message outside a Connect handler (the server always uses Connect).
func (e *Exchange) OnMessage(s exchange.Session, msg []byte) {
	e.handle(s, newConnID(), map[string]bool{}, msg)
}

func (e *Exchange) handle(s exchange.Session, connID string, subscribed map[string]bool, msg []byte) {
	var req request
	if json.Unmarshal(msg, &req) != nil {
		s.Send([]byte(`{"type":"error","ret_code":"400","ret_msg":"Invalid JSON!"}`))
		return
	}
	answer := func(ok bool, retMsg string) []byte {
		b, _ := json.Marshal(reply{Success: ok, RetMsg: retMsg, ConnID: connID, ReqID: req.ReqID, Op: req.Op})
		return b
	}
	switch req.Op {
	case "ping":
		s.Send(answer(true, "pong"))
		return
	case "subscribe", "unsubscribe":
	default:
		return // no reply
	}
	switch {
	case len(req.Args) == 0:
		s.Send(answer(false, "args required!"))
		return
	case req.Op == "subscribe" && len(req.Args) > MaxArgs:
		s.Send(answer(false, "args size >"+strconv.Itoa(MaxArgs)))
		return
	}
	for _, topic := range req.Args {
		if m, ok := strings.CutPrefix(topic, TopicPrefix); !ok || m == "" {
			s.Send(answer(false, "Invalid topic :["+topic+"]"))
			return
		}
	}
	if req.Op == "unsubscribe" {
		ack := answer(true, "unsubscribe")
		for i, topic := range req.Args {
			delete(subscribed, topic)
			if i == len(req.Args)-1 {
				s.Unsubscribe(strings.TrimPrefix(topic, TopicPrefix), ack)
			} else {
				s.Unsubscribe(strings.TrimPrefix(topic, TopicPrefix), nil)
			}
		}
		return
	}
	var fresh []string
	for _, topic := range req.Args {
		if !subscribed[topic] {
			subscribed[topic] = true
			fresh = append(fresh, topic)
		}
	}
	ack := answer(true, "subscribe")
	for i, topic := range fresh {
		var a func(int64) []byte
		if i == 0 { // the one ack goes before the first snapshot
			a = func(int64) []byte { return ack }
		}
		s.Subscribe(strings.TrimPrefix(topic, TopicPrefix), a, true)
	}
}

// newConnID looks like bybit's: 20 lowercase letters/digits, "-", 5 more.
func newConnID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 26)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = chars[int(b[i])%len(chars)]
	}
	b[20] = '-'
	return string(b)
}

// ServeREST is GET /v5/market/orderbook?category=spot&symbol=…&limit=…. As on bybit, an error
// is HTTP 200 with a non-zero retCode.
func (e *Exchange) ServeREST(w http.ResponseWriter, r *http.Request, books exchange.Books) {
	q := r.URL.Query()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	now := e.opts.TimeNow()
	fail := func(msg string) {
		_, _ = w.Write([]byte(`{"retCode":10001,"retMsg":"` + msg + `","result":{},"retExtInfo":{},"time":` +
			strconv.FormatInt(now.UnixMilli(), 10) + `}`))
	}
	if q.Get("category") != "spot" {
		fail("Illegal category")
		return
	}
	symbol := q.Get("symbol")
	if symbol == "" {
		fail("Parameter symbol [string] missing!")
		return
	}
	limit := 1 // bybit's default
	if n, err := strconv.Atoi(q.Get("limit")); err == nil && n > 0 {
		limit = n
	}
	asks, bids, u := books.Top(symbol, limit)
	sym, _ := json.Marshal(symbol)
	cts := now.UnixMilli()
	var out bytes.Buffer
	out.WriteString(`{"retCode":0,"retMsg":"OK","result":{"s":`)
	out.Write(sym)
	out.WriteString(`,"a":`)
	writeLevels(&out, asks)
	out.WriteString(`,"b":`)
	writeLevels(&out, bids)
	// u is the 1000-level stream's counter, not this book's: a different, smaller number.
	out.WriteString(`,"ts":` + strconv.FormatInt(cts+1, 10) + `,"u":` + strconv.FormatInt(u/5, 10) +
		`,"seq":` + strconv.FormatInt(e.seq(u), 10) + `,"cts":` + strconv.FormatInt(cts, 10) +
		`},"retExtInfo":{},"time":` + strconv.FormatInt(cts+150, 10) + `}`)
	_, _ = w.Write(out.Bytes())
}
