// Package lbank is the fake lbank (exchange 9): the V3 WebSocket at
// wss://www.lbank.com/old-wss/ccws/ws/V3, subscription fdepth, and the REST depth NiFi's LBANK
// group calls on a snapshot request (NiFi reaches both through its proxy). As real lbank
// behaves (captured 2026-10-21):
//
//   - A subscribe is acked in PLAIN TEXT, <40-char connection token>_fdepth_<pair>_<depth>
//     (the same again on a repeat), then the whole book at once and ~2 a second after. An
//     unsubscribe gets no reply.
//   - Every frame is the whole book: {"depth":{asks,bids},"SERVER":"V3","count":…,"limit":…,
//     "type":"fdepth","pair":…,"TS":…}, prices with the market's decimals, TS in CHINA TIME
//     (UTC+8) with no zone. lpa-core's parser reads TS as UTC, so on real lbank its event
//     times are 8 hours ahead; the simulator sends what lbank sends.
//   - The server pings {"ping":"<send ms>","action":"ping"} every 60 s, the first at a varying
//     point of the first minute. A pong for a ping this connection got gets no reply (even
//     twice); any other pong is "[<id>] is not a ping". After 6 pings in a row with no pong
//     the connection is closed 1001 "server-heartbeat-timeout" when the 7th is due. A client ping {"action":"ping","ping":X} gets
//     {"action":"pong","pong":X}.
//   - Text it cannot read is error "message" — NiFi meets this: its ping path takes every frame
//     of 60 bytes or less and so echoes the 59-byte ack back. Without depth: "Missing
//     parameter ['depth']". Another subscription type: no reply.
//   - REST {"result":"true","msg":"Success","data":{asks,bids,timestamp},"error_code":0,"ts":…},
//     prices without trailing zeros; with no size (or no symbol) there is no data at all.
//
// Not copied: an unknown pair is "Invalid order pairs:[x]" on real lbank; here every market
// exists. Inferred, not seen: any other action is error "message". lpa-core never sends lbank a
// snapshot request, and its parser drops the REST body (no depth/TS).
package lbank

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	mrand "math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"exchange-simulator/internal/domain"
	"exchange-simulator/internal/exchange"
)

// Subscription is the only subscription type served.
const Subscription = "fdepth"

// china is lbank's clock: TS is written in UTC+8 with no zone.
var china = time.FixedZone("UTC+8", 8*60*60)

// Options mirrors the LBANK_* settings in .env.
type Options struct {
	ID            int64
	WS            domain.Endpoint
	REST          domain.Endpoint
	PriceDecimals int           // prices are written with this many decimals (the book's price scale)
	PingFirst     time.Duration // 0 = a random point of the first PingEvery, as lbank's server-wide clock
	PingEvery     time.Duration // 0 = 60 s
	TimeNow       func() time.Time
}

// Exchange is the fake lbank.
type Exchange struct {
	opts  Options
	mu    sync.Mutex
	pings map[string]time.Time // connID|ping id → when sent, kept 10 minutes
}

// New builds the fake lbank.
func New(opts Options) *Exchange {
	if opts.PingEvery == 0 {
		opts.PingEvery = 60 * time.Second
	}
	if opts.TimeNow == nil {
		opts.TimeNow = time.Now
	}
	return &Exchange{opts: opts, pings: map[string]time.Time{}}
}

var (
	_ exchange.Exchange    = (*Exchange)(nil)
	_ exchange.Connector   = (*Exchange)(nil)
	_ exchange.Heartbeater = (*Exchange)(nil)
)

func (e *Exchange) Name() string                  { return "lbank" }
func (e *Exchange) ID() int64                     { return e.opts.ID }
func (e *Exchange) WS() domain.Endpoint           { return e.opts.WS }
func (e *Exchange) REST() (domain.Endpoint, bool) { return e.opts.REST, true }
func (e *Exchange) SeqStep(*mrand.Rand) int64     { return 1 } // no sequence on the wire
func (e *Exchange) Envelope(frame []byte, id string, simulation int) []byte {
	return exchange.PrependIDAndSimulation(frame, id, simulation)
}

// MissedLimit pings in a row with no pong close the connection (measured: closed when the 7th
// ping was due, 1001 "server-heartbeat-timeout").
const MissedLimit = 6

// Heartbeat is lbank's text ping. The id is its send time in ms, remembered per connection
// so the pong can be checked. It is called once per connection.
func (e *Exchange) Heartbeat() exchange.Heartbeat {
	first := e.opts.PingFirst
	if first == 0 {
		first = time.Duration(mrand.Int63n(int64(e.opts.PingEvery))) + 1
	}
	return exchange.Heartbeat{
		Every: e.opts.PingEvery, First: first,
		IsPong:      e.isPong,
		MissedLimit: MissedLimit, CloseCode: 1001, CloseText: "server-heartbeat-timeout",
		MsgFor: func(connID string, now time.Time) []byte {
			id := strconv.FormatInt(now.UnixMilli(), 10)
			e.mu.Lock()
			for k, at := range e.pings {
				if now.Sub(at) > 10*time.Minute {
					delete(e.pings, k)
				}
			}
			e.pings[connID+"|"+id] = now
			e.mu.Unlock()
			return []byte(`{"ping":"` + id + `","action":"ping"}`)
		},
	}
}

// isPong is true for a pong to a ping this connection got.
func (e *Exchange) isPong(connID string, msg []byte) bool {
	var req request
	if json.Unmarshal(msg, &req) != nil || req.Action != "pong" {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	_, sent := e.pings[connID+"|"+req.Pong]
	return sent
}

// Snapshot is the whole book.
func (e *Exchange) Snapshot(market string, b *domain.Book, now time.Time) []byte {
	return e.fdepth(market, b, now)
}

// Update is the whole book too: fdepth sends nothing else.
func (e *Exchange) Update(market string, b *domain.Book, _, _ []domain.Level, _ int64, now time.Time) []byte {
	return e.fdepth(market, b, now)
}

func (e *Exchange) fdepth(market string, b *domain.Book, now time.Time) []byte {
	asks, bids := b.Top(b.Depth())
	pair, _ := json.Marshal(market)
	n := strconv.Itoa(b.Depth())
	var w bytes.Buffer
	w.WriteString(`{"depth":{"asks":`)
	e.writeLevels(&w, asks, true)
	w.WriteString(`,"bids":`)
	e.writeLevels(&w, bids, true)
	w.WriteString(`},"SERVER":"V3","count":` + n + `,"limit":` + n + `,"type":"fdepth","pair":`)
	w.Write(pair)
	w.WriteString(`,"TS":"` + ts(now) + `"}`)
	return w.Bytes()
}

// writeLevels writes [["price","qty"],…]: on the WebSocket prices are padded to the market's
// decimals, on REST trailing zeros are dropped.
func (e *Exchange) writeLevels(w *bytes.Buffer, side []domain.Level, padded bool) {
	w.WriteByte('[')
	for i, l := range side {
		if i > 0 {
			w.WriteByte(',')
		}
		price := trim(l.Price)
		if padded {
			price = pad(l.Price, e.opts.PriceDecimals)
		}
		w.WriteString(`["` + price + `","` + l.Qty + `"]`)
	}
	w.WriteByte(']')
}

func ts(now time.Time) string { return now.In(china).Format("2006-01-02T15:04:05.000") }

// request is a client message; lbank sends every value as a string.
type request struct {
	Action    string `json:"action"`
	Subscribe string `json:"subscribe"`
	Pair      string `json:"pair"`
	Depth     string `json:"depth"`
	Ping      string `json:"ping"`
	Pong      string `json:"pong"`
}

// Connect returns the connection's handler; the connection's token names it in every ack.
func (e *Exchange) Connect(s exchange.Session) func(msg []byte) {
	token := randomToken(40)
	return func(msg []byte) { e.handle(s, token, msg) }
}

// OnMessage is a message outside a Connect handler (the server always uses Connect).
func (e *Exchange) OnMessage(s exchange.Session, msg []byte) {
	e.handle(s, randomToken(40), msg)
}

func (e *Exchange) handle(s exchange.Session, token string, msg []byte) {
	now := e.opts.TimeNow()
	fail := func(message string) {
		m, _ := json.Marshal(message)
		s.Send([]byte(`{"SERVER":"V3","message":` + string(m) + `,"status":"error","TS":"` + ts(now) + `"}`))
	}
	var req request
	if json.Unmarshal(msg, &req) != nil {
		fail("message")
		return
	}
	switch req.Action {
	case "ping":
		s.Send([]byte(`{"action":"pong","pong":` + quote(req.Ping) + `}`))
	case "pong":
		// A pong for an outstanding ping is the server's; this is a repeat or a stranger.
		if !e.isPong(s.ID(), msg) {
			fail("[" + req.Pong + "] is not a ping")
		}
	case "subscribe", "unsubscribe":
		switch {
		case req.Subscribe != Subscription:
			// no reply
		case req.Depth == "":
			fail("Missing parameter ['depth']")
		case req.Action == "subscribe":
			ack := []byte(token + "_" + Subscription + "_" + req.Pair + "_" + req.Depth)
			s.Subscribe(req.Pair, func(int64) []byte { return ack }, true)
		default:
			s.Unsubscribe(req.Pair, nil)
		}
	default:
		fail("message")
	}
}

// ServeREST is GET /v2/depth.do?symbol=…&size=…; with no size or no symbol there is no data.
func (e *Exchange) ServeREST(w http.ResponseWriter, r *http.Request, books exchange.Books) {
	q := r.URL.Query()
	now := strconv.FormatInt(e.opts.TimeNow().UnixMilli(), 10)
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	size, err := strconv.Atoi(q.Get("size"))
	symbol := q.Get("symbol")
	if err != nil || size < 1 || symbol == "" {
		_, _ = w.Write([]byte(`{"result":"true","msg":"Success","error_code":0,"ts":` + now + `}`))
		return
	}
	asks, bids, _ := books.Top(symbol, size)
	var out bytes.Buffer
	out.WriteString(`{"result":"true","msg":"Success","data":{"asks":`)
	e.writeLevels(&out, asks, false)
	out.WriteString(`,"bids":`)
	e.writeLevels(&out, bids, false)
	out.WriteString(`,"timestamp":` + now + `},"error_code":0,"ts":` + now + `}`)
	_, _ = w.Write(out.Bytes())
}

// pad gives a price at least decimals decimals: "84547.6" → "84547.60".
func pad(price string, decimals int) string {
	if decimals == 0 {
		return price
	}
	whole, frac, _ := strings.Cut(price, ".")
	if len(frac) >= decimals {
		return price
	}
	return whole + "." + frac + strings.Repeat("0", decimals-len(frac))
}

func trim(s string) string {
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	return s
}

func quote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func randomToken(n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-"
	b := make([]byte, n)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = chars[int(b[i])%len(chars)]
	}
	return string(b)
}
