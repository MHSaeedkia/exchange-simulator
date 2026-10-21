// Package bitget is the fake bitget (exchange 5): the v2 public WebSocket at
// wss://ws.bitget.com/v2/ws/public, channel books50, the way NiFi's BITGET group talks to it
// and the way real bitget behaves (captured 2026-10-21):
//
//   - {"op":"subscribe"|"unsubscribe","args":[{"channel":"books50","instType":"SPOT","instId":…}]}
//     gets one ack per arg, {"event":"subscribe","arg":{"instType":…,"channel":…,"instId":…}};
//     a repeat subscribe is acked again, and so is an unsubscribe of a market not subscribed.
//   - Every frame is a whole-book snapshot (50 levels per side, ~8 a second on BTCUSDT); no
//     extra snapshot on subscribe. seq moves forward by thousands (a book counter sampled).
//   - "ping" → "pong". Text it cannot read → error 30002 "Unrecognized request:<text>"; another
//     channel → error 30016 "Param error"; empty args → no reply.
//   - A client that sent no "ping" for 2 minutes is dropped with no close frame (cut 126–145 s
//     after the last ping; other messages do not count). NiFi never pings, so on real bitget
//     NiFi's connection is cut about every 2 minutes and it reconnects; so it is here.
//
// Not copied: an unknown instId is error 30001 on real bitget; here every market exists.
// The live NiFi group makes no REST call for bitget, so none is served.
package bitget

import (
	"bytes"
	"encoding/json"
	mrand "math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"exchange-simulator/internal/domain"
	"exchange-simulator/internal/exchange"
)

// Wire constants of the books50 channel.
const (
	Channel  = "books50"
	InstType = "SPOT"
)

// Error codes, as real bitget sends them.
const (
	CodeUnrecognized = 30002
	CodeParamError   = 30016
)

// Options mirrors the BITGET_* settings in .env.
type Options struct {
	ID        int64
	WS        domain.Endpoint
	StepMin   int64 // seq step per frame; real bitget steps by thousands
	StepMax   int64
	IdleLimit time.Duration // 0 = 2 min without a ping
	IdleCheck time.Duration // 0 = every 30 s
}

// Exchange is the fake bitget.
type Exchange struct{ opts Options }

// New builds the fake bitget.
func New(opts Options) *Exchange {
	if opts.IdleLimit == 0 {
		opts.IdleLimit = 2 * time.Minute
	}
	if opts.IdleCheck == 0 {
		opts.IdleCheck = 30 * time.Second
	}
	return &Exchange{opts: opts}
}

var (
	_ exchange.Exchange  = (*Exchange)(nil)
	_ exchange.IdleRules = (*Exchange)(nil)
)

func (e *Exchange) Name() string                  { return "bitget" }
func (e *Exchange) ID() int64                     { return e.opts.ID }
func (e *Exchange) WS() domain.Endpoint           { return e.opts.WS }
func (e *Exchange) REST() (domain.Endpoint, bool) { return domain.Endpoint{}, false }
func (e *Exchange) SeqStep(rng *mrand.Rand) int64 {
	return e.opts.StepMin + rng.Int63n(e.opts.StepMax-e.opts.StepMin+1)
}
func (e *Exchange) Envelope(frame []byte, id string, simulation int) []byte {
	return exchange.PrependIDAndSimulation(frame, id, simulation)
}

// ServeREST is never routed: REST reports none.
func (e *Exchange) ServeREST(w http.ResponseWriter, r *http.Request, _ exchange.Books) {
	http.NotFound(w, r)
}

// Idle is real bitget's: dropped after 2 minutes with no "ping".
func (e *Exchange) Idle() exchange.Idle {
	return exchange.Idle{
		Limit: e.opts.IdleLimit, Check: e.opts.IdleCheck,
		IsAlive: func(msg []byte) bool { return string(msg) == "ping" },
	}
}

// Snapshot is the whole book.
func (e *Exchange) Snapshot(market string, b *domain.Book, now time.Time) []byte {
	return books50(market, b, now)
}

// Update is the whole book too: books50 sends nothing but snapshots.
func (e *Exchange) Update(market string, b *domain.Book, _, _ []domain.Level, _ int64, now time.Time) []byte {
	return books50(market, b, now)
}

func books50(market string, b *domain.Book, now time.Time) []byte {
	asks, bids := b.Top(b.Depth())
	arg := mustJSON(Arg{InstType: InstType, Channel: Channel, InstID: market})
	var w bytes.Buffer
	w.WriteString(`{"action":"snapshot","arg":`)
	w.Write(arg)
	w.WriteString(`,"data":[{"asks":`)
	writeLevels(&w, asks)
	w.WriteString(`,"bids":`)
	writeLevels(&w, bids)
	// The outer ts is when the frame left: a little after the book's own ts (1–2 ms live).
	ms := now.UnixMilli()
	w.WriteString(`,"ts":"` + strconv.FormatInt(ms, 10) + `","seq":` + strconv.FormatInt(b.Seq(), 10) +
		`,"pseq":0}],"ts":` + strconv.FormatInt(ms+2, 10) + `}`)
	return w.Bytes()
}

// writeLevels writes [["price","qty"],…], quantities without trailing zeros as bitget does.
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

// Arg names one channel + instrument, keys in the order bitget's replies use.
type Arg struct {
	InstType string `json:"instType"`
	Channel  string `json:"channel"`
	InstID   string `json:"instId"`
}

type request struct {
	Op   string `json:"op"`
	Args []Arg  `json:"args"`
}

type event struct {
	Event string `json:"event"`
	Arg   *Arg   `json:"arg,omitempty"`
	Code  int    `json:"code,omitempty"`
	Msg   string `json:"msg,omitempty"`
	Op    string `json:"op,omitempty"`
}

// OnMessage answers one client message the way bitget does.
func (e *Exchange) OnMessage(s exchange.Session, msg []byte) {
	if string(msg) == "ping" {
		s.Send([]byte("pong"))
		return
	}
	var req request
	if json.Unmarshal(msg, &req) != nil || (req.Op != "subscribe" && req.Op != "unsubscribe") {
		s.Send(mustJSON(event{Event: "error", Code: CodeUnrecognized, Msg: "Unrecognized request:" + string(msg)}))
		return
	}
	for _, a := range req.Args {
		if a.Channel != Channel || a.InstType != InstType || a.InstID == "" {
			s.Send(mustJSON(event{Event: "error", Arg: &a, Code: CodeParamError, Msg: "Param error", Op: req.Op}))
			continue
		}
		ack := mustJSON(event{Event: req.Op, Arg: &a})
		if req.Op == "subscribe" {
			s.Subscribe(a.InstID, func(int64) []byte { return ack }, false)
		} else {
			s.Unsubscribe(a.InstID, ack)
		}
	}
}

// mustJSON marshals without HTML escaping, as bitget writes its replies.
func mustJSON(v any) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		panic(err) // fixed structs of strings and numbers
	}
	return bytes.TrimRight(b.Bytes(), "\n")
}
