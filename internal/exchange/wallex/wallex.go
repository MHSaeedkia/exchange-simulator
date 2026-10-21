// Package wallex is the fake wallex (exchange 3): the WebSocket at wss://api.wallex.ir/ws, the
// way NiFi's WALLEX group (10 connections) talks to it, and the way real wallex behaves:
//
//   - On connect it speaks first: {"sid":…,"upgrades":[],"pingInterval":20000,…}.
//   - A channel is one side of one market: BTCUSDT@buyDepth (bids) or BTCUSDT@sellDepth (asks).
//     Each frame is that side's whole book (50 levels), ["BTCUSDT@buyDepth",[{"price":…,
//     "quantity":…,"sum":…},…]], numbers not strings, sum = price × quantity.
//   - ["subscribe", {"channel": …}] / ["unsubscribe", …] get no reply at all, and a
//     subscribe gets no snapshot: data simply starts or stops, per channel. Bad messages are
//     ignored silently.
//   - A connection takes 51 subscribe requests in its whole life; later ones are ignored with
//     no reply. Repeats count; unsubscribes do not count and give no place back.
//   - A control PING ("ping") every 20 s; the connection is closed (1000) when no PONG came
//     for 30 s, and at 30 min whatever happens.
//
// Measured on real wallex (lpa-core 2026-09-30…10-03, and again 2026-10-21). Inferred: a
// well-formed subscribe for a channel that is not <market>@buyDepth|sellDepth still uses a
// place. The live NiFi group makes no REST call for wallex, so none is served.
package wallex

import (
	"bytes"
	"encoding/json"
	"math/big"
	"math/bits"
	mrand "math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"exchange-simulator/internal/domain"
	"exchange-simulator/internal/exchange"
)

// Channel suffixes: a market's bid side and ask side.
const (
	BuyDepth  = "@buyDepth"
	SellDepth = "@sellDepth"
)

// MaxSubscribes is how many subscribe requests one connection takes in its life.
const MaxSubscribes = 51

// Options mirrors the WALLEX_* settings in .env. The zero durations are real wallex's.
type Options struct {
	ID          int64
	WS          domain.Endpoint
	PingEvery   time.Duration // 0 = 20 s
	PongTimeout time.Duration // 0 = 30 s
	MaxLife     time.Duration // 0 = 30 min
}

// Exchange is the fake wallex.
type Exchange struct{ opts Options }

// New builds the fake wallex.
func New(opts Options) *Exchange {
	if opts.PingEvery == 0 {
		opts.PingEvery = 20 * time.Second
	}
	if opts.PongTimeout == 0 {
		opts.PongTimeout = 30 * time.Second
	}
	if opts.MaxLife == 0 {
		opts.MaxLife = 30 * time.Minute
	}
	return &Exchange{opts: opts}
}

var (
	_ exchange.Exchange       = (*Exchange)(nil)
	_ exchange.Connector      = (*Exchange)(nil)
	_ exchange.KeepaliveRules = (*Exchange)(nil)
	_ exchange.Channeler      = (*Exchange)(nil)
)

func (e *Exchange) Name() string                  { return "wallex" }
func (e *Exchange) ID() int64                     { return e.opts.ID }
func (e *Exchange) WS() domain.Endpoint           { return e.opts.WS }
func (e *Exchange) REST() (domain.Endpoint, bool) { return domain.Endpoint{}, false }

// SeqStep is 1. wallex frames carry no sequence number; the book's only counts frames.
func (e *Exchange) SeqStep(*mrand.Rand) int64 { return 1 }

// ServeREST is never routed: REST reports none.
func (e *Exchange) ServeREST(w http.ResponseWriter, r *http.Request, _ exchange.Books) {
	http.NotFound(w, r)
}

// Keepalive is real wallex's: PING every 20 s, closed after 30 s without PONG or at 30 min.
func (e *Exchange) Keepalive() exchange.Keepalive {
	return exchange.Keepalive{
		PingEvery: e.opts.PingEvery, PingPayload: []byte("ping"),
		PongTimeout: e.opts.PongTimeout, MaxLife: e.opts.MaxLife,
	}
}

// Channels are a database market's two streams, for kafka mode.
func (e *Exchange) Channels(market string) []string {
	return []string{market + BuyDepth, market + SellDepth}
}

// Envelope is NiFi's ReplaceText on wallex's array root: `\]\s*$` →
// `, {"simulation": 0, "id": "${uuid}"} ]`.
func (e *Exchange) Envelope(frame []byte, id string, simulation int) []byte {
	tail := `, {"simulation": ` + strconv.Itoa(simulation) + `, "id": "` + id + `"} ]`
	out := make([]byte, 0, len(frame)+len(tail))
	return append(append(out, frame[:len(frame)-1]...), tail...)
}

// Snapshot is the channel's side, whole. wallex has no other kind of frame.
func (e *Exchange) Snapshot(channel string, b *domain.Book, _ time.Time) []byte {
	return e.side(channel, b)
}

// Update is the channel's side, whole, as every wallex frame is.
func (e *Exchange) Update(channel string, b *domain.Book, _, _ []domain.Level, _ int64, _ time.Time) []byte {
	return e.side(channel, b)
}

func (e *Exchange) side(channel string, b *domain.Book) []byte {
	asks, bids := b.Top(b.Depth())
	levels := bids
	if strings.HasSuffix(channel, SellDepth) {
		levels = asks
	}
	name, _ := json.Marshal(channel)
	var w bytes.Buffer
	w.WriteByte('[')
	w.Write(name)
	w.WriteString(",[")
	for i, l := range levels {
		if i > 0 {
			w.WriteByte(',')
		}
		w.WriteString(`{"price":` + trim(l.Price) + `,"quantity":` + trim(l.Qty) + `,"sum":` + sum(l.Price, l.Qty) + `}`)
	}
	w.WriteString("]]")
	return w.Bytes()
}

// Connect sends the open message and returns the connection's handler, which keeps its
// subscribe budget.
func (e *Exchange) Connect(s exchange.Session) func(msg []byte) {
	s.Send([]byte(`{"sid":"` + exchange.NewUUID() + `","upgrades":[],"pingInterval":` +
		strconv.FormatInt(e.opts.PingEvery.Milliseconds(), 10) + `,"pingTimeout":` +
		strconv.FormatInt(e.opts.PongTimeout.Milliseconds(), 10) + `,"maxPayload":1048576}`))
	subscribes := 0
	return func(msg []byte) {
		op, channel, ok := parse(msg)
		if !ok {
			return
		}
		if op == "subscribe" {
			if subscribes++; subscribes > MaxSubscribes {
				return
			}
		}
		e.apply(s, op, channel)
	}
}

// OnMessage is a message outside a Connect handler (the server always uses Connect): the
// same answers, with no subscribe budget.
func (e *Exchange) OnMessage(s exchange.Session, msg []byte) {
	if op, channel, ok := parse(msg); ok {
		e.apply(s, op, channel)
	}
}

func (e *Exchange) apply(s exchange.Session, op, channel string) {
	if !valid(channel) {
		return
	}
	if op == "subscribe" {
		s.Subscribe(channel, nil, false)
	} else {
		s.Unsubscribe(channel, nil)
	}
}

// parse reads ["subscribe"|"unsubscribe", {"channel": …}].
func parse(msg []byte) (op, channel string, ok bool) {
	var parts []json.RawMessage
	if json.Unmarshal(msg, &parts) != nil || len(parts) != 2 || json.Unmarshal(parts[0], &op) != nil {
		return "", "", false
	}
	var arg struct {
		Channel string `json:"channel"`
	}
	if json.Unmarshal(parts[1], &arg) != nil || (op != "subscribe" && op != "unsubscribe") {
		return "", "", false
	}
	return op, arg.Channel, true
}

func valid(channel string) bool {
	for _, suffix := range []string{BuyDepth, SellDepth} {
		if m, ok := strings.CutSuffix(channel, suffix); ok && m != "" {
			return true
		}
	}
	return false
}

// trim drops trailing zeros: wallex writes JSON numbers ("0.002", "86328").
func trim(s string) string {
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	return s
}

// sum is price × quantity, exact.
func sum(price, qty string) string {
	p, err1 := domain.ParseDecimal(price)
	q, err2 := domain.ParseDecimal(qty)
	if err1 != nil || err2 != nil {
		panic("the book prints plain decimals") // never
	}
	var digits string
	if hi, lo := bits.Mul64(uint64(p.Units), uint64(q.Units)); hi == 0 {
		digits = strconv.FormatUint(lo, 10)
	} else {
		digits = new(big.Int).Mul(big.NewInt(p.Units), big.NewInt(q.Units)).String()
	}
	scale := p.Scale + q.Scale
	if scale == 0 {
		return digits
	}
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	return trim(digits[:len(digits)-scale] + "." + digits[len(digits)-scale:])
}
