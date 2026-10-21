// Package okx is the fake okx (exchange 8): the v5 public WebSocket `books` channel and the
// REST books endpoint, the way NiFi's okx group expects real okx to behave.
//
// After connect nothing is sent until a subscribe arrives; each subscribe gets an ack, then
// a snapshot (prevSeqId -1), then updates whose prevSeqId is the seqId before them. An
// unsubscribe gets an ack and stops that market.
//
// Shapes follow okx's docs and the frames captured in docs/sample-raw-data.md § ex8. The
// error codes and texts (60012, 60018, 50014, 51000) and the checksum rule are from okx's
// docs as far as they could be checked; NiFi reads none of them today.
package okx

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"exchange-simulator/internal/domain"
	"exchange-simulator/internal/exchange"
)

// Channel is the only channel served.
const Channel = "books"

// Actions of a push frame.
const (
	ActionSnapshot = "snapshot"
	ActionUpdate   = "update"
)

// Ops a client may send.
const (
	OpSubscribe   = "subscribe"
	OpUnsubscribe = "unsubscribe"
)

// WebSocket error codes.
const (
	CodeInvalidRequest = "60012" // not JSON, unknown op, no args
	CodeNoSuchChannel  = "60018" // channel is not `books`, or no instId
)

// REST error codes.
const (
	CodeEmptyParameter = "50014" // "Parameter {x} can not be empty."
	CodeBadParameter   = "51000" // "Parameter {x} error"
)

// checksumLevels is how many levels per side okx puts in the checksum.
const checksumLevels = 25

// Options mirrors the OKX_* settings in .env.
type Options struct {
	ID      int64
	WS      domain.Endpoint
	REST    domain.Endpoint
	StepMin int64 // seqId step per update; real okx steps by 3…960, not by 1
	StepMax int64
	TimeNow func() time.Time // nil = time.Now; for tests
}

// Exchange is the fake okx.
type Exchange struct{ opts Options }

// New builds the fake okx.
func New(opts Options) *Exchange {
	if opts.TimeNow == nil {
		opts.TimeNow = time.Now
	}
	return &Exchange{opts: opts}
}

var _ exchange.Exchange = (*Exchange)(nil)

func (e *Exchange) Name() string                  { return "okx" }
func (e *Exchange) ID() int64                     { return e.opts.ID }
func (e *Exchange) WS() domain.Endpoint           { return e.opts.WS }
func (e *Exchange) REST() (domain.Endpoint, bool) { return e.opts.REST, true }
func (e *Exchange) SeqStep(rng *rand.Rand) int64 {
	return e.opts.StepMin + rng.Int63n(e.opts.StepMax-e.opts.StepMin+1)
}
func (e *Exchange) Envelope(frame []byte, id string, simulation int) []byte {
	return exchange.PrependIDAndSimulation(frame, id, simulation)
}

// Arg names one channel + instrument.
type Arg struct {
	Channel string `json:"channel"`
	InstID  string `json:"instId"`
}

// Request is a client message: {"op":"subscribe","args":[…]}. ID is optional and echoed
// back in the reply, as okx does.
type Request struct {
	ID   string `json:"id,omitempty"`
	Op   string `json:"op"`
	Args []Arg  `json:"args"`
}

// Data is one element of a `books` frame's `data` array, fields in the order okx sends them.
type Data struct {
	Asks      [][4]string `json:"asks"`
	Bids      [][4]string `json:"bids"`
	TS        string      `json:"ts"`
	Checksum  int32       `json:"checksum"`
	PrevSeqID int64       `json:"prevSeqId"`
	SeqID     int64       `json:"seqId"`
}

type push struct {
	Arg    Arg    `json:"arg"`
	Action string `json:"action"`
	Data   []Data `json:"data"`
}

// Snapshot is the whole book at the current seqId, prevSeqId -1.
func (e *Exchange) Snapshot(market string, b *domain.Book, now time.Time) []byte {
	asks, bids := b.Top(b.Depth())
	return e.push(market, ActionSnapshot, Data{
		Asks: wire(asks), Bids: wire(bids), TS: exchange.Millis(now),
		Checksum: checksum(b), PrevSeqID: -1, SeqID: b.Seq(),
	})
}

// Update carries the changed levels; prevSeqId names the frame before it.
func (e *Exchange) Update(market string, b *domain.Book, asks, bids []domain.Level, prevSeq int64, now time.Time) []byte {
	return e.push(market, ActionUpdate, Data{
		Asks: wire(asks), Bids: wire(bids), TS: exchange.Millis(now),
		Checksum: checksum(b), PrevSeqID: prevSeq, SeqID: b.Seq(),
	})
}

func (e *Exchange) push(market, action string, d Data) []byte {
	return mustJSON(push{Arg: Arg{Channel: Channel, InstID: market}, Action: action, Data: []Data{d}})
}

// OnMessage answers one client message the way okx does.
func (e *Exchange) OnMessage(s exchange.Session, msg []byte) {
	if string(msg) == "ping" {
		s.Send([]byte("pong"))
		return
	}
	var req Request
	if err := json.Unmarshal(msg, &req); err != nil ||
		(req.Op != OpSubscribe && req.Op != OpUnsubscribe) || len(req.Args) == 0 {
		s.Send(ErrorEvent(req.ID, CodeInvalidRequest, "Invalid request: "+string(msg), s.ID()))
		return
	}
	for _, arg := range req.Args {
		if arg.Channel != Channel || arg.InstID == "" {
			s.Send(ErrorEvent(req.ID, CodeNoSuchChannel,
				fmt.Sprintf("Wrong URL or channel:%s,instId:%s doesn't exist.", arg.Channel, arg.InstID), s.ID()))
			continue
		}
		ack := Ack(req.ID, req.Op, arg, s.ID())
		if req.Op == OpSubscribe {
			s.Subscribe(arg.InstID, ack)
		} else {
			s.Unsubscribe(arg.InstID, ack)
		}
	}
}

type event struct {
	ID     string `json:"id,omitempty"`
	Event  string `json:"event"`
	Arg    *Arg   `json:"arg,omitempty"`
	Code   string `json:"code,omitempty"`
	Msg    string `json:"msg,omitempty"`
	ConnID string `json:"connId"`
}

// Ack answers a successful subscribe or unsubscribe for one arg.
func Ack(id, op string, arg Arg, connID string) []byte {
	return mustJSON(event{ID: id, Event: op, Arg: &arg, ConnID: connID})
}

// ErrorEvent is okx's {"event":"error",…} reply.
func ErrorEvent(id, code, msg, connID string) []byte {
	return mustJSON(event{ID: id, Event: "error", Code: code, Msg: msg, ConnID: connID})
}

// RestData is one element of the REST books body's `data`.
type RestData struct {
	Asks  [][4]string `json:"asks"`
	Bids  [][4]string `json:"bids"`
	TS    string      `json:"ts"`
	SeqID int64       `json:"seqId"`
}

// RestBody is GET /api/v5/market/books's body.
type RestBody struct {
	Code string     `json:"code"`
	Msg  string     `json:"msg"`
	Data []RestData `json:"data"`
}

// ServeREST is GET /api/v5/market/books?instId=…&sz=…. As on okx, an error is still HTTP
// 200 with a non-zero code in the body.
func (e *Exchange) ServeREST(w http.ResponseWriter, r *http.Request, books exchange.Books) {
	q := r.URL.Query()
	instID := q.Get("instId")
	if instID == "" {
		writeJSON(w, RestBody{Code: CodeEmptyParameter, Msg: "Parameter instId can not be empty.", Data: []RestData{}})
		return
	}
	sz := 1 // okx's default
	if raw := q.Get("sz"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeJSON(w, RestBody{Code: CodeBadParameter, Msg: "Parameter sz error", Data: []RestData{}})
			return
		}
		sz = n
	}
	asks, bids, seq := books.Top(instID, sz)
	writeJSON(w, RestBody{Code: "0", Msg: "", Data: []RestData{{
		Asks: wire(asks), Bids: wire(bids), TS: exchange.Millis(e.opts.TimeNow()), SeqID: seq,
	}}})
}

// checksum follows okx's `books` rule: the best 25 bids and asks interleaved as
// "bidPx:bidSz:askPx:askSz:…" (a side that runs out is just skipped), CRC32 of that
// string, as a signed 32-bit integer.
func checksum(b *domain.Book) int32 {
	return int32(crc32.ChecksumIEEE([]byte(checksumString(b.Top(checksumLevels)))))
}

func checksumString(asks, bids []domain.Level) string {
	parts := make([]string, 0, 4*checksumLevels)
	for i := range checksumLevels {
		if i < len(bids) {
			parts = append(parts, bids[i].Price, bids[i].Qty)
		}
		if i < len(asks) {
			parts = append(parts, asks[i].Price, asks[i].Qty)
		}
	}
	return strings.Join(parts, ":")
}

// wire is okx's four-element level: price, size, the deprecated liquidated-orders field
// (always "0"), number of orders. Never nil: an unchanged side is [], not null.
func wire(side []domain.Level) [][4]string {
	out := make([][4]string, len(side))
	for i, l := range side {
		out[i] = [4]string{l.Price, l.Qty, "0", "1"}
	}
	return out
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		// Every value here is a fixed struct of strings and numbers.
		panic(err)
	}
	return b
}
