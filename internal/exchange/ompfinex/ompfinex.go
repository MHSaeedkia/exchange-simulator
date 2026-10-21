// Package ompfinex is the fake ompfinex (exchange 7): the Centrifugo WebSocket at
// wss://stream.ompfinex.com/stream, channel public-market:r-depth-{numeric market id}, and the
// REST depth NiFi's OMPFINEX group calls on a snapshot request. As real ompfinex behaves
// (captured 2026-10-21):
//
//   - The protocol is package centrifugo, with ompfinex's differences: version "6.9.0 OSS", a
//     {} ping every 6 s that must be answered within 4 s (else 3012 "no pong"), and a plain
//     subscribe ack {"id":2,"subscribe":{}}. Another namespace is error 102.
//   - The WebSocket carries ONLY deltas: {"u":…,"U":…,"a":[…],"b":[…]}, the changed levels; U
//     is the u of the delta before. A book's start is the REST snapshot, which lpa-core asks
//     for (through NiFi) when a delta finds no baseline. So no snapshot is ever sent over the
//     WebSocket; in kafka mode the snapshot is the REST body as NiFi publishes it.
//   - REST {"status":"OK","data":{"lastUpdateId":…,"time":<epoch µs>,"bids":[…],"asks":[…]}},
//     25 levels unless limit says otherwise.
//
// Not copied: an unknown market is HTTP 400 {"status":"BAD_REQUEST","message":"Invalid market."}
// on real ompfinex (here every market exists); real deltas also delete (qty "0") and add levels.
package ompfinex

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
	"exchange-simulator/internal/exchange/centrifugo"
)

// ChannelPrefix + market is the channel of a market's deltas.
const ChannelPrefix = "public-market:r-depth-"

// Real ompfinex's protocol values.
const (
	Version     = "6.9.0 OSS"
	PingEvery   = 6 * time.Second
	PongTimeout = 4 * time.Second
)

// restSuffix ends the REST path: {REST.Path}{market}/depth.
const restSuffix = "/depth"

// offsetStart puts the Centrifugo offset near real ompfinex's (3752419 seen).
const offsetStart = 3_750_000

// Options mirrors the OMPFINEX_* settings in .env.
type Options struct {
	ID          int64
	WS          domain.Endpoint
	REST        domain.Endpoint // Path ends in "/": the market follows it
	StepMin     int64           // u step per delta (u − U): 1…3 seen
	StepMax     int64
	PingEvery   time.Duration // 0 = PingEvery
	PongTimeout time.Duration // 0 = PongTimeout
	TimeNow     func() time.Time
}

// Exchange is the fake ompfinex. Snapshot and Update run on the feed's goroutine only (the
// per-market offsets are not locked).
type Exchange struct {
	*centrifugo.Protocol
	opts    Options
	offsets map[string]int64 // Centrifugo offset of each market's last push
}

// New builds the fake ompfinex.
func New(opts Options) *Exchange {
	if opts.PingEvery == 0 {
		opts.PingEvery = PingEvery
	}
	if opts.PongTimeout == 0 {
		opts.PongTimeout = PongTimeout
	}
	if opts.TimeNow == nil {
		opts.TimeNow = time.Now
	}
	return &Exchange{Protocol: centrifugo.New(centrifugo.Options{
		Prefix: ChannelPrefix, Version: Version, PlainAck: true,
		PingEvery: opts.PingEvery, PongTimeout: opts.PongTimeout,
	}), opts: opts, offsets: map[string]int64{}}
}

var (
	_ exchange.Exchange    = (*Exchange)(nil)
	_ exchange.Heartbeater = (*Exchange)(nil)
)

func (e *Exchange) Name() string                  { return "ompfinex" }
func (e *Exchange) ID() int64                     { return e.opts.ID }
func (e *Exchange) WS() domain.Endpoint           { return e.opts.WS }
func (e *Exchange) REST() (domain.Endpoint, bool) { return e.opts.REST, true }

// SeqStep is u − U: how many internal updates one delta folds.
func (e *Exchange) SeqStep(rng *mrand.Rand) int64 {
	return e.opts.StepMin + rng.Int63n(e.opts.StepMax-e.opts.StepMin+1)
}

// WebSocketSnapshots is false: the WebSocket carries only deltas.
func (e *Exchange) WebSocketSnapshots() bool { return false }

// Update is a delta: the changed levels, u, and U = the u before.
func (e *Exchange) Update(market string, b *domain.Book, asks, bids []domain.Level, prevSeq int64, _ time.Time) []byte {
	var w bytes.Buffer
	w.WriteString(`{"u":` + strconv.FormatInt(b.Seq(), 10) + `,"U":` + strconv.FormatInt(prevSeq, 10) + `,"a":`)
	writeLevels(&w, asks)
	w.WriteString(`,"b":`)
	writeLevels(&w, bids)
	w.WriteByte('}')
	off, ok := e.offsets[market]
	if !ok {
		off = offsetStart
	}
	e.offsets[market] = off + 1
	return e.Push(market, off+1, w.Bytes())
}

// Snapshot is the REST body as NiFi publishes it (kafka mode): Jolt's default adds
// "action":"snapshot" and "pair" after it.
func (e *Exchange) Snapshot(market string, b *domain.Book, now time.Time) []byte {
	asks, bids := b.Top(b.Depth())
	body := depth(asks, bids, b.Seq(), now)
	pair, _ := json.Marshal(market)
	out := append(body[:len(body)-1:len(body)-1], `,"action":"snapshot","pair":`...)
	out = append(out, pair...)
	return append(out, '}')
}

// ServeREST is GET {REST.Path}{market}/depth?limit=….
func (e *Exchange) ServeREST(w http.ResponseWriter, r *http.Request, books exchange.Books) {
	rest, ok := strings.CutPrefix(r.URL.Path, e.opts.REST.Path)
	market, ok2 := strings.CutSuffix(rest, restSuffix)
	if !ok || !ok2 || market == "" || strings.Contains(market, "/") {
		http.NotFound(w, r)
		return
	}
	limit := 25 // ompfinex's default
	if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 {
		limit = n
	}
	asks, bids, seq := books.Top(market, limit)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(depth(asks, bids, seq, e.opts.TimeNow()))
}

// depth is the REST body: {"status":"OK","data":{"lastUpdateId":…,"time":<µs>,"bids":…,"asks":…}}.
func depth(asks, bids []domain.Level, seq int64, now time.Time) []byte {
	var w bytes.Buffer
	w.WriteString(`{"status":"OK","data":{"lastUpdateId":` + strconv.FormatInt(seq, 10) +
		`,"time":` + strconv.FormatInt(now.UnixMicro(), 10) + `,"bids":`)
	writeLevels(&w, bids)
	w.WriteString(`,"asks":`)
	writeLevels(&w, asks)
	w.WriteString(`}}`)
	return w.Bytes()
}

// writeLevels writes [["price","qty"],…]; never null.
func writeLevels(w *bytes.Buffer, side []domain.Level) {
	w.WriteByte('[')
	for i, l := range side {
		if i > 0 {
			w.WriteByte(',')
		}
		w.WriteString(`["` + l.Price + `","` + l.Qty + `"]`)
	}
	w.WriteByte(']')
}
