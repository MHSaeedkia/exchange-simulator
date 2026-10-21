// Package nobitex is the fake nobitex (exchange 1): the Centrifugo WebSocket at
// wss://ws.nobitex.ir/connection/websocket, channel public:orderbook-{market}, the way NiFi's
// NOBITEX group talks to it. The protocol (connect, subscribe, ping) is package centrifugo.
//
// Every push is the WHOLE book (24 levels per side on real nobitex), numbered by the
// channel's offset, which moves by one per push. A subscribe gets an ack and no snapshot:
// the next push comes on the normal beat. Frames as captured from real nobitex on
// 2026-10-21. The live NiFi group makes no REST call for nobitex, so none is served.
package nobitex

import (
	"encoding/json"
	"net/http"
	"time"

	"exchange-simulator/internal/domain"
	"exchange-simulator/internal/exchange"
	"exchange-simulator/internal/exchange/centrifugo"
)

// ChannelPrefix + market is the channel of a market's book.
const ChannelPrefix = "public:orderbook-"

// Version is what real nobitex's connect reply says.
const Version = "6.9.3 OSS"

// Options mirrors the NOBITEX_* settings in .env.
type Options struct {
	ID        int64
	WS        domain.Endpoint
	PingEvery time.Duration // 0 = 25 s, as real nobitex
}

// Exchange is the fake nobitex.
type Exchange struct {
	*centrifugo.Protocol
	opts Options
}

// New builds the fake nobitex.
func New(opts Options) *Exchange {
	return &Exchange{Protocol: centrifugo.New(ChannelPrefix, Version, opts.PingEvery), opts: opts}
}

var (
	_ exchange.Exchange    = (*Exchange)(nil)
	_ exchange.Heartbeater = (*Exchange)(nil)
)

func (e *Exchange) Name() string                  { return "nobitex" }
func (e *Exchange) ID() int64                     { return e.opts.ID }
func (e *Exchange) WS() domain.Endpoint           { return e.opts.WS }
func (e *Exchange) REST() (domain.Endpoint, bool) { return domain.Endpoint{}, false }

// ServeREST is never routed: REST reports none.
func (e *Exchange) ServeREST(w http.ResponseWriter, r *http.Request, _ exchange.Books) {
	http.NotFound(w, r)
}

// Data is a push's book, fields in the order nobitex sends them.
type Data struct {
	Asks           [][2]string `json:"asks"`
	Bids           [][2]string `json:"bids"`
	LastTradePrice string      `json:"lastTradePrice"`
	LastUpdate     int64       `json:"lastUpdate"`
}

// Snapshot is the whole book at the current offset.
func (e *Exchange) Snapshot(market string, b *domain.Book, now time.Time) []byte {
	return e.push(market, b, now)
}

// Update is the whole book too: a nobitex push never carries only the changed levels.
func (e *Exchange) Update(market string, b *domain.Book, _, _ []domain.Level, _ int64, now time.Time) []byte {
	return e.push(market, b, now)
}

func (e *Exchange) push(market string, b *domain.Book, now time.Time) []byte {
	asks, bids := b.Top(b.Depth())
	data, err := json.Marshal(Data{
		Asks: wire(asks), Bids: wire(bids),
		// The book has no trades; the best bid stands in, as it often equals it on nobitex.
		LastTradePrice: bids[0].Price, LastUpdate: now.UnixMilli(),
	})
	if err != nil {
		panic(err) // a fixed struct of strings and numbers
	}
	return e.Push(market, b.Seq(), data)
}

// wire is nobitex's level: [price, qty], both strings. Never nil.
func wire(side []domain.Level) [][2]string {
	out := make([][2]string, len(side))
	for i, l := range side {
		out[i] = [2]string{l.Price, l.Qty}
	}
	return out
}
