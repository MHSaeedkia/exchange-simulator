// Package bitpin is the fake bitpin (exchange 2): the Centrifugo WebSocket at
// wss://centrifugo.bitpin.ir/connection/websocket, channel orderbook:{market}, the way NiFi's
// BITPIN group talks to it. The protocol (connect, subscribe, ping) is package centrifugo.
//
// Every push is the WHOLE book (50 levels per side on real bitpin), numbered by the
// channel's offset, which moves by one per push. A subscribe gets an ack and no snapshot.
// The data object is written as real bitpin writes it (captured 2026-10-21): a space after
// every ':' and ',', `price` as a string, `event_time` ISO-8601 UTC with microseconds.
// Prices are padded to PriceDecimals as bitpin does ("84150.00" for BTC_USDT). The live NiFi
// group makes no REST call for bitpin, so none is served.
package bitpin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"exchange-simulator/internal/domain"
	"exchange-simulator/internal/exchange"
	"exchange-simulator/internal/exchange/centrifugo"
)

// ChannelPrefix + market is the channel of a market's book.
const ChannelPrefix = "orderbook:"

// Version is what real bitpin's connect reply says.
const Version = "6.9.2 OSS"

// Options mirrors the BITPIN_* settings in .env.
type Options struct {
	ID            int64
	WS            domain.Endpoint
	PingEvery     time.Duration // 0 = 25 s, as real bitpin
	PriceDecimals int           // prices are written with this many decimals (the book's price scale)
}

// Exchange is the fake bitpin.
type Exchange struct {
	*centrifugo.Protocol
	opts Options
}

// New builds the fake bitpin.
func New(opts Options) *Exchange {
	return &Exchange{Protocol: centrifugo.New(ChannelPrefix, Version, opts.PingEvery), opts: opts}
}

var (
	_ exchange.Exchange    = (*Exchange)(nil)
	_ exchange.Heartbeater = (*Exchange)(nil)
)

func (e *Exchange) Name() string                  { return "bitpin" }
func (e *Exchange) ID() int64                     { return e.opts.ID }
func (e *Exchange) WS() domain.Endpoint           { return e.opts.WS }
func (e *Exchange) REST() (domain.Endpoint, bool) { return domain.Endpoint{}, false }

// ServeREST is never routed: REST reports none.
func (e *Exchange) ServeREST(w http.ResponseWriter, r *http.Request, _ exchange.Books) {
	http.NotFound(w, r)
}

// Snapshot is the whole book at the current offset.
func (e *Exchange) Snapshot(market string, b *domain.Book, now time.Time) []byte {
	return e.push(market, b, now)
}

// Update is the whole book too: a bitpin push never carries only the changed levels.
func (e *Exchange) Update(market string, b *domain.Book, _, _ []domain.Level, _ int64, now time.Time) []byte {
	return e.push(market, b, now)
}

func (e *Exchange) push(market string, b *domain.Book, now time.Time) []byte {
	asks, bids := b.Top(b.Depth())
	symbol, _ := json.Marshal(market)
	var w bytes.Buffer
	w.WriteString(`{"bids": `)
	e.writeLevels(&w, bids)
	w.WriteString(`, "asks": `)
	e.writeLevels(&w, asks)
	w.WriteString(`, "volume_ask": "` + volume(asks) + `", "volume_bid": "` + volume(bids) + `", "symbol": `)
	w.Write(symbol)
	// The book has no trades; the best bid stands in for the last price.
	w.WriteString(`, "event": "market_data", "price": "` + pad(bids[0].Price, e.opts.PriceDecimals) +
		`", "event_time": "` + now.UTC().Format("2006-01-02T15:04:05.000000Z") + `"}`)
	return e.Push(market, b.Seq(), w.Bytes())
}

// writeLevels writes [["price", "qty"], …]. Prices and quantities are plain decimals, so
// they need no escaping.
func (e *Exchange) writeLevels(w *bytes.Buffer, side []domain.Level) {
	w.WriteByte('[')
	for i, l := range side {
		if i > 0 {
			w.WriteString(", ")
		}
		w.WriteString(`["` + pad(l.Price, e.opts.PriceDecimals) + `", "` + l.Qty + `"]`)
	}
	w.WriteByte(']')
}

// pad gives a price at least decimals decimals: "84150" → "84150.00".
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

// volume is the sum of a side's quantities.
func volume(side []domain.Level) string {
	var sum domain.Decimal
	for _, l := range side {
		d, err := domain.ParseDecimal(l.Qty)
		if err != nil {
			panic(err) // the book prints every quantity as a plain decimal
		}
		for ; d.Scale < sum.Scale; d.Scale++ {
			d.Units *= 10
		}
		for ; sum.Scale < d.Scale; sum.Scale++ {
			sum.Units *= 10
		}
		sum.Units += d.Units
	}
	return sum.String()
}
