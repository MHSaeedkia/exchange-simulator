// Package nobitex is the fake nobitex (exchange 1): the Centrifugo WebSocket at
// wss://ws.nobitex.ir/connection/websocket, channel public:orderbook-{market}, the way NiFi's
// NOBITEX group talks to it.
//
// NiFi sends {"id":1,"connect":{}} on connect, then one subscribe or unsubscribe per market,
// and answers the server's {} ping with {}. Every push is the WHOLE book (24 levels per side
// on real nobitex), numbered by the channel's offset, which moves by one per push. A
// subscribe gets an ack and no snapshot: the next push comes on the normal beat.
//
// The reply texts were captured from real nobitex on 2026-10-21 (Centrifugo "6.9.3 OSS").
// Two real behaviours are not copied, since NiFi never meets them: a message before connect
// or a bad message closes the socket (code 3501), and a second subscribe to the same channel
// gets error 105 "already subscribed" (here: acked again). Error 102 for a channel outside
// public:orderbook- is Centrifugo's documented code, not seen live. The live NiFi group makes
// no REST call for nobitex, so none is served.
package nobitex

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	mrand "math/rand"
	"net/http"
	"strings"
	"time"

	"exchange-simulator/internal/domain"
	"exchange-simulator/internal/exchange"
)

// ChannelPrefix + market is the channel of a market's book.
const ChannelPrefix = "public:orderbook-"

// Version is what real nobitex's connect reply says.
const Version = "6.9.3 OSS"

// CodeUnknownChannel is Centrifugo's error for a channel the server does not serve.
const CodeUnknownChannel = 102

// Options mirrors the NOBITEX_* settings in .env.
type Options struct {
	ID        int64
	WS        domain.Endpoint
	PingEvery time.Duration // 0 = 25 s, as real nobitex
}

// Exchange is the fake nobitex.
type Exchange struct {
	opts  Options
	epoch string // the stream epoch a subscribe ack names; fixed for the life of the process
}

// New builds the fake nobitex.
func New(opts Options) *Exchange {
	if opts.PingEvery == 0 {
		opts.PingEvery = 25 * time.Second
	}
	return &Exchange{opts: opts, epoch: randomLetters(8)}
}

var (
	_ exchange.Exchange    = (*Exchange)(nil)
	_ exchange.Heartbeater = (*Exchange)(nil)
)

func (e *Exchange) Name() string                       { return "nobitex" }
func (e *Exchange) ID() int64                          { return e.opts.ID }
func (e *Exchange) WS() domain.Endpoint                { return e.opts.WS }
func (e *Exchange) REST() (domain.Endpoint, bool)      { return domain.Endpoint{}, false }
func (e *Exchange) SeqStep(*mrand.Rand) int64          { return 1 }
func (e *Exchange) Heartbeat() ([]byte, time.Duration) { return []byte("{}"), e.opts.PingEvery }
func (e *Exchange) Envelope(frame []byte, id string, simulation int) []byte {
	return exchange.PrependIDAndSimulation(frame, id, simulation)
}

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

type pub struct {
	Data   Data  `json:"data"`
	Offset int64 `json:"offset"`
}

type push struct {
	Push struct {
		Channel string `json:"channel"`
		Pub     pub    `json:"pub"`
	} `json:"push"`
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
	var p push
	p.Push.Channel = ChannelPrefix + market
	p.Push.Pub = pub{Offset: b.Seq(), Data: Data{
		Asks: wire(asks), Bids: wire(bids),
		// The book has no trades; the best bid stands in, as it often equals it on nobitex.
		LastTradePrice: bids[0].Price, LastUpdate: now.UnixMilli(),
	}}
	return mustJSON(p)
}

// command is one client message. {} (NiFi's answer to a ping) has none of the fields.
type command struct {
	ID          uint32           `json:"id"`
	Connect     *json.RawMessage `json:"connect"`
	Subscribe   *channel         `json:"subscribe"`
	Unsubscribe *channel         `json:"unsubscribe"`
}

type channel struct {
	Channel string `json:"channel"`
}

// OnMessage answers one client message the way nobitex does.
func (e *Exchange) OnMessage(s exchange.Session, msg []byte) {
	var c command
	if err := json.Unmarshal(msg, &c); err != nil {
		return // real nobitex closes with 3501 "bad request"
	}
	switch {
	case c.Connect != nil:
		s.Send(mustJSON(reply{ID: c.ID, Connect: &connectReply{
			Client: newUUID(), Version: Version, Ping: int(e.opts.PingEvery / time.Second), Pong: true,
		}}))
	case c.Subscribe != nil:
		market, ok := marketOf(c.Subscribe.Channel)
		if !ok {
			s.Send(errorReply(c.ID))
			return
		}
		s.Subscribe(market, mustJSON(reply{ID: c.ID, Subscribe: &subscribeReply{
			Recoverable: true, Epoch: e.epoch, Positioned: true,
		}}), false)
	case c.Unsubscribe != nil:
		market, ok := marketOf(c.Unsubscribe.Channel)
		if !ok {
			s.Send(errorReply(c.ID))
			return
		}
		s.Unsubscribe(market, []byte(fmt.Sprintf(`{"id":%d,"unsubscribe":{}}`, c.ID)))
	}
}

// reply is a server answer, "id" first as nobitex sends it.
type reply struct {
	ID        uint32          `json:"id"`
	Connect   *connectReply   `json:"connect,omitempty"`
	Subscribe *subscribeReply `json:"subscribe,omitempty"`
}

type connectReply struct {
	Client  string `json:"client"`
	Version string `json:"version"`
	Ping    int    `json:"ping"`
	Pong    bool   `json:"pong"`
}

// subscribeReply leaves out "offset": real nobitex sends it only once the channel has one,
// and the ack is built before the book is read. NiFi does not read it.
type subscribeReply struct {
	Recoverable bool   `json:"recoverable"`
	Epoch       string `json:"epoch"`
	Positioned  bool   `json:"positioned"`
}

func errorReply(id uint32) []byte {
	return []byte(fmt.Sprintf(`{"id":%d,"error":{"code":%d,"message":"unknown channel"}}`, id, CodeUnknownChannel))
}

func marketOf(ch string) (string, bool) {
	m, ok := strings.CutPrefix(ch, ChannelPrefix)
	return m, ok && m != ""
}

// wire is nobitex's level: [price, qty], both strings. Never nil.
func wire(side []domain.Level) [][2]string {
	out := make([][2]string, len(side))
	for i, l := range side {
		out[i] = [2]string{l.Price, l.Qty}
	}
	return out
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func randomLetters(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = letters[int(b[i])%len(letters)]
	}
	return string(b)
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		// Every value here is a fixed struct of strings and numbers.
		panic(err)
	}
	return b
}
