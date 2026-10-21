// Package centrifugo is the server side of the Centrifugo JSON protocol, shared by the
// exchanges that use it (nobitex, bitpin, …). An exchange package embeds a *Protocol and
// adds only its own book frame.
//
// NiFi sends {"id":1,"connect":{}} on connect, then one subscribe or unsubscribe per market,
// and answers the server's {} ping with {}. The reply texts were captured from real nobitex
// and bitpin on 2026-10-21. Two real behaviours are not copied, since NiFi never meets them:
// a message before connect or a bad message closes the socket (code 3501), and a second
// subscribe to the same channel gets error 105 "already subscribed" (here: acked again).
package centrifugo

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	mrand "math/rand"
	"strconv"
	"strings"
	"time"

	"exchange-simulator/internal/exchange"
)

// CodeUnknownChannel is the error for a channel the server does not serve (seen on bitpin).
const CodeUnknownChannel = 102

// Protocol answers one exchange's Centrifugo messages.
type Protocol struct {
	prefix    string // a market's channel is prefix + market
	version   string // what the connect reply names
	pingEvery time.Duration
	epoch     string // the stream epoch a subscribe ack names; fixed for the life of the process
}

// New builds the protocol for channels prefix+market. pingEvery 0 = 25 s, as the real servers.
func New(prefix, version string, pingEvery time.Duration) *Protocol {
	if pingEvery == 0 {
		pingEvery = 25 * time.Second
	}
	return &Protocol{prefix: prefix, version: version, pingEvery: pingEvery, epoch: randomLetters(8)}
}

var _ exchange.Heartbeater = (*Protocol)(nil)

// SeqStep is 1: a Centrifugo offset moves by one per publication.
func (p *Protocol) SeqStep(*mrand.Rand) int64 { return 1 }

// Heartbeat is the server's {} ping.
func (p *Protocol) Heartbeat() ([]byte, time.Duration) { return []byte("{}"), p.pingEvery }

// Envelope is NiFi's ReplaceText on the object-root push.
func (p *Protocol) Envelope(frame []byte, id string, simulation int) []byte {
	return exchange.PrependIDAndSimulation(frame, id, simulation)
}

// Push wraps a market's data, kept byte for byte, in a publication at offset.
func (p *Protocol) Push(market string, offset int64, data []byte) []byte {
	ch, _ := json.Marshal(p.prefix + market)
	out := make([]byte, 0, len(data)+len(ch)+64)
	out = append(out, `{"push":{"channel":`...)
	out = append(out, ch...)
	out = append(out, `,"pub":{"data":`...)
	out = append(out, data...)
	out = append(out, `,"offset":`...)
	out = strconv.AppendInt(out, offset, 10)
	return append(out, "}}}"...)
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

// OnMessage answers one client message. A subscribe gets no snapshot.
func (p *Protocol) OnMessage(s exchange.Session, msg []byte) {
	var c command
	if err := json.Unmarshal(msg, &c); err != nil {
		return // the real servers close with 3501 "bad request"
	}
	switch {
	case c.Connect != nil:
		s.Send(mustJSON(reply{ID: c.ID, Connect: &connectReply{
			Client: exchange.NewUUID(), Version: p.version, Ping: int(p.pingEvery / time.Second), Pong: true,
		}}))
	case c.Subscribe != nil:
		market, ok := p.marketOf(c.Subscribe.Channel)
		if !ok {
			s.Send(errorReply(c.ID))
			return
		}
		s.Subscribe(market, mustJSON(reply{ID: c.ID, Subscribe: &subscribeReply{
			Recoverable: true, Epoch: p.epoch, Positioned: true,
		}}), false)
	case c.Unsubscribe != nil:
		market, ok := p.marketOf(c.Unsubscribe.Channel)
		if !ok {
			s.Send(errorReply(c.ID))
			return
		}
		s.Unsubscribe(market, []byte(fmt.Sprintf(`{"id":%d,"unsubscribe":{}}`, c.ID)))
	}
}

// reply is a server answer, "id" first as the real servers send it.
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

// subscribeReply leaves out "offset": the real servers send it only once the channel has
// one, and the ack is built before the book is read. NiFi does not read it.
type subscribeReply struct {
	Recoverable bool   `json:"recoverable"`
	Epoch       string `json:"epoch"`
	Positioned  bool   `json:"positioned"`
}

func errorReply(id uint32) []byte {
	return []byte(fmt.Sprintf(`{"id":%d,"error":{"code":%d,"message":"unknown channel"}}`, id, CodeUnknownChannel))
}

func (p *Protocol) marketOf(ch string) (string, bool) {
	m, ok := strings.CutPrefix(ch, p.prefix)
	return m, ok && m != ""
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
