// Package centrifugo is the server side of the Centrifugo JSON protocol, shared by the
// exchanges that use it (nobitex, bitpin, …). An exchange package embeds a *Protocol and
// adds only its own book frame.
//
// NiFi sends {"id":1,"connect":{}} on connect, then one subscribe or unsubscribe per market,
// and answers the server's {} ping with {}. Everything here was seen on real nobitex and
// bitpin on 2026-10-21:
//
//   - Before connect, anything but a connect with an id closes the socket: 3501 "bad request".
//   - After connect, so do bad JSON, a second connect, an unknown command, a command with no
//     id (except {}, the answer to a ping) and a subscribe with no channel.
//   - A subscribe is acked with the channel's current offset and gets no snapshot. A second
//     subscribe to the same channel on one connection is error 105. The prefix with no market
//     is acked (no offset) and gets no data. Another namespace is error 102.
//   - An unsubscribe is acked, subscribed or not.
//
// Inferred, not seen: an unsubscribe with no channel closes like a subscribe with none.
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

// Centrifugo's codes, as the real servers send them.
const (
	CodeUnknownChannel    = 102  // error: a channel of another namespace
	CodeAlreadySubscribed = 105  // error: a second subscribe to a channel on one connection
	CloseBadRequest       = 3501 // close: a message the server does not take
)

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

var (
	_ exchange.Heartbeater = (*Protocol)(nil)
	_ exchange.Connector   = (*Protocol)(nil)
)

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

// conn is what the server remembers about one connection.
type conn struct {
	connected  bool
	subscribed map[string]bool // channels
}

// Connect returns the connection's handler: it keeps whether the client connected and which
// channels it subscribed.
func (p *Protocol) Connect(s exchange.Session) func(msg []byte) {
	c := &conn{subscribed: map[string]bool{}}
	return func(msg []byte) { p.handle(s, c, msg) }
}

// OnMessage is a message outside a Connect handler (the server always uses Connect): the
// answers of a connected client with no memory of earlier messages.
func (p *Protocol) OnMessage(s exchange.Session, msg []byte) {
	p.handle(s, &conn{connected: true, subscribed: map[string]bool{}}, msg)
}

func (p *Protocol) handle(s exchange.Session, c *conn, msg []byte) {
	cmd, ok := parse(msg)
	switch {
	case !ok:
	case !c.connected:
		if cmd.Connect != nil && cmd.ID != 0 {
			c.connected = true
			s.Send(mustJSON(reply{ID: cmd.ID, Connect: &connectReply{
				Client: exchange.NewUUID(), Version: p.version, Ping: int(p.pingEvery / time.Second), Pong: true,
			}}))
			return
		}
	case cmd.isPong():
		return
	case cmd.ID == 0 || cmd.Connect != nil:
	case cmd.Subscribe != nil && cmd.Subscribe.Channel != "":
		p.subscribe(s, c, cmd.ID, cmd.Subscribe.Channel)
		return
	case cmd.Unsubscribe != nil && cmd.Unsubscribe.Channel != "":
		p.unsubscribe(s, c, cmd.ID, cmd.Unsubscribe.Channel)
		return
	}
	s.Close(CloseBadRequest, "bad request")
}

func (p *Protocol) subscribe(s exchange.Session, c *conn, id uint32, ch string) {
	market, ok := strings.CutPrefix(ch, p.prefix)
	switch {
	case !ok:
		s.Send(errorReply(id, CodeUnknownChannel, "unknown channel"))
	case c.subscribed[ch]:
		s.Send(errorReply(id, CodeAlreadySubscribed, "already subscribed"))
	case market == "":
		// No such market: acked, no stream, so no offset and no data.
		c.subscribed[ch] = true
		s.Send(mustJSON(reply{ID: id, Subscribe: &subscribeReply{Recoverable: true, Epoch: p.epoch, Positioned: true}}))
	default:
		c.subscribed[ch] = true
		s.Subscribe(market, func(offset int64) []byte {
			return mustJSON(reply{ID: id, Subscribe: &subscribeReply{
				Recoverable: true, Epoch: p.epoch, Offset: offset, Positioned: true,
			}})
		}, false)
	}
}

func (p *Protocol) unsubscribe(s exchange.Session, c *conn, id uint32, ch string) {
	market, ok := strings.CutPrefix(ch, p.prefix)
	if !ok {
		s.Send(errorReply(id, CodeUnknownChannel, "unknown channel"))
		return
	}
	delete(c.subscribed, ch)
	ack := []byte(fmt.Sprintf(`{"id":%d,"unsubscribe":{}}`, id))
	if market == "" {
		s.Send(ack)
		return
	}
	s.Unsubscribe(market, ack)
}

// parse reads one command. ok is false for anything that is not a JSON object, or that
// carries a key the protocol does not know.
func parse(msg []byte) (command, bool) {
	var keys map[string]json.RawMessage
	if json.Unmarshal(msg, &keys) != nil || keys == nil {
		return command{}, false
	}
	for k := range keys {
		if k != "id" && k != "connect" && k != "subscribe" && k != "unsubscribe" {
			return command{}, false
		}
	}
	var c command
	if json.Unmarshal(msg, &c) != nil {
		return command{}, false
	}
	return c, true
}

func (c command) isPong() bool {
	return c.ID == 0 && c.Connect == nil && c.Subscribe == nil && c.Unsubscribe == nil
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

// subscribeReply has no "offset" for a channel with no stream, as on the real servers.
type subscribeReply struct {
	Recoverable bool   `json:"recoverable"`
	Epoch       string `json:"epoch"`
	Offset      int64  `json:"offset,omitempty"`
	Positioned  bool   `json:"positioned"`
}

func errorReply(id uint32, code int, message string) []byte {
	return []byte(fmt.Sprintf(`{"id":%d,"error":{"code":%d,"message":%q}}`, id, code, message))
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
