package centrifugo

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decode(t *testing.T, frame []byte) map[string]any {
	t.Helper()
	var v map[string]any
	require.NoError(t, json.Unmarshal(frame, &v), string(frame))
	return v
}

// offset is the book's current offset, as the feed hands it to the ack.
const offset = 3873262

// session records what the connection handler asked for.
type session struct {
	sent         []string
	subscribed   []string
	snapshots    []bool
	unsubscribed []string
	closed       []string
}

func (s *session) ID() string         { return "c0e31519" }
func (s *session) Send(b []byte) bool { s.sent = append(s.sent, string(b)); return true }
func (s *session) Subscribe(m string, ack func(int64) []byte, snapshot bool) {
	s.subscribed = append(s.subscribed, m)
	s.snapshots = append(s.snapshots, snapshot)
	s.Send(ack(offset))
}
func (s *session) Unsubscribe(m string, ack []byte) {
	s.unsubscribed = append(s.unsubscribed, m)
	s.Send(ack)
}
func (s *session) Close(code int, text string) {
	s.closed = append(s.closed, strconv.Itoa(code)+" "+text)
}

func newProtocol() *Protocol { return New("orderbook:", "6.9.2 OSS", 0) }

// connected is a connection handler after NiFi's {"id":1,"connect":{}}, with the reply dropped.
func connected(s *session) func([]byte) {
	on := newProtocol().Connect(s)
	on([]byte(`{"id":1,"connect":{}}`))
	s.sent = nil
	return on
}

// NiFi's Centrifugo groups send {"id":1,"connect":{}} on connect. Real nobitex and bitpin
// (2026-10-21): {"id":1,"connect":{"client":"<uuid>","version":"6.9.x OSS","ping":25,"pong":true}}.
func TestConnect(t *testing.T) {
	s := &session{}
	newProtocol().Connect(s)([]byte(`{"id":1,"connect":{}}`))
	require.Len(t, s.sent, 1)
	assert.Regexp(t, `^\{"id":1,"connect":\{"client":"[0-9a-f-]{36}","version":"6\.9\.2 OSS","ping":25,"pong":true\}\}$`, s.sent[0],
		"field order as the real server sends it")
	assert.Empty(t, s.closed)
}

// NiFi sends {"id":1,"${status:replace('pending-', ”)}":{"channel":"<prefix>${market}"}}.
// The real servers answer with the channel's current offset and send no snapshot.
func TestSubscribeAndUnsubscribe(t *testing.T) {
	s := &session{}
	on := connected(s)
	on([]byte(`{"id":1,"subscribe":{"channel":"orderbook:BTC_USDT"}}`))
	on([]byte(`{"id":1,"unsubscribe":{"channel":"orderbook:BTC_USDT"}}`))

	assert.Equal(t, []string{"BTC_USDT"}, s.subscribed)
	assert.Equal(t, []bool{false}, s.snapshots, "no snapshot on subscribe")
	assert.Equal(t, []string{"BTC_USDT"}, s.unsubscribed)
	require.Len(t, s.sent, 2)
	assert.Regexp(t, `^\{"id":1,"subscribe":\{"recoverable":true,"epoch":"[A-Za-z]{8}","offset":3873262,"positioned":true\}\}$`, s.sent[0])
	assert.Equal(t, `{"id":1,"unsubscribe":{}}`, s.sent[1])
	assert.Empty(t, s.closed)
}

// Seen live: a second subscribe to the same channel on one connection is error 105; after
// an unsubscribe it may subscribe again. An unsubscribe of a channel not subscribed is acked.
func TestRepeatSubscribeIsError105(t *testing.T) {
	s := &session{}
	on := connected(s)
	on([]byte(`{"id":1,"subscribe":{"channel":"orderbook:BTC_USDT"}}`))
	on([]byte(`{"id":2,"subscribe":{"channel":"orderbook:BTC_USDT"}}`))
	on([]byte(`{"id":3,"unsubscribe":{"channel":"orderbook:BTC_USDT"}}`))
	on([]byte(`{"id":4,"subscribe":{"channel":"orderbook:BTC_USDT"}}`))
	on([]byte(`{"id":5,"unsubscribe":{"channel":"orderbook:ETH_USDT"}}`))

	assert.Equal(t, []string{"BTC_USDT", "BTC_USDT"}, s.subscribed)
	require.Len(t, s.sent, 5)
	assert.Equal(t, `{"id":2,"error":{"code":105,"message":"already subscribed"}}`, s.sent[1])
	assert.Contains(t, s.sent[3], `"id":4,"subscribe"`)
	assert.Equal(t, `{"id":5,"unsubscribe":{}}`, s.sent[4])
}

// Seen live: the prefix with no market is accepted (an ack with no offset, and no data).
func TestEmptyMarketIsAckedWithNoData(t *testing.T) {
	s := &session{}
	connected(s)([]byte(`{"id":4,"subscribe":{"channel":"orderbook:"}}`))
	assert.Empty(t, s.subscribed)
	require.Len(t, s.sent, 1)
	assert.Regexp(t, `^\{"id":4,"subscribe":\{"recoverable":true,"epoch":"[A-Za-z]{8}","positioned":true\}\}$`, s.sent[0])
}

// NiFi answers the server's {} ping with {}: nothing to send back.
func TestPongIsQuiet(t *testing.T) {
	s := &session{}
	connected(s)([]byte(`{}`))
	assert.Empty(t, s.sent)
	assert.Empty(t, s.closed)
}

// Seen live (nobitex, bitpin): a channel of another namespace is error 102.
func TestOtherNamespaceIsError102(t *testing.T) {
	for _, raw := range []string{
		`{"id":3,"subscribe":{"channel":"trades:BTC_USDT"}}`,
		`{"id":3,"subscribe":{"channel":"public:orderbook-BTCUSDT"}}`,
		`{"id":3,"unsubscribe":{"channel":"trades:BTC_USDT"}}`,
	} {
		s := &session{}
		connected(s)([]byte(raw))
		require.Len(t, s.sent, 1, raw)
		assert.Equal(t, `{"id":3,"error":{"code":102,"message":"unknown channel"}}`, s.sent[0], raw)
		assert.Empty(t, s.subscribed, raw)
		assert.Empty(t, s.unsubscribed, raw)
		assert.Empty(t, s.closed, raw)
	}
}

// Seen live (nobitex, bitpin): these close the connection with 3501 "bad request".
func TestBadRequestsClose3501(t *testing.T) {
	before := []string{
		`{}`, `{"connect":{}}`, `{"id":1,"subscribe":{"channel":"orderbook:BTC_USDT"}}`,
	}
	after := []string{
		`hello`, `[]`, `{"id":2,"foo":{}}`, `{"id":2,"connect":{}}`,
		`{"subscribe":{"channel":"orderbook:BTC_USDT"}}`, `{"id":3,"subscribe":{}}`,
		`{"unsubscribe":{"channel":"orderbook:BTC_USDT"}}`,
	}
	for _, raw := range before {
		s := &session{}
		newProtocol().Connect(s)([]byte(raw))
		assert.Equal(t, []string{"3501 bad request"}, s.closed, "before connect: "+raw)
		assert.Empty(t, s.sent, raw)
	}
	for _, raw := range after {
		s := &session{}
		connected(s)([]byte(raw))
		assert.Equal(t, []string{"3501 bad request"}, s.closed, "after connect: "+raw)
		assert.Empty(t, s.sent, raw)
		assert.Empty(t, s.subscribed, raw)
	}
}

func TestHeartbeatIsAnEmptyObjectEvery25s(t *testing.T) {
	msg, every := newProtocol().Heartbeat()
	assert.Equal(t, "{}", string(msg))
	assert.Equal(t, 25*time.Second, every)
}

func TestPushEnvelope(t *testing.T) {
	got := newProtocol().Push("BTC_USDT", 3873263, []byte(`{"bids": []}`))
	assert.Equal(t, `{"push":{"channel":"orderbook:BTC_USDT","pub":{"data":{"bids": []},"offset":3873263}}}`, string(got),
		"data is kept byte for byte: some exchanges write it with spaces")
	assert.NotNil(t, decode(t, got))
}

func TestSeqStepIsOne(t *testing.T) {
	assert.Equal(t, int64(1), newProtocol().SeqStep(nil))
}

func TestEnvelopeIsNiFisReplaceText(t *testing.T) {
	got := newProtocol().Envelope([]byte(`{"push":{}}`), "u-1", 1)
	assert.Equal(t, `{"id":"u-1","simulation":1,"push":{}}`, string(got))
}
