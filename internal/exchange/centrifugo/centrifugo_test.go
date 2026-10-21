package centrifugo

import (
	"encoding/json"
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

// session records what OnMessage asked for.
type session struct {
	sent         []string
	subscribed   []string
	snapshots    []bool
	unsubscribed []string
}

func (s *session) ID() string         { return "c0e31519" }
func (s *session) Send(b []byte) bool { s.sent = append(s.sent, string(b)); return true }
func (s *session) Subscribe(m string, ack []byte, snapshot bool) {
	s.subscribed = append(s.subscribed, m)
	s.snapshots = append(s.snapshots, snapshot)
	s.Send(ack)
}
func (s *session) Unsubscribe(m string, ack []byte) {
	s.unsubscribed = append(s.unsubscribed, m)
	s.Send(ack)
}

func newProtocol() *Protocol { return New("orderbook:", "6.9.2 OSS", 0) }

// NiFi's Centrifugo groups send {"id":1,"connect":{}} on connect. Real nobitex and bitpin
// (2026-10-21): {"id":1,"connect":{"client":"<uuid>","version":"6.9.x OSS","ping":25,"pong":true}}.
func TestConnect(t *testing.T) {
	s := &session{}
	newProtocol().OnMessage(s, []byte(`{"id":1,"connect":{}}`))
	require.Len(t, s.sent, 1)
	assert.Regexp(t, `^\{"id":1,"connect":\{"client":"[0-9a-f-]{36}","version":"6\.9\.2 OSS","ping":25,"pong":true\}\}$`, s.sent[0],
		"field order as the real server sends it")
}

// NiFi sends {"id":1,"${status:replace('pending-', ”)}":{"channel":"<prefix>${market}"}}.
// The real servers answer a subscribe with no snapshot: the next push comes on its normal beat.
func TestSubscribeAndUnsubscribe(t *testing.T) {
	s := &session{}
	p := newProtocol()
	p.OnMessage(s, []byte(`{"id":1,"subscribe":{"channel":"orderbook:BTC_USDT"}}`))
	p.OnMessage(s, []byte(`{"id":1,"unsubscribe":{"channel":"orderbook:BTC_USDT"}}`))

	assert.Equal(t, []string{"BTC_USDT"}, s.subscribed)
	assert.Equal(t, []bool{false}, s.snapshots, "no snapshot on subscribe")
	assert.Equal(t, []string{"BTC_USDT"}, s.unsubscribed)
	require.Len(t, s.sent, 2)
	assert.Regexp(t, `^\{"id":1,"subscribe":\{"recoverable":true,"epoch":"[A-Za-z]{8}","positioned":true\}\}$`, s.sent[0])
	assert.Equal(t, `{"id":1,"unsubscribe":{}}`, s.sent[1])
}

// NiFi answers the server's {} ping with {}: nothing to send back.
func TestPongIsQuiet(t *testing.T) {
	s := &session{}
	newProtocol().OnMessage(s, []byte(`{}`))
	assert.Empty(t, s.sent)
	assert.Empty(t, s.subscribed)
}

// Seen live on bitpin: a channel outside the book namespace is error 102.
func TestUnknownChannelIsAnError(t *testing.T) {
	for _, raw := range []string{
		`{"id":3,"subscribe":{"channel":"trades:BTC_USDT"}}`,
		`{"id":3,"subscribe":{"channel":"orderbook:"}}`,
		`{"id":3,"subscribe":{}}`,
		`{"id":3,"unsubscribe":{"channel":"trades:BTC_USDT"}}`,
	} {
		s := &session{}
		newProtocol().OnMessage(s, []byte(raw))
		require.Len(t, s.sent, 1, raw)
		assert.Equal(t, `{"id":3,"error":{"code":102,"message":"unknown channel"}}`, s.sent[0], raw)
		assert.Empty(t, s.subscribed, raw)
		assert.Empty(t, s.unsubscribed, raw)
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
