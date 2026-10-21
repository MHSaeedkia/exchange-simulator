package nobitex

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"exchange-simulator/internal/domain"
)

var fixed = time.UnixMilli(1792609360731)

func book(t *testing.T, depth int) *domain.Book {
	t.Helper()
	shape, err := domain.NewShape("84000", "0.01", "0.000001", "0.200000")
	require.NoError(t, err)
	return domain.NewBook(shape, depth, 105411, rand.New(rand.NewSource(1)))
}

func decode(t *testing.T, frame []byte) map[string]any {
	t.Helper()
	var v map[string]any
	require.NoError(t, json.Unmarshal(frame, &v), string(frame))
	return v
}

// The push must read like the Centrifugo publication lpa-core's NobitexParser takes
// (docs/sample-raw-data.md § ex1, and real frames captured 2026-10-21):
// {"push":{"channel":"public:orderbook-BTCUSDT","pub":{"data":{asks,bids,lastTradePrice,lastUpdate},"offset":N}}}.
func checkPush(t *testing.T, frame []byte, market string, depth int, offset int64) {
	t.Helper()
	got := decode(t, frame)
	require.Equal(t, []string{"push"}, keys(got), "nothing but the push: NiFi adds id and simulation")
	push := got["push"].(map[string]any)
	assert.Equal(t, "public:orderbook-"+market, push["channel"], "the channel is the only market key")
	pub := push["pub"].(map[string]any)
	assert.Equal(t, float64(offset), pub["offset"], "offset is a number")
	data := pub["data"].(map[string]any)
	assert.ElementsMatch(t, []string{"asks", "bids", "lastTradePrice", "lastUpdate"}, keys(data))
	assert.Len(t, data["asks"], depth)
	assert.Len(t, data["bids"], depth)
	ask := data["asks"].([]any)[0].([]any)
	require.Len(t, ask, 2, "[price, qty]")
	assert.IsType(t, "", ask[0], "price is a string")
	assert.IsType(t, "", ask[1], "qty is a string")
	assert.IsType(t, "", data["lastTradePrice"], "lastTradePrice is a string")
	assert.Equal(t, float64(fixed.UnixMilli()), data["lastUpdate"], "lastUpdate is epoch ms as a number")
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestSnapshotIsTheWholeBookAtTheCurrentOffset(t *testing.T) {
	b := book(t, 24)
	checkPush(t, New(Options{ID: 1}).Snapshot("BTCUSDT", b, fixed), "BTCUSDT", 24, 105411)
}

// Every nobitex push resends the WHOLE book: a level missing from a push is deleted. So an
// update carries every level, not only the changed ones.
func TestUpdateIsTheWholeBookToo(t *testing.T) {
	ex, b := New(Options{ID: 1}), book(t, 24)
	rng := rand.New(rand.NewSource(2))
	asks, bids := b.Requote(2, rng)
	prev, _ := b.Advance(ex.SeqStep(rng))
	frame := ex.Update("BTCUSDT", b, asks, bids, prev, fixed)
	checkPush(t, frame, "BTCUSDT", 24, prev+1)

	data := decode(t, frame)["push"].(map[string]any)["pub"].(map[string]any)["data"].(map[string]any)
	top, _ := b.Top(1)
	assert.Equal(t, top[0].Price, data["asks"].([]any)[0].([]any)[0], "asks best (lowest) first")
}

// Centrifugo offsets move by exactly one per publication (seen live: 105412, 105413, …).
func TestSeqStepIsOne(t *testing.T) {
	ex, rng := New(Options{ID: 1}), rand.New(rand.NewSource(1))
	for range 100 {
		assert.Equal(t, int64(1), ex.SeqStep(rng))
	}
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

// NiFi's NOBITEX group sends {"id":1,"connect":{}} on connect. Real nobitex (2026-10-21):
// {"id":1,"connect":{"client":"<uuid>","version":"6.9.3 OSS","ping":25,"pong":true}}.
func TestConnect(t *testing.T) {
	s := &session{}
	New(Options{ID: 1}).OnMessage(s, []byte(`{"id":1,"connect":{}}`))
	require.Len(t, s.sent, 1)
	got := decode(t, []byte(s.sent[0]))
	assert.Equal(t, 1.0, got["id"], "the request id is echoed")
	c := got["connect"].(map[string]any)
	assert.Len(t, c["client"], 36, "a uuid")
	assert.Equal(t, "6.9.3 OSS", c["version"])
	assert.Equal(t, 25.0, c["ping"])
	assert.Equal(t, true, c["pong"])
}

// NiFi sends {"id":1,"${status:replace('pending-', ”)}":{"channel":"public:orderbook-${market}"}}.
// Real nobitex answers a subscribe with no snapshot (the next push comes on its normal beat).
func TestSubscribeAndUnsubscribe(t *testing.T) {
	s := &session{}
	ex := New(Options{ID: 1})
	ex.OnMessage(s, []byte(`{"id":1,"subscribe":{"channel":"public:orderbook-BTCUSDT"}}`))
	ex.OnMessage(s, []byte(`{"id":1,"unsubscribe":{"channel":"public:orderbook-BTCUSDT"}}`))

	assert.Equal(t, []string{"BTCUSDT"}, s.subscribed)
	assert.Equal(t, []bool{false}, s.snapshots, "no snapshot on subscribe")
	assert.Equal(t, []string{"BTCUSDT"}, s.unsubscribed)
	require.Len(t, s.sent, 2)
	ack := decode(t, []byte(s.sent[0]))
	assert.Equal(t, 1.0, ack["id"])
	sub := ack["subscribe"].(map[string]any)
	assert.Equal(t, true, sub["recoverable"])
	assert.Equal(t, true, sub["positioned"])
	assert.Len(t, sub["epoch"], 8)
	assert.JSONEq(t, `{"id":1,"unsubscribe":{}}`, s.sent[1])
}

// NiFi answers the server's {} ping with {}: nothing to send back.
func TestPongIsQuiet(t *testing.T) {
	s := &session{}
	New(Options{ID: 1}).OnMessage(s, []byte(`{}`))
	assert.Empty(t, s.sent)
	assert.Empty(t, s.subscribed)
}

func TestUnknownChannelIsAnError(t *testing.T) {
	for _, raw := range []string{
		`{"id":3,"subscribe":{"channel":"public:trades-BTCUSDT"}}`,
		`{"id":3,"subscribe":{"channel":"public:orderbook-"}}`,
		`{"id":3,"subscribe":{}}`,
	} {
		s := &session{}
		New(Options{ID: 1}).OnMessage(s, []byte(raw))
		require.Len(t, s.sent, 1, raw)
		assert.JSONEq(t, `{"id":3,"error":{"code":102,"message":"unknown channel"}}`, s.sent[0], raw)
		assert.Empty(t, s.subscribed, raw)
	}
}

func TestHeartbeatIsAnEmptyObjectEvery25s(t *testing.T) {
	msg, every := New(Options{ID: 1}).Heartbeat()
	assert.Equal(t, "{}", string(msg))
	assert.Equal(t, 25*time.Second, every)
}

func TestNoRest(t *testing.T) {
	_, ok := New(Options{ID: 1}).REST()
	assert.False(t, ok, "the live NiFi group has no REST call for nobitex")
}

func TestEnvelopeIsNiFisReplaceText(t *testing.T) {
	got := New(Options{ID: 1}).Envelope([]byte(`{"push":{}}`), "u-1", 1)
	assert.Equal(t, `{"id":"u-1","simulation":1,"push":{}}`, string(got))
}
