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

// session records what OnMessage asked for.
type session struct {
	sent       []string
	subscribed []string
}

func (s *session) ID() string         { return "c0e31519" }
func (s *session) Send(b []byte) bool { s.sent = append(s.sent, string(b)); return true }
func (s *session) Subscribe(m string, ack func(int64) []byte, _ bool) {
	s.subscribed = append(s.subscribed, m)
	s.Send(ack(0))
}
func (s *session) Unsubscribe(string, []byte) {}
func (s *session) Close(int, string)          {}

// The Centrifugo protocol itself is tested in package centrifugo; here only what is
// nobitex's: NiFi's channel `public:orderbook-${market}` and the version real nobitex names.
func TestNobitexChannelAndVersion(t *testing.T) {
	s := &session{}
	on := New(Options{ID: 1}).Connect(s)
	on([]byte(`{"id":1,"connect":{}}`))
	on([]byte(`{"id":1,"subscribe":{"channel":"public:orderbook-BTCUSDT"}}`))
	assert.Equal(t, []string{"BTCUSDT"}, s.subscribed)
	require.Len(t, s.sent, 2)
	assert.Contains(t, s.sent[0], `"version":"6.9.3 OSS"`)
}

func TestNoRest(t *testing.T) {
	_, ok := New(Options{ID: 1}).REST()
	assert.False(t, ok, "the live NiFi group has no REST call for nobitex")
}
