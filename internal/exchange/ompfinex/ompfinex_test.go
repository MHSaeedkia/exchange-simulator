package ompfinex

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"exchange-simulator/internal/domain"
)

var fixed = time.UnixMicro(1792613274020009)

func book(t *testing.T, depth int) *domain.Book {
	t.Helper()
	shape, err := domain.NewShape("84516.5", "0.1", "0.00000100", "0.02000000")
	require.NoError(t, err)
	return domain.NewBook(shape, depth, 7745589, rand.New(rand.NewSource(1)))
}

func newOmpfinex() *Exchange {
	return New(Options{ID: 7, StepMin: 1, StepMax: 3,
		REST: domain.Endpoint{Host: "api.ompfinex.com", Port: 443, Path: "/v1/market/"}, TimeNow: func() time.Time { return fixed }})
}

type push struct {
	Push struct {
		Channel string `json:"channel"`
		Pub     struct {
			Data struct {
				U     int64      `json:"u"`
				First int64      `json:"U"`
				A     [][]string `json:"a"`
				B     [][]string `json:"b"`
			} `json:"data"`
			Offset int64 `json:"offset"`
		} `json:"pub"`
	} `json:"push"`
}

// Real ompfinex (captured 2026-10-21), as lpa-core's OmpfinexParser takes it:
// {"push":{"channel":"public-market:r-depth-14","pub":{"data":{"u":7745591,"U":7745590,"a":[…],"b":[]},"offset":3752420}}}
// — the changed levels only; U is the u of the delta before; offset moves by one per push.
func TestDeltaHasOmpfinexShape(t *testing.T) {
	ex, b := newOmpfinex(), book(t, 50)
	rng := rand.New(rand.NewSource(2))
	var offsets []int64
	for range 3 {
		asks, _ := b.Requote(2, rng)
		prev, _ := b.Advance(ex.SeqStep(rng))
		raw := ex.Update("14", b, asks, []domain.Level{}, prev, fixed)
		assert.Regexp(t, `^\{"push":\{"channel":"public-market:r-depth-14","pub":\{"data":\{"u":\d+,"U":\d+,"a":\[\["[0-9.]+","[0-9.]+"\]`, string(raw))
		assert.Contains(t, string(raw), `"b":[]`, "an unchanged side is []")

		var p push
		require.NoError(t, json.Unmarshal(raw, &p))
		assert.Equal(t, prev, p.Push.Pub.Data.First, "U is the previous delta's u")
		assert.Equal(t, b.Seq(), p.Push.Pub.Data.U)
		assert.Len(t, p.Push.Pub.Data.A, 2)
		offsets = append(offsets, p.Push.Pub.Offset)
	}
	assert.Equal(t, []int64{offsets[0], offsets[0] + 1, offsets[0] + 2}, offsets)
}

func TestSeqStepStaysInRange(t *testing.T) {
	ex, rng := newOmpfinex(), rand.New(rand.NewSource(1))
	for range 1000 {
		s := ex.SeqStep(rng)
		assert.GreaterOrEqual(t, s, int64(1))
		assert.LessOrEqual(t, s, int64(3))
	}
}

// ompfinex's WebSocket carries only deltas: a book's start is the REST snapshot.
func TestNoWebSocketSnapshots(t *testing.T) {
	assert.False(t, newOmpfinex().WebSocketSnapshots())
}

type snapshot struct {
	Status string `json:"status"`
	Data   struct {
		LastUpdateID int64      `json:"lastUpdateId"`
		Time         int64      `json:"time"`
		Bids         [][]string `json:"bids"`
		Asks         [][]string `json:"asks"`
	} `json:"data"`
	Action string `json:"action"`
	Pair   string `json:"pair"`
}

// The snapshot (kafka mode) is the REST body as NiFi publishes it: Jolt's default adds
// "action":"snapshot" and "pair" after it. time is epoch MICROseconds.
func TestSnapshotIsTheRestBodyAsNiFiPublishesIt(t *testing.T) {
	b := book(t, 50)
	raw := newOmpfinex().Snapshot("14", b, fixed)
	assert.Regexp(t, `^\{"status":"OK","data":\{"lastUpdateId":7745589,"time":1792613274020009,"bids":\[\["`, string(raw))
	assert.Regexp(t, `\]\]\},"action":"snapshot","pair":"14"\}$`, string(raw))
	var s snapshot
	require.NoError(t, json.Unmarshal(raw, &s))
	assert.Len(t, s.Data.Bids, 50)
	assert.Len(t, s.Data.Asks, 50)
	asks, bids := b.Top(1)
	assert.Equal(t, bids[0].Price, s.Data.Bids[0][0], "bids best (highest) first")
	assert.Equal(t, asks[0].Price, s.Data.Asks[0][0], "asks best (lowest) first")
}

// books answers every market with one fixed book; lastMarket is the last market asked for.
type books struct{ b *domain.Book }

var lastMarket string

func (b books) Top(market string, n int) ([]domain.Level, []domain.Level, int64) {
	lastMarket = market
	asks, bids := b.b.Top(n)
	return asks, bids, b.b.Seq()
}

func rest(t *testing.T, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	newOmpfinex().ServeREST(rec, httptest.NewRequest(http.MethodGet, path, nil), books{book(t, 50)})
	return rec
}

// NiFi's snapshot branch: GET {api}/market/${pair}/depth?limit=50 (api = https://api.ompfinex.com/v1).
// Real: {"status":"OK","data":{"lastUpdateId":…,"time":<µs>,"bids":[…],"asks":[…]}}; 25 levels
// without limit.
func TestRestDepth(t *testing.T) {
	rec := rest(t, "/v1/market/14/depth?limit=50")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.Regexp(t, `^\{"status":"OK","data":\{"lastUpdateId":7745589,"time":1792613274020009,"bids":`, rec.Body.String())
	assert.Regexp(t, `\]\]\}\}$`, rec.Body.String(), "no action/pair: NiFi adds those")
	var s snapshot
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &s))
	assert.Len(t, s.Data.Bids, 50)
	assert.Equal(t, "14", lastMarket)

	require.NoError(t, json.Unmarshal(rest(t, "/v1/market/14/depth").Body.Bytes(), &s))
	assert.Len(t, s.Data.Bids, 25, "25 levels by default")
}

func TestRestWrongPathIs404(t *testing.T) {
	assert.Equal(t, http.StatusNotFound, rest(t, "/v1/market/14/trades").Code)
}

// session records what the connection handler asked for.
type session struct {
	sent       []string
	subscribed []string
	closed     int
}

func (s *session) ID() string         { return "c0e31519" }
func (s *session) Send(b []byte) bool { s.sent = append(s.sent, string(b)); return true }
func (s *session) Subscribe(m string, ack func(int64) []byte, _ bool) {
	s.subscribed = append(s.subscribed, m)
	s.Send(ack(0))
}
func (s *session) Unsubscribe(string, []byte) {}
func (s *session) Close(int, string)          { s.closed++ }

// The protocol is package centrifugo; here only what is ompfinex's (seen live 2026-10-21):
// NiFi's {"id":2,"subscribe":{"channel":"public-market:r-depth-${market}","flag":1}}, version
// 6.9.0, a ping every 6 s answered within 4 s, a plain {} subscribe ack, another namespace 102.
func TestOmpfinexProtocol(t *testing.T) {
	s := &session{}
	ex := newOmpfinex()
	on := ex.Connect(s)
	on([]byte(`{"id":1,"connect":{}}`))
	on([]byte(`{"id":2,"subscribe":{"channel":"public-market:r-depth-14","flag":1}}`))
	on([]byte(`{"id":3,"subscribe":{"channel":"orderbook:14"}}`))
	assert.Equal(t, []string{"14"}, s.subscribed)
	require.Len(t, s.sent, 3)
	assert.Regexp(t, `^\{"id":1,"connect":\{"client":"[0-9a-f-]{36}","version":"6\.9\.0 OSS","ping":6,"pong":true\}\}$`, s.sent[0])
	assert.Equal(t, `{"id":2,"subscribe":{}}`, s.sent[1])
	assert.Equal(t, `{"id":3,"error":{"code":102,"message":"unknown channel"}}`, s.sent[2])
	h := ex.Heartbeat()
	assert.Equal(t, 6*time.Second, h.Every)
	assert.Equal(t, 4*time.Second, h.PongTimeout)
}

func TestEnvelopeIsNiFisReplaceText(t *testing.T) {
	got := newOmpfinex().Envelope([]byte(`{"push":{}}`), "u-1", 1)
	assert.Equal(t, `{"id":"u-1","simulation":1,"push":{}}`, string(got))
}
