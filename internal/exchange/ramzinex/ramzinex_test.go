package ramzinex

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"exchange-simulator/internal/domain"
)

var fixed = time.UnixMilli(1792611313223)

func book(t *testing.T, depth int) *domain.Book {
	t.Helper()
	shape, err := domain.NewShape("84200", "0.01", "0.0000010", "0.0200000")
	require.NoError(t, err)
	return domain.NewBook(shape, depth, 1924388, rand.New(rand.NewSource(1)))
}

func newRamzinex() *Exchange { return New(Options{ID: 4}) }

// levels decodes a side keeping the numbers as written.
func levels(t *testing.T, raw json.RawMessage) [][]any {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var out [][]any
	require.NoError(t, d.Decode(&out))
	return out
}

// checkSide asserts the real ramzinex level layout (captured 2026-10-21):
// [84048,0.000042,3.530016,false,null,10,1792611279396] — price and quantity as JSON numbers,
// price × quantity, false, null, a small int, an epoch-ms time; sorted price-DESCENDING on
// both sides (the best ask is the LAST sell).
func checkSide(t *testing.T, got [][]any, want []domain.Level) {
	t.Helper()
	require.Len(t, got, len(want))
	for _, l := range got {
		require.Len(t, l, 7)
		p, _ := strconv.ParseFloat(l[0].(json.Number).String(), 64)
		q, _ := strconv.ParseFloat(l[1].(json.Number).String(), 64)
		n, _ := strconv.ParseFloat(l[2].(json.Number).String(), 64)
		assert.InDelta(t, p*q, n, 1e-9, "notional = price × quantity")
		assert.Equal(t, false, l[3])
		assert.Nil(t, l[4])
		k, err := strconv.Atoi(l[5].(json.Number).String())
		require.NoError(t, err)
		assert.GreaterOrEqual(t, k, 8)
		assert.LessOrEqual(t, k, 90)
		ms, err := strconv.ParseInt(l[6].(json.Number).String(), 10, 64)
		require.NoError(t, err)
		assert.LessOrEqual(t, ms, fixed.UnixMilli())
		assert.Greater(t, ms, fixed.Add(-30*time.Minute).UnixMilli())
	}
	for i := 1; i < len(got); i++ {
		a, _ := strconv.ParseFloat(got[i-1][0].(json.Number).String(), 64)
		b, _ := strconv.ParseFloat(got[i][0].(json.Number).String(), 64)
		assert.Greater(t, a, b, "price-descending")
	}
}

type push struct {
	Push struct {
		Channel string `json:"channel"`
		Pub     struct {
			Data   map[string]json.RawMessage `json:"data"`
			Offset int64                      `json:"offset"`
		} `json:"pub"`
	} `json:"push"`
}

// The push must read like the one lpa-core's RamzinexParser takes (docs/sample-raw-data.md
// § ex4): channel orderbook:{numeric id}, data {buys, sells}, the whole book on every push.
func TestPushHasRamzinexShape(t *testing.T) {
	b := book(t, 50)
	raw := newRamzinex().Snapshot("12", b, fixed)
	assert.Regexp(t, `^\{"push":\{"channel":"orderbook:12","pub":\{"data":\{"buys":\[\[[0-9.]+,[0-9.]+,[0-9.e-]+,false,null,\d+,\d{13}\],`, string(raw),
		"no spaces, buys first")

	var p push
	require.NoError(t, json.Unmarshal(raw, &p), string(raw))
	assert.Equal(t, "orderbook:12", p.Push.Channel)
	assert.Equal(t, int64(1924388), p.Push.Pub.Offset)
	require.Len(t, p.Push.Pub.Data, 2)
	asks, bids := b.Top(50)
	buys, sells := levels(t, p.Push.Pub.Data["buys"]), levels(t, p.Push.Pub.Data["sells"])
	checkSide(t, buys, bids)
	checkSide(t, sells, asks)
	assert.Equal(t, noTrailingZeros(bids[0].Price), buys[0][0].(json.Number).String(), "the best bid is first")
	assert.Equal(t, noTrailingZeros(asks[0].Price), sells[49][0].(json.Number).String(), "the best ask is LAST")
	assert.Equal(t, noTrailingZeros(asks[0].Qty), sells[49][1].(json.Number).String(), "quantity without trailing zeros")
}

// Every ramzinex push resends the WHOLE book.
func TestUpdateIsTheWholeBookToo(t *testing.T) {
	ex, b := newRamzinex(), book(t, 50)
	rng := rand.New(rand.NewSource(2))
	asks, bids := b.Requote(3, rng)
	prev, _ := b.Advance(ex.SeqStep(rng))
	var p push
	require.NoError(t, json.Unmarshal(ex.Update("12", b, asks, bids, prev, fixed), &p))
	assert.Len(t, levels(t, p.Push.Pub.Data["buys"]), 50)
	assert.Equal(t, prev+1, p.Push.Pub.Offset)
}

// NiFi's snapshot branch: GET {api}/exchange/api/v1.0/exchange/orderbooks/${pair}/buys_sells.
// Real ramzinex: {"data":{"buys":[…],"sells":[…]},"status":0}, the same levels as the push.
func TestRestOrderbook(t *testing.T) {
	rec := httptest.NewRecorder()
	ex := New(Options{ID: 4, REST: domain.Endpoint{Host: "publicapi.ramzinex.com", Port: 443, Path: "/exchange/api/v1.0/exchange/orderbooks/"}, TimeNow: func() time.Time { return fixed }})
	ex.ServeREST(rec, httptest.NewRequest(http.MethodGet, "/exchange/api/v1.0/exchange/orderbooks/12/buys_sells", nil), books{book(t, 50)})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
	assert.Regexp(t, `^\{"data":\{"buys":\[\[`, rec.Body.String())
	assert.Regexp(t, `\]\]\},"status":0\}$`, rec.Body.String())

	var body struct {
		Data   map[string]json.RawMessage `json:"data"`
		Status *int                       `json:"status"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.NotNil(t, body.Status)
	assert.Zero(t, *body.Status)
	asks, bids := book(t, 50).Top(50)
	checkSide(t, levels(t, body.Data["buys"]), bids)
	checkSide(t, levels(t, body.Data["sells"]), asks)
	assert.Equal(t, "12", lastMarket, "the pair is read from the path")
}

func TestRestWrongPathIs404(t *testing.T) {
	rec := httptest.NewRecorder()
	ex := New(Options{ID: 4, REST: domain.Endpoint{Path: "/exchange/api/v1.0/exchange/orderbooks/"}})
	ex.ServeREST(rec, httptest.NewRequest(http.MethodGet, "/exchange/api/v1.0/exchange/orderbooks/12/trades", nil), books{book(t, 5)})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// books answers every market with one fixed book; lastMarket is the last market asked for.
type books struct{ b *domain.Book }

var lastMarket string

func (b books) Top(market string, n int) ([]domain.Level, []domain.Level, int64) {
	lastMarket = market
	asks, bids := b.b.Top(n)
	return asks, bids, b.b.Seq()
}

// session records what the connection handler asked for.
type session struct {
	sent       []string
	subscribed []string
}

func (s *session) ID() string         { return "c0e31519" }
func (s *session) Send(b []byte) bool { s.sent = append(s.sent, string(b)); return true }
func (s *session) Subscribe(m string, ack func(int64) []byte, _ bool) {
	s.subscribed = append(s.subscribed, m)
	s.Send(ack(1924388))
}
func (s *session) Unsubscribe(string, []byte) {}
func (s *session) Close(int, string)          {}

// The protocol is tested in package centrifugo; here only what is ramzinex's (seen live
// 2026-10-21): NiFi's channel `orderbook:${market}`, no "version" in the connect reply, a
// number-like epoch, any namespace acked, and 10 s to answer a ping.
func TestRamzinexProtocol(t *testing.T) {
	s := &session{}
	ex := newRamzinex()
	on := ex.Connect(s)
	on([]byte(`{"id":1,"connect":{}}`))
	on([]byte(`{"id":1,"subscribe":{"channel":"orderbook:12"}}`))
	on([]byte(`{"id":3,"subscribe":{"channel":"public:orderbook-12"}}`))
	assert.Equal(t, []string{"12"}, s.subscribed)
	require.Len(t, s.sent, 3)
	assert.NotContains(t, s.sent[0], "version")
	assert.Regexp(t, `^\{"id":1,"subscribe":\{"recoverable":true,"epoch":"\d{10}","offset":1924388,"positioned":true\}\}$`, s.sent[1])
	assert.Regexp(t, `^\{"id":3,"subscribe":\{"recoverable":true,"epoch":"\d{10}","positioned":true\}\}$`, s.sent[2])
	assert.Equal(t, 10*time.Second, ex.Heartbeat().PongTimeout)
}

func noTrailingZeros(s string) string {
	if bytes.ContainsRune([]byte(s), '.') {
		s = string(bytes.TrimRight(bytes.TrimRight([]byte(s), "0"), "."))
	}
	return s
}
