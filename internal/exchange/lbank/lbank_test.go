package lbank

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"exchange-simulator/internal/domain"
)

// fixed is 2026-10-21T20:16:24.032Z; lbank writes it as China time, 2026-10-22T04:16:24.032.
var fixed = time.Date(2026, 10, 21, 20, 16, 24, 32_000_000, time.UTC)

func book(t *testing.T, depth int) *domain.Book {
	t.Helper()
	shape, err := domain.NewShape("84525.40", "0.01", "0.00001", "9.50000")
	require.NoError(t, err)
	return domain.NewBook(shape, depth, 1, rand.New(rand.NewSource(1)))
}

func newLbank() *Exchange {
	return New(Options{ID: 9, PriceDecimals: 2, REST: domain.Endpoint{Host: "api.lbkex.com", Port: 443, Path: "/v2/depth.do"},
		TimeNow: func() time.Time { return fixed }})
}

type frame struct {
	Depth struct {
		Asks [][]string `json:"asks"`
		Bids [][]string `json:"bids"`
	} `json:"depth"`
	Server string `json:"SERVER"`
	Count  int    `json:"count"`
	Limit  int    `json:"limit"`
	Type   string `json:"type"`
	Pair   string `json:"pair"`
	TS     string `json:"TS"`
}

// Real lbank (captured 2026-10-21), as lpa-core's LBankParser takes it:
// {"depth":{"asks":[["84525.42","9.41085"],…],"bids":[…]},"SERVER":"V3","count":50,"limit":50,
// "type":"fdepth","pair":"btc_usdt","TS":"2026-10-22T04:16:24.032"} — the whole book, prices with
// two decimals, TS in China time (UTC+8) with no zone.
func TestFrameHasFdepthShape(t *testing.T) {
	b := book(t, 50)
	raw := newLbank().Snapshot("btc_usdt", b, fixed)
	assert.Regexp(t, `^\{"depth":\{"asks":\[\["84525\.40","\d\.\d{5}"\],\["84525\.41",`, string(raw), "prices padded, quantities keep their decimals")
	assert.Regexp(t, `\]\]\},"SERVER":"V3","count":50,"limit":50,"type":"fdepth","pair":"btc_usdt","TS":"2026-10-22T04:16:24\.032"\}$`, string(raw),
		"TS is UTC+8, as real lbank sends it")
	var f frame
	require.NoError(t, json.Unmarshal(raw, &f))
	assert.Len(t, f.Depth.Asks, 50)
	assert.Len(t, f.Depth.Bids, 50)
	assert.Equal(t, "84525.39", f.Depth.Bids[0][0], "bids best (highest) first")

	rng := rand.New(rand.NewSource(2))
	asks, bids := b.Requote(3, rng)
	ex := newLbank()
	prev, _ := b.Advance(ex.SeqStep(rng))
	require.NoError(t, json.Unmarshal(ex.Update("btc_usdt", b, asks, bids, prev, fixed), &f))
	assert.Len(t, f.Depth.Asks, 50, "every frame is the whole book")
}

// session records what the connection handler asked for.
type session struct {
	id           string // "" = "c0e31519"
	sent         []string
	subscribed   []string
	snapshots    []bool
	unsubscribed []string
}

func (s *session) ID() string {
	if s.id == "" {
		return "c0e31519"
	}
	return s.id
}
func (s *session) Send(b []byte) bool { s.sent = append(s.sent, string(b)); return true }
func (s *session) Subscribe(m string, ack func(int64) []byte, snapshot bool) {
	s.subscribed = append(s.subscribed, m)
	s.snapshots = append(s.snapshots, snapshot)
	s.Send(ack(0))
}
func (s *session) Unsubscribe(m string, ack []byte) {
	s.unsubscribed = append(s.unsubscribed, m)
	if ack != nil {
		s.Send(ack)
	}
}
func (s *session) Close(int, string) {}

const nifiSubscribe = `{"symbol": "btc_usdt","pair": "btc_usdt","action": "subscribe","subscribe": "fdepth","depth": "50","limit": "50","clientType": ""}`

// NiFi sends {"symbol": "${market}","pair": "${market}","action": "subscribe","subscribe": "fdepth",
// "depth": "50","limit": "50","clientType": ""}. Real lbank acks in plain text,
// <40-char connection token>_fdepth_btc_usdt_50 (the same again on a repeat), then the book;
// an unsubscribe gets no reply.
func TestSubscribeAndUnsubscribe(t *testing.T) {
	s := &session{}
	on := newLbank().Connect(s)
	on([]byte(nifiSubscribe))
	on([]byte(nifiSubscribe))
	on([]byte(strings.Replace(nifiSubscribe, `"subscribe","subscribe"`, `"unsubscribe","subscribe"`, 1)))

	assert.Equal(t, []string{"btc_usdt", "btc_usdt"}, s.subscribed)
	assert.Equal(t, []bool{true, true}, s.snapshots, "the book follows the ack at once")
	assert.Equal(t, []string{"btc_usdt"}, s.unsubscribed)
	require.Len(t, s.sent, 2)
	assert.Regexp(t, `^[A-Za-z0-9-]{40}_fdepth_btc_usdt_50$`, s.sent[0], "letters, digits and - (both seen live)")
	assert.Equal(t, s.sent[0], s.sent[1], "one token per connection")
}

// Seen live. Errors are {"SERVER":"V3","message":…,"status":"error","TS":<UTC+8>}.
func TestErrorsAndPing(t *testing.T) {
	errorOf := func(msg string) string {
		return `{"SERVER":"V3","message":"` + msg + `","status":"error","TS":"2026-10-22T04:16:24.032"}`
	}
	for raw, want := range map[string]string{
		`hello`: errorOf("message"),
		// NiFi's ping path takes every frame of 60 bytes or less, so it echoes the 59-byte ack.
		`Cozy6KWvhHHUMo9nw8nNhOYJxlFqY2CwyrHo6S5j_fdepth_btc_usdt_50`:      errorOf("message"),
		`{"pair": "btc_usdt","action": "subscribe","subscribe": "fdepth"}`: errorOf(`Missing parameter ['depth']`),
		`{"action":"ping","ping":"abc-123"}`:                               `{"action":"pong","pong":"abc-123"}`,
		`{"pong":"1792613597010","action":"pong"}`:                         errorOf(`[1792613597010] is not a ping`),
	} {
		s := &session{}
		newLbank().Connect(s)([]byte(raw))
		assert.Equal(t, []string{want}, s.sent, raw)
		assert.Empty(t, s.subscribed, raw)
	}
	s := &session{}
	newLbank().Connect(s)([]byte(`{"pair": "btc_usdt","action": "subscribe","subscribe": "foo"}`))
	assert.Empty(t, s.sent, "another subscription type: no reply")
}

// A pong for a ping this connection got is taken with no reply, even twice.
func TestPongForASentPing(t *testing.T) {
	ex := newLbank()
	s := &session{}
	on := ex.Connect(s)
	ping := ex.Heartbeat().MsgFor(s.ID(), fixed)
	assert.Equal(t, `{"ping":"1792613784032","action":"ping"}`, string(ping))
	pong := `{"pong":"1792613784032","action":"pong"}`
	on([]byte(pong))
	on([]byte(pong))
	assert.Empty(t, s.sent)

	other := &session{id: "77aa0011"}
	ex.Connect(other)([]byte(pong))
	assert.Len(t, other.sent, 1, "another connection's ping id is not a ping there")
}

// Real lbank: a ping every 60 s, the first at a varying point of the first minute (0.5, 3.7,
// 6.8, 19.9 s seen: a server-wide clock); after 6 pings with no pong, closed 1001
// "server-heartbeat-timeout" when the 7th is due.
func TestHeartbeat(t *testing.T) {
	h := newLbank().Heartbeat()
	assert.Equal(t, 60*time.Second, h.Every)
	assert.Equal(t, 6, h.MissedLimit)
	assert.Equal(t, 1001, h.CloseCode)
	assert.Equal(t, "server-heartbeat-timeout", h.CloseText)
	firsts := map[time.Duration]bool{}
	for range 20 {
		f := newLbank().Heartbeat().First
		assert.Positive(t, f)
		assert.Less(t, f, 60*time.Second)
		firsts[f] = true
	}
	assert.Greater(t, len(firsts), 1, "the first ping's time varies")
}

// The server takes a pong only for a ping it sent on that connection.
func TestIsPongChecksTheID(t *testing.T) {
	ex := newLbank()
	h := ex.Heartbeat()
	h.MsgFor("c1", fixed)
	assert.True(t, h.IsPong("c1", []byte(`{"pong":"1792613784032","action":"pong"}`)))
	assert.False(t, h.IsPong("c2", []byte(`{"pong":"1792613784032","action":"pong"}`)), "another connection's ping")
	assert.False(t, h.IsPong("c1", []byte(`{"pong":"1","action":"pong"}`)))
	assert.False(t, h.IsPong("c1", []byte(`{"ping":"1792613784032","action":"ping"}`)))
}

func rest(t *testing.T, query string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	newLbank().ServeREST(rec, httptest.NewRequest(http.MethodGet, "/v2/depth.do"+query, nil), books{book(t, 50)})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json;charset=UTF-8", rec.Header().Get("Content-Type"))
	return rec.Body.String()
}

// books answers every market with one fixed book.
type books struct{ b *domain.Book }

func (b books) Top(_ string, n int) ([]domain.Level, []domain.Level, int64) {
	asks, bids := b.b.Top(n)
	return asks, bids, b.b.Seq()
}

// NiFi's snapshot branch (through the proxy): GET {api}/v2/depth.do?symbol=${pair}&size=50.
// Real: {"result":"true","msg":"Success","data":{"asks":…,"bids":…,"timestamp":…},"error_code":0,"ts":…},
// prices without trailing zeros; with no size there is no data at all.
func TestRestDepth(t *testing.T) {
	body := rest(t, "?symbol=btc_usdt&size=50")
	assert.Regexp(t, `^\{"result":"true","msg":"Success","data":\{"asks":\[\["84525\.4","`, body)
	assert.Regexp(t, `\]\],"timestamp":1792613784032\},"error_code":0,"ts":1792613784032\}$`, body)
	var r struct {
		Data struct {
			Asks [][]string `json:"asks"`
			Bids [][]string `json:"bids"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &r))
	assert.Len(t, r.Data.Asks, 50)
	assert.Len(t, r.Data.Bids, 50)

	assert.Equal(t, `{"result":"true","msg":"Success","error_code":0,"ts":1792613784032}`, rest(t, "?symbol=btc_usdt"))
	assert.Equal(t, `{"result":"true","msg":"Success","error_code":0,"ts":1792613784032}`, rest(t, "?size=50"))
}

func TestEnvelopeIsNiFisReplaceText(t *testing.T) {
	got := newLbank().Envelope([]byte(`{"depth":{}}`), "u-1", 1)
	assert.Equal(t, `{"id":"u-1","simulation":1,"depth":{}}`, string(got))
}
