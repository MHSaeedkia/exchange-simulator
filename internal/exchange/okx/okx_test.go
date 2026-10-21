package okx

import (
	"encoding/json"
	"hash/crc32"
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

var fixed = time.UnixMilli(1800000000900)

func newOkx() *Exchange {
	return New(Options{ID: 8, StepMin: 3, StepMax: 960, TimeNow: func() time.Time { return fixed }})
}

func book(t *testing.T, depth int) *domain.Book {
	t.Helper()
	shape, err := domain.NewShape("100", "1", "0.1", "9.9")
	require.NoError(t, err)
	return domain.NewBook(shape, depth, 4429784550, rand.New(rand.NewSource(1)))
}

func decode(t *testing.T, frame []byte) map[string]any {
	t.Helper()
	var v map[string]any
	require.NoError(t, json.Unmarshal(frame, &v), string(frame))
	return v
}

// The frames must read exactly like the okx `books` frames lpa-core's parser takes
// (docs/sample-raw-data.md § ex8, lpa-core e2e/scenario/data_ex8.go).
func TestSnapshotHasOkxBooksShape(t *testing.T) {
	b := book(t, 3)
	got := decode(t, newOkx().Snapshot("BTC-USDT", b, fixed))

	assert.Equal(t, map[string]any{"channel": "books", "instId": "BTC-USDT"}, got["arg"])
	assert.Equal(t, "snapshot", got["action"])
	data := got["data"].([]any)
	require.Len(t, data, 1)
	d := data[0].(map[string]any)
	assert.Len(t, d["asks"], 3)
	assert.Len(t, d["bids"], 3)
	assert.Equal(t, "1800000000900", d["ts"], "ts is a string")
	assert.Equal(t, -1.0, d["prevSeqId"], "a snapshot's prevSeqId is -1")
	assert.Equal(t, 4429784550.0, d["seqId"], "seqId is a number")
	assert.Equal(t, []any{"100", d["asks"].([]any)[0].([]any)[1], "0", "1"}, d["asks"].([]any)[0], "four-element level")
	assert.NotContains(t, got, "id", "NiFi adds id and simulation, not the exchange")
}

func TestUpdateChainsToTheFrameBefore(t *testing.T) {
	ex, b := newOkx(), book(t, 5)
	rng := rand.New(rand.NewSource(2))
	asks, _ := b.Requote(2, rng)
	prev, _ := b.Advance(ex.SeqStep(rng))

	got := decode(t, ex.Update("BTC-USDT", b, asks, []domain.Level{}, prev, fixed))
	assert.Equal(t, "update", got["action"])
	d := got["data"].([]any)[0].(map[string]any)
	assert.Equal(t, float64(prev), d["prevSeqId"])
	assert.Equal(t, float64(b.Seq()), d["seqId"])
	assert.Len(t, d["asks"], 2)
	assert.Equal(t, []any{}, d["bids"], "an unchanged side is [], never null or missing")
}

func TestSeqStepStaysInRange(t *testing.T) {
	ex, rng := newOkx(), rand.New(rand.NewSource(1))
	for range 1000 {
		s := ex.SeqStep(rng)
		assert.GreaterOrEqual(t, s, int64(3))
		assert.LessOrEqual(t, s, int64(960))
	}
}

func TestChecksumInterleavesBestBidsAndAsks(t *testing.T) {
	bids := []domain.Level{{Price: "3366.1", Qty: "7"}, {Price: "3366", Qty: "6"}}
	asks := []domain.Level{{Price: "3366.8", Qty: "9"}, {Price: "3368", Qty: "8"}, {Price: "3372", Qty: "8"}}
	assert.Equal(t, "3366.1:7:3366.8:9:3366:6:3368:8:3372:8", checksumString(asks, bids))
}

func TestChecksumUsesOnlyTheBest25(t *testing.T) {
	b := book(t, 30)
	s := checksumString(b.Top(checksumLevels))
	assert.Len(t, strings.Split(s, ":"), 25*4)
	assert.Equal(t, int32(crc32.ChecksumIEEE([]byte(s))), checksum(b))
}

func TestAckAndError(t *testing.T) {
	assert.JSONEq(t,
		`{"event":"subscribe","arg":{"channel":"books","instId":"ETH-USDT"},"connId":"a4d3ae55"}`,
		string(Ack("", OpSubscribe, Arg{Channel: Channel, InstID: "ETH-USDT"}, "a4d3ae55")))
	assert.JSONEq(t,
		`{"id":"7","event":"unsubscribe","arg":{"channel":"books","instId":"ETH-USDT"},"connId":"a4d3ae55"}`,
		string(Ack("7", OpUnsubscribe, Arg{Channel: Channel, InstID: "ETH-USDT"}, "a4d3ae55")),
		"the request id is echoed")
	assert.JSONEq(t,
		`{"event":"error","code":"60012","msg":"Invalid request: x","connId":"c"}`,
		string(ErrorEvent("", CodeInvalidRequest, "Invalid request: x", "c")))
}

// session records what OnMessage asked for.
type session struct {
	sent         []string
	subscribed   []string
	unsubscribed []string
}

func (s *session) ID() string         { return "a4d3ae55" }
func (s *session) Send(b []byte) bool { s.sent = append(s.sent, string(b)); return true }
func (s *session) Subscribe(m string, ack func(int64) []byte, _ bool) {
	s.subscribed = append(s.subscribed, m)
	s.Send(ack(0))
}
func (s *session) Close(int, string) {}
func (s *session) Unsubscribe(m string, ack []byte) {
	s.unsubscribed = append(s.unsubscribed, m)
	s.Send(ack)
}

// NiFi's okx group sends {"op":"${status:replace('pending-',”)}","args":[{"channel":"books","instId":"${market}"}]}.
func TestOnMessageSubscribeAndUnsubscribe(t *testing.T) {
	s := &session{}
	ex := newOkx()
	ex.OnMessage(s, []byte(`{"op":"subscribe","args":[{"channel":"books","instId":"A-USDT"},{"channel":"books","instId":"B-USDT"}]}`))
	ex.OnMessage(s, []byte(`{"op":"unsubscribe","args":[{"channel":"books","instId":"A-USDT"}]}`))

	assert.Equal(t, []string{"A-USDT", "B-USDT"}, s.subscribed, "one subscribe per arg")
	assert.Equal(t, []string{"A-USDT"}, s.unsubscribed)
	require.Len(t, s.sent, 3)
	assert.Contains(t, s.sent[0], `"event":"subscribe"`)
	assert.Contains(t, s.sent[2], `"event":"unsubscribe"`)
}

func TestOnMessagePing(t *testing.T) {
	s := &session{}
	newOkx().OnMessage(s, []byte("ping"))
	assert.Equal(t, []string{"pong"}, s.sent)
}

func TestOnMessageErrors(t *testing.T) {
	for raw, code := range map[string]string{
		"hello":                        CodeInvalidRequest,
		`{"op":"login","args":[]}`:     CodeInvalidRequest,
		`{"op":"subscribe","args":[]}`: CodeInvalidRequest,
		`{"op":"subscribe","args":[{"channel":"tickers","instId":"BTC-USDT"}]}`: CodeNoSuchChannel,
		`{"op":"subscribe","args":[{"channel":"books"}]}`:                       CodeNoSuchChannel,
	} {
		s := &session{}
		newOkx().OnMessage(s, []byte(raw))
		require.Len(t, s.sent, 1, raw)
		got := decode(t, []byte(s.sent[0]))
		assert.Equal(t, "error", got["event"], raw)
		assert.Equal(t, code, got["code"], raw)
		assert.Empty(t, s.subscribed, raw)
	}
}

// books answers every market with the first sz levels of one fixed book.
type books struct{ b *domain.Book }

func (b books) Top(_ string, n int) ([]domain.Level, []domain.Level, int64) {
	asks, bids := b.b.Top(n)
	return asks, bids, b.b.Seq()
}

func rest(t *testing.T, query string) RestBody {
	t.Helper()
	rec := httptest.NewRecorder()
	newOkx().ServeREST(rec, httptest.NewRequest(http.MethodGet, "/api/v5/market/books"+query, nil), books{book(t, 5)})
	assert.Equal(t, http.StatusOK, rec.Code)
	var body RestBody
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	return body
}

func TestRestBooks(t *testing.T) {
	body := rest(t, "?instId=ZEC-USDT&sz=3")
	assert.Equal(t, "0", body.Code)
	require.Len(t, body.Data, 1)
	assert.Len(t, body.Data[0].Asks, 3)
	assert.Len(t, body.Data[0].Bids, 3)
	assert.Equal(t, int64(4429784550), body.Data[0].SeqID)
	assert.Equal(t, "1800000000900", body.Data[0].TS)

	assert.Len(t, rest(t, "?instId=ZEC-USDT").Data[0].Asks, 1, "sz defaults to 1")
}

func TestRestBooksErrors(t *testing.T) {
	assert.Equal(t, CodeEmptyParameter, rest(t, "").Code)
	assert.Equal(t, CodeBadParameter, rest(t, "?instId=X&sz=abc").Code)
	assert.Equal(t, CodeBadParameter, rest(t, "?instId=X&sz=0").Code)
}

func TestEnvelopeIsNiFisReplaceText(t *testing.T) {
	got := newOkx().Envelope([]byte(`{"arg":{}}`), "u-1", 1)
	assert.Equal(t, `{"id":"u-1","simulation":1,"arg":{}}`, string(got))
}
