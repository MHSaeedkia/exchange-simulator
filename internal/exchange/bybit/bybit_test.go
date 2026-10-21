package bybit

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"exchange-simulator/internal/domain"
)

var fixed = time.UnixMilli(1792612740674)

func book(t *testing.T, depth int) *domain.Book {
	t.Helper()
	shape, err := domain.NewShape("84545", "0.1", "0.000001", "0.900000")
	require.NoError(t, err)
	return domain.NewBook(shape, depth, 346401778, rand.New(rand.NewSource(1)))
}

func newBybit() *Exchange {
	return New(Options{ID: 6, REST: domain.Endpoint{Host: "api.bybit.com", Port: 443, Path: "/v5/market/orderbook"},
		TimeNow: func() time.Time { return fixed }})
}

type frame struct {
	Topic string `json:"topic"`
	TS    int64  `json:"ts"`
	Type  string `json:"type"`
	Data  struct {
		S   string     `json:"s"`
		B   [][]string `json:"b"`
		A   [][]string `json:"a"`
		U   int64      `json:"u"`
		Seq int64      `json:"seq"`
	} `json:"data"`
	CTS int64 `json:"cts"`
}

// Real bybit (captured 2026-10-21, as lpa-core's BybitParser takes it):
// {"topic":"orderbook.50.BTCUSDT","ts":…,"type":"snapshot","data":{"s":"BTCUSDT","b":[…],"a":[…],"u":…,"seq":…},"cts":…}
func TestSnapshotHasBybitShape(t *testing.T) {
	b := book(t, 50)
	raw := newBybit().Snapshot("BTCUSDT", b, fixed)
	assert.Regexp(t, `^\{"topic":"orderbook\.50\.BTCUSDT","ts":\d+,"type":"snapshot","data":\{"s":"BTCUSDT","b":\[\["`, string(raw))
	assert.Regexp(t, `\]\],"u":346401778,"seq":\d+\},"cts":1792612740674\}$`, string(raw))

	var f frame
	require.NoError(t, json.Unmarshal(raw, &f))
	assert.Len(t, f.Data.B, 50)
	assert.Len(t, f.Data.A, 50)
	asks, bids := b.Top(1)
	assert.Equal(t, bids[0].Price, f.Data.B[0][0], "bids best (highest) first")
	assert.Equal(t, asks[0].Price, f.Data.A[0][0], "asks best (lowest) first")
	assert.NotRegexp(t, `0$`, f.Data.A[0][1], "quantities without trailing zeros")
	assert.Greater(t, f.TS, f.CTS, "ts (gateway) is a few ms after cts (matching engine)")
	assert.Less(t, f.TS-f.CTS, int64(10))
}

// A delta carries only the changed levels; an unchanged side is [], never missing. u moves by
// exactly one; seq moves forward by a few (4…30 seen) — it is a counter shared across topics.
func TestDeltaCarriesTheChangesAndUMovesByOne(t *testing.T) {
	ex, b := newBybit(), book(t, 50)
	before := ex.seq(b.Seq())
	rng := rand.New(rand.NewSource(2))
	asks, _ := b.Requote(3, rng)
	prev, _ := b.Advance(ex.SeqStep(rng))
	raw := ex.Update("BTCUSDT", b, asks, []domain.Level{}, prev, fixed)
	assert.Contains(t, string(raw), `"b":[]`, "an unchanged side is []")

	var f frame
	require.NoError(t, json.Unmarshal(raw, &f))
	assert.Equal(t, "delta", f.Type)
	assert.Len(t, f.Data.A, 3)
	assert.Empty(t, f.Data.B)
	assert.Equal(t, prev+1, f.Data.U)
	assert.Greater(t, f.Data.Seq, before)
	assert.LessOrEqual(t, f.Data.Seq-before, int64(30))
}

func TestSeqStepIsOne(t *testing.T) {
	assert.Equal(t, int64(1), newBybit().SeqStep(nil))
}

// session records what the connection handler asked for.
type session struct {
	sent         []string
	subscribed   []string
	snapshots    []bool
	unsubscribed []string
}

func (s *session) ID() string         { return "c0e31519" }
func (s *session) Send(b []byte) bool { s.sent = append(s.sent, string(b)); return true }
func (s *session) Subscribe(m string, ack func(int64) []byte, snapshot bool) {
	s.subscribed = append(s.subscribed, m)
	s.snapshots = append(s.snapshots, snapshot)
	if ack != nil {
		s.Send(ack(0))
	}
}
func (s *session) Unsubscribe(m string, ack []byte) {
	s.unsubscribed = append(s.unsubscribed, m)
	if ack != nil {
		s.Send(ack)
	}
}
func (s *session) Close(int, string) {}

const connIDPattern = `[a-z0-9]{20}-[a-z0-9]{5}`

// NiFi sends {"op":"${status:replace('pending-', ”)}","args": ["orderbook.50.${market}"]}.
// Real bybit: one ack per message, then the snapshot; a repeat subscribe on the connection
// gets NO reply and no snapshot; an unsubscribe is acked, subscribed or not.
func TestSubscribeAndUnsubscribe(t *testing.T) {
	s := &session{}
	on := newBybit().Connect(s)
	on([]byte(`{"op":"subscribe","args": ["orderbook.50.BTCUSDT"]}`))
	on([]byte(`{"op":"subscribe","args": ["orderbook.50.BTCUSDT"]}`))
	on([]byte(`{"op":"subscribe","args":["orderbook.50.ETHUSDT"],"req_id":"x9"}`))
	on([]byte(`{"op":"unsubscribe","args":["orderbook.50.BTCUSDT"]}`))
	on([]byte(`{"op":"subscribe","args": ["orderbook.50.BTCUSDT"]}`))

	assert.Equal(t, []string{"BTCUSDT", "ETHUSDT", "BTCUSDT"}, s.subscribed, "the repeat is ignored until unsubscribed")
	assert.Equal(t, []bool{true, true, true}, s.snapshots, "a snapshot follows the ack")
	assert.Equal(t, []string{"BTCUSDT"}, s.unsubscribed)
	require.Len(t, s.sent, 4)
	assert.Regexp(t, `^\{"success":true,"ret_msg":"subscribe","conn_id":"`+connIDPattern+`","op":"subscribe"\}$`, s.sent[0])
	assert.Regexp(t, `^\{"success":true,"ret_msg":"subscribe","conn_id":"`+connIDPattern+`","req_id":"x9","op":"subscribe"\}$`, s.sent[1])
	assert.Regexp(t, `^\{"success":true,"ret_msg":"unsubscribe","conn_id":"`+connIDPattern+`","op":"unsubscribe"\}$`, s.sent[2])
	id := regexp.MustCompile(`"conn_id":"([^"]+)"`)
	assert.Equal(t, id.FindStringSubmatch(s.sent[0])[1], id.FindStringSubmatch(s.sent[2])[1], "one conn_id per connection")
}

// Seen live, all with the connection's conn_id. More than 10 args subscribes NOTHING.
func TestErrorsAndPing(t *testing.T) {
	eleven := `{"op":"subscribe","args":["orderbook.50.A1","orderbook.50.A2","orderbook.50.A3","orderbook.50.A4","orderbook.50.A5","orderbook.50.A6","orderbook.50.A7","orderbook.50.A8","orderbook.50.A9","orderbook.50.A10","orderbook.50.A11"]}`
	for raw, want := range map[string]string{
		`hello`:                        `^\{"type":"error","ret_code":"400","ret_msg":"Invalid JSON!"\}$`,
		`ping`:                         `^\{"type":"error","ret_code":"400","ret_msg":"Invalid JSON!"\}$`,
		`{"op":"ping"}`:                `^\{"success":true,"ret_msg":"pong","conn_id":"` + connIDPattern + `","op":"ping"\}$`,
		`{"op":"ping","req_id":"r1"}`:  `^\{"success":true,"ret_msg":"pong","conn_id":"` + connIDPattern + `","req_id":"r1","op":"ping"\}$`,
		eleven:                         `^\{"success":false,"ret_msg":"args size \\u003e10","conn_id":"` + connIDPattern + `","op":"subscribe"\}$`,
		`{"op":"subscribe","args":[]}`: `^\{"success":false,"ret_msg":"args required!","conn_id":"` + connIDPattern + `","op":"subscribe"\}$`,
		`{"op":"subscribe","args": ["orderbook.77.BTCUSDT"]}`: `^\{"success":false,"ret_msg":"Invalid topic :\[orderbook\.77\.BTCUSDT\]","conn_id":"` + connIDPattern + `","op":"subscribe"\}$`,
	} {
		s := &session{}
		newBybit().Connect(s)([]byte(raw))
		require.Len(t, s.sent, 1, raw)
		assert.Regexp(t, want, s.sent[0], raw)
		assert.Empty(t, s.subscribed, raw)
	}
	s := &session{}
	newBybit().Connect(s)([]byte(`{"op":"login"}`))
	assert.Empty(t, s.sent, "an unknown op gets no reply")

	s = &session{}
	ten := `{"op":"subscribe","args":["orderbook.50.A1","orderbook.50.A2","orderbook.50.A3","orderbook.50.A4","orderbook.50.A5","orderbook.50.A6","orderbook.50.A7","orderbook.50.A8","orderbook.50.A9","orderbook.50.A10"]}`
	newBybit().Connect(s)([]byte(ten))
	assert.Len(t, s.subscribed, 10, "10 args are fine")
	assert.Len(t, s.sent, 1, "one ack for the message")
}

type restBody struct {
	RetCode int             `json:"retCode"`
	RetMsg  string          `json:"retMsg"`
	Result  json.RawMessage `json:"result"`
	Time    int64           `json:"time"`
}

func rest(t *testing.T, query string) (restBody, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	newBybit().ServeREST(rec, httptest.NewRequest(http.MethodGet, "/v5/market/orderbook"+query, nil), books{book(t, 50)})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
	var body restBody
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body), rec.Body.String())
	return body, rec.Body.String()
}

// NiFi's snapshot branch (through the proxy): GET …/v5/market/orderbook?category=spot&symbol=${pair}&limit=50.
// Real: {"retCode":0,"retMsg":"OK","result":{"s","a","b","ts","u","seq","cts"},"retExtInfo":{},"time":…}.
// Its u is NOT the WS u (another stream's counter); lpa-core's parser ignores it.
func TestRestOrderbook(t *testing.T) {
	body, raw := rest(t, "?category=spot&symbol=BTCUSDT&limit=50")
	assert.Zero(t, body.RetCode)
	assert.Equal(t, "OK", body.RetMsg)
	assert.Regexp(t, `^\{"retCode":0,"retMsg":"OK","result":\{"s":"BTCUSDT","a":\[\[`, raw)
	assert.Regexp(t, `"ts":\d+,"u":\d+,"seq":\d+,"cts":\d+\},"retExtInfo":\{\},"time":\d+\}$`, raw)
	var r struct {
		A   [][]string `json:"a"`
		B   [][]string `json:"b"`
		U   int64      `json:"u"`
		CTS int64      `json:"cts"`
	}
	require.NoError(t, json.Unmarshal(body.Result, &r))
	assert.Len(t, r.A, 50)
	assert.Len(t, r.B, 50)
	assert.NotEqual(t, int64(346401778), r.U, "not the WS counter")
	assert.Equal(t, fixed.UnixMilli(), r.CTS)
	assert.Greater(t, body.Time, r.CTS)

	one, _ := rest(t, "?category=spot&symbol=BTCUSDT")
	require.NoError(t, json.Unmarshal(one.Result, &r))
	assert.Len(t, r.A, 1, "limit defaults to 1")
}

func TestRestErrors(t *testing.T) {
	for query, msg := range map[string]string{
		"?symbol=BTCUSDT&limit=50":  "Illegal category",
		"?category=spot&limit=50":   "Parameter symbol [string] missing!",
		"?category=linear&symbol=X": "Illegal category",
	} {
		body, raw := rest(t, query)
		assert.Equal(t, 10001, body.RetCode, query)
		assert.Equal(t, msg, body.RetMsg, query)
		assert.Contains(t, raw, `"result":{},"retExtInfo":{}`, query)
	}
}

// books answers every market with one fixed book.
type books struct{ b *domain.Book }

func (b books) Top(_ string, n int) ([]domain.Level, []domain.Level, int64) {
	asks, bids := b.b.Top(n)
	return asks, bids, b.b.Seq()
}

func TestEnvelopeIsNiFisReplaceText(t *testing.T) {
	got := newBybit().Envelope([]byte(`{"topic":"x"}`), "u-1", 1)
	assert.Equal(t, `{"id":"u-1","simulation":1,"topic":"x"}`, string(got))
}
