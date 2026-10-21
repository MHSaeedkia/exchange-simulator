package wallex

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"exchange-simulator/internal/domain"
)

var fixed = time.UnixMilli(1792609360731)

func book(t *testing.T, depth int) *domain.Book {
	t.Helper()
	shape, err := domain.NewShape("84356.47", "0.01", "0.00035000", "0.20000000")
	require.NoError(t, err)
	return domain.NewBook(shape, depth, 1, rand.New(rand.NewSource(1)))
}

// side decodes ["BTCUSDT@sellDepth",[{"price":…,"quantity":…,"sum":…},…]] keeping the
// numbers as written.
func side(t *testing.T, frame []byte) (string, []map[string]json.Number) {
	t.Helper()
	var raw []json.RawMessage
	require.NoError(t, json.Unmarshal(frame, &raw), string(frame))
	require.Len(t, raw, 2, "[channel, levels]: NiFi adds the third element")
	var ch string
	require.NoError(t, json.Unmarshal(raw[0], &ch))
	d := json.NewDecoder(bytes.NewReader(raw[1]))
	d.UseNumber()
	var levels []map[string]json.Number
	require.NoError(t, d.Decode(&levels))
	return ch, levels
}

// The frame must read like the one lpa-core's WallexParser takes and like real wallex
// (captured 2026-10-21): one side's whole book, 50 levels, numbers not strings, no trailing
// zeros, sum = price × quantity exactly.
func TestSellDepthIsTheWholeAskSide(t *testing.T) {
	b := book(t, 50)
	frame := New(Options{ID: 3}).Snapshot("BTCUSDT@sellDepth", b, fixed)
	assert.Regexp(t, `^\["BTCUSDT@sellDepth",\[\{"price":[0-9.]+,"quantity":[0-9.]+,"sum":[0-9.]+\},\{`, string(frame), "keys in wallex's order, no spaces")
	ch, levels := side(t, frame)
	assert.Equal(t, "BTCUSDT@sellDepth", ch)
	require.Len(t, levels, 50)
	asks, _ := b.Top(50)
	for i, l := range levels {
		assert.Equal(t, noTrailingZeros(asks[i].Price), l["price"].String(), "asks best (lowest) first")
		assert.Equal(t, noTrailingZeros(asks[i].Qty), l["quantity"].String(), "no trailing zeros")
		p, _ := strconv.ParseFloat(l["price"].String(), 64)
		q, _ := strconv.ParseFloat(l["quantity"].String(), 64)
		s, _ := strconv.ParseFloat(l["sum"].String(), 64)
		assert.InDelta(t, p*q, s, 1e-9)
	}
}

func TestBuyDepthIsTheWholeBidSide(t *testing.T) {
	ex, b := New(Options{ID: 3}), book(t, 50)
	rng := rand.New(rand.NewSource(2))
	asks, bids := b.Requote(5, rng)
	prev, _ := b.Advance(ex.SeqStep(rng))
	ch, levels := side(t, ex.Update("BTCUSDT@buyDepth", b, asks, bids, prev, fixed))
	assert.Equal(t, "BTCUSDT@buyDepth", ch)
	require.Len(t, levels, 50, "every frame is the whole side, not the changed levels")
	_, top := b.Top(1)
	assert.Equal(t, noTrailingZeros(top[0].Price), levels[0]["price"].String(), "bids best (highest) first")
}

func TestSumIsExact(t *testing.T) {
	assert.Equal(t, "30.1118855312", sum("84356.47", "0.00035696"))
	assert.Equal(t, "168.71296", sum("84356.48", "0.00200000"))
	assert.Equal(t, "219.46873", sum("84411.05", "0.0026"))
	assert.Equal(t, "0", sum("84356.47", "0"))
	// Too big for int64 units: still exact.
	assert.Equal(t, "4217823500", sum("84356.47000000", "50000.00000000"))
}

// NiFi's ReplaceText on wallex: `\]\s*$` → `, {"simulation": 0, "id": "${uuid}"} ]`.
func TestEnvelopeIsNiFisReplaceText(t *testing.T) {
	got := New(Options{ID: 3}).Envelope([]byte(`["BTCUSDT@buyDepth",[{"price":1,"quantity":2,"sum":2}]]`), "u-1", 1)
	assert.Equal(t, `["BTCUSDT@buyDepth",[{"price":1,"quantity":2,"sum":2}], {"simulation": 1, "id": "u-1"} ]`, string(got))
}

func TestKafkaModeSendsBothSidesOfADBMarket(t *testing.T) {
	assert.Equal(t, []string{"BTCUSDT@buyDepth", "BTCUSDT@sellDepth"}, New(Options{ID: 3}).Channels("BTCUSDT"))
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
func (s *session) Subscribe(m string, ack []byte, snapshot bool) {
	s.subscribed = append(s.subscribed, m)
	s.snapshots = append(s.snapshots, snapshot)
	if ack != nil {
		s.Send(ack)
	}
}
func (s *session) Unsubscribe(m string, ack []byte) {
	s.unsubscribed = append(s.unsubscribed, m)
	if ack != nil {
		s.Send(ack)
	}
}

// Real wallex speaks first: {"sid":"<uuid>","upgrades":[],"pingInterval":20000,"pingTimeout":30000,"maxPayload":1048576}.
func TestConnectSendsTheOpenMessage(t *testing.T) {
	s := &session{}
	New(Options{ID: 3}).Connect(s)
	require.Len(t, s.sent, 1)
	assert.Regexp(t, `^\{"sid":"[0-9a-f-]{36}","upgrades":\[\],"pingInterval":20000,"pingTimeout":30000,"maxPayload":1048576\}$`, s.sent[0])
}

// NiFi sends ["${status:replace('pending-', ”)}", {"channel": "${market}@buyDepth"}] and the
// same for @sellDepth. Real wallex answers nothing: no ack, no snapshot; data simply starts or
// stops, per channel.
func TestSubscribeAndUnsubscribePerChannelWithNoReply(t *testing.T) {
	s := &session{}
	on := New(Options{ID: 3}).Connect(s)
	s.sent = nil
	on([]byte(`["subscribe", {"channel": "BTCUSDT@buyDepth"}]`))
	on([]byte(`["subscribe", {"channel": "BTCUSDT@sellDepth"}]`))
	on([]byte(`["unsubscribe", {"channel": "BTCUSDT@sellDepth"}]`))

	assert.Equal(t, []string{"BTCUSDT@buyDepth", "BTCUSDT@sellDepth"}, s.subscribed)
	assert.Equal(t, []bool{false, false}, s.snapshots)
	assert.Equal(t, []string{"BTCUSDT@sellDepth"}, s.unsubscribed)
	assert.Empty(t, s.sent, "wallex never replies")
}

// Seen live: bad text and channels that are not <market>@buyDepth|sellDepth get nothing.
func TestBadMessagesAreIgnored(t *testing.T) {
	s := &session{}
	on := New(Options{ID: 3}).Connect(s)
	s.sent = nil
	for _, raw := range []string{`hello`, `["subscribe", {"channel": "BTCUSDT"}]`,
		`["subscribe", {"channel": "BTCUSDT@trades"}]`, `["subscribe", {"channel": "@buyDepth"}]`, `["ping"]`, `{}`} {
		on([]byte(raw))
	}
	assert.Empty(t, s.sent)
	assert.Empty(t, s.subscribed)
}

// Measured on real wallex (lpa-core, 2026-10-03): a connection takes 51 subscribe requests in
// its whole life; #52 and later are ignored with no reply. Repeats count, unsubscribes do not
// count and give no place back, bad text does not count.
func TestOnly51SubscribesPerConnection(t *testing.T) {
	ex := New(Options{ID: 3})
	s := &session{}
	on := ex.Connect(s)
	for range 49 {
		on([]byte(`["subscribe", {"channel": "A@buyDepth"}]`))
	}
	for range 30 {
		on([]byte(`["unsubscribe", {"channel": "A@buyDepth"}]`))
		on([]byte(`hello`))
	}
	on([]byte(`["subscribe", {"channel": "B@buyDepth"}]`))
	on([]byte(`["subscribe", {"channel": "C@buyDepth"}]`))
	on([]byte(`["subscribe", {"channel": "D@buyDepth"}]`))
	assert.Len(t, s.subscribed, 51)
	assert.Equal(t, "C@buyDepth", s.subscribed[50], "#51 is taken")
	assert.NotContains(t, s.subscribed, "D@buyDepth", "#52 is ignored")
	assert.Len(t, s.unsubscribed, 30, "unsubscribes still work")

	fresh := &session{}
	ex.Connect(fresh)([]byte(`["subscribe", {"channel": "D@buyDepth"}]`))
	assert.Equal(t, []string{"D@buyDepth"}, fresh.subscribed, "per connection, not per exchange")
}

// Real wallex: a control PING ("ping") every 20 s; closed (1000) with no PONG for 30 s, and
// closed at 30 min whatever happens.
func TestKeepalive(t *testing.T) {
	k := New(Options{ID: 3}).Keepalive()
	assert.Equal(t, 20*time.Second, k.PingEvery)
	assert.Equal(t, "ping", string(k.PingPayload))
	assert.Equal(t, 30*time.Second, k.PongTimeout)
	assert.Equal(t, 30*time.Minute, k.MaxLife)
}

func TestNoRest(t *testing.T) {
	_, ok := New(Options{ID: 3}).REST()
	assert.False(t, ok)
}

func noTrailingZeros(s string) string {
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	return s
}
