package bitget

import (
	"encoding/json"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"exchange-simulator/internal/domain"
)

var fixed = time.UnixMilli(1792612005301)

func book(t *testing.T, depth int) *domain.Book {
	t.Helper()
	shape, err := domain.NewShape("84550.84", "0.01", "0.000001", "0.600000")
	require.NoError(t, err)
	return domain.NewBook(shape, depth, 941001660936, rand.New(rand.NewSource(1)))
}

func newBitget() *Exchange { return New(Options{ID: 5, StepMin: 2000, StepMax: 12000}) }

type frame struct {
	Action string `json:"action"`
	Arg    Arg    `json:"arg"`
	Data   []struct {
		Asks [][]string `json:"asks"`
		Bids [][]string `json:"bids"`
		TS   string     `json:"ts"`
		Seq  int64      `json:"seq"`
		Pseq int64      `json:"pseq"`
	} `json:"data"`
	TS int64 `json:"ts"`
}

// The frame must read like the books50 frame lpa-core's BitgetParser takes and like real
// bitget (captured 2026-10-21): {"action":"snapshot","arg":{"instType":"SPOT","channel":"books50",
// "instId":"BTCUSDT"},"data":[{"asks":[["84550.84","0.587544"],…],"bids":[…],"ts":"1792612005301",
// "seq":941001660936,"pseq":0}],"ts":1792612005303} — the whole book on every frame.
func TestFrameHasBooks50Shape(t *testing.T) {
	b := book(t, 50)
	raw := newBitget().Snapshot("BTCUSDT", b, fixed)
	assert.Regexp(t, `^\{"action":"snapshot","arg":\{"instType":"SPOT","channel":"books50","instId":"BTCUSDT"\},"data":\[\{"asks":\[\["`, string(raw))
	assert.Regexp(t, `\]\],"ts":"1792612005301","seq":941001660936,"pseq":0\}\],"ts":1792612005303\}$`, string(raw),
		"inner ts a string, outer ts a number a little later, pseq always 0")

	var f frame
	require.NoError(t, json.Unmarshal(raw, &f))
	require.Len(t, f.Data, 1)
	d := f.Data[0]
	assert.Len(t, d.Asks, 50)
	assert.Len(t, d.Bids, 50)
	asks, bids := b.Top(1)
	assert.Equal(t, asks[0].Price, d.Asks[0][0], "asks best (lowest) first")
	assert.Equal(t, bids[0].Price, d.Bids[0][0], "bids best (highest) first")
	q, err := strconv.ParseFloat(d.Asks[0][1], 64)
	require.NoError(t, err)
	assert.Positive(t, q)
	assert.NotRegexp(t, `0"$`, `"`+d.Asks[0][1]+`"`, "quantities without trailing zeros, as bitget writes them")
}

// Every books50 frame is a whole-book snapshot; seq only moves forward, by thousands (a book
// counter sampled every ~100 ms: steps 2816…11122 seen).
func TestUpdateIsAWholeSnapshotWithALaterSeq(t *testing.T) {
	ex, b := newBitget(), book(t, 50)
	rng := rand.New(rand.NewSource(2))
	asks, bids := b.Requote(3, rng)
	prev, _ := b.Advance(ex.SeqStep(rng))
	var f frame
	require.NoError(t, json.Unmarshal(ex.Update("BTCUSDT", b, asks, bids, prev, fixed), &f))
	assert.Equal(t, "snapshot", f.Action, "books50 sends nothing but snapshots")
	assert.Len(t, f.Data[0].Asks, 50)
	assert.Greater(t, f.Data[0].Seq, prev)
	assert.Zero(t, f.Data[0].Pseq)
}

func TestSeqStepStaysInRange(t *testing.T) {
	ex, rng := newBitget(), rand.New(rand.NewSource(1))
	for range 1000 {
		s := ex.SeqStep(rng)
		assert.GreaterOrEqual(t, s, int64(2000))
		assert.LessOrEqual(t, s, int64(12000))
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
func (s *session) Subscribe(m string, ack func(int64) []byte, snapshot bool) {
	s.subscribed = append(s.subscribed, m)
	s.snapshots = append(s.snapshots, snapshot)
	s.Send(ack(0))
}
func (s *session) Unsubscribe(m string, ack []byte) {
	s.unsubscribed = append(s.unsubscribed, m)
	s.Send(ack)
}
func (s *session) Close(int, string) {}

// NiFi sends {"op":"${status:replace('pending-', ”)}","args":[{"channel":"books50","instType":"SPOT","instId":"${market}"}]}.
// Real bitget acks each arg (arg keys in its own order) and sends no extra snapshot: the next
// frame comes on the normal beat. A repeat subscribe is acked again; so is an unsubscribe of
// a market not subscribed.
func TestSubscribeAndUnsubscribe(t *testing.T) {
	s := &session{}
	ex := newBitget()
	ex.OnMessage(s, []byte(`{"op":"subscribe","args":[{"channel":"books50","instType":"SPOT","instId":"BTCUSDT"}]}`))
	ex.OnMessage(s, []byte(`{"op":"subscribe","args":[{"channel":"books50","instType":"SPOT","instId":"BTCUSDT"}]}`))
	ex.OnMessage(s, []byte(`{"op":"unsubscribe","args":[{"channel":"books50","instType":"SPOT","instId":"ETHUSDT"}]}`))

	assert.Equal(t, []string{"BTCUSDT", "BTCUSDT"}, s.subscribed)
	assert.Equal(t, []bool{false, false}, s.snapshots)
	assert.Equal(t, []string{"ETHUSDT"}, s.unsubscribed)
	require.Len(t, s.sent, 3)
	assert.Equal(t, `{"event":"subscribe","arg":{"instType":"SPOT","channel":"books50","instId":"BTCUSDT"}}`, s.sent[0])
	assert.Equal(t, `{"event":"unsubscribe","arg":{"instType":"SPOT","channel":"books50","instId":"ETHUSDT"}}`, s.sent[2])
}

// Seen live.
func TestErrorsAndPing(t *testing.T) {
	for raw, want := range map[string]string{
		`ping`:           `pong`,
		`hello`:          `{"event":"error","code":30002,"msg":"Unrecognized request:hello"}`,
		`{"op":"login"}`: `{"event":"error","code":30002,"msg":"Unrecognized request:{\"op\":\"login\"}"}`,
		`{"op":"subscribe","args":[{"channel":"books99","instType":"SPOT","instId":"BTCUSDT"}]}`: `{"event":"error","arg":{"instType":"SPOT","channel":"books99","instId":"BTCUSDT"},"code":30016,"msg":"Param error","op":"subscribe"}`,
	} {
		s := &session{}
		newBitget().OnMessage(s, []byte(raw))
		assert.Equal(t, []string{want}, s.sent, raw)
		assert.Empty(t, s.subscribed, raw)
	}
	s := &session{}
	newBitget().OnMessage(s, []byte(`{"op":"subscribe","args":[]}`))
	assert.Empty(t, s.sent, "empty args: no reply")
}

// Real bitget drops (no close frame) a client that sent no "ping" for 2 minutes; other
// messages do not count. Measured: cut 126–145 s after the last ping.
func TestIdle(t *testing.T) {
	i := newBitget().Idle()
	assert.Equal(t, 120*time.Second, i.Limit)
	assert.Equal(t, 30*time.Second, i.Check)
	assert.True(t, i.IsAlive([]byte("ping")))
	assert.False(t, i.IsAlive([]byte(`{"op":"subscribe","args":[]}`)))
}

func TestNoRest(t *testing.T) {
	_, ok := newBitget().REST()
	assert.False(t, ok)
}

func TestEnvelopeIsNiFisReplaceText(t *testing.T) {
	got := newBitget().Envelope([]byte(`{"action":"snapshot"}`), "u-1", 1)
	assert.Equal(t, `{"id":"u-1","simulation":1,"action":"snapshot"}`, string(got))
}
