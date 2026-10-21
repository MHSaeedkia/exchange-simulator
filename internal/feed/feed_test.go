package feed

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"exchange-simulator/internal/domain"
	"exchange-simulator/internal/exchange/okx"
)

// sub is a Subscriber that keeps everything it is sent.
type sub struct {
	id string
	ch chan []byte
}

func newSub(id string) *sub       { return &sub{id: id, ch: make(chan []byte, 100000)} }
func (s *sub) ID() string         { return s.id }
func (s *sub) Send(b []byte) bool { s.ch <- b; return true }
func (s *sub) drain() (out []frame) {
	for {
		select {
		case b := <-s.ch:
			out = append(out, decode(b))
		default:
			return out
		}
	}
}

// pub is a Publisher that keeps everything it is sent.
type pub struct {
	mu     sync.Mutex
	frames []frame
}

func (p *pub) Publish(b []byte) { p.mu.Lock(); p.frames = append(p.frames, decode(b)); p.mu.Unlock() }
func (p *pub) all() []frame     { p.mu.Lock(); defer p.mu.Unlock(); return append([]frame{}, p.frames...) }

// frame is a decoded push or event.
type frame struct {
	Event  string `json:"event"`
	Action string `json:"action"`
	Arg    struct {
		InstID string `json:"instId"`
	} `json:"arg"`
	Data []okx.Data `json:"data"`
}

func decode(b []byte) frame {
	var f frame
	_ = json.Unmarshal(b, &f)
	return f
}

func options(t *testing.T, s domain.Settings) Options {
	t.Helper()
	shape, err := domain.NewShape("100", "1", "0.1", "9.9")
	require.NoError(t, err)
	return Options{
		Encoder: okx.New(okx.Options{ID: 8, StepMin: 1, StepMax: 5}),
		Shape:   shape, Settings: s, SeqStart: 1000, Tick: time.Millisecond, Running: true, Seed: 1,
	}
}

func start(t *testing.T, f *Feed) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go f.Run(ctx)
}

// chained asserts every update of one market names the frame before it.
func chained(t *testing.T, frames []frame, instID string) {
	t.Helper()
	last := int64(-1)
	for _, f := range frames {
		if f.Arg.InstID != instID || f.Event != "" {
			continue
		}
		d := f.Data[0]
		if f.Action == okx.ActionSnapshot {
			assert.Equal(t, int64(-1), d.PrevSeqID)
		} else {
			require.NotEqual(t, int64(-1), last, "an update before any snapshot")
			assert.Equal(t, last, d.PrevSeqID, "chain broken")
		}
		assert.Greater(t, d.SeqID, last)
		last = d.SeqID
	}
}

func TestNothingIsSentWithoutASubscriber(t *testing.T) {
	f := New(options(t, domain.Settings{Records: 100, PeriodMS: 10, BookDepth: 5, UpdateLevels: 2}))
	start(t, f)
	time.Sleep(50 * time.Millisecond)
	assert.Zero(t, f.Counters().Updates)
	assert.Zero(t, f.Counters().ActiveMillis, "idle time is not active time")
}

func TestSubscribeSendsAckThenSnapshotThenUpdates(t *testing.T) {
	f := New(options(t, domain.Settings{Records: 10, PeriodMS: 10, BookDepth: 5, UpdateLevels: 2}))
	start(t, f)
	s := newSub("c1")

	f.Subscribe(s, "BTC-USDT", okx.Ack("", okx.OpSubscribe, okx.Arg{Channel: okx.Channel, InstID: "BTC-USDT"}, "c1"), true)
	time.Sleep(60 * time.Millisecond)
	got := s.drain()

	require.Greater(t, len(got), 3)
	assert.Equal(t, "subscribe", got[0].Event)
	assert.Equal(t, okx.ActionSnapshot, got[1].Action)
	assert.Len(t, got[1].Data[0].Asks, 5)
	for _, fr := range got[2:] {
		assert.Equal(t, okx.ActionUpdate, fr.Action)
		assert.Len(t, fr.Data[0].Asks, 2)
		assert.Len(t, fr.Data[0].Bids, 2)
	}
	chained(t, got, "BTC-USDT")
}

// An exchange whose every push is the whole book (nobitex) sends a new subscriber no
// snapshot: one at the current number would repeat the last push's number, and the
// pipeline drops that as stale_or_duplicate.
func TestSubscribeWithoutSnapshotSendsAckThenUpdates(t *testing.T) {
	f := New(options(t, domain.Settings{Records: 10, PeriodMS: 10, BookDepth: 5, UpdateLevels: 2}))
	start(t, f)
	s := newSub("c1")

	f.Subscribe(s, "BTC-USDT", []byte(`{"event":"subscribe"}`), false)
	time.Sleep(60 * time.Millisecond)
	got := s.drain()

	require.Greater(t, len(got), 2)
	assert.Equal(t, "subscribe", got[0].Event)
	for _, fr := range got[1:] {
		assert.Equal(t, okx.ActionUpdate, fr.Action)
	}
	assert.Zero(t, f.Counters().Snapshots)
}

// An exchange that never answers (wallex) passes a nil ack: nothing is sent for it.
func TestNilAckSendsNothing(t *testing.T) {
	f := New(options(t, domain.Settings{Records: 10, PeriodMS: 10, BookDepth: 5, UpdateLevels: 2}))
	start(t, f)
	s := newSub("c1")
	f.Subscribe(s, "BTC-USDT", nil, false)
	time.Sleep(40 * time.Millisecond)
	f.Unsubscribe(s, "BTC-USDT", nil)
	got := s.drain()
	require.NotEmpty(t, got)
	for _, fr := range got {
		assert.Equal(t, okx.ActionUpdate, fr.Action, "no empty frame for the missing acks")
	}
}

func TestUnsubscribeStopsTheMarket(t *testing.T) {
	f := New(options(t, domain.Settings{Records: 10, PeriodMS: 10, BookDepth: 5, UpdateLevels: 1}))
	start(t, f)
	s := newSub("c1")
	f.Subscribe(s, "BTC-USDT", []byte(`{"event":"subscribe"}`), true)
	time.Sleep(30 * time.Millisecond)

	f.Unsubscribe(s, "BTC-USDT", []byte(`{"event":"unsubscribe"}`))
	got := s.drain()
	assert.Equal(t, "unsubscribe", got[len(got)-1].Event, "the ack is the last thing sent")

	time.Sleep(30 * time.Millisecond)
	assert.Empty(t, s.drain())
	assert.Zero(t, f.ActiveMarkets())
}

func TestALateSubscriberJoinsTheSameChain(t *testing.T) {
	f := New(options(t, domain.Settings{Records: 20, PeriodMS: 10, BookDepth: 5, UpdateLevels: 1}))
	start(t, f)
	early, late := newSub("early"), newSub("late")
	f.Subscribe(early, "BTC-USDT", []byte(`{"event":"subscribe"}`), true)
	time.Sleep(30 * time.Millisecond)

	f.Subscribe(late, "BTC-USDT", []byte(`{"event":"subscribe"}`), true)
	time.Sleep(30 * time.Millisecond)

	got := late.drain()
	chained(t, got, "BTC-USDT")
	assert.Greater(t, got[1].Data[0].SeqID, int64(1000), "the late snapshot carries the current seqId, not the start")
	chained(t, early.drain(), "BTC-USDT")
}

func TestRateIsSharedByAllMarkets(t *testing.T) {
	f := New(options(t, domain.Settings{Records: 50, PeriodMS: 100, BookDepth: 5, UpdateLevels: 1}))
	start(t, f)
	s := newSub("c1")
	for _, m := range []string{"A-USDT", "B-USDT"} {
		f.Subscribe(s, m, []byte(`{"event":"subscribe"}`), true)
	}

	time.Sleep(time.Second)
	n := f.Counters().Updates
	// 500 per second in total, not per market. Wide bounds: the CI clock is not exact.
	assert.InDelta(t, 500, n, 100)

	per := map[string]int{}
	for _, fr := range s.drain() {
		if fr.Action == okx.ActionUpdate {
			per[fr.Arg.InstID]++
		}
	}
	assert.InDelta(t, per["A-USDT"], per["B-USDT"], 1, "round robin")
}

func TestApplyNewDepthSendsAFreshSnapshotThatMovesForward(t *testing.T) {
	f := New(options(t, domain.Settings{Records: 10, PeriodMS: 10, BookDepth: 5, UpdateLevels: 1}))
	start(t, f)
	s := newSub("c1")
	f.Subscribe(s, "BTC-USDT", []byte(`{"event":"subscribe"}`), true)
	time.Sleep(30 * time.Millisecond)

	require.NoError(t, f.Apply(domain.Settings{Records: 10, PeriodMS: 10, BookDepth: 8, UpdateLevels: 3}))
	time.Sleep(30 * time.Millisecond)

	got := s.drain()
	chained(t, got, "BTC-USDT")
	var snaps []frame
	for _, fr := range got {
		if fr.Action == okx.ActionSnapshot {
			snaps = append(snaps, fr)
		}
	}
	require.Len(t, snaps, 2)
	assert.Len(t, snaps[1].Data[0].Asks, 8)
	assert.Equal(t, 3, len(got[len(got)-1].Data[0].Asks), "updates use the new size")
	assert.Equal(t, 8, f.Settings().BookDepth)
}

func TestApplyRejectsBadSettings(t *testing.T) {
	f := New(options(t, domain.Settings{Records: 10, PeriodMS: 10, BookDepth: 5, UpdateLevels: 1}))
	start(t, f)
	assert.Error(t, f.Apply(domain.Settings{Records: 10, PeriodMS: 10, BookDepth: 500, UpdateLevels: 1}), "base 100 / step 1 allows 99")
	assert.Equal(t, 5, f.Settings().BookDepth, "nothing changed")
}

func TestKafkaModeSendsTheDBMarkets(t *testing.T) {
	p := &pub{}
	opts := options(t, domain.Settings{Records: 20, PeriodMS: 10, BookDepth: 5, UpdateLevels: 1})
	opts.Publisher = p
	f := New(opts)
	start(t, f)

	f.SetMarkets([]string{"BTC-USDT", "ETH-USDT"})
	time.Sleep(30 * time.Millisecond)
	f.SetMarkets([]string{"ETH-USDT"})
	before := len(p.all())
	time.Sleep(30 * time.Millisecond)

	all := p.all()
	assert.Equal(t, okx.ActionSnapshot, all[0].Action)
	assert.Equal(t, okx.ActionSnapshot, all[1].Action)
	chained(t, all, "BTC-USDT")
	chained(t, all, "ETH-USDT")
	for _, fr := range all[before:] {
		assert.Equal(t, "ETH-USDT", fr.Arg.InstID, "BTC-USDT left the list")
	}
}

func TestTopAnswersTheCurrentBook(t *testing.T) {
	f := New(options(t, domain.Settings{Records: 10, PeriodMS: 10, BookDepth: 5, UpdateLevels: 1}))
	start(t, f)

	asks, bids, seq := f.Top("ZEC-USDT", 3)
	assert.Len(t, asks, 3)
	assert.Len(t, bids, 3)
	assert.Equal(t, int64(1000), seq)
	assert.Equal(t, int64(1), f.Counters().RestCalls)
	assert.Zero(t, f.ActiveMarkets(), "a REST call does not start a market")
}

func TestAStoppedFeedAcksButSendsNoData(t *testing.T) {
	opts := options(t, domain.Settings{Records: 20, PeriodMS: 10, BookDepth: 5, UpdateLevels: 1})
	opts.Running = false
	f := New(opts)
	start(t, f)
	s := newSub("c1")

	f.Subscribe(s, "BTC-USDT", []byte(`{"event":"subscribe"}`), true)
	time.Sleep(40 * time.Millisecond)
	got := s.drain()
	require.Len(t, got, 1, "only the ack")
	assert.Equal(t, "subscribe", got[0].Event)
	assert.Zero(t, f.Counters().Updates)
	assert.Zero(t, f.Counters().ActiveMillis, "a stopped feed is not active")
	assert.False(t, f.Status().Running)
}

func TestStartSendsASnapshotFirstThenUpdates(t *testing.T) {
	opts := options(t, domain.Settings{Records: 20, PeriodMS: 10, BookDepth: 5, UpdateLevels: 1})
	opts.Running = false
	f := New(opts)
	start(t, f)
	s := newSub("c1")
	f.Subscribe(s, "BTC-USDT", []byte(`{"event":"subscribe"}`), true)
	s.drain()

	f.Start()
	time.Sleep(40 * time.Millisecond)
	got := s.drain()
	require.Greater(t, len(got), 2)
	assert.Equal(t, domain.Settings{Records: 20, PeriodMS: 10, BookDepth: 5, UpdateLevels: 1}, f.Settings())
	assert.Equal(t, okx.ActionSnapshot, got[0].Action, "subscribers got nothing while stopped")
	chained(t, got, "BTC-USDT")
	assert.True(t, f.Running())
}

func TestStopMakesItSilentAndStartChainsOn(t *testing.T) {
	f := New(options(t, domain.Settings{Records: 20, PeriodMS: 10, BookDepth: 5, UpdateLevels: 1}))
	start(t, f)
	s := newSub("c1")
	f.Subscribe(s, "BTC-USDT", []byte(`{"event":"subscribe"}`), true)
	time.Sleep(30 * time.Millisecond)

	f.Stop()
	before := s.drain()
	time.Sleep(30 * time.Millisecond)
	assert.Empty(t, s.drain(), "nothing while stopped")
	assert.False(t, f.Running())

	f.Start()
	time.Sleep(30 * time.Millisecond)
	chained(t, append(before, s.drain()...), "BTC-USDT")
}

func TestKafkaModeWhileStoppedWaitsForStart(t *testing.T) {
	p := &pub{}
	opts := options(t, domain.Settings{Records: 20, PeriodMS: 10, BookDepth: 5, UpdateLevels: 1})
	opts.Publisher, opts.Running = p, false
	f := New(opts)
	start(t, f)

	f.SetMarkets([]string{"BTC-USDT"})
	time.Sleep(30 * time.Millisecond)
	assert.Empty(t, p.all())

	f.Start()
	time.Sleep(30 * time.Millisecond)
	all := p.all()
	require.NotEmpty(t, all)
	assert.Equal(t, okx.ActionSnapshot, all[0].Action)
	chained(t, all, "BTC-USDT")
}

func TestDue(t *testing.T) {
	assert.Equal(t, int64(5), due(50*time.Millisecond, 10, 100*time.Millisecond, 0))
	assert.Equal(t, int64(2), due(50*time.Millisecond, 10, 100*time.Millisecond, 3))
	assert.Equal(t, int64(0), due(time.Millisecond, 10, time.Second, 0))
}

// emit is driven by hand here: no Run, no clock.
func TestEmitCountsMissedWhenMoreThanAPeriodBehind(t *testing.T) {
	f := New(options(t, domain.Settings{Records: 10, PeriodMS: 100, BookDepth: 5, UpdateLevels: 1}))
	s := newSub("c1")
	f.subs["BTC-USDT"] = map[string]domain.Subscriber{s.id: s}
	f.refreshActive()
	t0 := time.Now()
	f.rebase(t0)

	f.emit(t0.Add(50 * time.Millisecond))
	assert.Equal(t, int64(5), f.Counters().Updates)

	f.emit(t0.Add(300 * time.Millisecond)) // 30 due by now, 5 sent: 25 behind
	assert.Equal(t, int64(15), f.Counters().Updates, "one period's worth is sent")
	assert.Equal(t, int64(15), f.Counters().Missed, "the rest is counted, not burst out")
}

func TestEmitSlidesTheScheduleEveryPeriod(t *testing.T) {
	f := New(options(t, domain.Settings{Records: 10, PeriodMS: 100, BookDepth: 5, UpdateLevels: 1}))
	s := newSub("c1")
	f.subs["BTC-USDT"] = map[string]domain.Subscriber{s.id: s}
	f.refreshActive()
	t0 := time.Now()
	f.rebase(t0)

	for i := 1; i <= 100; i++ {
		f.emit(t0.Add(time.Duration(i) * 10 * time.Millisecond))
	}
	assert.Equal(t, int64(100), f.Counters().Updates, "10 per 100 ms for 1 s")
	assert.Zero(t, f.Counters().Missed)
	assert.Equal(t, int64(990), f.Counters().ActiveMillis, "from the first tick to the last")
}
