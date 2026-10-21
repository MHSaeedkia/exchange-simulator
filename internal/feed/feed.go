// Package feed is the generator of ONE exchange. One goroutine owns every book of the
// exchange and sends every frame, so a market's frames always leave in sequence order — a
// snapshot can never overtake an older update. Other goroutines (WebSocket connections,
// the REST handler, the control API, the DB refresh) talk to it through commands that run
// on that same goroutine. Each exchange has its own Feed, so each has its own rate.
//
// Sending blocks: when a WebSocket client or Kafka cannot keep up, the generator waits,
// and the frames it could not send on time are counted as "missed". That is the
// measurement — the tool never drops a frame from the middle of a chain.
//
// A stopped feed is silent: subscriptions are still taken and acked, but no snapshot and
// no update is sent until it is started again.
package feed

import (
	"context"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"exchange-simulator/internal/domain"
)

// Encoder is the exchange, as the generator needs it: its frames and how its sequence
// number moves.
type Encoder interface {
	SeqStep(rng *rand.Rand) int64
	Snapshot(market string, b *domain.Book, now time.Time) []byte
	Update(market string, b *domain.Book, asks, bids []domain.Level, prevSeq int64, now time.Time) []byte
}

// Publisher takes frames in kafka mode.
type Publisher interface {
	Publish(frame []byte)
}

// Options configure a Feed.
type Options struct {
	Encoder  Encoder
	Shape    domain.Shape
	Settings domain.Settings
	SeqStart int64
	Tick     time.Duration
	// Running is whether the feed sends market data from the start.
	Running bool
	// Publisher set = kafka mode: every active market's frames go to it.
	// Nil = websocket mode: frames go to the market's subscribers.
	Publisher Publisher
	// Seed for the random quantities, levels and sequence steps.
	Seed int64
}

// Status is a copy of the generator's state.
type Status struct {
	Running       bool            `json:"running"`
	Settings      domain.Settings `json:"settings"`
	Markets       []string        `json:"markets"`
	Subscriptions int             `json:"subscriptions"`
}

// Feed is the generator. Create it with New and start it with Run.
type Feed struct {
	opts    Options
	cmds    chan func()
	stopped chan struct{}

	// Owned by Run's goroutine.
	settings     domain.Settings
	running      bool
	books        map[string]*domain.Book
	subs         map[string]map[string]domain.Subscriber // market → subscriber id → subscriber
	kafkaMarkets map[string]bool
	active       []string // markets that get updates, sorted
	next         int      // round-robin position in active
	base         time.Time
	emitted      int64 // updates sent since base
	lastTick     time.Time
	rng          *rand.Rand

	// Readable from any goroutine.
	current                                                       atomic.Pointer[domain.Settings]
	isRunning                                                     atomic.Bool
	activeCount                                                   atomic.Int64
	updates, snapshots, deliveries, missed, sendErrors, restCalls atomic.Int64
	activeNanos                                                   atomic.Int64
}

// New builds a Feed with no markets.
func New(opts Options) *Feed {
	f := &Feed{
		opts:         opts,
		cmds:         make(chan func()),
		stopped:      make(chan struct{}),
		settings:     opts.Settings,
		running:      opts.Running,
		books:        map[string]*domain.Book{},
		subs:         map[string]map[string]domain.Subscriber{},
		kafkaMarkets: map[string]bool{},
		rng:          rand.New(rand.NewSource(opts.Seed)),
	}
	s := opts.Settings
	f.current.Store(&s)
	f.isRunning.Store(opts.Running)
	return f
}

// Run sends updates until ctx ends.
func (f *Feed) Run(ctx context.Context) {
	defer close(f.stopped)
	ticker := time.NewTicker(f.opts.Tick)
	defer ticker.Stop()
	f.base = time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case fn := <-f.cmds:
			fn()
		case now := <-ticker.C:
			f.emit(now)
		}
	}
}

// do runs fn on Run's goroutine and waits for it. After Run has stopped it does nothing.
func (f *Feed) do(fn func()) {
	done := make(chan struct{})
	select {
	case f.cmds <- func() { fn(); close(done) }:
		<-done
	case <-f.stopped:
	}
}

// Subscribe sends ack, then (when running) a snapshot of the market's current book, then
// its updates — what a real exchange does on a subscribe.
func (f *Feed) Subscribe(s domain.Subscriber, market string, ack []byte) {
	f.do(func() {
		if !s.Send(ack) {
			return
		}
		if f.subs[market] == nil {
			f.subs[market] = map[string]domain.Subscriber{}
		}
		f.subs[market][s.ID()] = s
		if f.running {
			f.snapshotTo(market, []domain.Subscriber{s})
		}
		f.refreshActive()
	})
}

// Unsubscribe stops a market's frames on one connection and sends ack.
func (f *Feed) Unsubscribe(s domain.Subscriber, market string, ack []byte) {
	f.do(func() {
		delete(f.subs[market], s.ID())
		f.refreshActive()
		s.Send(ack)
	})
}

// Drop forgets a closed connection.
func (f *Feed) Drop(s domain.Subscriber) {
	f.do(func() {
		for _, subs := range f.subs {
			delete(subs, s.ID())
		}
		f.refreshActive()
	})
}

// SetMarkets is kafka mode's market list. A new market gets a snapshot first (when
// running), a market that left the list stops.
func (f *Feed) SetMarkets(ids []string) {
	f.do(func() {
		want := map[string]bool{}
		for _, id := range ids {
			want[id] = true
			if !f.kafkaMarkets[id] && f.running {
				f.snapshotTo(id, nil)
			}
		}
		f.kafkaMarkets = want
		f.refreshActive()
	})
}

// Start makes a stopped feed send again. Every active market gets a fresh snapshot first,
// since its subscribers got nothing (or stale data) while it was stopped.
func (f *Feed) Start() {
	f.do(func() {
		if f.running {
			return
		}
		f.running = true
		f.isRunning.Store(true)
		f.rebase(time.Now())
		for _, id := range f.active {
			f.snapshotTo(id, nil)
		}
	})
}

// Stop makes the feed silent. Connections and subscriptions stay.
func (f *Feed) Stop() {
	f.do(func() {
		f.running = false
		f.isRunning.Store(false)
	})
}

// Apply changes the settings. A new book depth rebuilds every book and (when running)
// sends a fresh snapshot, whose sequence number moves forward (see snapshotTo).
func (f *Feed) Apply(s domain.Settings) error {
	if err := s.Validate(f.opts.Shape.MaxDepth()); err != nil {
		return err
	}
	f.do(func() {
		old := f.settings
		f.settings = s
		f.current.Store(&s)
		f.rebase(time.Now())
		if s.BookDepth == old.BookDepth {
			return
		}
		for id, b := range f.books {
			f.books[id] = domain.NewBook(f.opts.Shape, s.BookDepth, b.Seq(), f.rng)
		}
		if f.running {
			for _, id := range f.active {
				f.snapshotTo(id, nil)
			}
		}
	})
	return nil
}

// Top is what a REST snapshot reads: the best n levels per side at the current sequence
// number. A market nobody subscribed to still has a book, as on a real exchange.
func (f *Feed) Top(market string, n int) (asks, bids []domain.Level, seq int64) {
	f.do(func() {
		b := f.book(market)
		asks, bids = b.Top(n)
		seq = b.Seq()
	})
	f.restCalls.Add(1)
	return asks, bids, seq
}

// Status copies the generator's state.
func (f *Feed) Status() Status {
	var st Status
	f.do(func() {
		st.Running = f.running
		st.Settings = f.settings
		st.Markets = append([]string{}, f.active...)
		for _, subs := range f.subs {
			st.Subscriptions += len(subs)
		}
	})
	return st
}

// Settings are the current settings.
func (f *Feed) Settings() domain.Settings { return *f.current.Load() }

// Running is whether the feed sends market data now.
func (f *Feed) Running() bool { return f.isRunning.Load() }

// ActiveMarkets is how many markets have a subscriber (or are in the DB list) now.
func (f *Feed) ActiveMarkets() int { return int(f.activeCount.Load()) }

// SendError counts a frame that could not be written (WebSocket write or Kafka produce
// failure).
func (f *Feed) SendError() { f.sendErrors.Add(1) }

// Counters reads the totals.
func (f *Feed) Counters() domain.Counters {
	return domain.Counters{
		Updates: f.updates.Load(), Snapshots: f.snapshots.Load(), Deliveries: f.deliveries.Load(),
		Missed: f.missed.Load(), SendErrors: f.sendErrors.Load(), RestCalls: f.restCalls.Load(),
		ActiveMillis: f.activeNanos.Load() / int64(time.Millisecond),
	}
}

// emit sends the updates that are due at now. Records updates per PeriodMS, spread over
// the period tick by tick rather than sent in one burst.
func (f *Feed) emit(now time.Time) {
	prev := f.lastTick
	f.lastTick = now
	s := f.settings
	if !f.running || s.Records == 0 || len(f.active) == 0 {
		f.rebase(now)
		return
	}
	if !prev.IsZero() {
		// Time spent blocked in a send counts too: the tool was trying to send.
		f.activeNanos.Add(int64(now.Sub(prev)))
	}
	period := time.Duration(s.PeriodMS) * time.Millisecond
	// Slide the schedule forward one finished period at a time, so the numbers stay
	// small however long a stage runs.
	for now.Sub(f.base) >= period && f.emitted >= s.Records {
		f.base = f.base.Add(period)
		f.emitted -= s.Records
	}
	n := due(now.Sub(f.base), s.Records, period, f.emitted)
	if n > s.Records {
		// More than a whole period behind: sending blocked. Count what was not sent on
		// time and restart the schedule, rather than burst it all.
		f.missed.Add(n - s.Records)
		f.rebase(now)
		n = s.Records
	}
	for range n {
		f.update(now)
	}
	f.emitted += n
}

// due is how many updates should have been sent elapsed into the schedule, minus those
// already sent.
func due(elapsed time.Duration, records int64, period time.Duration, emitted int64) int64 {
	return int64(elapsed)*records/int64(period) - emitted
}

func (f *Feed) rebase(now time.Time) {
	f.base, f.emitted = now, 0
}

func (f *Feed) update(now time.Time) {
	id := f.active[f.next%len(f.active)]
	f.next++
	b := f.book(id)
	asks, bids := b.Requote(f.settings.UpdateLevels, f.rng)
	prev, _ := b.Advance(f.opts.Encoder.SeqStep(f.rng))
	f.updates.Add(1)
	f.deliver(id, f.opts.Encoder.Update(id, b, asks, bids, prev, now), nil)
}

// snapshotTo sends id's current book to subs, or to everyone that gets id's frames when
// subs is nil.
//
// A snapshot to everyone (start, new depth, a market back in the DB list) first moves an
// existing book's sequence one step forward. The pipeline drops a sequenced snapshot whose
// number is not above the last one it took (lpa-core's type validator: stale_or_duplicate),
// and these snapshots must replace what it holds. A snapshot to one new subscriber keeps
// the current number, as a real exchange does.
func (f *Feed) snapshotTo(id string, subs []domain.Subscriber) {
	_, existed := f.books[id]
	b := f.book(id)
	if subs == nil && existed {
		b.Advance(f.opts.Encoder.SeqStep(f.rng))
	}
	f.snapshots.Add(1)
	f.deliver(id, f.opts.Encoder.Snapshot(id, b, time.Now()), subs)
}

func (f *Feed) deliver(id string, frame []byte, subs []domain.Subscriber) {
	if f.opts.Publisher != nil {
		f.opts.Publisher.Publish(frame)
		f.deliveries.Add(1)
		return
	}
	if subs == nil {
		for _, s := range f.subs[id] {
			subs = append(subs, s)
		}
	}
	for _, s := range subs {
		if s.Send(frame) {
			f.deliveries.Add(1)
		}
	}
}

func (f *Feed) book(id string) *domain.Book {
	b, ok := f.books[id]
	if !ok {
		b = domain.NewBook(f.opts.Shape, f.settings.BookDepth, f.opts.SeqStart, f.rng)
		f.books[id] = b
	}
	return b
}

func (f *Feed) refreshActive() {
	f.active = f.active[:0]
	if f.opts.Publisher != nil {
		for id := range f.kafkaMarkets {
			f.active = append(f.active, id)
		}
	} else {
		for id, subs := range f.subs {
			if len(subs) == 0 {
				delete(f.subs, id)
				continue
			}
			f.active = append(f.active, id)
		}
	}
	sort.Strings(f.active)
	f.activeCount.Store(int64(len(f.active)))
}
