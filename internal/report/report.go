// Package report keeps the stages of one run and writes them to a folder of its own:
//
//	stages.csv / stages.json — one row per stage: totals, then for each exchange its
//	                           state, settings, target and sent rate and counters, then
//	                           avg/max of every sampled system number
//	samples.csv              — every sampled number: time, stage, metric, value
//
// A stage is the time between two changes; any start, stop or settings change of any
// exchange starts a new one. The stage files are rewritten whole at the end of each
// stage, so they are complete even if the tool is killed later.
package report

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	"exchange-simulator/internal/domain"
)

// State is one exchange at a moment: what a stage starts from and ends at.
type State struct {
	Running  bool
	Settings domain.Settings
	Counters domain.Counters
	Markets  int
}

// MetricStat sums up one number over a stage.
type MetricStat struct {
	Avg     float64 `json:"avg"`
	Max     float64 `json:"max"`
	Samples int     `json:"samples"`
}

// ExchangeStage is one exchange during one stage.
type ExchangeStage struct {
	Running      bool            `json:"running"`
	Settings     domain.Settings `json:"settings"`
	Markets      int             `json:"markets"`        // most markets active at once
	ActiveSecs   float64         `json:"active_seconds"` // time it was sending to ≥ 1 market
	TargetPerSec float64         `json:"target_per_sec"` // 0 when stopped
	SentPerSec   float64         `json:"sent_per_sec"`   // updates sent, per active second
	Counters     domain.Counters `json:"counters"`       // this stage only
}

// Stage is one finished stage.
type Stage struct {
	Number       int                      `json:"stage"`
	Start        time.Time                `json:"start"`
	End          time.Time                `json:"end"`
	Seconds      float64                  `json:"seconds"`
	TargetPerSec float64                  `json:"target_per_sec"` // all exchanges
	SentPerSec   float64                  `json:"sent_per_sec"`   // all exchanges
	Exchanges    map[string]ExchangeStage `json:"exchanges"`
	Metrics      map[string]MetricStat    `json:"metrics"`
}

type open struct {
	number  int
	start   time.Time
	states  map[string]State
	markets map[string]int
	metrics map[string]*acc
}

type acc struct {
	sum, max float64
	n        int
}

// Recorder is safe for concurrent use.
type Recorder struct {
	mu      sync.Mutex
	dir     string
	done    []Stage
	cur     *open
	samples *os.File
	csv     *csv.Writer
}

// New makes base/run-<time>/ and opens its samples file.
func New(base string, now time.Time) (*Recorder, error) {
	dir := filepath.Join(base, "run-"+now.Format("20060102-150405"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("make report dir: %w", err)
	}
	f, err := os.Create(filepath.Join(dir, "samples.csv"))
	if err != nil {
		return nil, fmt.Errorf("create samples.csv: %w", err)
	}
	w := csv.NewWriter(f)
	_ = w.Write([]string{"time", "stage", "metric", "value"})
	w.Flush()
	return &Recorder{dir: dir, samples: f, csv: w}, w.Error()
}

// Dir is this run's folder.
func (r *Recorder) Dir() string { return r.dir }

// Current is the number of the stage running now, 0 before the first.
func (r *Recorder) Current() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cur == nil {
		return 0
	}
	return r.cur.number
}

// Begin starts the first stage.
func (r *Recorder) Begin(now time.Time, states map[string]State) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.begin(now, states)
}

// Switch ends the running stage at before (the moment just before a change) and starts the
// next with after's running state and settings, in one step. The new stage counts from
// before's counters, so whatever the change itself sends (a start's snapshots, say) belongs
// to the new stage, and nothing is lost or counted twice.
func (r *Recorder) Switch(now time.Time, before, after map[string]State) (Stage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, err := r.end(now, before)
	next := map[string]State{}
	for name, s := range after {
		s.Counters = before[name].Counters
		next[name] = s
	}
	r.begin(now, next)
	return st, err
}

// End ends the running stage; no stage runs after it.
func (r *Recorder) End(now time.Time, states map[string]State) (Stage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.end(now, states)
}

// Sample adds one sample of system numbers, and each exchange's market count, to the
// running stage.
func (r *Recorder) Sample(now time.Time, metrics map[string]float64, markets map[string]int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cur == nil {
		return nil
	}
	for name, n := range markets {
		r.cur.markets[name] = max(r.cur.markets[name], n)
	}
	stage := strconv.Itoa(r.cur.number)
	ts := now.Format(time.RFC3339)
	for _, name := range sortedKeys(metrics) {
		v := metrics[name]
		if math.IsNaN(v) || math.IsInf(v, 0) {
			continue // JSON cannot hold it, and it would spoil the average
		}
		a := r.cur.metrics[name]
		if a == nil {
			a = &acc{max: math.Inf(-1)}
			r.cur.metrics[name] = a
		}
		a.sum += v
		a.max = max(a.max, v)
		a.n++
		_ = r.csv.Write([]string{ts, stage, name, num(v)})
	}
	r.csv.Flush()
	return r.csv.Error()
}

// Close closes the samples file.
func (r *Recorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.csv.Flush()
	return r.samples.Close()
}

func (r *Recorder) begin(now time.Time, states map[string]State) {
	o := &open{
		number: len(r.done) + 1, start: now, states: map[string]State{},
		markets: map[string]int{}, metrics: map[string]*acc{},
	}
	for name, s := range states {
		o.states[name] = s
		o.markets[name] = s.Markets
	}
	r.cur = o
}

func (r *Recorder) end(now time.Time, states map[string]State) (Stage, error) {
	if r.cur == nil {
		return Stage{}, fmt.Errorf("no stage is running")
	}
	o := r.cur
	r.cur = nil
	st := Stage{
		Number: o.number, Start: o.start, End: now, Seconds: now.Sub(o.start).Seconds(),
		Exchanges: map[string]ExchangeStage{}, Metrics: map[string]MetricStat{},
	}
	for name, from := range o.states {
		es := ExchangeStage{
			Running: from.Running, Settings: from.Settings,
			Markets:  max(o.markets[name], states[name].Markets),
			Counters: states[name].Counters.Sub(from.Counters),
		}
		es.ActiveSecs = float64(es.Counters.ActiveMillis) / 1000
		if es.Running {
			es.TargetPerSec = from.Settings.PerSecond()
		}
		if es.ActiveSecs > 0 {
			es.SentPerSec = float64(es.Counters.Updates) / es.ActiveSecs
		}
		st.TargetPerSec += es.TargetPerSec
		st.SentPerSec += es.SentPerSec
		st.Exchanges[name] = es
	}
	for name, a := range o.metrics {
		st.Metrics[name] = MetricStat{Avg: a.sum / float64(a.n), Max: a.max, Samples: a.n}
	}
	r.done = append(r.done, st)
	return st, r.write()
}

// write rewrites stages.json and stages.csv from every finished stage. The columns are the
// union over all stages, so they line up.
func (r *Recorder) write() error {
	js, err := json.MarshalIndent(r.done, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(r.dir, "stages.json"), js, 0o644); err != nil {
		return fmt.Errorf("write stages.json: %w", err)
	}

	exNames, metricNames := map[string]float64{}, map[string]float64{}
	for _, st := range r.done {
		for n := range st.Exchanges {
			exNames[n] = 0
		}
		for n := range st.Metrics {
			metricNames[n] = 0
		}
	}
	exchanges, metrics := sortedKeys(exNames), sortedKeys(metricNames)

	header := []string{"stage", "start", "end", "seconds", "target_per_sec", "sent_per_sec"}
	for _, ex := range exchanges {
		for _, col := range exchangeColumns {
			header = append(header, ex+"."+col)
		}
	}
	for _, m := range metrics {
		header = append(header, m+"_avg", m+"_max")
	}
	rows := [][]string{header}
	for _, st := range r.done {
		row := []string{strconv.Itoa(st.Number), st.Start.Format(time.RFC3339), st.End.Format(time.RFC3339),
			num(st.Seconds), num(st.TargetPerSec), num(st.SentPerSec)}
		for _, ex := range exchanges {
			es, ok := st.Exchanges[ex]
			if !ok {
				row = append(row, make([]string, len(exchangeColumns))...)
				continue
			}
			s, c := es.Settings, es.Counters
			row = append(row, strconv.FormatBool(es.Running), i64(s.Records), i64(s.PeriodMS),
				strconv.Itoa(s.BookDepth), strconv.Itoa(s.UpdateLevels), strconv.Itoa(es.Markets),
				num(es.ActiveSecs), num(es.TargetPerSec), num(es.SentPerSec), i64(c.Updates),
				i64(c.Snapshots), i64(c.Deliveries), i64(c.Missed), i64(c.SendErrors), i64(c.RestCalls))
		}
		for _, m := range metrics {
			if ms, ok := st.Metrics[m]; ok {
				row = append(row, num(ms.Avg), num(ms.Max))
			} else {
				row = append(row, "", "")
			}
		}
		rows = append(rows, row)
	}

	f, err := os.Create(filepath.Join(r.dir, "stages.csv"))
	if err != nil {
		return fmt.Errorf("write stages.csv: %w", err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.WriteAll(rows)
	return w.Error()
}

// exchangeColumns are the per-exchange columns of stages.csv, in order.
var exchangeColumns = []string{"running", "records", "period_ms", "book_depth", "update_levels",
	"markets", "active_seconds", "target_per_sec", "sent_per_sec", "updates", "snapshots",
	"deliveries", "missed", "send_errors", "rest_calls"}

func sortedKeys(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func num(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }
func i64(v int64) string   { return strconv.FormatInt(v, 10) }
