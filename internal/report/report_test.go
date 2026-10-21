package report

import (
	"encoding/csv"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"exchange-simulator/internal/domain"
)

func readCSV(t *testing.T, path string) []map[string]string {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	require.NoError(t, err)
	var out []map[string]string
	for _, row := range rows[1:] {
		m := map[string]string{}
		for i, col := range rows[0] {
			m[col] = row[i]
		}
		out = append(out, m)
	}
	return out
}

var (
	slow = domain.Settings{Records: 100, PeriodMS: 1000, BookDepth: 400, UpdateLevels: 5}
	fast = domain.Settings{Records: 500, PeriodMS: 100, BookDepth: 50, UpdateLevels: 5}
	t0   = time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
)

// Two exchanges, two stages: okx runs throughout and goes fast in stage 2; bybit starts
// stopped and is started at the switch.
func TestStagesPerExchange(t *testing.T) {
	r, err := New(t.TempDir(), t0)
	require.NoError(t, err)
	defer r.Close()

	r.Begin(t0, map[string]State{
		"okx":   {Running: true, Settings: slow, Markets: 2},
		"bybit": {Running: false, Settings: slow},
	})
	assert.Equal(t, 1, r.Current())
	require.NoError(t, r.Sample(t0.Add(5*time.Second), map[string]float64{"docker.nifi.cpu_pct": 10}, map[string]int{"okx": 3}))
	require.NoError(t, r.Sample(t0.Add(10*time.Second), map[string]float64{"docker.nifi.cpu_pct": 30}, map[string]int{"okx": 2}))

	first, err := r.Switch(t0.Add(10*time.Second), map[string]State{
		"okx":   {Running: true, Settings: slow, Markets: 2, Counters: domain.Counters{Updates: 990, ActiveMillis: 10000}},
		"bybit": {Running: false, Settings: slow},
	}, map[string]State{
		// After the change: okx goes fast, bybit is started and its start snapshot is
		// already counted — it must land in stage 2, not stage 1.
		"okx":   {Running: true, Settings: fast, Markets: 2, Counters: domain.Counters{Updates: 990, ActiveMillis: 10000}},
		"bybit": {Running: true, Settings: slow, Markets: 1, Counters: domain.Counters{Snapshots: 1}},
	})
	require.NoError(t, err)
	assert.Equal(t, 2, r.Current())
	assert.Equal(t, 100.0, first.TargetPerSec, "only okx was running")
	assert.Equal(t, 99.0, first.SentPerSec)
	assert.Equal(t, ExchangeStage{Running: true, Settings: slow, Markets: 3, ActiveSecs: 10, TargetPerSec: 100, SentPerSec: 99,
		Counters: domain.Counters{Updates: 990, ActiveMillis: 10000}}, first.Exchanges["okx"])
	assert.False(t, first.Exchanges["bybit"].Running)
	assert.Zero(t, first.Exchanges["bybit"].TargetPerSec, "a stopped exchange has no target")
	assert.Equal(t, MetricStat{Avg: 20, Max: 30, Samples: 2}, first.Metrics["docker.nifi.cpu_pct"])

	second, err := r.End(t0.Add(20*time.Second), map[string]State{
		"okx":   {Running: true, Settings: fast, Markets: 2, Counters: domain.Counters{Updates: 990 + 40000, Missed: 5, ActiveMillis: 18000}},
		"bybit": {Running: true, Settings: slow, Markets: 1, Counters: domain.Counters{Snapshots: 1, Updates: 1000, ActiveMillis: 10000}},
	})
	require.NoError(t, err)
	assert.Zero(t, first.Exchanges["bybit"].Counters.Snapshots, "the start snapshot is not stage 1's")
	assert.Equal(t, int64(1), second.Exchanges["bybit"].Counters.Snapshots, "it is stage 2's")
	assert.Equal(t, 5100.0, second.TargetPerSec)
	assert.Equal(t, 5000.0, second.Exchanges["okx"].SentPerSec, "this stage's updates over its 8 active seconds")
	assert.Equal(t, 100.0, second.Exchanges["bybit"].SentPerSec)
	assert.Equal(t, 5100.0, second.SentPerSec)
	assert.Equal(t, int64(5), second.Exchanges["okx"].Counters.Missed)
	assert.Zero(t, r.Current())

	rows := readCSV(t, filepath.Join(r.Dir(), "stages.csv"))
	require.Len(t, rows, 2)
	assert.Equal(t, "false", rows[0]["bybit.running"])
	assert.Equal(t, "true", rows[1]["bybit.running"])
	assert.Equal(t, "500", rows[1]["okx.records"])
	assert.Equal(t, "8.00", rows[1]["okx.active_seconds"])
	assert.Equal(t, "5100.00", rows[1]["target_per_sec"])
	assert.Equal(t, "20.00", rows[0]["docker.nifi.cpu_pct_avg"])
	assert.Equal(t, "", rows[1]["docker.nifi.cpu_pct_avg"], "no samples in stage 2")

	var stages []Stage
	b, err := os.ReadFile(filepath.Join(r.Dir(), "stages.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, &stages))
	assert.Len(t, stages, 2)

	f, err := os.Open(filepath.Join(r.Dir(), "samples.csv"))
	require.NoError(t, err)
	defer f.Close()
	samples, err := csv.NewReader(f).ReadAll()
	require.NoError(t, err)
	assert.Equal(t, []string{"time", "stage", "metric", "value"}, samples[0])
	assert.Equal(t, []string{"2026-10-21T12:00:05Z", "1", "docker.nifi.cpu_pct", "10.00"}, samples[1])
}

func TestNotANumberIsSkipped(t *testing.T) {
	r, err := New(t.TempDir(), time.Now())
	require.NoError(t, err)
	defer r.Close()
	r.Begin(time.Now(), map[string]State{"okx": {Settings: slow}})
	require.NoError(t, r.Sample(time.Now(), map[string]float64{"a": math.NaN(), "b": math.Inf(1), "c": 1}, nil))
	st, err := r.End(time.Now(), map[string]State{"okx": {Settings: slow}})
	require.NoError(t, err, "stages.json must still be written")
	assert.Equal(t, map[string]MetricStat{"c": {Avg: 1, Max: 1, Samples: 1}}, st.Metrics)
}

func TestEndWithoutAStageFails(t *testing.T) {
	r, err := New(t.TempDir(), time.Now())
	require.NoError(t, err)
	defer r.Close()
	_, err = r.End(time.Now(), nil)
	assert.Error(t, err)
}

func TestEachRunGetsItsOwnFolder(t *testing.T) {
	base := t.TempDir()
	a, err := New(base, t0)
	require.NoError(t, err)
	defer a.Close()
	b, err := New(base, t0.Add(time.Second))
	require.NoError(t, err)
	defer b.Close()
	assert.NotEqual(t, a.Dir(), b.Dir())
}
