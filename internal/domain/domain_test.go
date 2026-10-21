package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSettingsValidate(t *testing.T) {
	ok := Settings{Records: 10, PeriodMS: 100, BookDepth: 50, UpdateLevels: 5}
	assert.NoError(t, ok.Validate(400))

	paused := ok
	paused.Records = 0
	assert.NoError(t, paused.Validate(400), "0 records pauses sending")

	for name, s := range map[string]Settings{
		"negative records":       {Records: -1, PeriodMS: 100, BookDepth: 50, UpdateLevels: 5},
		"zero period":            {Records: 1, PeriodMS: 0, BookDepth: 50, UpdateLevels: 5},
		"zero depth":             {Records: 1, PeriodMS: 100, BookDepth: 0, UpdateLevels: 0},
		"too deep":               {Records: 1, PeriodMS: 100, BookDepth: 401, UpdateLevels: 5},
		"update above the depth": {Records: 1, PeriodMS: 100, BookDepth: 5, UpdateLevels: 6},
	} {
		assert.Error(t, s.Validate(400), name)
	}
}

func TestPerSecond(t *testing.T) {
	assert.Equal(t, 5000.0, Settings{Records: 500, PeriodMS: 100}.PerSecond())
	assert.Equal(t, 0.5, Settings{Records: 1, PeriodMS: 2000}.PerSecond())
}

func TestCountersSub(t *testing.T) {
	a := Counters{Updates: 10, Snapshots: 1, Deliveries: 12, Missed: 0, SendErrors: 1, RestCalls: 2, ActiveMillis: 100}
	b := Counters{Updates: 25, Snapshots: 3, Deliveries: 30, Missed: 4, SendErrors: 1, RestCalls: 5, ActiveMillis: 1100}
	assert.Equal(t, Counters{Updates: 15, Snapshots: 2, Deliveries: 18, Missed: 4, SendErrors: 0, RestCalls: 3, ActiveMillis: 1000}, b.Sub(a))
}

func TestEndpointURL(t *testing.T) {
	e := Endpoint{Host: "ws.okx.com", Port: 8443, Path: "/ws/v5/public"}
	assert.Equal(t, "wss://ws.okx.com:8443/ws/v5/public", e.URL("wss"))
}

// A run started later must start every book above anything an earlier run could reach, or
// lpa-core (whose state outlives the simulator) rejects the new frames as stale.
func TestClockSeqStartMovesForwardWithTime(t *testing.T) {
	t0 := time.Date(2026, 10, 21, 20, 0, 0, 0, time.UTC)
	a := ClockSeqStart(1000, 100, t0)
	b := ClockSeqStart(1000, 100, t0.Add(10*time.Second))
	assert.Greater(t, a, int64(1000))
	// 10 s of an earlier run at up to 1000 frames/s on one market, each step at most 100:
	assert.Greater(t, b, a+10*1000*100-1, "a later start is above what 10 s at 1000/s could reach")
	assert.Equal(t, int64(1000), ClockSeqStart(1000, 100, SeqEpoch), "SEQ_START at the epoch")
	// Stays exact for JSON readers that use doubles, for decades, with bitget's 12000 step.
	assert.Less(t, ClockSeqStart(941000000000, 12000, time.Date(2046, 1, 1, 0, 0, 0, 0, time.UTC)), int64(1)<<53)
}
