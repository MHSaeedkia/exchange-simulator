package domain

import (
	"testing"

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
