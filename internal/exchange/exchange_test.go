package exchange

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrependIDAndSimulationPutsThemFirst(t *testing.T) {
	frame := []byte(`{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[]}`)
	got := PrependIDAndSimulation(frame, "ca0f8d01-294f-42fa-853a-7e7289f636c6", 1)

	assert.Equal(t,
		`{"id":"ca0f8d01-294f-42fa-853a-7e7289f636c6","simulation":1,"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[]}`,
		string(got))
	var v map[string]any
	require.NoError(t, json.Unmarshal(got, &v), "still valid JSON")
}

func TestMillis(t *testing.T) {
	assert.Equal(t, "1800000000600", Millis(time.UnixMilli(1800000000600)))
}
