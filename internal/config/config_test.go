package config

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"exchange-simulator/internal/domain"
)

var globalKeys = []string{"OUTPUT_MODE", "EXCHANGES_RUNNING", "LISTEN_HOST", "TLS_ENABLED", "TLS_CA_CERT",
	"TLS_CA_KEY", "TLS_CERT", "TLS_KEY", "PROXY_PORT", "WS_SEND_BUFFER", "WS_WRITE_TIMEOUT_MS", "KAFKA_BROKERS",
	"KAFKA_TOPIC", "SIMULATION", "DATABASE_URL", "MARKET_STATUS", "DB_REFRESH_MS", "TICK_MS", "API_ADDR",
	"PROMETHEUS_URL", "PROM_QUERIES_FILE", "DOCKER_HOST", "MONITOR_CONTAINERS", "SAMPLE_INTERVAL_MS", "REPORT_DIR"}

// clearEnv unsets every key for one test — the global ones and every exchange block — so a
// value in the developer's shell (DOCKER_HOST is a common one) cannot change what it sees.
func clearEnv(t *testing.T) {
	t.Helper()
	keys := append([]string{}, globalKeys...)
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		for _, b := range known {
			if strings.HasPrefix(k, strings.ToUpper(b.name)+"_") {
				keys = append(keys, k)
			}
		}
	}
	for _, k := range keys {
		prev, had := os.LookupEnv(k)
		require.NoError(t, os.Unsetenv(k))
		t.Cleanup(func() {
			if had {
				os.Setenv(k, prev)
			} else {
				os.Unsetenv(k)
			}
		})
	}
}

func TestFromEnv_FallsBackToDefaults(t *testing.T) {
	clearEnv(t)
	cfg, err := FromEnv()
	require.NoError(t, err)

	assert.Equal(t, ModeWebSocket, cfg.OutputMode)
	assert.Equal(t, []string{"okx"}, cfg.Running)
	assert.True(t, cfg.TLSEnabled)
	assert.Equal(t, "certs/ca.crt", cfg.TLSCACert)
	assert.Equal(t, 2053, cfg.ProxyPort, "the port of NiFi's real proxy: a test changes only its host")
	assert.Equal(t, "ex8-raw", cfg.Topic(8))
	assert.Equal(t, time.Millisecond, cfg.Tick)
	assert.Equal(t, defaultDockerHost, cfg.DockerHost)
	assert.Equal(t, []string{"nifi", "kafka", "jobmanager"}, cfg.MonitorContainers)
	assert.Empty(t, cfg.PrometheusURL, "no Prometheus unless asked for")

	okx, ok := cfg.Exchange("okx")
	require.True(t, ok)
	assert.Equal(t, int64(8), okx.ID)
	assert.Equal(t, domain.Endpoint{Host: "ws.okx.com", Port: 8443, Path: "/ws/v5/public"}, okx.WS, "the REAL address")
	assert.Equal(t, domain.Endpoint{Host: "www.okx.com", Port: 443, Path: "/api/v5/market/books"}, okx.REST)
	assert.True(t, okx.HasREST)
	assert.Equal(t, domain.Settings{Records: 100, PeriodMS: 1000, BookDepth: 400, UpdateLevels: 5}, okx.Defaults)
	assert.Equal(t, []string{"ws.okx.com", "www.okx.com"}, cfg.Hosts())
}

func TestFromEnv_ExchangeBlockIsPrefixed(t *testing.T) {
	clearEnv(t)
	t.Setenv("OKX_RECORDS", "500")
	t.Setenv("OKX_PERIOD_MS", "100")
	t.Setenv("OKX_WS_HOST", "ws.example.test")
	t.Setenv("OKX_WS_PORT", "9443")

	cfg, err := FromEnv()
	require.NoError(t, err)
	okx, _ := cfg.Exchange("okx")
	assert.Equal(t, int64(500), okx.Defaults.Records)
	assert.Equal(t, int64(100), okx.Defaults.PeriodMS)
	assert.Equal(t, domain.Endpoint{Host: "ws.example.test", Port: 9443, Path: "/ws/v5/public"}, okx.WS)
	assert.Contains(t, cfg.Hosts(), "ws.example.test")
}

func TestFromEnv_GlobalsWin(t *testing.T) {
	clearEnv(t)
	t.Setenv("OUTPUT_MODE", "kafka")
	t.Setenv("KAFKA_BROKERS", "a:1, b:2")
	t.Setenv("KAFKA_TOPIC", "test-ex{id}")
	t.Setenv("PROMETHEUS_URL", "http://prom:9090/")
	t.Setenv("TLS_ENABLED", "false")

	cfg, err := FromEnv()
	require.NoError(t, err)
	assert.Equal(t, ModeKafka, cfg.OutputMode)
	assert.Equal(t, []string{"a:1", "b:2"}, cfg.KafkaBrokers)
	assert.Equal(t, "test-ex8", cfg.Topic(8))
	assert.Equal(t, "http://prom:9090", cfg.PrometheusURL)
	assert.False(t, cfg.TLSEnabled)
}

func TestFromEnv_NoneTurnsAListOff(t *testing.T) {
	clearEnv(t)
	t.Setenv("MONITOR_CONTAINERS", "none")
	t.Setenv("EXCHANGES_RUNNING", "none")
	cfg, err := FromEnv()
	require.NoError(t, err)
	assert.Empty(t, cfg.MonitorContainers)
	assert.Empty(t, cfg.Running, "start with every exchange stopped")
}

func TestFromEnv_ProxyPortZeroTurnsTheProxyOff(t *testing.T) {
	clearEnv(t)
	t.Setenv("PROXY_PORT", "0")
	cfg, err := FromEnv()
	require.NoError(t, err)
	assert.Zero(t, cfg.ProxyPort)
}

func TestFromEnv_BadValuesFailBeforeAnythingStarts(t *testing.T) {
	for key, value := range map[string]string{
		"OUTPUT_MODE":       "udp",
		"EXCHANGES_RUNNING": "okx,binance",
		"TLS_ENABLED":       "maybe",
		"PROXY_PORT":        "70000",
		"TICK_MS":           "0",
		"WS_SEND_BUFFER":    "0",
		"OKX_RECORDS":       "ten",
		"OKX_PERIOD_MS":     "0",
		"OKX_BOOK_DEPTH":    "1000000", // the lowest bid would go below 0
		"OKX_SEQ_STEP_MIN":  "0",
		"OKX_PRICE_STEP":    "0",
		"OKX_WS_PORT":       "70000",
		"OKX_WS_PATH":       "ws/v5/public",
	} {
		t.Run(key, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(key, value)
			_, err := FromEnv()
			assert.Error(t, err)
		})
	}
}
