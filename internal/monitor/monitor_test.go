package monitor

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var quietLog = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestLoadQueries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.txt")
	require.NoError(t, os.WriteFile(path, []byte(`
# comment
flink_lag_max: max(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max)
raw_in: sum(rate(kafka_topic_partition_current_offset{topic="ex8-raw"}[1m:15s]))
`), 0o600))

	qs, err := LoadQueries(path)
	require.NoError(t, err)
	assert.Equal(t, []Query{
		{"flink_lag_max", "max(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max)"},
		{"raw_in", `sum(rate(kafka_topic_partition_current_offset{topic="ex8-raw"}[1m:15s]))`},
	}, qs, "a colon inside the query is kept")
}

func TestLoadQueriesRejectsBadLines(t *testing.T) {
	for _, line := range []string{"no colon here", "bad name: up", "name:   "} {
		path := filepath.Join(t.TempDir(), "q.txt")
		require.NoError(t, os.WriteFile(path, []byte(line), 0o600))
		_, err := LoadQueries(path)
		assert.Error(t, err, line)
	}
}

const statsBody = `{
  "cpu_stats":    {"cpu_usage":{"total_usage":%d},"system_cpu_usage":%d,"online_cpus":4},
  "precpu_stats": {"cpu_usage":{"total_usage":1000},"system_cpu_usage":100000,"online_cpus":4},
  "memory_stats": {"usage":%d,"stats":{"inactive_file":1048576}},
  "networks": {"eth0":{"rx_bytes":%d,"tx_bytes":0},"eth1":{"rx_bytes":0,"tx_bytes":%d}}
}`

func TestDockerNumbers(t *testing.T) {
	var calls atomic.Int64
	docker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/containers/nifi/stats", r.URL.Path)
		assert.Equal(t, "false", r.URL.Query().Get("stream"))
		n := calls.Add(1)
		// CPU: 25 000 of 100 000 system ticks on 4 CPUs = 100 %.
		// Memory: 11 MiB used, 1 MiB of it page cache.
		// Network: +10 KiB in and +20 KiB out between the two samples.
		_, _ = fmt.Fprintf(w, statsBody, 26000, 200000, 11<<20, n*10240, n*20480)
	}))
	defer docker.Close()

	s, err := New(Options{
		DockerHost: "tcp://" + strings.TrimPrefix(docker.URL, "http://"),
		Containers: []string{"nifi"}, Timeout: time.Second,
	}, quietLog)
	require.NoError(t, err)

	first := s.Sample(context.Background())
	assert.InDelta(t, 100, first["docker.nifi.cpu_pct"], 0.001)
	assert.InDelta(t, 10, first["docker.nifi.mem_mib"], 0.001)
	assert.NotContains(t, first, "docker.nifi.net_rx_kib_s", "a rate needs two samples")

	// Pretend the first sample was a second earlier, so the rate is per second.
	prev := s.prev["nifi"]
	prev.at = prev.at.Add(-time.Second)
	s.prev["nifi"] = prev
	second := s.Sample(context.Background())
	assert.InDelta(t, 10, second["docker.nifi.net_rx_kib_s"], 1)
	assert.InDelta(t, 20, second["docker.nifi.net_tx_kib_s"], 2)
}

func TestMissingContainerIsSkipped(t *testing.T) {
	docker := httptest.NewServer(http.NotFoundHandler())
	defer docker.Close()
	s, err := New(Options{
		DockerHost: "tcp://" + strings.TrimPrefix(docker.URL, "http://"),
		Containers: []string{"gone"}, Timeout: time.Second,
	}, quietLog)
	require.NoError(t, err)
	assert.Empty(t, s.Sample(context.Background()))
}

func TestBadDockerHost(t *testing.T) {
	_, err := New(Options{DockerHost: "npipe:////./pipe/docker", Containers: []string{"x"}}, quietLog)
	assert.Error(t, err)
}

func TestPrometheusQueries(t *testing.T) {
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/query", r.URL.Path)
		switch r.URL.Query().Get("query") {
		case "one":
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1.7e9,"42.5"]}]}}`)
		case "scalar":
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"scalar","result":[1.7e9,"7"]}}`)
		case "many":
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"1"]},{"value":[1,"2"]}]}}`)
		case "nan":
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"NaN"]}]}}`)
		case "empty":
			_, _ = io.WriteString(w, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
		default:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"status":"error","error":"parse error"}`)
		}
	}))
	defer prom.Close()

	s, err := New(Options{PrometheusURL: prom.URL, Timeout: time.Second, Queries: []Query{
		{"one", "one"}, {"scalar", "scalar"}, {"many", "many"}, {"nan", "nan"}, {"empty", "empty"}, {"bad", "bad"},
	}}, quietLog)
	require.NoError(t, err)

	got := s.Sample(context.Background())
	assert.Equal(t, map[string]float64{"prom.one": 42.5, "prom.scalar": 7}, got,
		"several series, NaN, no series and an error are all skipped, not guessed")
}

func TestShutdownIsNotABrokenSource(t *testing.T) {
	docker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer docker.Close()
	s, err := New(Options{
		DockerHost: "tcp://" + strings.TrimPrefix(docker.URL, "http://"),
		Containers: []string{"nifi"}, Timeout: time.Second,
	}, quietLog)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.Empty(t, s.Sample(ctx))
	assert.Empty(t, s.warned, "no warning for a sample cut short by shutdown")
}
