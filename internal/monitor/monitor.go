// Package monitor samples system numbers while a stage runs: CPU, memory and network of
// chosen Docker containers (Docker Engine API, no docker CLI), and the value of each
// PromQL query in a file (Prometheus HTTP API).
//
// A source that fails is logged once and skipped: a missing container or a Prometheus
// that is down must not stop a load test.
package monitor

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Query is one named PromQL expression.
type Query struct {
	Name string
	Expr string
}

var queryName = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// LoadQueries reads a file of `name: promql` lines. Blank lines and lines starting with #
// are skipped. The name ends at the first colon, so the query itself may hold colons.
func LoadQueries(path string) ([]Query, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open queries file: %w", err)
	}
	defer f.Close()

	var out []Query
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, expr, ok := strings.Cut(line, ":")
		name, expr = strings.TrimSpace(name), strings.TrimSpace(expr)
		if !ok || !queryName.MatchString(name) || expr == "" {
			return nil, fmt.Errorf("%s line %d: want `name: promql`, got %q", path, n, line)
		}
		out = append(out, Query{Name: name, Expr: expr})
	}
	return out, sc.Err()
}

// Options mirrors the monitoring settings in .env.
type Options struct {
	DockerHost    string   // unix:///path or tcp://host:port
	Containers    []string // empty = no Docker numbers
	PrometheusURL string   // empty = no Prometheus numbers
	Queries       []Query
	Timeout       time.Duration // per HTTP call
}

// Sampler takes one sample at a time. Not safe for concurrent Sample calls.
type Sampler struct {
	opts       Options
	docker     *http.Client
	dockerBase string
	prom       *http.Client
	prev       map[string]netCounters // container → network totals at the last sample
	warned     map[string]bool
	log        *slog.Logger
}

type netCounters struct {
	at     time.Time
	rx, tx uint64
}

// New builds a Sampler.
func New(opts Options, log *slog.Logger) (*Sampler, error) {
	s := &Sampler{
		opts: opts, log: log,
		prom: &http.Client{Timeout: opts.Timeout},
		prev: map[string]netCounters{}, warned: map[string]bool{},
	}
	if len(opts.Containers) > 0 {
		client, base, err := dockerClient(opts.DockerHost, opts.Timeout)
		if err != nil {
			return nil, err
		}
		s.docker, s.dockerBase = client, base
	}
	return s, nil
}

func dockerClient(host string, timeout time.Duration) (*http.Client, string, error) {
	switch {
	case strings.HasPrefix(host, "unix://"):
		path := strings.TrimPrefix(host, "unix://")
		tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		}}
		return &http.Client{Transport: tr, Timeout: timeout}, "http://docker", nil
	case strings.HasPrefix(host, "tcp://"):
		return &http.Client{Timeout: timeout}, "http://" + strings.TrimPrefix(host, "tcp://"), nil
	default:
		return nil, "", fmt.Errorf("DOCKER_HOST must start with unix:// or tcp://, got %q", host)
	}
}

// Sample reads every source once. Docker calls run in parallel: each one-shot stats call
// takes about a second, because Docker measures CPU over that time.
func (s *Sampler) Sample(ctx context.Context) map[string]float64 {
	out := map[string]float64{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	now := time.Now()

	for _, c := range s.opts.Containers {
		wg.Go(func() {
			st, err := s.dockerStats(ctx, c)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if ctx.Err() == nil { // a sample cut short by shutdown is not a broken source
					s.warnOnce("docker:"+c, "docker stats failed", "container", c, "err", err)
				}
				return
			}
			s.recover("docker:" + c)
			s.addDocker(out, c, st, now)
		})
	}
	if s.opts.PrometheusURL != "" {
		for _, q := range s.opts.Queries {
			wg.Go(func() {
				v, err := s.query(ctx, q.Expr)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					if ctx.Err() == nil {
						s.warnOnce("prom:"+q.Name, "prometheus query failed", "query", q.Name, "err", err)
					}
					return
				}
				s.recover("prom:" + q.Name)
				out["prom."+q.Name] = v
			})
		}
	}
	wg.Wait()
	return out
}

// dockerStatsBody is the part of GET /containers/{id}/stats this tool reads.
type dockerStatsBody struct {
	CPUStats    cpuStats `json:"cpu_stats"`
	PreCPUStats cpuStats `json:"precpu_stats"`
	MemoryStats struct {
		Usage uint64            `json:"usage"`
		Stats map[string]uint64 `json:"stats"`
	} `json:"memory_stats"`
	Networks map[string]struct {
		RxBytes uint64 `json:"rx_bytes"`
		TxBytes uint64 `json:"tx_bytes"`
	} `json:"networks"`
}

type cpuStats struct {
	CPUUsage struct {
		TotalUsage uint64 `json:"total_usage"`
	} `json:"cpu_usage"`
	SystemCPUUsage uint64 `json:"system_cpu_usage"`
	OnlineCPUs     uint64 `json:"online_cpus"`
}

func (s *Sampler) dockerStats(ctx context.Context, container string) (dockerStatsBody, error) {
	var st dockerStatsBody
	u := s.dockerBase + "/containers/" + url.PathEscape(container) + "/stats?stream=false"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return st, err
	}
	resp, err := s.docker.Do(req)
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("docker answered %s", resp.Status)
	}
	return st, json.NewDecoder(resp.Body).Decode(&st)
}

// addDocker turns one stats body into numbers, the way `docker stats` computes them.
func (s *Sampler) addDocker(out map[string]float64, c string, st dockerStatsBody, now time.Time) {
	prefix := "docker." + c + "."

	cpu := float64(st.CPUStats.CPUUsage.TotalUsage) - float64(st.PreCPUStats.CPUUsage.TotalUsage)
	sys := float64(st.CPUStats.SystemCPUUsage) - float64(st.PreCPUStats.SystemCPUUsage)
	if cpu >= 0 && sys > 0 {
		out[prefix+"cpu_pct"] = cpu / sys * float64(st.CPUStats.OnlineCPUs) * 100
	}

	// Page cache is not memory the process needs: subtract it as `docker stats` does
	// (inactive_file on cgroup v2, total_inactive_file on v1).
	mem := st.MemoryStats.Usage
	for _, k := range []string{"inactive_file", "total_inactive_file"} {
		if v, ok := st.MemoryStats.Stats[k]; ok && v <= mem {
			mem -= v
			break
		}
	}
	out[prefix+"mem_mib"] = float64(mem) / (1 << 20)

	var cur netCounters
	cur.at = now
	for _, n := range st.Networks {
		cur.rx += n.RxBytes
		cur.tx += n.TxBytes
	}
	if prev, ok := s.prev[c]; ok && cur.at.After(prev.at) && cur.rx >= prev.rx && cur.tx >= prev.tx {
		secs := cur.at.Sub(prev.at).Seconds()
		out[prefix+"net_rx_kib_s"] = float64(cur.rx-prev.rx) / 1024 / secs
		out[prefix+"net_tx_kib_s"] = float64(cur.tx-prev.tx) / 1024 / secs
	}
	s.prev[c] = cur
}

// query runs one instant query. It must come back as one number: a query that returns
// several series is refused rather than summed, since a sum is wrong for a ratio.
func (s *Sampler) query(ctx context.Context, expr string) (float64, error) {
	u := s.opts.PrometheusURL + "/api/v1/query?query=" + url.QueryEscape(expr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	resp, err := s.prom.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var body struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, fmt.Errorf("prometheus answered %s: %w", resp.Status, err)
	}
	if body.Status != "success" {
		return 0, fmt.Errorf("prometheus: %s", body.Error)
	}

	var value [2]any
	switch body.Data.ResultType {
	case "scalar":
		if err := json.Unmarshal(body.Data.Result, &value); err != nil {
			return 0, err
		}
	case "vector":
		var vec []struct {
			Value [2]any `json:"value"`
		}
		if err := json.Unmarshal(body.Data.Result, &vec); err != nil {
			return 0, err
		}
		if len(vec) != 1 {
			return 0, fmt.Errorf("query returned %d series, want 1: wrap it in sum(), max() or avg()", len(vec))
		}
		value = vec[0].Value
	default:
		return 0, fmt.Errorf("query returned a %s, want one number", body.Data.ResultType)
	}
	raw, _ := value[1].(string)
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, err
	}
	// Flink reports NaN for a lag it cannot measure yet (no data). It is not a number the
	// report can average, and JSON cannot hold it.
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("query returned %s", raw)
	}
	return v, nil
}

func (s *Sampler) warnOnce(key, msg string, args ...any) {
	if s.warned[key] {
		s.log.Debug(msg, args...)
		return
	}
	s.warned[key] = true
	s.log.Warn(msg+" (logged once until it works again)", args...)
}

func (s *Sampler) recover(key string) {
	if s.warned[key] {
		delete(s.warned, key)
		s.log.Info("monitor source works again", "source", key)
	}
}
