package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"exchange-simulator/internal/domain"
	"exchange-simulator/internal/feed"
	"exchange-simulator/internal/report"
)

type fakeFeed struct {
	settings domain.Settings
	running  bool
	applyErr error
}

func (f *fakeFeed) Apply(s domain.Settings) error {
	if f.applyErr != nil {
		return f.applyErr
	}
	f.settings = s
	return nil
}
func (f *fakeFeed) Settings() domain.Settings { return f.settings }
func (f *fakeFeed) Start()                    { f.running = true }
func (f *fakeFeed) Stop()                     { f.running = false }
func (f *fakeFeed) Running() bool             { return f.running }
func (f *fakeFeed) Status() feed.Status {
	return feed.Status{Running: f.running, Settings: f.settings, Markets: []string{"BTC-USDT"}, Subscriptions: 2}
}
func (f *fakeFeed) Counters() domain.Counters { return domain.Counters{Updates: 42} }

type fakeRecorder struct {
	stage    int
	switches int
}

func (r *fakeRecorder) Switch(time.Time, map[string]report.State, map[string]report.State) (report.Stage, error) {
	r.switches++
	r.stage++
	return report.Stage{Number: r.stage - 1}, nil
}
func (r *fakeRecorder) Current() int { return r.stage }
func (r *fakeRecorder) Dir() string  { return "reports/run-x" }

type env struct {
	srv     *httptest.Server
	okx     *fakeFeed
	bybit   *fakeFeed
	rec     *fakeRecorder
	stopped *bool
}

func setup(t *testing.T) env {
	t.Helper()
	e := env{
		okx:   &fakeFeed{settings: domain.Settings{Records: 100, PeriodMS: 1000, BookDepth: 400, UpdateLevels: 5}, running: true},
		bybit: &fakeFeed{settings: domain.Settings{Records: 10, PeriodMS: 1000, BookDepth: 50, UpdateLevels: 1}},
		rec:   &fakeRecorder{stage: 1}, stopped: new(bool),
	}
	h := New([]Exchange{
		{Name: "okx", Feed: e.okx, WebSocketURL: "wss://ws.okx.com:8443/ws/v5/public", RESTURL: "https://www.okx.com:443/api/v5/market/books", Connections: func() int { return 1 }},
		{Name: "bybit", Feed: e.bybit, WebSocketURL: "wss://stream.bybit.com:443/v5/public/spot", Connections: func() int { return 0 }},
	}, e.rec, Options{
		Mode: "websocket", States: func() map[string]report.State { return nil }, Stop: func() { *e.stopped = true },
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	e.srv = httptest.NewServer(h.Routes())
	t.Cleanup(e.srv.Close)
	return e
}

func post(t *testing.T, url, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return resp.StatusCode, out
}

func TestSettingsChangeOneExchangeOnlyAndStartAStage(t *testing.T) {
	e := setup(t)
	code, body := post(t, e.srv.URL+"/exchanges/okx/settings", `{"records":500,"period_ms":100}`)
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, domain.Settings{Records: 500, PeriodMS: 100, BookDepth: 400, UpdateLevels: 5}, e.okx.settings)
	assert.Equal(t, domain.Settings{Records: 10, PeriodMS: 1000, BookDepth: 50, UpdateLevels: 1}, e.bybit.settings, "the other exchange is untouched")
	assert.Equal(t, 1, e.rec.switches)
	assert.Equal(t, 2.0, body["stage"])
	assert.Equal(t, 5000.0, body["target_per_sec"])
}

func TestStartAndStopAnExchange(t *testing.T) {
	e := setup(t)
	code, body := post(t, e.srv.URL+"/exchanges/bybit/start", ``)
	assert.Equal(t, http.StatusOK, code)
	assert.True(t, e.bybit.running)
	assert.Equal(t, true, body["changed"])
	assert.Equal(t, 1, e.rec.switches)

	code, body = post(t, e.srv.URL+"/exchanges/bybit/start", ``)
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, false, body["changed"], "already running")
	assert.Equal(t, 1, e.rec.switches, "no stage for no change")

	_, _ = post(t, e.srv.URL+"/exchanges/okx/stop", ``)
	assert.False(t, e.okx.running)
	assert.Equal(t, 2, e.rec.switches)
}

func TestUnknownExchangeIs404(t *testing.T) {
	e := setup(t)
	for _, path := range []string{"/exchanges/binance/start", "/exchanges/binance/stop", "/exchanges/binance/settings"} {
		code, out := post(t, e.srv.URL+path, `{"records":1}`)
		assert.Equal(t, http.StatusNotFound, code, path)
		assert.NotEmpty(t, out["error"])
	}
	assert.Zero(t, e.rec.switches)
}

func TestSettingsRejectsBadBodies(t *testing.T) {
	e := setup(t)
	for _, body := range []string{``, `{}`, `{"record":5}`, `{"records":"5"}`, `not json`} {
		code, out := post(t, e.srv.URL+"/exchanges/okx/settings", body)
		assert.Equal(t, http.StatusBadRequest, code, body)
		assert.NotEmpty(t, out["error"], body)
	}
	assert.Zero(t, e.rec.switches, "no stage for a refused change")
}

func TestSettingsRefusedByTheFeedStartsNoStage(t *testing.T) {
	e := setup(t)
	e.okx.applyErr = errors.New("book_depth too deep")
	code, out := post(t, e.srv.URL+"/exchanges/okx/settings", `{"book_depth":100000}`)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "book_depth too deep", out["error"])
	assert.Zero(t, e.rec.switches)
}

func TestStatusShowsEveryExchange(t *testing.T) {
	e := setup(t)
	resp, err := http.Get(e.srv.URL + "/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	var body struct {
		Mode         string                    `json:"mode"`
		Stage        int                       `json:"stage"`
		TargetPerSec float64                   `json:"target_per_sec"`
		Exchanges    map[string]map[string]any `json:"exchanges"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))

	assert.Equal(t, "websocket", body.Mode)
	assert.Equal(t, 1, body.Stage)
	assert.Equal(t, 100.0, body.TargetPerSec, "only running exchanges count")
	require.Contains(t, body.Exchanges, "okx")
	require.Contains(t, body.Exchanges, "bybit")
	assert.Equal(t, true, body.Exchanges["okx"]["running"])
	assert.Equal(t, "wss://ws.okx.com:8443/ws/v5/public", body.Exchanges["okx"]["websocket_url"])
	assert.Equal(t, 1.0, body.Exchanges["okx"]["connections"])
	assert.Equal(t, false, body.Exchanges["bybit"]["running"])
	assert.Equal(t, 0.0, body.Exchanges["bybit"]["target_per_sec"])
	assert.NotContains(t, body.Exchanges["bybit"], "rest_url", "bybit has no REST here")
}

func TestStop(t *testing.T) {
	e := setup(t)
	code, body := post(t, e.srv.URL+"/stop", ``)
	assert.Equal(t, http.StatusAccepted, code)
	assert.Equal(t, true, body["stopping"])
	assert.True(t, *e.stopped)
}

func TestWrongMethod(t *testing.T) {
	e := setup(t)
	resp, err := http.Get(e.srv.URL + "/exchanges/okx/start")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}
