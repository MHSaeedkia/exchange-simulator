// Package httpapi is the control API: start or stop one exchange, change its settings while
// the tool runs (each change starts a new report stage), read the status, stop the tool.
package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"exchange-simulator/internal/domain"
	"exchange-simulator/internal/feed"
	"exchange-simulator/internal/report"
)

// Feed is one exchange's generator, as this API needs it.
type Feed interface {
	Apply(s domain.Settings) error
	Settings() domain.Settings
	Start()
	Stop()
	Running() bool
	Status() feed.Status
	Counters() domain.Counters
}

// Recorder is the report, as this API needs it.
type Recorder interface {
	Switch(now time.Time, before, after map[string]report.State) (report.Stage, error)
	Current() int
	Dir() string
}

// Exchange is one exchange as the API shows it.
type Exchange struct {
	Name         string
	Feed         Feed
	WebSocketURL string     // empty in kafka mode
	RESTURL      string     // empty in kafka mode or without REST
	Connections  func() int // nil in kafka mode
}

// Options is what the handler needs besides the exchanges.
type Options struct {
	Mode   string
	States func() map[string]report.State // every exchange now, for a stage switch
	Stop   func()                         // ends the run
}

// Handler serves the control API.
type Handler struct {
	exchanges map[string]Exchange
	order     []string
	rec       Recorder
	opts      Options
	log       *slog.Logger
	mu        sync.Mutex // one change at a time, so stages never interleave
}

// New builds a Handler.
func New(exchanges []Exchange, rec Recorder, opts Options, log *slog.Logger) *Handler {
	h := &Handler{exchanges: map[string]Exchange{}, rec: rec, opts: opts, log: log}
	for _, e := range exchanges {
		h.exchanges[e.Name] = e
		h.order = append(h.order, e.Name)
	}
	return h
}

// Routes is the endpoints.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", h.status)
	mux.HandleFunc("POST /exchanges/{name}/start", h.start)
	mux.HandleFunc("POST /exchanges/{name}/stop", h.stopExchange)
	mux.HandleFunc("POST /exchanges/{name}/settings", h.settings)
	mux.HandleFunc("POST /stop", h.stopRun)
	return mux
}

// SettingsRequest is POST /exchanges/{name}/settings's body. Every field is optional; a
// missing one keeps its current value.
type SettingsRequest struct {
	Records      *int64 `json:"records"`
	PeriodMS     *int64 `json:"period_ms"`
	BookDepth    *int   `json:"book_depth"`
	UpdateLevels *int   `json:"update_levels"`
}

func (h *Handler) settings(w http.ResponseWriter, r *http.Request) {
	ex, ok := h.exchange(w, r)
	if !ok {
		return
	}
	var req SettingsRequest
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<16))
	// A typo like "record" would otherwise change nothing and still start a new stage.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad body: "+err.Error())
		return
	}
	if req.Records == nil && req.PeriodMS == nil && req.BookDepth == nil && req.UpdateLevels == nil {
		writeError(w, http.StatusBadRequest, "nothing to change: send records, period_ms, book_depth and/or update_levels")
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	before := h.opts.States()
	next := ex.Feed.Settings()
	set(&next.Records, req.Records)
	set(&next.PeriodMS, req.PeriodMS)
	set(&next.BookDepth, req.BookDepth)
	set(&next.UpdateLevels, req.UpdateLevels)
	if err := ex.Feed.Apply(next); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.log.Info("settings changed", "exchange", ex.Name, "records", next.Records, "period_ms", next.PeriodMS,
		"book_depth", next.BookDepth, "update_levels", next.UpdateLevels, "target_per_sec", next.PerSecond())
	h.newStage(w, ex.Name, before)
}

func (h *Handler) start(w http.ResponseWriter, r *http.Request) {
	h.toggle(w, r, true)
}

func (h *Handler) stopExchange(w http.ResponseWriter, r *http.Request) {
	h.toggle(w, r, false)
}

func (h *Handler) toggle(w http.ResponseWriter, r *http.Request, run bool) {
	ex, ok := h.exchange(w, r)
	if !ok {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if ex.Feed.Running() == run {
		// Nothing changes, so no new stage.
		writeJSON(w, http.StatusOK, map[string]any{"exchange": ex.Name, "running": run, "stage": h.rec.Current(), "changed": false})
		return
	}
	before := h.opts.States()
	if run {
		ex.Feed.Start()
	} else {
		ex.Feed.Stop()
	}
	h.log.Info("exchange "+map[bool]string{true: "started", false: "stopped"}[run], "exchange", ex.Name)
	h.newStage(w, ex.Name, before)
}

// newStage ends the running stage at before (taken just before the change) and starts the
// next. Must hold h.mu.
func (h *Handler) newStage(w http.ResponseWriter, name string, before map[string]report.State) {
	ended, err := h.rec.Switch(time.Now(), before, h.opts.States())
	LogStage(h.log, ended)
	h.log.Info("stage started", "stage", h.rec.Current())
	if err != nil {
		h.log.Error("report not written", "err", err)
		writeError(w, http.StatusInternalServerError, "change applied, but the report could not be written: "+err.Error())
		return
	}
	ex := h.exchanges[name]
	writeJSON(w, http.StatusOK, map[string]any{
		"exchange": name, "running": ex.Feed.Running(), "settings": ex.Feed.Settings(),
		"target_per_sec": ex.Feed.Settings().PerSecond(), "stage": h.rec.Current(), "changed": true, "ended": ended,
	})
}

func (h *Handler) status(w http.ResponseWriter, _ *http.Request) {
	exchanges := map[string]any{}
	total := 0.0
	for _, name := range h.order {
		ex := h.exchanges[name]
		st := ex.Feed.Status()
		target := 0.0
		if st.Running {
			target = st.Settings.PerSecond()
		}
		total += target
		body := map[string]any{
			"running": st.Running, "settings": st.Settings, "target_per_sec": target,
			"markets": st.Markets, "counters": ex.Feed.Counters(),
		}
		if ex.Connections != nil {
			body["websocket_url"] = ex.WebSocketURL
			body["connections"] = ex.Connections()
			body["subscriptions"] = st.Subscriptions
			if ex.RESTURL != "" {
				body["rest_url"] = ex.RESTURL
			}
		}
		exchanges[name] = body
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"mode": h.opts.Mode, "stage": h.rec.Current(), "report_dir": h.rec.Dir(),
		"target_per_sec": total, "exchanges": exchanges,
	})
}

func (h *Handler) stopRun(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusAccepted, map[string]any{"stopping": true, "report_dir": h.rec.Dir()})
	h.opts.Stop()
}

func (h *Handler) exchange(w http.ResponseWriter, r *http.Request) (Exchange, bool) {
	ex, ok := h.exchanges[r.PathValue("name")]
	if !ok {
		writeError(w, http.StatusNotFound, "no exchange "+r.PathValue("name"))
	}
	return ex, ok
}

// LogStage writes one finished stage to the log: the total, then one line per exchange
// that was running. main uses it for the last stage.
func LogStage(log *slog.Logger, st report.Stage) {
	if st.Number == 0 {
		return
	}
	log.Info("stage ended", "stage", st.Number, "seconds", round(st.Seconds),
		"target_per_sec", round(st.TargetPerSec), "sent_per_sec", round(st.SentPerSec))
	for name, es := range st.Exchanges {
		if !es.Running {
			continue
		}
		log.Info("stage ended", "stage", st.Number, "exchange", name,
			"target_per_sec", round(es.TargetPerSec), "sent_per_sec", round(es.SentPerSec),
			"missed", es.Counters.Missed, "send_errors", es.Counters.SendErrors, "markets", es.Markets)
	}
}

func set[T any](dst *T, v *T) {
	if v != nil {
		*dst = *v
	}
}

func round(v float64) float64 { return float64(int64(v*100+0.5)) / 100 }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
