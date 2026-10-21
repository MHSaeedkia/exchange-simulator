// Package domain holds the vocabulary of the simulator: the order book every fake exchange
// keeps, the settings that make up a stage, the counters a stage is measured with, and the
// address an exchange serves.
package domain

import (
	"fmt"
	"strconv"
	"time"
)

// Endpoint is one address a real exchange serves and the simulator answers on instead:
// the host name NiFi dials, the port and the path.
type Endpoint struct {
	Host string `json:"host"`
	Port int    `json:"port"`
	Path string `json:"path"`
}

// URL is the endpoint as the client sees it, for logs and /status.
func (e Endpoint) URL(scheme string) string {
	return scheme + "://" + e.Host + ":" + strconv.Itoa(e.Port) + e.Path
}

// Settings are the four numbers of one exchange that can change while the tool runs. Each
// change starts a new stage in the report.
type Settings struct {
	// Records update frames are sent every PeriodMS, shared by all markets of the exchange.
	Records  int64 `json:"records"`
	PeriodMS int64 `json:"period_ms"`
	// BookDepth is levels per side in a snapshot, UpdateLevels levels per side
	// in each update.
	BookDepth    int `json:"book_depth"`
	UpdateLevels int `json:"update_levels"`
}

// Validate checks s against the deepest book the price shape allows.
func (s Settings) Validate(maxDepth int) error {
	switch {
	case s.Records < 0:
		return fmt.Errorf("records must be 0 or more (0 = pause)")
	case s.PeriodMS < 1:
		return fmt.Errorf("period_ms must be at least 1")
	case s.BookDepth < 1:
		return fmt.Errorf("book_depth must be at least 1")
	case s.BookDepth > maxDepth:
		return fmt.Errorf("book_depth %d is too deep: with BASE_PRICE and PRICE_STEP the lowest bid would be 0 or less (max %d)", s.BookDepth, maxDepth)
	case s.UpdateLevels < 0 || s.UpdateLevels > s.BookDepth:
		return fmt.Errorf("update_levels must be between 0 and book_depth (%d)", s.BookDepth)
	}
	return nil
}

// PerSecond is the target update rate.
func (s Settings) PerSecond() float64 {
	return float64(s.Records) * 1000 / float64(s.PeriodMS)
}

// Counters are totals since the start. A report stage is the difference of two.
type Counters struct {
	// Updates are update frames generated: the "records" of the rate.
	Updates   int64 `json:"updates"`
	Snapshots int64 `json:"snapshots"`
	// Deliveries are frames handed to a WebSocket connection or to Kafka. With
	// two connections on one market, one update is two deliveries.
	Deliveries int64 `json:"deliveries"`
	// Missed are updates that were due but not sent, because sending blocked
	// for more than a whole period.
	Missed     int64 `json:"missed"`
	SendErrors int64 `json:"send_errors"`
	RestCalls  int64 `json:"rest_calls"`
	// ActiveMillis is the time at least one market was getting updates (records > 0).
	// A stage's sent rate is measured over this, not over the stage's whole length,
	// so the minutes before NiFi subscribes do not drag it down.
	ActiveMillis int64 `json:"active_ms"`
}

// Sub is b − a, field by field.
func (b Counters) Sub(a Counters) Counters {
	return Counters{
		Updates: b.Updates - a.Updates, Snapshots: b.Snapshots - a.Snapshots,
		Deliveries: b.Deliveries - a.Deliveries, Missed: b.Missed - a.Missed,
		SendErrors: b.SendErrors - a.SendErrors, RestCalls: b.RestCalls - a.RestCalls,
		ActiveMillis: b.ActiveMillis - a.ActiveMillis,
	}
}

// Subscriber is one WebSocket connection, as the generator sees it.
type Subscriber interface {
	ID() string
	// Send queues msg. It blocks while the queue is full and returns false once
	// the subscriber is gone.
	Send(msg []byte) bool
}

// SeqEpoch is the zero of ClockSeqStart.
var SeqEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// ClockSeqStart is the sequence number a new book starts at: base plus the milliseconds since
// SeqEpoch times the largest step. lpa-core's state outlives a simulator run, and it rejects a
// frame whose number is not above the last it took; starting from the clock puts a new run
// above anything an earlier run reached, as long as no market ran faster than 1000 frames a
// second. For 20 years even bitget's 12000 step stays below 2^53, exact for double readers.
func ClockSeqStart(base, stepMax int64, now time.Time) int64 {
	return base + now.Sub(SeqEpoch).Milliseconds()*stepMax
}
