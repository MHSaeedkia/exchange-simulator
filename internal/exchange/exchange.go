// Package exchange is the contract every simulated exchange implements. Each exchange lives
// in its own sub-package (exchange/okx, …) and knows only its own wire protocol: how a
// client subscribes, how a frame looks, how the sequence number moves, what its REST
// snapshot answers, and what NiFi adds before publishing. Books, rate, sockets, TLS, Kafka
// and the report are shared and live elsewhere.
//
// Mock ordering (place an order → its result on a private WebSocket) is planned for the
// exchanges that support it. It will be a second, optional interface next to this one; the
// market-data contract below does not change for it.
package exchange

import (
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"exchange-simulator/internal/domain"
)

// Session is one WebSocket connection, as an exchange sees it.
type Session interface {
	ID() string
	// Send queues msg. It blocks while the queue is full and returns false once the
	// connection is gone.
	Send(msg []byte) bool
	// Subscribe sends ack, then — when the exchange is running — a snapshot of the
	// market's book, then its updates. Unsubscribe sends ack and stops the market.
	Subscribe(market string, ack []byte)
	Unsubscribe(market string, ack []byte)
}

// Books reads a market's current book, for a REST snapshot.
type Books interface {
	// Top is the best n levels per side and the book's current sequence number.
	Top(market string, n int) (asks, bids []domain.Level, seq int64)
}

// Exchange is one simulated exchange.
type Exchange interface {
	// Name is the exchange's key: "okx". It is the .env prefix (OKX_…) and the control
	// API's path segment.
	Name() string
	// ID is lpa-core's exchanges.id: 8 for okx.
	ID() int64
	// WS is where its market-data WebSocket is served.
	WS() domain.Endpoint
	// REST is where its REST book snapshot is served; ok is false when it has none.
	REST() (e domain.Endpoint, ok bool)

	// SeqStep is how far a market's sequence number moves with one update.
	SeqStep(rng *rand.Rand) int64
	// Snapshot is a whole-book frame at b's current sequence number.
	Snapshot(market string, b *domain.Book, now time.Time) []byte
	// Update is a frame of the levels that changed; b is already advanced and prevSeq is
	// the sequence number before it.
	Update(market string, b *domain.Book, asks, bids []domain.Level, prevSeq int64, now time.Time) []byte

	// OnMessage answers one client text message: subscribe, unsubscribe, ping, or an error.
	OnMessage(s Session, msg []byte)
	// ServeREST answers the REST snapshot endpoint.
	ServeREST(w http.ResponseWriter, r *http.Request, books Books)

	// Envelope is a frame as NiFi would publish it to ex{id}-raw (kafka mode).
	Envelope(frame []byte, id string, simulation int) []byte
}

// PrependIDAndSimulation is NiFi's ReplaceText step for a JSON-object payload
// (`^\s*\{` → `{"id":"${uuid}","simulation":0,`): "id" and "simulation" become the first
// two fields.
func PrependIDAndSimulation(frame []byte, id string, simulation int) []byte {
	head := `{"id":"` + id + `","simulation":` + strconv.Itoa(simulation) + `,`
	out := make([]byte, 0, len(head)+len(frame))
	return append(append(out, head...), frame[1:]...)
}

// Millis is a time as the epoch-milliseconds string most exchanges send.
func Millis(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }
