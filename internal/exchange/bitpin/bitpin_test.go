package bitpin

import (
	"encoding/json"
	"math/rand"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"exchange-simulator/internal/domain"
)

var fixed = time.Date(2026, 10, 21, 19, 14, 48, 495001789, time.UTC)

func book(t *testing.T, depth int) *domain.Book {
	t.Helper()
	shape, err := domain.NewShape("84000", "0.01", "0.00000001", "0.20000000")
	require.NoError(t, err)
	return domain.NewBook(shape, depth, 3873262, rand.New(rand.NewSource(1)))
}

type data struct {
	Bids      [][]string `json:"bids"`
	Asks      [][]string `json:"asks"`
	VolumeAsk string     `json:"volume_ask"`
	VolumeBid string     `json:"volume_bid"`
	Symbol    string     `json:"symbol"`
	Event     string     `json:"event"`
	Price     string     `json:"price"`
	EventTime string     `json:"event_time"`
}

type frame struct {
	Push struct {
		Channel string `json:"channel"`
		Pub     struct {
			Data   data  `json:"data"`
			Offset int64 `json:"offset"`
		} `json:"pub"`
	} `json:"push"`
}

// The push must read like the Centrifugo publication lpa-core's BitpinParser takes
// (docs/sample-raw-data.md § ex2) and look like a real one, captured 2026-10-21:
// {"push":{"channel":"orderbook:BTC_USDT","pub":{"data":{"bids": [["84264.78", "0.00713113"], …],
// "asks": […], "volume_ask": "2.52660052", "volume_bid": "2.33289293", "symbol": "BTC_USDT",
// "event": "market_data", "price": "84300.00", "event_time": "2026-10-21T19:14:48.495001Z"},"offset":3873263}}}
// The data object is written with a space after every ':' and ',', as bitpin does.
func TestPushHasBitpinShape(t *testing.T) {
	b := book(t, 50)
	raw := New(Options{ID: 2, PriceDecimals: 2}).Snapshot("BTC_USDT", b, fixed)

	var f frame
	require.NoError(t, json.Unmarshal(raw, &f), string(raw))
	assert.Equal(t, "orderbook:BTC_USDT", f.Push.Channel, "the market key, and NiFi's subscribe channel")
	assert.Equal(t, int64(3873262), f.Push.Pub.Offset)
	d := f.Push.Pub.Data
	assert.Len(t, d.Bids, 50)
	assert.Len(t, d.Asks, 50)
	assert.Equal(t, "BTC_USDT", d.Symbol)
	assert.Equal(t, "market_data", d.Event)
	assert.Equal(t, d.Bids[0][0], d.Price, "the book has no trades: the best bid stands in")
	assert.Equal(t, "2026-10-21T19:14:48.495001Z", d.EventTime, "ISO-8601 UTC with microseconds: the parser's Instant.parse")

	assert.Equal(t, "84000.00", d.Asks[0][0], "asks best (lowest) first; prices padded to two decimals, as bitpin writes them")
	assert.Equal(t, "83999.99", d.Bids[0][0], "bids best (highest) first")
	assert.Equal(t, "84000.10", d.Asks[10][0])
	asks, bids := b.Top(50)
	assert.Equal(t, sum(t, asks), d.VolumeAsk, "volume_ask is the sum of the asks' quantities")
	assert.Equal(t, sum(t, bids), d.VolumeBid)

	assert.Regexp(t, `^\{"push":\{"channel":"orderbook:BTC_USDT","pub":\{"data":\{"bids": \[\["[0-9.]+", "[0-9.]+"\], \[`, string(raw))
	assert.Regexp(t, regexp.QuoteMeta(`"]], "volume_ask": "`)+`[0-9.]+", "volume_bid": "[0-9.]+", "symbol": "BTC_USDT", "event": "market_data", "price": "[0-9.]+", "event_time": "[^"]+"\},"offset":3873262\}\}\}$`, string(raw))
}

// Every bitpin push resends the WHOLE book (lpa-core 2026-09-02): an update carries every level.
func TestUpdateIsTheWholeBookToo(t *testing.T) {
	ex, b := New(Options{ID: 2}), book(t, 50)
	rng := rand.New(rand.NewSource(2))
	asks, bids := b.Requote(3, rng)
	prev, _ := b.Advance(ex.SeqStep(rng))

	var f frame
	require.NoError(t, json.Unmarshal(ex.Update("BTC_USDT", b, asks, bids, prev, fixed), &f))
	assert.Len(t, f.Push.Pub.Data.Asks, 50)
	assert.Equal(t, prev+1, f.Push.Pub.Offset)
}

// The market string is the client's: it must stay valid JSON whatever it holds.
func TestOddMarketStaysValidJSON(t *testing.T) {
	var f frame
	require.NoError(t, json.Unmarshal(New(Options{ID: 2}).Snapshot(`A"B\C`, book(t, 2), fixed), &f))
	assert.Equal(t, `A"B\C`, f.Push.Pub.Data.Symbol)
}

func TestPad(t *testing.T) {
	assert.Equal(t, "84150.00", pad("84150", 2))
	assert.Equal(t, "84150.10", pad("84150.1", 2))
	assert.Equal(t, "84150.12", pad("84150.12", 2))
	assert.Equal(t, "84150", pad("84150", 0))
}

func sum(t *testing.T, side []domain.Level) string {
	t.Helper()
	var units int64
	scale := 0
	for _, l := range side {
		d, err := domain.ParseDecimal(l.Qty)
		require.NoError(t, err)
		units, scale = units+d.Units, d.Scale
	}
	return domain.Decimal{Units: units, Scale: scale}.String()
}

// session records what OnMessage asked for.
type session struct {
	sent       []string
	subscribed []string
}

func (s *session) ID() string         { return "c0e31519" }
func (s *session) Send(b []byte) bool { s.sent = append(s.sent, string(b)); return true }
func (s *session) Subscribe(m string, ack func(int64) []byte, _ bool) {
	s.subscribed = append(s.subscribed, m)
	s.Send(ack(0))
}
func (s *session) Unsubscribe(string, []byte) {}
func (s *session) Close(int, string)          {}

// The protocol is tested in package centrifugo; here only what is bitpin's: NiFi's channel
// `orderbook:${market}` and the version real bitpin names.
func TestBitpinChannelAndVersion(t *testing.T) {
	s := &session{}
	on := New(Options{ID: 2}).Connect(s)
	on([]byte(`{"id":1,"connect":{}}`))
	on([]byte(`{"id":1,"subscribe":{"channel":"orderbook:BTC_USDT"}}`))
	assert.Equal(t, []string{"BTC_USDT"}, s.subscribed)
	require.Len(t, s.sent, 2)
	assert.Contains(t, s.sent[0], `"version":"6.9.2 OSS"`)
}

func TestNoRest(t *testing.T) {
	_, ok := New(Options{ID: 2}).REST()
	assert.False(t, ok, "the live NiFi group has no REST call for bitpin")
}
