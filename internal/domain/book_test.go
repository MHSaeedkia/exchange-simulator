package domain

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func shape(t *testing.T) Shape {
	t.Helper()
	s, err := NewShape("100", "0.5", "0.001", "2.000")
	require.NoError(t, err)
	return s
}

func TestParseDecimal(t *testing.T) {
	d, err := ParseDecimal("62770.10")
	require.NoError(t, err)
	assert.Equal(t, Decimal{Units: 6277010, Scale: 2}, d)

	for _, bad := range []string{"", ".5", "-1", "1e3", "1.2.3", "abc"} {
		_, err := ParseDecimal(bad)
		assert.Error(t, err, bad)
	}
}

func TestFormat(t *testing.T) {
	assert.Equal(t, "62770", format(627700, 1, true))
	assert.Equal(t, "62770.5", format(627705, 1, true))
	assert.Equal(t, "0.001", format(1, 3, true))
	assert.Equal(t, "0.00100000", format(100000, 8, false))
}

func TestNewShapeRejects(t *testing.T) {
	for _, c := range [][4]string{
		{"100", "0", "1", "2"}, // no step
		{"1", "1", "1", "2"},   // base not above the step
		{"100", "1", "0", "2"}, // zero quantity would mean "delete"
		{"100", "1", "3", "2"}, // max below min
		{"100", "x", "1", "2"}, // not a number
	} {
		_, err := NewShape(c[0], c[1], c[2], c[3])
		assert.Error(t, err, c)
	}
}

func TestMaxDepthKeepsEveryBidAboveZero(t *testing.T) {
	s := shape(t) // base 100, step 0.5 → bids 99.5 … 0.5
	assert.Equal(t, 199, s.MaxDepth())
	b := NewBook(s, s.MaxDepth(), 1, rand.New(rand.NewSource(1)))
	_, bids := b.Top(b.Depth())
	assert.Equal(t, "0.5", bids[len(bids)-1].Price)
}

func TestNewBookLaysOutBothSidesFromTheBasePrice(t *testing.T) {
	b := NewBook(shape(t), 3, 1000, rand.New(rand.NewSource(1)))
	asks, bids := b.Top(3)

	assert.Equal(t, []string{"100", "100.5", "101"}, prices(asks))
	assert.Equal(t, []string{"99.5", "99", "98.5"}, prices(bids))
	for _, l := range append(asks, bids...) {
		assert.Len(t, l.Qty, len("0.001"), "quantity keeps the fixed scale")
	}
	assert.Equal(t, int64(1000), b.Seq())
}

func TestTopIsACopy(t *testing.T) {
	b := NewBook(shape(t), 3, 1, rand.New(rand.NewSource(1)))
	asks, _ := b.Top(3)
	asks[0].Qty = "changed"
	again, _ := b.Top(3)
	assert.NotEqual(t, "changed", again[0].Qty)
}

func TestTopNeverGoesPastTheDepth(t *testing.T) {
	b := NewBook(shape(t), 3, 1, rand.New(rand.NewSource(1)))
	asks, bids := b.Top(400)
	assert.Len(t, asks, 3)
	assert.Len(t, bids, 3)
}

func TestRequoteChangesNLevelsPerSideAndKeepsDepth(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	b := NewBook(shape(t), 10, 1, rng)
	asks, bids := b.Requote(4, rng)

	require.Len(t, asks, 4)
	require.Len(t, bids, 4)
	assert.Equal(t, 10, b.Depth())
	assert.IsIncreasing(t, floats(prices(asks)), "asks best first")
	assert.IsDecreasing(t, floats(prices(bids)), "bids best first")
	assert.Equal(t, int64(1), b.Seq(), "requoting does not move the sequence")

	// The changed quantities are now the book's.
	all, _ := b.Top(10)
	byPrice := map[string]string{}
	for _, l := range all {
		byPrice[l.Price] = l.Qty
	}
	for _, l := range asks {
		assert.Equal(t, byPrice[l.Price], l.Qty)
	}
}

func TestRequoteAboveDepthIsCappedAndZeroIsEmptyNotNil(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	b := NewBook(shape(t), 2, 1, rng)
	asks, bids := b.Requote(5, rng)
	assert.Len(t, asks, 2)
	assert.Len(t, bids, 2)

	asks, bids = b.Requote(0, rng)
	assert.NotNil(t, asks)
	assert.NotNil(t, bids)
	assert.Empty(t, asks)
}

func TestAdvance(t *testing.T) {
	b := NewBook(shape(t), 2, 1000, rand.New(rand.NewSource(1)))
	prev, next := b.Advance(7)
	assert.Equal(t, int64(1000), prev)
	assert.Equal(t, int64(1007), next)
	assert.Equal(t, int64(1007), b.Seq())
}

func TestPickIsDistinctSortedAndInRange(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	for range 200 {
		got := pick(rng, 10, 6)
		require.Len(t, got, 6)
		assert.IsIncreasing(t, got)
		assert.GreaterOrEqual(t, got[0], 0)
		assert.Less(t, got[5], 10)
	}
}

func prices(levels []Level) []string {
	out := make([]string, len(levels))
	for i, l := range levels {
		out[i] = l.Price
	}
	return out
}

func floats(ss []string) []float64 {
	out := make([]float64, len(ss))
	for i, s := range ss {
		d, _ := ParseDecimal(s)
		out[i] = float64(d.Units)
		for range d.Scale {
			out[i] /= 10
		}
	}
	return out
}
