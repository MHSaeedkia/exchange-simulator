package domain

import (
	"fmt"
	"math/rand"
	"sort"
)

// Shape is how every book of an exchange is laid out: asks start at the base price and go
// up by the step, bids start one step below it and go down. Quantities are random between
// QtyMin and QtyMax.
type Shape struct {
	priceScale int
	base, step int64

	qtyScale       int
	qtyMin, qtyMax int64
}

// NewShape parses the four decimals that come from .env.
func NewShape(basePrice, priceStep, qtyMin, qtyMax string) (Shape, error) {
	var ds [4]Decimal
	for i, raw := range []string{basePrice, priceStep, qtyMin, qtyMax} {
		d, err := ParseDecimal(raw)
		if err != nil {
			return Shape{}, err
		}
		ds[i] = d
	}
	base, step, lo, hi := ds[0], ds[1], ds[2], ds[3]

	priceScale := max(base.Scale, step.Scale)
	qtyScale := max(lo.Scale, hi.Scale)
	s := Shape{
		priceScale: priceScale, base: base.at(priceScale), step: step.at(priceScale),
		qtyScale: qtyScale, qtyMin: lo.at(qtyScale), qtyMax: hi.at(qtyScale),
	}
	switch {
	case s.step <= 0:
		return Shape{}, fmt.Errorf("price step must be above 0")
	case s.base <= s.step:
		return Shape{}, fmt.Errorf("base price must be above the price step")
	case s.qtyMin <= 0:
		// A quantity of 0 means "delete this level" on most exchanges' wire.
		return Shape{}, fmt.Errorf("minimum quantity must be above 0")
	case s.qtyMax < s.qtyMin:
		return Shape{}, fmt.Errorf("maximum quantity must not be below the minimum")
	}
	return s, nil
}

// MaxDepth is the deepest book whose lowest bid is still above 0.
func (s Shape) MaxDepth() int {
	return int((s.base - 1) / s.step)
}

// Level is one price level, as plain decimal strings. Each exchange writes it in its own
// wire form.
type Level struct {
	Price string
	Qty   string
}

// Book is one market's book and the last sequence number its frames carried. Each exchange
// decides how that number moves. It is not safe for concurrent use.
type Book struct {
	shape Shape
	asks  []Level // price ascending, best first
	bids  []Level // price descending, best first
	seq   int64
}

// NewBook builds a full book of depth levels per side whose last sequence number is seq.
func NewBook(shape Shape, depth int, seq int64, rng *rand.Rand) *Book {
	b := &Book{shape: shape, seq: seq, asks: make([]Level, depth), bids: make([]Level, depth)}
	for i := range depth {
		b.asks[i] = Level{shape.price(shape.base + int64(i)*shape.step), shape.qty(rng)}
		b.bids[i] = Level{shape.price(shape.base - int64(i+1)*shape.step), shape.qty(rng)}
	}
	return b
}

// Seq is the sequence number of the last frame this book produced.
func (b *Book) Seq() int64 { return b.seq }

// Depth is the number of levels per side.
func (b *Book) Depth() int { return len(b.asks) }

// Top is a copy of the best n levels of each side.
func (b *Book) Top(n int) (asks, bids []Level) {
	return clone(b.asks[:min(n, len(b.asks))]), clone(b.bids[:min(n, len(b.bids))])
}

// Requote gives n random levels per side a new quantity (the depth stays the same) and
// returns the changed levels, best first. It does not move the sequence number.
func (b *Book) Requote(n int, rng *rand.Rand) (asks, bids []Level) {
	return b.requote(b.asks, n, rng), b.requote(b.bids, n, rng)
}

// Advance moves the sequence number forward by step and returns the old and the new one.
func (b *Book) Advance(step int64) (prev, next int64) {
	prev = b.seq
	b.seq += step
	return prev, b.seq
}

func (b *Book) requote(side []Level, n int, rng *rand.Rand) []Level {
	idx := pick(rng, len(side), min(n, len(side)))
	out := make([]Level, 0, len(idx))
	for _, i := range idx {
		side[i].Qty = b.shape.qty(rng)
		out = append(out, side[i])
	}
	return out
}

func (s Shape) price(units int64) string { return format(units, s.priceScale, true) }

func (s Shape) qty(rng *rand.Rand) string {
	return format(s.qtyMin+rng.Int63n(s.qtyMax-s.qtyMin+1), s.qtyScale, false)
}

func clone(side []Level) []Level { return append([]Level{}, side...) }

// pick returns n distinct indices below length, in ascending order (Floyd's sampling:
// O(n), whatever the depth).
func pick(rng *rand.Rand, length, n int) []int {
	chosen := make(map[int]bool, n)
	for j := length - n; j < length; j++ {
		if t := rng.Intn(j + 1); chosen[t] {
			chosen[j] = true
		} else {
			chosen[t] = true
		}
	}
	out := make([]int, 0, n)
	for i := range chosen {
		out = append(out, i)
	}
	sort.Ints(out)
	return out
}
