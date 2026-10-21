package domain

import (
	"fmt"
	"strconv"
	"strings"
)

// Decimal is a non-negative fixed-point number: Units × 10^-Scale. Prices and
// quantities are built from it so they print exactly, never through a float.
type Decimal struct {
	Units int64
	Scale int
}

// ParseDecimal reads a plain non-negative decimal such as "62770" or "0.1".
func ParseDecimal(s string) (Decimal, error) {
	s = strings.TrimSpace(s)
	whole, frac, _ := strings.Cut(s, ".")
	if whole == "" || !digits(whole) || !digits(frac) || len(frac) > 18 {
		return Decimal{}, fmt.Errorf("%q is not a plain decimal like 62770 or 0.1", s)
	}
	units, err := strconv.ParseInt(whole+frac, 10, 64)
	if err != nil {
		return Decimal{}, fmt.Errorf("%q is too big: %w", s, err)
	}
	return Decimal{Units: units, Scale: len(frac)}, nil
}

// String prints d with all Scale decimals: 6277010 at scale 2 is "62770.10".
func (d Decimal) String() string { return format(d.Units, d.Scale, false) }

// at returns d's units at a scale at least d.Scale.
func (d Decimal) at(scale int) int64 {
	u := d.Units
	for i := d.Scale; i < scale; i++ {
		u *= 10
	}
	return u
}

// format prints units × 10^-scale. With trim, trailing zeros (and a bare dot)
// are dropped: 627700 at scale 1 prints "62770".
func format(units int64, scale int, trim bool) string {
	s := strconv.FormatInt(units, 10)
	if scale == 0 {
		return s
	}
	if len(s) <= scale {
		s = strings.Repeat("0", scale-len(s)+1) + s
	}
	out := s[:len(s)-scale] + "." + s[len(s)-scale:]
	if trim {
		out = strings.TrimRight(strings.TrimRight(out, "0"), ".")
	}
	return out
}

func digits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
