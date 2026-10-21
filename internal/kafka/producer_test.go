package kafka

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewIDIsAVersion4UUID(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := map[string]bool{}
	for range 100 {
		id := NewID()
		assert.Regexp(t, re, id)
		assert.False(t, seen[id])
		seen[id] = true
	}
}
