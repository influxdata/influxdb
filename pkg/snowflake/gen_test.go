package snowflake

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
)

func TestEncode(t *testing.T) {
	tests := []struct {
		v   uint64
		exp string
	}{
		{0x000, "00000000000"},
		{0x001, "00000000001"},
		{0x03f, "0000000000~"},
		{0x07f, "0000000001~"},
		{0xf07f07f07f07f07f, "F1~1~1~1~1~"},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("0x%03x→%s", test.v, test.exp), func(t *testing.T) {
			var s [11]byte
			encode(&s, test.v)
			assert.Equal(t, string(s[:]), test.exp)
		})
	}
}

// TestSorting verifies numbers using base 63 encoding are ordered according to their numerical representation.
func TestSorting(t *testing.T) {
	var (
		vals = make([]string, 1000)
		exp  = make([]string, 1000)
	)

	for i := 0; i < len(vals); i++ {
		var s [11]byte
		encode(&s, uint64(i*47))
		vals[i] = string(s[:])
		exp[i] = string(s[:])
	}

	// randomize them
	shuffle(len(vals), func(i, j int) {
		vals[i], vals[j] = vals[j], vals[i]
	})

	sort.Strings(vals)
	assert.Equal(t, vals, exp)
}

func BenchmarkEncode(b *testing.B) {
	b.ReportAllocs()
	var s [11]byte
	for i := 0; i < b.N; i++ {
		encode(&s, 100)
	}
}

func shuffle(n int, swap func(i, j int)) {
	for i := n - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		swap(i, j)
	}
}
