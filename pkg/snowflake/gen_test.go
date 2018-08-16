package snowflake

import (
	"fmt"
	"math/rand"
	"sort"
	"sync/atomic"
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

func TestMachineID(t *testing.T) {
	for i := 0; i < serverMax; i++ {
		assert.Equal(t, New(i).MachineID(), i)
	}
}

func TestNextMonotonic(t *testing.T) {
	g := New(10)
	out := make([]string, 10000)

	for i := range out {
		out[i] = g.NextString()
	}

	// ensure they are all distinct and increasing
	for i := range out[1:] {
		if out[i] >= out[i+1] {
			t.Fatal("bad entries:", out[i], out[i+1])
		}
	}
}

func BenchmarkEncode(b *testing.B) {
	b.ReportAllocs()
	var s [11]byte
	for i := 0; i < b.N; i++ {
		encode(&s, 100)
	}
}

var blackhole uint64 // to make sure the g.Next calls are not removed

func BenchmarkNext(b *testing.B) {
	g := New(10)

	for i := 0; i < b.N; i++ {
		blackhole += g.Next()
	}
}

func BenchmarkNextParallel(b *testing.B) {
	g := New(1)

	b.RunParallel(func(pb *testing.PB) {
		var lblackhole uint64
		for pb.Next() {
			lblackhole += g.Next()
		}
		atomic.AddUint64(&blackhole, lblackhole)
	})
}

func shuffle(n int, swap func(i, j int)) {
	for i := n - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		swap(i, j)
	}
}
