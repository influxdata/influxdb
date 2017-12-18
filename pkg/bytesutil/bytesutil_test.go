package bytesutil_test

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/pkg/bytesutil"
)

func TestSearchBytes(t *testing.T) {
	in := toByteSlices("bbb", "ccc", "eee", "fff", "ggg", "hhh")
	tests := []struct {
		name string
		x    string
		exp  int
	}{
		{"exists first", "bbb", 0},
		{"exists middle", "eee", 2},
		{"exists last", "hhh", 5},
		{"not exists last", "zzz", 6},
		{"not exists first", "aaa", 0},
		{"not exists mid", "ddd", 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := bytesutil.SearchBytes(in, []byte(test.x))
			if got != test.exp {
				t.Errorf("got %d, expected %d", got, test.exp)
			}
		})
	}
}

func TestContains(t *testing.T) {
	in := toByteSlices("bbb", "ccc", "eee", "fff", "ggg", "hhh")
	tests := []struct {
		name string
		x    string
		exp  bool
	}{
		{"exists first", "bbb", true},
		{"exists middle", "eee", true},
		{"exists last", "hhh", true},
		{"not exists last", "zzz", false},
		{"not exists first", "aaa", false},
		{"not exists mid", "ddd", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := bytesutil.Contains(in, []byte(test.x))
			if got != test.exp {
				t.Errorf("got %t, expected %t", got, test.exp)
			}
		})
	}
}

func toByteSlices(s ...string) [][]byte {
	r := make([][]byte, len(s))
	for i, v := range s {
		r[i] = []byte(v)
	}
	return r
}

func TestSortDedup(t *testing.T) {
	tests := []struct {
		name string
		in   [][]byte
		exp  [][]byte
	}{
		{
			name: "mixed dupes",
			in:   toByteSlices("bbb", "aba", "bbb", "aba", "ccc", "bbb", "aba"),
			exp:  toByteSlices("aba", "bbb", "ccc"),
		},
		{
			name: "no dupes",
			in:   toByteSlices("bbb", "ccc", "ddd"),
			exp:  toByteSlices("bbb", "ccc", "ddd"),
		},
		{
			name: "dupe at end",
			in:   toByteSlices("ccc", "ccc", "aaa"),
			exp:  toByteSlices("aaa", "ccc"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out := bytesutil.SortDedup(test.in)
			if !cmp.Equal(out, test.exp) {
				t.Error("invalid result")
			}
		})
	}
}

func BenchmarkSortDedup(b *testing.B) {
	data := toByteSlices("bbb", "aba", "bbb", "aba", "ccc", "bbb", "aba")
	in := append([][]byte{}, data...)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		x := in
		x = bytesutil.SortDedup(x)
		copy(in, data)
	}
}

func BenchmarkContains_True(b *testing.B) {
	var in [][]byte
	for i := 'a'; i <= 'z'; i++ {
		in = append(in, []byte(strings.Repeat(string(i), 3)))
	}
	for i := 0; i < b.N; i++ {
		bytesutil.Contains(in, []byte("xxx"))
	}
}

func BenchmarkContains_False(b *testing.B) {
	var in [][]byte
	for i := 'a'; i <= 'z'; i++ {
		in = append(in, []byte(strings.Repeat(string(i), 3)))
	}
	for i := 0; i < b.N; i++ {
		bytesutil.Contains(in, []byte("a"))
	}
}

func BenchmarkSearchBytes_Exists(b *testing.B) {
	var in [][]byte
	for i := 'a'; i <= 'z'; i++ {
		in = append(in, []byte(strings.Repeat(string(i), 3)))
	}
	for i := 0; i < b.N; i++ {
		bytesutil.SearchBytes(in, []byte("xxx"))
	}
}

func BenchmarkSearchBytes_NotExits(b *testing.B) {
	var in [][]byte
	for i := 'a'; i <= 'z'; i++ {
		in = append(in, []byte(strings.Repeat(string(i), 3)))
	}
	for i := 0; i < b.N; i++ {
		bytesutil.SearchBytes(in, []byte("a"))
	}
}
