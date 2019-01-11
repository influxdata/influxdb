package bytesutil_test

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/pkg/bytesutil"
)

func TestSearchBytesFixed(t *testing.T) {
	n, sz := 5, 8
	a := make([]byte, n*sz) // 5 - 8 byte int64s

	for i := 0; i < 5; i++ {
		binary.BigEndian.PutUint64(a[i*sz:i*sz+sz], uint64(i))
	}

	var x [8]byte

	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint64(x[:], uint64(i))
		if exp, got := i*sz, bytesutil.SearchBytesFixed(a, len(x), func(v []byte) bool {
			return bytes.Compare(v, x[:]) >= 0
		}); exp != got {
			t.Fatalf("index mismatch: exp %v, got %v", exp, got)
		}
	}

	if exp, got := len(a)-1, bytesutil.SearchBytesFixed(a, 1, func(v []byte) bool {
		return bytes.Compare(v, []byte{99}) >= 0
	}); exp != got {
		t.Fatalf("index mismatch: exp %v, got %v", exp, got)
	}
}

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

func TestPack_WidthOne_One(t *testing.T) {
	a := make([]byte, 8)

	a[4] = 1

	a = bytesutil.Pack(a, 1, 0)
	if got, exp := len(a), 1; got != exp {
		t.Fatalf("len mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range []byte{1} {
		if got, exp := a[i], v; got != exp {
			t.Fatalf("value mismatch: a[%d] = %v, exp %v", i, got, exp)
		}
	}
}

func TestPack_WidthOne_Two(t *testing.T) {
	a := make([]byte, 8)

	a[4] = 1
	a[6] = 2

	a = bytesutil.Pack(a, 1, 0)
	if got, exp := len(a), 2; got != exp {
		t.Fatalf("len mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range []byte{1, 2} {
		if got, exp := a[i], v; got != exp {
			t.Fatalf("value mismatch: a[%d] = %v, exp %v", i, got, exp)
		}
	}
}

func TestPack_WidthTwo_Two(t *testing.T) {
	a := make([]byte, 8)

	a[2] = 1
	a[3] = 1
	a[6] = 2
	a[7] = 2

	a = bytesutil.Pack(a, 2, 0)
	if got, exp := len(a), 4; got != exp {
		t.Fatalf("len mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range []byte{1, 1, 2, 2} {
		if got, exp := a[i], v; got != exp {
			t.Fatalf("value mismatch: a[%d] = %v, exp %v", i, got, exp)
		}
	}
}

func TestPack_WidthOne_Last(t *testing.T) {
	a := make([]byte, 8)

	a[6] = 2
	a[7] = 2

	a = bytesutil.Pack(a, 2, 255)
	if got, exp := len(a), 8; got != exp {
		t.Fatalf("len mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range []byte{0, 0, 0, 0, 0, 0, 2, 2} {
		if got, exp := a[i], v; got != exp {
			t.Fatalf("value mismatch: a[%d] = %v, exp %v", i, got, exp)
		}
	}
}

func TestPack_WidthOne_LastFill(t *testing.T) {
	a := make([]byte, 8)

	a[0] = 255
	a[1] = 255
	a[2] = 2
	a[3] = 2
	a[4] = 2
	a[5] = 2
	a[6] = 2
	a[7] = 2

	a = bytesutil.Pack(a, 2, 255)
	if got, exp := len(a), 6; got != exp {
		t.Fatalf("len mismatch: got %v, exp %v", got, exp)
	}

	for i, v := range []byte{2, 2, 2, 2, 2, 2} {
		if got, exp := a[i], v; got != exp {
			t.Fatalf("value mismatch: a[%d] = %v, exp %v", i, got, exp)
		}
	}
}

var result [][]byte

func BenchmarkSortDedup(b *testing.B) {
	b.Run("sort-deduplicate", func(b *testing.B) {
		data := toByteSlices("bbb", "aba", "bbb", "aba", "ccc", "bbb", "aba")
		in := append([][]byte{}, data...)
		b.ReportAllocs()

		copy(in, data)
		for i := 0; i < b.N; i++ {
			result = bytesutil.SortDedup(in)

			b.StopTimer()
			copy(in, data)
			b.StartTimer()
		}
	})
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
