package slices

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/pkg/bytesutil"
)

func TestCopyChunkedByteSlices_oneChunk(t *testing.T) {
	src := [][]byte{
		[]byte("influx"),
		[]byte("data"),
	}

	dst := CopyChunkedByteSlices(src, 3)
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("destination should match source src: %v dst: %v", src, dst)
	}

	dst[0][1] = 'z'
	if reflect.DeepEqual(src, dst) {
		t.Error("destination should not match source")
	}
}

func TestCopyChunkedByteSlices_multipleChunks(t *testing.T) {
	src := [][]byte{
		[]byte("influx"),
		[]byte("data"),
		[]byte("is"),
		[]byte("the"),
		[]byte("best"),
		[]byte("time"),
		[]byte("series"),
		[]byte("database"),
		[]byte("in"),
		[]byte("the"),
		[]byte("whole"),
		[]byte("wide"),
		[]byte("world"),
		[]byte(":-)"),
	}

	chunkSize := 4
	dst := CopyChunkedByteSlices(src, chunkSize)
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("destination should match source src: %v dst: %v", src, dst)
	}

	for i := 0; i < int(math.Ceil(float64(len(src))/float64(chunkSize))); i++ {
		thisChunkSize := chunkSize
		if len(src)-thisChunkSize*i < thisChunkSize {
			thisChunkSize = len(src) - thisChunkSize*i
		}

		chunk := dst[i*thisChunkSize : (i+1)*thisChunkSize]

		for j := 0; j < thisChunkSize-1; j++ {
			a := (*reflect.SliceHeader)(unsafe.Pointer(&chunk[j]))
			b := (*reflect.SliceHeader)(unsafe.Pointer(&chunk[j+1]))
			if b.Data-a.Data != uintptr(a.Len) {
				t.Error("chunk elements do not appear to be adjacent, so not part of one chunk")
			}
			if a.Cap != a.Len {
				t.Errorf("slice length != capacity; %d vs %d", a.Len, a.Cap)
			}
			if b.Cap != b.Len {
				t.Errorf("slice length != capacity; %d vs %d", b.Len, b.Cap)
			}
		}
	}

	dst[0][5] = 'z'
	if reflect.DeepEqual(src, dst) {
		t.Error("destination should not match source")
	}
}

const NIL = "<nil>"

// ss returns a sorted slice of byte slices.
func ss(s ...string) [][]byte {
	r := make([][]byte, len(s))
	for i := range s {
		if s[i] != NIL {
			r[i] = []byte(s[i])
		}
	}
	bytesutil.Sort(r)
	return r
}

func TestCompareSlice(t *testing.T) {
	name := func(a, b [][]byte, exp int) string {
		var as string
		if a != nil {
			as = string(bytes.Join(a, nil))
		} else {
			as = NIL
		}
		var bs string
		if b != nil {
			bs = string(bytes.Join(b, nil))
		} else {
			bs = NIL
		}
		return fmt.Sprintf("%s <=> %s is %d", as, bs, exp)
	}
	tests := []struct {
		a, b [][]byte
		exp  int
	}{
		{
			a:   ss("aaa", "bbb", "ccc"),
			b:   ss("aaa", "bbb", "ccc"),
			exp: 0,
		},

		{
			a:   ss("aaa", "bbb", "ccc", "ddd"),
			b:   ss("aaa", "bbb", "ccc"),
			exp: 1,
		},

		{
			a:   ss("aaa", "bbb"),
			b:   ss("aaa", "bbb", "ccc"),
			exp: -1,
		},

		{
			a:   ss("aaa", "bbbb"),
			b:   ss("aaa", "bbb", "ccc"),
			exp: 1,
		},

		{
			a:   ss("aaa", "ccc"),
			b:   ss("aaa", "bbb", "ccc"),
			exp: 1,
		},

		{
			a:   ss("aaa", "bbb", NIL),
			b:   ss("aaa", "bbb", "ccc"),
			exp: -1,
		},

		{
			a:   ss("aaa", NIL, "ccc"),
			b:   ss("aaa", NIL, "ccc"),
			exp: 0,
		},

		{
			a:   ss(NIL, "bbb", "ccc"),
			b:   ss("aaa", "bbb", "ccc"),
			exp: -1,
		},

		{
			a:   ss("aaa", "aaa"),
			b:   ss("aaa", "bbb", "ccc"),
			exp: -1,
		},

		{
			a:   nil,
			b:   ss("aaa", "bbb", "ccc"),
			exp: -1,
		},

		{
			a:   ss("aaa", "bbb"),
			b:   nil,
			exp: 1,
		},

		{
			a:   nil,
			b:   nil,
			exp: 0,
		},

		{
			a:   [][]byte{},
			b:   nil,
			exp: 0,
		},
	}
	for _, test := range tests {
		t.Run(name(test.a, test.b, test.exp), func(t *testing.T) {
			if got := CompareSlice(test.a, test.b); got != test.exp {
				t.Errorf("unexpected result, -got/+exp\n%s", cmp.Diff(got, test.exp))
			}
		})
	}
}
