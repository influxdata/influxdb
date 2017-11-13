package bytesutil_test

import (
	"bytes"
	"encoding/binary"
	"testing"

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
