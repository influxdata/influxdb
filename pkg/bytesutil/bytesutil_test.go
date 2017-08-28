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
