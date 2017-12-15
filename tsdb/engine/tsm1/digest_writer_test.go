package tsm1_test

import (
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func TestEngine_DigestWriterReader(t *testing.T) {
	f := MustTempFile("")
	w, err := tsm1.NewDigestWriter(f)
	if err != nil {
		t.Fatalf("NewDigestWriter: %v", err)
	}

	ts := &tsm1.DigestTimeSpan{}
	ts.Add(1, 2, 3, 4)

	if err := w.WriteTimeSpan("cpu", ts); err != nil {
		t.Fatalf("WriteTimeSpan: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err = os.Open(f.Name())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	r, err := tsm1.NewDigestReader(f)
	if err != nil {
		t.Fatalf("NewDigestReader: %v", err)
	}
	for {

		key, ts, err := r.ReadTimeSpan()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("ReadTimeSpan: %v", err)
		}

		if exp, got := "cpu", key; exp != got {
			t.Fatalf("key mismatch: exp %v, got %v", exp, got)
		}

		if exp, got := 1, len(ts.Ranges); exp != got {
			t.Fatalf("range len mismatch: exp %v, got %v", exp, got)
		}

		exp := tsm1.DigestTimeRange{Min: 1, Max: 2, N: 3, CRC: 4}
		if got := ts.Ranges[0]; !reflect.DeepEqual(exp, got) {
			t.Fatalf("time range mismatch: exp %v, got %v", exp, got)
		}
	}
}
