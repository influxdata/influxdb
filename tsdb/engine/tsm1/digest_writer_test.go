package tsm1_test

import (
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// Test that an error is returned if a manifest isn't the first thing written
// to a digest.
func TestEngine_DigestManifestNotWritten(t *testing.T) {
	f := MustTempFile("")
	w, err := tsm1.NewDigestWriter(f)
	if err != nil {
		t.Fatalf("NewDigestWriter: %v", err)
	}
	defer w.Close()

	ts := &tsm1.DigestTimeSpan{}
	ts.Add(1, 2, 3, 4)

	if err := w.WriteTimeSpan("cpu", ts); err != tsm1.ErrNoDigestManifest {
		t.Fatalf("exp: tsm1.ErrNoDigestManifest, got: %v", err)
	}
}

// Test that a digest reader will skip over the manifest without error
// if needed.
func TestEngine_DigestReadSkipsManifest(t *testing.T) {
	f := MustTempFile("")
	w, err := tsm1.NewDigestWriter(f)
	if err != nil {
		t.Fatalf("NewDigestWriter: %v", err)
	}

	// Write an empty manifest.
	if err := w.WriteManifest(&tsm1.DigestManifest{}); err != nil {
		t.Fatal(err)
	}

	// Write a time span.
	ts := &tsm1.DigestTimeSpan{}
	ts.Add(1, 2, 3, 4)

	if err := w.WriteTimeSpan("cpu", ts); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Open the digest and create a reader.
	f, err = os.Open(f.Name())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	r, err := tsm1.NewDigestReader(f)
	if err != nil {
		t.Fatalf("NewDigestReader: %v", err)
	}

	// Test that we can read the timespan without first reading the manifest.
	key, ts, err := r.ReadTimeSpan()
	if err != nil {
		t.Fatal(err)
	} else if key != "cpu" {
		t.Fatalf("exp: cpu, got: %s", key)
	} else if len(ts.Ranges) != 1 {
		t.Fatalf("exp: 1, got: %d", len(ts.Ranges))
	} else if ts.Ranges[0].Min != 1 {
		t.Fatalf("exp: 1, got: %d", ts.Ranges[0].Min)
	} else if ts.Ranges[0].Max != 2 {
		t.Fatalf("exp: 1, got: %d", ts.Ranges[0].Min)
	} else if ts.Ranges[0].N != 3 {
		t.Fatalf("exp: 1, got: %d", ts.Ranges[0].N)
	} else if ts.Ranges[0].CRC != 4 {
		t.Fatalf("exp: 1, got: %d", ts.Ranges[0].CRC)
	}
}

// Test that we get an error if a digest manifest is written twice.
func TestEngine_DigestManifestDoubleWrite(t *testing.T) {
	f := MustTempFile("")
	w, err := tsm1.NewDigestWriter(f)
	if err != nil {
		t.Fatalf("NewDigestWriter: %v", err)
	}
	defer w.Close()

	if err := w.WriteManifest(&tsm1.DigestManifest{}); err != nil {
		t.Fatal(err)
	}

	if err := w.WriteManifest(&tsm1.DigestManifest{}); err != tsm1.ErrDigestAlreadyWritten {
		t.Fatalf("exp: %s, got: %s", tsm1.ErrDigestAlreadyWritten, err)
	}
}

// Test that we get an error if the manifest is read twice.
func TestEngine_DigestManifestDoubleRead(t *testing.T) {
	f := MustTempFile("")
	w, err := tsm1.NewDigestWriter(f)
	if err != nil {
		t.Fatalf("NewDigestWriter: %v", err)
	}

	// Write the manifest.
	if err := w.WriteManifest(&tsm1.DigestManifest{Dir: "test"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Open the digest and create a reader.
	f, err = os.Open(f.Name())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	r, err := tsm1.NewDigestReader(f)
	if err != nil {
		t.Fatalf("NewDigestReader: %v", err)
	}

	// Read the manifest.
	if m, err := r.ReadManifest(); err != nil {
		t.Fatal(err)
	} else if m.Dir != "test" {
		t.Fatalf("exp: test, got: %s", m.Dir)
	}

	// Attempt to read the manifest a second time (should fail).
	if _, err := r.ReadManifest(); err != tsm1.ErrDigestManifestAlreadyRead {
		t.Fatalf("exp: digest manifest already read, got: %v", err)
	}
}

// Test writing and reading a digest.
func TestEngine_DigestWriterReader(t *testing.T) {
	f := MustTempFile("")
	w, err := tsm1.NewDigestWriter(f)
	if err != nil {
		t.Fatalf("NewDigestWriter: %v", err)
	}

	if err := w.WriteManifest(&tsm1.DigestManifest{}); err != nil {
		t.Fatal(err)
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
