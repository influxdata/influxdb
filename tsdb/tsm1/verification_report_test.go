package tsm1_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/tsdb/tsm1"
)

func TestVerificationReport_VerifyFile(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		dir := MustTempDir()
		defer os.RemoveAll(dir)

		// Write TSM file.
		if f, err := os.Create(filepath.Join(dir, "0000.tsm")); err != nil {
			t.Fatal(err)
		} else if w, err := tsm1.NewTSMWriter(f); err != nil {
			t.Fatal(err)
		} else if err := w.Write([]byte("cpu"), []tsm1.Value{tsm1.NewValue(0, 1.0)}); err != nil {
			t.Fatal(err)
		} else if err := w.WriteIndex(); err != nil {
			t.Fatal(err)
		} else if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		// Process single healthy file.
		cmd := NewVerificationReport()
		if stats, err := cmd.VerifyFile(filepath.Join(dir, "0000.tsm")); err != nil {
			t.Fatal(err)
		} else if got, want := stats.TotalBlockN, 1; got != want {
			t.Fatalf("TotalBlockN=%d, want %d", got, want)
		} else if got, want := stats.CorruptBlockN, 0; got != want {
			t.Fatalf("CorruptBlockN=%d, want %d", got, want)
		} else if stdout := cmd.Stdout.String(); !strings.Contains(stdout, "0000.tsm: healthy") {
			t.Fatal("expected health report")
		}
	})

	t.Run("ErrChecksumMismatch", func(t *testing.T) {
		dir := MustTempDir()
		defer os.RemoveAll(dir)

		// Generate TSM file data.
		var buf bytes.Buffer
		if w, err := tsm1.NewTSMWriter(&buf); err != nil {
			t.Fatal(err)
		} else if err := w.Write([]byte("cpu"), []tsm1.Value{tsm1.NewValue(0, 1.0)}); err != nil {
			t.Fatal(err)
		} else if err := w.WriteIndex(); err != nil {
			t.Fatal(err)
		} else if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		// Corrupt single non-checksum byte and write to disk.
		b := buf.Bytes()
		b[10] = 200
		if err := ioutil.WriteFile(filepath.Join(dir, "0000.tsm"), buf.Bytes(), 0666); err != nil {
			t.Fatal(err)
		}

		// Process single healthy file.
		cmd := NewVerificationReport()
		if stats, err := cmd.VerifyFile(filepath.Join(dir, "0000.tsm")); err != nil {
			t.Fatal(err)
		} else if got, want := stats.TotalBlockN, 1; got != want {
			t.Fatalf("TotalBlockN=%d, want %d", got, want)
		} else if got, want := stats.CorruptBlockN, 1; got != want {
			t.Fatalf("CorruptBlockN=%d, want %d", got, want)
		} else if stdout := cmd.Stdout.String(); !strings.Contains(stdout, "0000.tsm: got 0xe3f3ee14 but expected 0x1b277449 for key 0x637075, block 0") {
			t.Fatal("expected corruption report")
		}
	})
}

// VerificationReport represents a test wrapper for tsm1.VerificationReport.
type VerificationReport struct {
	*tsm1.VerificationReport

	Stdout bytes.Buffer
	Stderr bytes.Buffer
}

// NewVerificationReport returns a new instance of VerificationReport.
func NewVerificationReport() *VerificationReport {
	var cmd VerificationReport
	cmd.VerificationReport = tsm1.NewVerificationReport()
	cmd.VerificationReport.Stdout = &cmd.Stdout
	cmd.VerificationReport.Stderr = &cmd.Stderr
	if testing.Verbose() {
		cmd.VerificationReport.Stdout = io.MultiWriter(cmd.VerificationReport.Stdout, os.Stderr)
		cmd.VerificationReport.Stderr = io.MultiWriter(cmd.VerificationReport.Stderr, os.Stderr)
	}
	return &cmd
}
