package tsdb_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/tsdb"
)

func TestSeriesSegment(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	// Create a new initial segment (4mb) and initialize for writing.
	segment, err := tsdb.CreateSeriesSegment(0, filepath.Join(dir, "0000"))
	if err != nil {
		t.Fatal(err)
	} else if err := segment.InitForWrite(); err != nil {
		t.Fatal(err)
	}
	defer segment.Close()

	// Write initial entry.
	key1 := tsdb.AppendSeriesKey(nil, []byte("m0"), nil)
	offset, err := segment.WriteLogEntry(tsdb.AppendSeriesEntry(nil, tsdb.SeriesEntryInsertFlag, 1, key1))
	if err != nil {
		t.Fatal(err)
	} else if offset != tsdb.SeriesSegmentHeaderSize {
		t.Fatalf("unexpected offset: %d", offset)
	}

	// Write a large entry (3mb).
	key2 := tsdb.AppendSeriesKey(nil, bytes.Repeat([]byte("m"), 3*(1<<20)), nil)
	if _, err := segment.WriteLogEntry(tsdb.AppendSeriesEntry(nil, tsdb.SeriesEntryInsertFlag, 2, key2)); err != nil {
		t.Fatal(err)
	} else if offset != tsdb.SeriesSegmentHeaderSize {
		t.Fatalf("unexpected offset: %d", offset)
	}

	// Write another entry that is too large for the remaining segment space.
	if _, err := segment.WriteLogEntry(tsdb.AppendSeriesEntry(nil, tsdb.SeriesEntryInsertFlag, 3, tsdb.AppendSeriesKey(nil, bytes.Repeat([]byte("n"), 3*(1<<20)), nil))); err != tsdb.ErrSeriesSegmentNotWritable {
		t.Fatalf("unexpected error: %s", err)
	}

	// Verify two entries exist.
	var n int
	segment.ForEachEntry(func(flag uint8, id uint64, offset int64, key []byte) error {
		switch n {
		case 0:
			if flag != tsdb.SeriesEntryInsertFlag || id != 1 || !bytes.Equal(key1, key) {
				t.Fatalf("unexpected entry(0): %d, %d, %q", flag, id, key)
			}
		case 1:
			if flag != tsdb.SeriesEntryInsertFlag || id != 2 || !bytes.Equal(key2, key) {
				t.Fatalf("unexpected entry(1): %d, %d, %q", flag, id, key)
			}
		default:
			t.Fatalf("too many entries")
		}
		n++
		return nil
	})
	if n != 2 {
		t.Fatalf("unexpected entry count: %d", n)
	}
}

func TestSeriesSegment_AppendSeriesIDs(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	segment, err := tsdb.CreateSeriesSegment(0, filepath.Join(dir, "0000"))
	if err != nil {
		t.Fatal(err)
	} else if err := segment.InitForWrite(); err != nil {
		t.Fatal(err)
	}
	defer segment.Close()

	// Write entries.
	if _, err := segment.WriteLogEntry(tsdb.AppendSeriesEntry(nil, tsdb.SeriesEntryInsertFlag, 10, tsdb.AppendSeriesKey(nil, []byte("m0"), nil))); err != nil {
		t.Fatal(err)
	} else if _, err := segment.WriteLogEntry(tsdb.AppendSeriesEntry(nil, tsdb.SeriesEntryInsertFlag, 11, tsdb.AppendSeriesKey(nil, []byte("m1"), nil))); err != nil {
		t.Fatal(err)
	} else if err := segment.Flush(); err != nil {
		t.Fatal(err)
	}

	// Collect series ids with existing set.
	a := segment.AppendSeriesIDs([]uint64{1, 2})
	if diff := cmp.Diff(a, []uint64{1, 2, 10, 11}); diff != "" {
		t.Fatal(diff)
	}
}

func TestSeriesSegment_MaxSeriesID(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	segment, err := tsdb.CreateSeriesSegment(0, filepath.Join(dir, "0000"))
	if err != nil {
		t.Fatal(err)
	} else if err := segment.InitForWrite(); err != nil {
		t.Fatal(err)
	}
	defer segment.Close()

	// Write entries.
	if _, err := segment.WriteLogEntry(tsdb.AppendSeriesEntry(nil, tsdb.SeriesEntryInsertFlag, 10, tsdb.AppendSeriesKey(nil, []byte("m0"), nil))); err != nil {
		t.Fatal(err)
	} else if _, err := segment.WriteLogEntry(tsdb.AppendSeriesEntry(nil, tsdb.SeriesEntryInsertFlag, 11, tsdb.AppendSeriesKey(nil, []byte("m1"), nil))); err != nil {
		t.Fatal(err)
	} else if err := segment.Flush(); err != nil {
		t.Fatal(err)
	}

	// Verify maximum.
	if max := segment.MaxSeriesID(); max != 11 {
		t.Fatalf("unexpected max: %d", max)
	}
}

func TestSeriesSegmentHeader(t *testing.T) {
	// Verify header initializes correctly.
	hdr := tsdb.NewSeriesSegmentHeader()
	if hdr.Version != tsdb.SeriesSegmentVersion {
		t.Fatalf("unexpected version: %d", hdr.Version)
	}

	// Marshal/unmarshal.
	var buf bytes.Buffer
	if _, err := hdr.WriteTo(&buf); err != nil {
		t.Fatal(err)
	} else if other, err := tsdb.ReadSeriesSegmentHeader(buf.Bytes()); err != nil {
		t.Fatal(err)
	} else if diff := cmp.Diff(hdr, other); diff != "" {
		t.Fatal(diff)
	}
}

func TestSeriesSegment_PartialWrite(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	// Create a new initial segment (4mb) and initialize for writing.
	segment, err := tsdb.CreateSeriesSegment(0, filepath.Join(dir, "0000"))
	if err != nil {
		t.Fatal(err)
	} else if err := segment.InitForWrite(); err != nil {
		t.Fatal(err)
	}
	defer segment.Close()

	// Write two entries.
	if _, err := segment.WriteLogEntry(tsdb.AppendSeriesEntry(nil, tsdb.SeriesEntryInsertFlag, 1, tsdb.AppendSeriesKey(nil, []byte("A"), nil))); err != nil {
		t.Fatal(err)
	} else if _, err := segment.WriteLogEntry(tsdb.AppendSeriesEntry(nil, tsdb.SeriesEntryInsertFlag, 2, tsdb.AppendSeriesKey(nil, []byte("B"), nil))); err != nil {
		t.Fatal(err)
	}
	sz := segment.Size()
	entrySize := len(tsdb.AppendSeriesEntry(nil, tsdb.SeriesEntryInsertFlag, 2, tsdb.AppendSeriesKey(nil, []byte("B"), nil)))

	// Close segment.
	if err := segment.Close(); err != nil {
		t.Fatal(err)
	}

	// Truncate at each point and reopen.
	for i := entrySize; i > 0; i-- {
		if err := os.Truncate(filepath.Join(dir, "0000"), sz-int64(entrySize-i)); err != nil {
			t.Fatal(err)
		}
		segment := tsdb.NewSeriesSegment(0, filepath.Join(dir, "0000"))
		if err := segment.Open(); err != nil {
			t.Fatal(err)
		} else if err := segment.InitForWrite(); err != nil {
			t.Fatal(err)
		} else if err := segment.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestJoinSeriesOffset(t *testing.T) {
	if offset := tsdb.JoinSeriesOffset(0x1234, 0x56789ABC); offset != 0x123456789ABC {
		t.Fatalf("unexpected offset: %x", offset)
	}
}

func TestSplitSeriesOffset(t *testing.T) {
	if segmentID, pos := tsdb.SplitSeriesOffset(0x123456789ABC); segmentID != 0x1234 || pos != 0x56789ABC {
		t.Fatalf("unexpected segmentID/pos: %x/%x", segmentID, pos)
	}
}

func TestIsValidSeriesSegmentFilename(t *testing.T) {
	if tsdb.IsValidSeriesSegmentFilename("") {
		t.Fatal("expected invalid")
	} else if tsdb.IsValidSeriesSegmentFilename("0ab") {
		t.Fatal("expected invalid")
	} else if !tsdb.IsValidSeriesSegmentFilename("192a") {
		t.Fatal("expected valid")
	}
}

func TestParseSeriesSegmentFilename(t *testing.T) {
	if v, err := tsdb.ParseSeriesSegmentFilename("a90b"); err != nil {
		t.Fatal(err)
	} else if v != 0xA90B {
		t.Fatalf("unexpected value: %x", v)
	}
	if v, err := tsdb.ParseSeriesSegmentFilename("0001"); err != nil {
		t.Fatal(err)
	} else if v != 1 {
		t.Fatalf("unexpected value: %x", v)
	}
	if _, err := tsdb.ParseSeriesSegmentFilename("invalid"); err == nil {
		t.Fatal("expected error")
	}
}

func TestSeriesSegmentSize(t *testing.T) {
	const mb = (1 << 20)
	if sz := tsdb.SeriesSegmentSize(0); sz != 4*mb {
		t.Fatalf("unexpected size: %d", sz)
	} else if sz := tsdb.SeriesSegmentSize(1); sz != 8*mb {
		t.Fatalf("unexpected size: %d", sz)
	} else if sz := tsdb.SeriesSegmentSize(2); sz != 16*mb {
		t.Fatalf("unexpected size: %d", sz)
	} else if sz := tsdb.SeriesSegmentSize(3); sz != 32*mb {
		t.Fatalf("unexpected size: %d", sz)
	} else if sz := tsdb.SeriesSegmentSize(4); sz != 64*mb {
		t.Fatalf("unexpected size: %d", sz)
	} else if sz := tsdb.SeriesSegmentSize(5); sz != 128*mb {
		t.Fatalf("unexpected size: %d", sz)
	} else if sz := tsdb.SeriesSegmentSize(6); sz != 256*mb {
		t.Fatalf("unexpected size: %d", sz)
	} else if sz := tsdb.SeriesSegmentSize(7); sz != 256*mb {
		t.Fatalf("unexpected size: %d", sz)
	}
}

func TestSeriesEntry(t *testing.T) {
	seriesKey := tsdb.AppendSeriesKey(nil, []byte("m0"), nil)
	buf := tsdb.AppendSeriesEntry(nil, 1, 2, seriesKey)
	if flag, id, key, sz := tsdb.ReadSeriesEntry(buf); flag != 1 {
		t.Fatalf("unexpected flag: %d", flag)
	} else if id != 2 {
		t.Fatalf("unexpected id: %d", id)
	} else if !bytes.Equal(seriesKey, key) {
		t.Fatalf("unexpected key: %q", key)
	} else if sz != int64(tsdb.SeriesEntryHeaderSize+len(key)) {
		t.Fatalf("unexpected size: %d", sz)
	}
}
