package tsm1_test

import (
	"testing"
	"time"

	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

// TestDataFiles_Add tests that adding the same file multiple times does not
// create duplicates
func TestDataFiles_Add(t *testing.T) {
	f1 := &FakeDataFile{name: "file1"}
	f2 := &FakeDataFile{name: "file2"}
	d := &tsm1.DataFiles{}

	if exp, got := 0, len(d.Copy()); exp != got {
		t.Fatalf("file count mismatch: exp %v, got %v", exp, got)
	}
	d.Add(f1)

	if exp, got := 1, len(d.Copy()); exp != got {
		t.Fatalf("file count mismatch: exp %v, got %v", exp, got)
	}

	d.Add(f1)

	if exp, got := 1, len(d.Copy()); exp != got {
		t.Fatalf("file count mismatch: exp %v, got %v", exp, got)
	}

	d.Add(f1, f2)

	if exp, got := 2, len(d.Copy()); exp != got {
		t.Fatalf("file count mismatch: exp %v, got %v", exp, got)
	}
}

// TestDataFiles_Remove tests that remove the same file multiple times does not
// create duplicates
func TestDataFiles_Remove(t *testing.T) {
	f1 := &FakeDataFile{name: "file1"}
	f2 := &FakeDataFile{name: "file2"}
	d := &tsm1.DataFiles{}

	d.Remove(f1)
	if exp, got := 0, len(d.Copy()); exp != got {
		t.Fatalf("file count mismatch: exp %v, got %v", exp, got)
	}

	d.Add(f1)

	if exp, got := 1, len(d.Copy()); exp != got {
		t.Fatalf("file count mismatch: exp %v, got %v", exp, got)
	}

	d.Remove(f2)

	if exp, got := 1, len(d.Copy()); exp != got {
		t.Fatalf("file count mismatch: exp %v, got %v", exp, got)
	}

	d.Remove(f1, f2)

	if exp, got := 0, len(d.Copy()); exp != got {
		t.Fatalf("file count mismatch: exp %v, got %v", exp, got)
	}
}

func TestDataFiles_Overlapping(t *testing.T) {
	f1 := &FakeDataFile{name: "file1", minTime: 0, maxTime: 10}
	f2 := &FakeDataFile{name: "file2", minTime: 10, maxTime: 20}
	f3 := &FakeDataFile{name: "file3", minTime: 15, maxTime: 25}
	f4 := &FakeDataFile{name: "file4", minTime: 0, maxTime: 40}

	d := &tsm1.DataFiles{}

	d.Add(f1, f2, f3, f4)

	// Make sure all the files are there
	if exp, got := 4, len(d.Copy()); exp != got {
		t.Fatalf("file count mismatch: exp %v, got %v", exp, got)
	}

	// Should match f1, f2, f3, f4
	if exp, got := 4, len(d.Overlapping(0, 30)); exp != got {
		t.Fatalf("file count mismatch: exp %v, got %v", exp, got)
	}

	// Should match f1, f2, f3, f4
	if exp, got := 4, len(d.Overlapping(10, 20)); exp != got {
		t.Fatalf("file count mismatch: exp %v, got %v", exp, got)
	}

	// Should match f2, f3, f4
	if exp, got := 3, len(d.Overlapping(12, 18)); exp != got {
		t.Fatalf("file count mismatch: exp %v, got %v", exp, got)
	}

	// Should match f3, f4
	if exp, got := 2, len(d.Overlapping(24, 30)); exp != got {
		t.Fatalf("file count mismatch: exp %v, got %v", exp, got)
	}
}

type FakeDataFile struct {
	name    string
	minTime int64
	maxTime int64
	modTime time.Time
	size    uint32
}

func (f *FakeDataFile) Name() string {
	return f.name
}

func (f *FakeDataFile) MinTime() int64 {
	return f.minTime
}

func (f *FakeDataFile) MaxTime() int64 {
	return f.maxTime
}

func (f *FakeDataFile) ModTime() time.Time {
	return f.modTime
}

func (f *FakeDataFile) Size() uint32 {
	return f.size
}

func (f *FakeDataFile) Unreference()          {}
func (f *FakeDataFile) Reference()            {}
func (f *FakeDataFile) Delete() error         { return nil }
func (f *FakeDataFile) Deleted() bool         { return false }
func (f *FakeDataFile) Close() error          { return nil }
func (f *FakeDataFile) IndexPosition() uint32 { return 0 }
func (f *FakeDataFile) Block(pos uint32) (key string, encodedBlock []byte, nextBlockStart uint32) {
	return "", nil, 0
}
func (f *FakeDataFile) Bytes(from, to uint32) []byte                { return nil }
func (f *FakeDataFile) CompressedBlockMinTime(block []byte) int64   { return 0 }
func (f *FakeDataFile) IndexMinMaxTimes() map[uint64]tsm1.TimeRange { return nil }
func (f *FakeDataFile) StartingPositionForID(id uint64) uint32      { return 0 }
