package durablequeue

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func BenchmarkQueueAppend(b *testing.B) {
	q, dir := newTestQueue(b, withMaxSize(1024*1024*1024))
	defer os.RemoveAll(dir)

	for i := 0; i < b.N; i++ {
		if err := q.Append([]byte(fmt.Sprintf("%d", i))); err != nil {
			println(q.diskUsage())
			b.Fatalf("Queue.Append failed: %v", err)
		}
	}
}

func TestQueueAppendOne(t *testing.T) {
	q, dir := newTestQueue(t)
	defer os.RemoveAll(dir)

	if err := q.Append([]byte("test")); err != nil {
		t.Fatalf("Queue.Append failed: %v", err)
	}

	exp := filepath.Join(dir, "1")
	stats, err := os.Stat(exp)
	if os.IsNotExist(err) {
		t.Fatalf("Queue.Append file not exists. exp %v to exist", exp)
	}

	// 8 byte header ptr + 8 byte record len + record len
	if exp := int64(8 + 8 + 4); stats.Size() != exp {
		t.Fatalf("Queue.Append file size mismatch. got %v, exp %v", stats.Size(), exp)
	}

	cur, err := q.Current()
	if err != nil {
		t.Fatalf("Queue.Current failed: %v", err)
	}

	if exp := "test"; string(cur) != exp {
		t.Errorf("Queue.Current mismatch: got %v, exp %v", string(cur), exp)
	}
}

func TestQueueAppendMultiple(t *testing.T) {
	q, dir := newTestQueue(t)
	defer os.RemoveAll(dir)

	if err := q.Append([]byte("one")); err != nil {
		t.Fatalf("Queue.Append failed: %v", err)
	}

	if err := q.Append([]byte("two")); err != nil {
		t.Fatalf("Queue.Append failed: %v", err)
	}

	for _, exp := range []string{"one", "two"} {
		cur, err := q.Current()
		if err != nil {
			t.Fatalf("Queue.Current failed: %v", err)
		}

		if string(cur) != exp {
			t.Errorf("Queue.Current mismatch: got %v, exp %v", string(cur), exp)
		}

		if err := q.Advance(); err != nil {
			t.Fatalf("Queue.Advance failed: %v", err)
		}
	}
}

func TestQueueAdvancePastEnd(t *testing.T) {
	q, dir := newTestQueue(t)
	defer os.RemoveAll(dir)

	// append one entry, should go to the first segment
	if err := q.Append([]byte("one")); err != nil {
		t.Fatalf("Queue.Append failed: %v", err)
	}

	// set the segment size low to force a new segment to be created
	q.SetMaxSegmentSize(12)

	// Should go into a new segment
	if err := q.Append([]byte("two")); err != nil {
		t.Fatalf("Queue.Append failed: %v", err)
	}

	// should read from first segment
	cur, err := q.Current()
	if err != nil {
		t.Fatalf("Queue.Current failed: %v", err)
	}

	if exp := "one"; string(cur) != exp {
		t.Errorf("Queue.Current mismatch: got %v, exp %v", string(cur), exp)
	}

	if err := q.Advance(); err != nil {
		t.Fatalf("Queue.Advance failed: %v", err)
	}

	// ensure the first segment file is removed since we've advanced past the end
	_, err = os.Stat(filepath.Join(dir, "1"))
	if !os.IsNotExist(err) {
		t.Fatalf("Queue.Advance should have removed the segment")
	}

	// should read from second segment
	cur, err = q.Current()
	if err != nil {
		t.Fatalf("Queue.Current failed: %v", err)
	}

	if exp := "two"; string(cur) != exp {
		t.Errorf("Queue.Current mismatch: got %v, exp %v", string(cur), exp)
	}

	_, err = os.Stat(filepath.Join(dir, "2"))
	if os.IsNotExist(err) {
		t.Fatalf("Queue.Advance should have removed the segment")
	}

	if err := q.Advance(); err != nil {
		t.Fatalf("Queue.Advance failed: %v", err)
	}

	if _, err = q.Current(); err != io.EOF {
		t.Fatalf("Queue.Current should have returned error")
	}

	// Should go into a new segment because the existing segment
	// is full
	if err := q.Append([]byte("two")); err != nil {
		t.Fatalf("Queue.Append failed: %v", err)
	}

	// should read from the new segment
	cur, err = q.Current()
	if err != nil {
		t.Fatalf("Queue.Current failed: %v", err)
	}

	if exp := "two"; string(cur) != exp {
		t.Errorf("Queue.Current mismatch: got %v, exp %v", string(cur), exp)
	}
}

func TestQueueFull(t *testing.T) {
	q, dir := newTestQueue(t, withMaxSize(8))
	defer os.RemoveAll(dir)

	if err := q.Append([]byte("ninebytes")); err != ErrQueueFull {
		t.Fatalf("Queue.Append expected to return queue full")
	}
}

func TestQueueReopen(t *testing.T) {
	q, dir := newTestQueue(t, withVerify(func([]byte) error { return nil }))
	defer os.RemoveAll(dir)

	if err := q.Append([]byte("one")); err != nil {
		t.Fatalf("Queue.Append failed: %v", err)
	}

	cur, err := q.Current()
	if err != nil {
		t.Fatalf("Queue.Current failed: %v", err)
	}

	if exp := "one"; string(cur) != exp {
		t.Errorf("Queue.Current mismatch: got %v, exp %v", string(cur), exp)
	}

	// close and re-open the queue
	if err := q.Close(); err != nil {
		t.Fatalf("Queue.Close failed: %v", err)
	}

	if err := q.Open(); err != nil {
		t.Fatalf("failed to re-open queue: %v", err)
	}

	// Make sure we can read back the last current value
	cur, err = q.Current()
	if err != nil {
		t.Fatalf("Queue.Current failed: %v", err)
	}

	if exp := "one"; string(cur) != exp {
		t.Errorf("Queue.Current mismatch: got %v, exp %v", string(cur), exp)
	}

	if err := q.Append([]byte("two")); err != nil {
		t.Fatalf("Queue.Append failed: %v", err)
	}

	if err := q.Advance(); err != nil {
		t.Fatalf("Queue.Advance failed: %v", err)
	}

	cur, err = q.Current()
	if err != nil {
		t.Fatalf("Queue.Current failed: %v", err)
	}

	if exp := "two"; string(cur) != exp {
		t.Errorf("Queue.Current mismatch: got %v, exp %v", string(cur), exp)
	}
}

func TestPurgeQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping purge queue")
	}

	q, dir := newTestQueue(t)
	defer os.RemoveAll(dir)

	if err := q.Append([]byte("one")); err != nil {
		t.Fatalf("Queue.Append failed: %v", err)
	}

	cur, err := q.Current()
	if err != nil {
		t.Fatalf("Queue.Current failed: %v", err)
	}

	if exp := "one"; string(cur) != exp {
		t.Errorf("Queue.Current mismatch: got %v, exp %v", string(cur), exp)
	}

	time.Sleep(time.Second)

	if err := q.PurgeOlderThan(time.Now()); err != nil {
		t.Errorf("Queue.PurgeOlderThan failed: %v", err)
	}

	if _, err := q.LastModified(); err != nil {
		t.Errorf("Queue.LastModified returned error: %v", err)
	}

	_, err = q.Current()
	if err != io.EOF {
		t.Fatalf("Queue.Current expected io.EOF, got: %v", err)
	}
}

func TestQueue_TotalBytes(t *testing.T) {
	q, dir := newTestQueue(t, withVerify(func([]byte) error { return nil }))
	defer os.RemoveAll(dir)

	if n := q.TotalBytes(); n != 0 {
		t.Fatalf("Queue.TotalBytes mismatch: got %v, exp %v", n, 0)
	}

	if err := q.Append([]byte("one")); err != nil {
		t.Fatalf("Queue.Append failed: %v", err)
	}

	if n := q.TotalBytes(); n != 11 { // 8 byte size + 3 byte content
		t.Fatalf("Queue.TotalBytes mismatch: got %v, exp %v", n, 11)
	}

	// Close that queue, open a new one from the same file(s).
	if err := q.Close(); err != nil {
		t.Fatalf("Queue.Close failed: %v", err)
	}
	q, err := NewQueue(dir, 1024, &SharedCount{}, MaxWritesPending, func([]byte) error { return nil })
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	if err := q.Open(); err != nil {
		t.Fatalf("failed to open queue: %v", err)
	}

	if n := q.TotalBytes(); n != 11 {
		t.Fatalf("Queue.TotalBytes mismatch: got %v, exp %v", n, 11)
	}

	if err := q.Append([]byte("13 characters")); err != nil {
		t.Fatalf("Queue.Append failed: %v", err)
	}

	if n := q.TotalBytes(); n != 32 { // 11 + 13 + 8
		t.Fatalf("Queue.TotalBytes mismatch: got %v, exp %v", n, 32)
	}
}

// This test verifies the queue will advance in the following scenario:
//
//    * There is one segment
//    * The segment is not full
//    * The segment record size entry is corrupted, resulting in
//      currentRecordSize + pos > fileSize and
//      therefore the Advance would fail.
func TestQueue_AdvanceSingleCorruptSegment(t *testing.T) {
	q, dir := newTestQueue(t, withVerify(func([]byte) error { return nil }))
	defer os.RemoveAll(dir)

	var err error
	appendN := func(n int) {
		for i := 0; i < n; i++ {
			// 12 bytes per entry + length = 20 bytes per record
			err = q.Append([]byte(strings.Repeat("<>", 6)))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}
	}

	appendN(5)

	mustScan := func(fn func(Scanner) int64) {
		t.Helper()
		// scan a couple of entries
		scan, err := q.NewScanner()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		exp := fn(scan)

		got, err := scan.Advance()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if got != exp {
			t.Errorf("unexpected value; -got/+exp\n%s", cmp.Diff(got, exp))
		}
	}

	mustScan(func(scan Scanner) int64 {
		scan.Next()
		scan.Next()
		return 2
	})

	seg := q.segments[0].path
	f, err := os.OpenFile(seg, os.O_RDWR, os.ModePerm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	f.Seek(60, io.SeekStart)
	// corrupt a size entry
	binary.Write(f, binary.BigEndian, int64(1e9))
	f.Sync()
	f.Close()

	// expect only two Next calls to succeed due to corruption
	mustScan(func(scan Scanner) int64 {
		scan.Next()
		scan.Next()
		scan.Next()
		return 2
	})

	if got, exp := q.TotalBytes(), int64(0); got != exp {
		// queue should have been truncated due to error
		t.Errorf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
	}

	// at this point, the segment should have been trimmed, so the queue is empty

	appendN(5)
	mustScan(func(scan Scanner) int64 {
		scan.Next()
		scan.Next()
		scan.Next()
		scan.Next()
		return 4
	})

	// the queue should have one record left (20 bytes per record)
	if got, exp := q.TotalBytes(), int64(20); got != exp {
		t.Errorf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
	}

	// drain the last entry
	mustScan(func(scan Scanner) int64 {
		scan.Next()
		return 1
	})

	// queue should now be empty again
	if got, exp := q.TotalBytes(), int64(0); got != exp {
		t.Errorf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

// A TestSegment is a simple representation of a segment. Blocks are represented
// by a slice of sizes. When TestSegments are written to a file, they're
// unpacked into the correct representation.
type TestSegment struct {
	reportedBlockSizes []uint64
	actualBlockSizes   []uint64
	position           uint64
}

// Encode encodes the TestSegment to the provided writer, ensuring that the
// correct space is left between subsequent blocks, and that the position is
// written into last eight bytes.
func (s *TestSegment) Encode(w io.Writer) error {
	if len(s.reportedBlockSizes) != len(s.actualBlockSizes) {
		return fmt.Errorf("invalid TestSegment. Block lengths must be equal: %d - %d", s.reportedBlockSizes, s.actualBlockSizes)
	}

	var buf [8]byte
	for i, size := range s.reportedBlockSizes {
		// Write the record size.
		binary.BigEndian.PutUint64(buf[:], size)
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}

		// Pad the rest of the block with the actual size in the TestSegment
		block := make([]byte, s.actualBlockSizes[i])
		if _, err := w.Write(block); err != nil {
			return err
		}
	}

	// Write the position into end of writer.
	binary.BigEndian.PutUint64(buf[:], s.position)
	_, err := w.Write(buf[:])
	return err
}

// String prints a hexadecimal representation of the TestSegment.
func (s *TestSegment) String() string {

	leftPad := func(s string) string {
		out := make([]string, 0, 8)
		for i := 0; i < 16-len(s); i++ {
			out = append(out, "0")
		}
		return strings.Join(out, "") + s
	}

	// Build the blocks
	var out string
	for i, repSize := range s.reportedBlockSizes {
		out += leftPad(fmt.Sprintf("%x", repSize))

		block := make([]string, 0, int(s.actualBlockSizes[i])*2) // Two words per byte
		for i := 0; i < cap(block); i++ {
			block = append(block, "0")
		}
		out += strings.Join(block, "")
	}

	// Write out the position
	return out + leftPad(fmt.Sprintf("%x", s.position))
}

// mustCreateSegment creates a new segment from the provided TestSegment, and
// a directory to store the segment file.
//
// mustCreateSegment calls newSegment, which means it calls open on the segment,
// and possibly attempts to repair the TestSegment.
func mustCreateSegment(ts *TestSegment, dir string, vf func([]byte) error) *segment {
	fd, err := ioutil.TempFile(dir, "")
	if err != nil {
		panic(err)
	}

	// Encode TestSegment into file.
	if err := ts.Encode(fd); err != nil {
		panic(err)
	}

	// Close the file, so the segment can open it safely.
	if err := fd.Close(); err != nil {
		panic(err)
	}

	// Create a new segment.
	segment, err := newSegment(fd.Name(), defaultSegmentSize, vf)
	if err != nil {
		panic(err)
	}
	return segment
}

// ReadSegment returns a hexadecimal representation of a segment.
func ReadSegment(segment *segment) string {
	data, err := ioutil.ReadFile(segment.path)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", data)
}

func TestSegment_repair(t *testing.T) {
	dir, err := ioutil.TempDir("", "hh_queue")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	examples := []struct {
		In       *TestSegment
		Expected *TestSegment
		VerifyFn func([]byte) error
	}{
		{ // Valid segment
			In: &TestSegment{
				reportedBlockSizes: []uint64{8, 16, 24},
				actualBlockSizes:   []uint64{8, 16, 24},
				position:           16,
			},
			Expected: &TestSegment{
				reportedBlockSizes: []uint64{8, 16, 24},
				actualBlockSizes:   []uint64{8, 16, 24},
				position:           16,
			},
		},
		{ // Valid segment
			In: &TestSegment{
				reportedBlockSizes: []uint64{8, 16},
				actualBlockSizes:   []uint64{8, 16},
				position:           16,
			},
			Expected: &TestSegment{
				reportedBlockSizes: []uint64{8, 16},
				actualBlockSizes:   []uint64{8, 16},
				position:           16,
			},
		},
		{ // Valid segment with a corrupted position value
			In: &TestSegment{
				reportedBlockSizes: []uint64{8, 16, 24},
				actualBlockSizes:   []uint64{8, 16, 24},
				position:           10292901,
			},
			Expected: &TestSegment{
				reportedBlockSizes: []uint64{8, 16, 24},
				actualBlockSizes:   []uint64{8, 16, 24},
				position:           0,
			},
		},
		{ // Corrupted last block
			In: &TestSegment{
				reportedBlockSizes: []uint64{8, 16, 24},
				actualBlockSizes:   []uint64{8, 16, 13},
				position:           998172398,
			},
			Expected: &TestSegment{
				reportedBlockSizes: []uint64{8, 16},
				actualBlockSizes:   []uint64{8, 16},
				position:           0,
			},
		},
		{ // Corrupted block followed by valid later blocks with valid position
			In: &TestSegment{
				reportedBlockSizes: []uint64{8, 16, 24, 8},
				actualBlockSizes:   []uint64{8, 16, 18, 8},
				position:           40, // Will point to third block.
			},
			Expected: &TestSegment{
				reportedBlockSizes: []uint64{8, 16},
				actualBlockSizes:   []uint64{8, 16},
				position:           0,
			},
			// Mock out VerifyFn to determine third block is invalid.
			VerifyFn: func(b []byte) error {
				if len(b) == 24 {
					// Second block in example segment.
					return fmt.Errorf("a verification error")
				}
				return nil
			},
		},
		{ // Block size overflows when converting to int64
			In: &TestSegment{
				reportedBlockSizes: []uint64{math.MaxUint64},
				actualBlockSizes:   []uint64{8},
				position:           0,
			},
			Expected: &TestSegment{
				reportedBlockSizes: []uint64{},
				actualBlockSizes:   []uint64{},
				position:           0,
			},
		},
	}

	for i, example := range examples {
		if example.VerifyFn == nil {
			example.VerifyFn = func([]byte) error { return nil }
		}
		segment := mustCreateSegment(example.In, dir, example.VerifyFn)

		if got, exp := ReadSegment(segment), example.Expected.String(); got != exp {
			t.Errorf("[example %d]\ngot: %s\nexp: %s\n\n", i+1, got, exp)
		}
	}
}
