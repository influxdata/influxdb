package durablequeue

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

type opts struct {
	fn             func([]byte) error
	maxSize        int64
	maxSegmentSize int64
}

func withVerify(fn func([]byte) error) func(o *opts) {
	return func(o *opts) {
		o.fn = fn
	}
}

func withMaxSize(maxSize int64) func(o *opts) {
	return func(o *opts) {
		o.maxSize = maxSize
	}
}

func withMaxSegmentSize(maxSegmentSize int64) func(o *opts) {
	return func(o *opts) {
		o.maxSegmentSize = maxSegmentSize
	}
}

// newTestQueue creates and opens a new Queue with a default
// maxSize of 1024
func newTestQueue(t testing.TB, fns ...func(o *opts)) (*Queue, string) {
	t.Helper()

	opts := &opts{maxSize: 1024, maxSegmentSize: 512}
	for _, fn := range fns {
		fn(opts)
	}

	tmp := ""
	if htmp, ok := os.LookupEnv("HH_TMP"); ok {
		tmp = os.ExpandEnv(htmp)
	}

	dir, err := os.MkdirTemp(tmp, "hh_queue")
	require.NoError(t, err)

	q, err := NewQueue(dir, opts.maxSize, opts.maxSegmentSize, &SharedCount{}, MaxWritesPending, opts.fn)
	require.NoError(t, err)

	require.NoError(t, q.Open())
	return q, dir
}

func TestQueue_NewScanner(t *testing.T) {
	q, dir := newTestQueue(t)
	defer os.RemoveAll(dir)

	for i := 0; i < 50; i++ {
		q.Append([]byte(fmt.Sprintf("%d", i)))
	}

	scan, err := q.NewScanner()
	if err != nil {
		t.Fatalf("Queue.NewScanner failed: %v", err)
	}

	var have []string

	for i := 0; i < 3; i++ {
		scan.Next()
		if scan.Err() != nil {
			t.Fatalf("Next failed: %v", scan.Err())
		}

		have = append(have, string(scan.Bytes()))
	}

	if want := []string{"0", "1", "2"}; !reflect.DeepEqual(have, want) {
		t.Fatalf("Next failed: have %v, want %v", have, want)
	}

	n, err := scan.Advance()
	if err != nil {
		t.Fatalf("Advance failed: %v", err)
	}

	if want := int64(3); n != want {
		t.Fatalf("Advance failed: have %d, want %d", n, want)
	}

	v, err := q.Current()
	if err != nil {
		t.Fatalf("Queue.Current failed: %v", err)
	}

	if want := "3"; string(v) != want {
		t.Fatalf("Queue.Current failed: have %s, want %s", v, want)
	}
}

func TestQueue_NewScanner_ScanToEnd(t *testing.T) {
	q, dir := newTestQueue(t)
	defer os.RemoveAll(dir)

	var want []string
	for i := 0; i < 50; i++ {
		want = append(want, fmt.Sprintf("%d", i))
	}

	for _, v := range want {
		q.Append([]byte(v))
	}

	scan, err := q.NewScanner()
	if err != nil {
		t.Fatalf("Queue.NewScanner failed: %v", err)
	}

	var have []string
	for scan.Next() {
		if scan.Err() != nil {
			t.Fatalf("Next failed: %v", scan.Err())
		}

		have = append(have, string(scan.Bytes()))
	}

	if !reflect.DeepEqual(have, want) {
		t.Fatalf("Next failed: have %v, want %v", have, want)
	}

	n, err := scan.Advance()
	if err != nil {
		t.Fatalf("Advance failed: %v", err)
	}

	if want := int64(50); n != want {
		t.Fatalf("Advance failed: have %d, want %d", n, want)
	}

	_, err = q.Current()
	if err != io.EOF {
		t.Fatalf("Queue.Current failed: %v", err)
	}
}

func TestQueue_NewScanner_EmptyQueue(t *testing.T) {
	q, dir := newTestQueue(t)
	defer os.RemoveAll(dir)

	_, err := q.NewScanner()
	if err != io.EOF {
		t.Fatalf("Queue.NewScanner failed: have %v, expected io.EOF", err)
	}
}

func TestQueue_NewScanner_EmptyAppendAndScanMore(t *testing.T) {
	q, dir := newTestQueue(t)
	defer os.RemoveAll(dir)

	for i := 0; i < 2; i++ {

		q.Append([]byte("one"))
		q.Append([]byte("two"))

		scan, err := q.NewScanner()
		if err != nil {
			t.Fatalf("Queue.NewScanner failed: %v", err)
		}

		var have []string
		for scan.Next() {
			if scan.Err() != nil {
				t.Fatalf("Next failed: %v", scan.Err())
			}

			have = append(have, string(scan.Bytes()))
		}

		if want := []string{"one", "two"}; !reflect.DeepEqual(have, want) {
			t.Fatalf("Next failed: have %v, want %v", have, want)
		}

		n, err := scan.Advance()
		if err != nil {
			t.Fatalf("Advance failed: %v", err)
		}

		if want := int64(2); n != want {
			t.Fatalf("Advance failed: have %d, want %d", n, want)
		}

		if !q.Empty() {
			t.Fatal("Queue.Empty failed; expected true")
		}
	}
}

// AppendWhileScan tests that whilst scanning,
// the scanner will pick up the additional blocks
func TestQueue_NewScanner_AppendWhileScan(t *testing.T) {
	q, dir := newTestQueue(t)
	defer os.RemoveAll(dir)

	q.Append([]byte("one"))

	scan, err := q.NewScanner()
	if err != nil {
		t.Fatalf("Queue.NewScanner failed: %v", err)
	}

	scan.Next()
	have := string(scan.Bytes())
	if want := "one"; have != want {
		t.Fatalf("Next failed: have %v, want %v", have, want)
	}

	q.Append([]byte("two"))

	scan.Next()
	have = string(scan.Bytes())
	if want := "two"; have != want {
		t.Fatalf("Next failed: have %v, want %v", have, want)
	}

	scan.Advance()

	_, err = q.Current()
	if err != io.EOF {
		t.Fatalf("Queue.Current failed: %v", err)
	}
}

func TestQueue_NewScanner_Corrupted(t *testing.T) {
	q, dir := newTestQueue(t, withMaxSize(100), withMaxSegmentSize(25))
	defer os.RemoveAll(dir)
	_ = dir

	q.SetMaxSegmentSize(10)
	q.Append([]byte("block number 0"))
	exp := []byte("block number 1")
	q.Append(exp)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(1<<63))
	err := os.WriteFile(q.segments[0].path, buf[:], os.ModePerm)
	if err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	s, err := q.NewScanner()
	if err != nil {
		t.Fatal(err)
	}

	s.Next()
	s.Advance()
	s, err = q.NewScanner()
	if err != nil {
		t.Fatal(err)
	}
	s.Next()
	got := s.Bytes()
	if !cmp.Equal(got, exp) {
		t.Errorf("unpexected -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestQueue_NewScanner_Corruption(t *testing.T) {
	type bytes [][]byte

	type expseg struct {
		expe []string
		expb bytes
	}

	bs := func(s ...string) bytes {
		r := make(bytes, len(s))
		for i := 0; i < len(s); i++ {
			if s[i] != "" {
				r[i] = []byte(s[i])
			}
		}
		return r
	}

	ss := func(s ...string) []string { return s }

	cases := []struct {
		name   string
		blocks bytes
		pre    func(t *testing.T, q *Queue)
		exp    []expseg
	}{
		{
			name:   "no corruption",
			blocks: bs("0#0123456789", "1#0123456789", "2#0123456789"),
			exp: []expseg{
				{ss("", ""), bs("0#0123456789", "1#0123456789")},
				{ss(""), bs("2#0123456789")},
			},
		},
		{
			name:   "corrupt first block size",
			blocks: bs("0#0123456789", "1#0123456789", "2#0123456789"),
			pre: func(t *testing.T, q *Queue) {
				f, err := os.OpenFile(q.segments[0].path, os.O_WRONLY, os.ModePerm)
				if err != nil {
					t.Fatalf("Open: %v", err)
				}
				defer f.Close()

				var buf [8]byte
				binary.BigEndian.PutUint64(buf[:], uint64(1000))
				_, err = f.Write(buf[:])
				if err != nil {
					t.Fatalf("Write: %v", err)
				}
			},
			exp: []expseg{
				{ss("record size out of range: max 30: got 1000", "record size out of range: max 30: got 1000"), bs("", "")},
				{ss(""), bs("2#0123456789")},
			},
		},
		{
			name:   "corrupt second block size",
			blocks: bs("0#0123456789", "1#0123456789", "2#0123456789"),
			pre: func(t *testing.T, q *Queue) {
				f, err := os.OpenFile(q.segments[0].path, os.O_WRONLY, os.ModePerm)
				if err != nil {
					t.Fatalf("Open: %v", err)
				}
				defer f.Close()

				var buf [8]byte
				binary.BigEndian.PutUint64(buf[:], uint64(50))
				_, err = f.WriteAt(buf[:], 20)
				if err != nil {
					t.Fatalf("Write: %v", err)
				}
			},
			exp: []expseg{
				{ss("", "record size out of range: max 30: got 50"), bs("0#0123456789", "")},
				{ss(""), bs("2#0123456789")},
			},
		},
		{
			name:   "truncate file",
			blocks: bs("0#0123456789", "1#0123456789", "2#0123456789"),
			pre: func(t *testing.T, q *Queue) {
				f, err := os.OpenFile(q.segments[0].path, os.O_WRONLY, os.ModePerm)
				if err != nil {
					t.Fatalf("Open: %v", err)
				}
				defer f.Close()

				err = f.Truncate(25)
				if err != nil {
					t.Fatalf("Truncate: %v", err)
				}
			},
			exp: []expseg{
				{ss("", "bad read. exp 8, got 5"), bs("0#0123456789", "")},
				{ss(""), bs("2#0123456789")},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, dir := newTestQueue(t)
			defer os.RemoveAll(dir)

			q.SetMaxSegmentSize(30)
			for _, b := range tc.blocks {
				q.Append([]byte(b))
			}

			if tc.pre != nil {
				tc.pre(t, q)
			}

			// invariant: len(segments) ≤ len(blocks)
			for _, exp := range tc.exp {
				s, err := q.NewScanner()
				if err == io.EOF {
					break
				}

				if err != nil {
					t.Fatal("NewSegment", err)
				}

				for i := 0; i < len(exp.expb); i++ {
					s.Next()

					{
						got := s.Bytes()
						if exp := []byte(exp.expb[i]); !cmp.Equal(got, exp) {
							t.Errorf("unpexected block %d -got/+exp\n%s", i, cmp.Diff(got, exp))
						}
					}

					if len(exp.expe) > 0 {
						var got string
						if s.Err() != nil {
							got = s.Err().Error()
						}
						if exp := exp.expe[i]; !cmp.Equal(got, exp) {
							t.Errorf("unpexected err %d -got/+exp\n%s", i, cmp.Diff(got, exp))
						}
					}
				}

				if exp := s.Next(); exp != false {
					t.Error("expected Next to return false")
				}

				s.Advance()
			}
		})
	}
}
