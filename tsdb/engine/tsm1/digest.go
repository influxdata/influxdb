package tsm1

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
)

type DigestOptions struct {
	MinTime, MaxTime int64
	MinKey, MaxKey   []byte
}

// DigestWithOptions writes a digest of dir to w using options to filter by
// time and key range.
func DigestWithOptions(dir string, opts DigestOptions, w io.WriteCloser) error {
	if dir == "" {
		return fmt.Errorf("dir is required")
	}

	files, err := filepath.Glob(filepath.Join(dir, fmt.Sprintf("*.%s", TSMFileExtension)))
	if err != nil {
		return err
	}

	readers := make([]*TSMReader, 0, len(files))

	for _, fi := range files {
		f, err := os.Open(fi)
		if err != nil {
			return err
		}

		r, err := NewTSMReader(f)
		if err != nil {
			return err
		}
		readers = append(readers, r)
	}

	ch := make([]chan seriesKey, 0, len(files))
	for _, fi := range files {
		f, err := os.Open(fi)
		if err != nil {
			return err
		}

		r, err := NewTSMReader(f)
		if err != nil {
			return err
		}
		defer r.Close()

		s := make(chan seriesKey)
		ch = append(ch, s)
		go func() {
			for i := 0; i < r.KeyCount(); i++ {
				key, typ := r.KeyAt(i)
				if len(opts.MinKey) > 0 && bytes.Compare(key, opts.MinKey) < 0 {
					continue
				}

				if len(opts.MaxKey) > 0 && bytes.Compare(key, opts.MaxKey) > 0 {
					continue
				}

				s <- seriesKey{key: key, typ: typ}
			}
			close(s)
		}()

	}

	dw, err := NewDigestWriter(w)
	if err != nil {
		return err
	}
	defer dw.Close()

	var n int
	for key := range merge(ch...) {

		ts := &DigestTimeSpan{}
		n++
		kstr := string(key.key)

		for _, r := range readers {
			entries := r.Entries(key.key)
			for _, entry := range entries {
				crc, b, err := r.ReadBytes(&entry, nil)
				if err != nil {
					return err
				}

				// Filter blocks that are outside the time filter.  If they overlap, we
				// still include them.
				if entry.MaxTime < opts.MinTime || entry.MinTime > opts.MaxTime {
					continue
				}

				cnt := BlockCount(b)
				ts.Add(entry.MinTime, entry.MaxTime, cnt, crc)
			}
		}

		sort.Sort(ts)
		if err := dw.WriteTimeSpan(kstr, ts); err != nil {
			return err
		}
	}
	return dw.Close()
}

// Digest writes a digest of dir to w of a full shard dir.
func Digest(dir string, w io.WriteCloser) error {
	return DigestWithOptions(dir, DigestOptions{
		MinTime: math.MinInt64,
		MaxTime: math.MaxInt64,
	}, w)
}
