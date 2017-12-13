package tsm1

import (
	"compress/gzip"
	"encoding/binary"
	"io"
)

type writeFlushCloser interface {
	Close() error
	Write(b []byte) (int, error)
	Flush() error
}

// DigestWriter allows for writing a digest of a shard.  A digest is a condensed
// representation of the contents of a shard.  It can be scoped to one or more series
// keys, ranges of times or sets of files.
type DigestWriter struct {
	w io.WriteCloser
	F writeFlushCloser
}

func NewDigestWriter(w io.WriteCloser) (*DigestWriter, error) {
	gw := gzip.NewWriter(w)
	return &DigestWriter{w: w, F: gw}, nil
}

func (w *DigestWriter) WriteTimeSpan(key string, t *DigestTimeSpan) error {
	if err := binary.Write(w.F, binary.BigEndian, uint16(len(key))); err != nil {
		return err
	}

	if _, err := w.F.Write([]byte(key)); err != nil {
		return err
	}

	if err := binary.Write(w.F, binary.BigEndian, uint32(t.Len())); err != nil {
		return err
	}

	for _, tr := range t.Ranges {
		if err := binary.Write(w.F, binary.BigEndian, tr.Min); err != nil {
			return err
		}

		if err := binary.Write(w.F, binary.BigEndian, tr.Max); err != nil {
			return err
		}

		if err := binary.Write(w.F, binary.BigEndian, tr.CRC); err != nil {
			return err
		}

		if err := binary.Write(w.F, binary.BigEndian, uint16(tr.N)); err != nil {
			return err
		}
	}

	return nil
}

func (w *DigestWriter) Flush() error {
	return w.F.Flush()
}

func (w *DigestWriter) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}

	if err := w.F.Close(); err != nil {
		return err
	}

	return w.w.Close()
}

type DigestTimeSpan struct {
	Ranges []DigestTimeRange
}

func (a DigestTimeSpan) Len() int      { return len(a.Ranges) }
func (a DigestTimeSpan) Swap(i, j int) { a.Ranges[i], a.Ranges[j] = a.Ranges[j], a.Ranges[i] }
func (a DigestTimeSpan) Less(i, j int) bool {
	return a.Ranges[i].Min < a.Ranges[j].Min
}

func (t *DigestTimeSpan) Add(min, max int64, n int, crc uint32) {
	for _, v := range t.Ranges {
		if v.Min == min && v.Max == max && v.N == n && v.CRC == crc {
			return
		}
	}
	t.Ranges = append(t.Ranges, DigestTimeRange{Min: min, Max: max, N: n, CRC: crc})
}

type DigestTimeRange struct {
	Min, Max int64
	N        int
	CRC      uint32
}
