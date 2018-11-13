package tsm1

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"

	"github.com/golang/snappy"
)

var (
	// ErrNoDigestManifest is returned if an attempt is made to write other parts of a
	// digest before writing the manifest.
	ErrNoDigestManifest = errors.New("no digest manifest")

	// ErrDigestAlreadyWritten is returned if the client attempts to write more than
	// one manifest.
	ErrDigestAlreadyWritten = errors.New("digest manifest already written")
)

// DigestWriter allows for writing a digest of a shard.  A digest is a condensed
// representation of the contents of a shard.  It can be scoped to one or more series
// keys, ranges of times or sets of files.
type DigestWriter struct {
	w               io.WriteCloser
	sw              *snappy.Writer
	manifestWritten bool
}

func NewDigestWriter(w io.WriteCloser) (*DigestWriter, error) {
	return &DigestWriter{w: w, sw: snappy.NewBufferedWriter(w)}, nil
}

func (w *DigestWriter) WriteManifest(m *DigestManifest) error {
	if w.manifestWritten {
		return ErrDigestAlreadyWritten
	}

	b, err := json.Marshal(m)
	if err != nil {
		return err
	}

	// Write length of manifest.
	if err := binary.Write(w.sw, binary.BigEndian, uint32(len(b))); err != nil {
		return err
	}

	// Write manifest.
	if _, err = w.sw.Write(b); err != nil {
		return err
	}

	w.manifestWritten = true

	return err
}

func (w *DigestWriter) WriteTimeSpan(key string, t *DigestTimeSpan) error {
	if !w.manifestWritten {
		return ErrNoDigestManifest
	}

	if err := binary.Write(w.sw, binary.BigEndian, uint16(len(key))); err != nil {
		return err
	}

	if _, err := w.sw.Write([]byte(key)); err != nil {
		return err
	}

	if err := binary.Write(w.sw, binary.BigEndian, uint32(t.Len())); err != nil {
		return err
	}

	for _, tr := range t.Ranges {
		if err := binary.Write(w.sw, binary.BigEndian, tr.Min); err != nil {
			return err
		}

		if err := binary.Write(w.sw, binary.BigEndian, tr.Max); err != nil {
			return err
		}

		if err := binary.Write(w.sw, binary.BigEndian, tr.CRC); err != nil {
			return err
		}

		if err := binary.Write(w.sw, binary.BigEndian, uint16(tr.N)); err != nil {
			return err
		}
	}

	return nil
}

func (w *DigestWriter) Flush() error {
	return w.sw.Flush()
}

func (w *DigestWriter) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}

	if err := w.sw.Close(); err != nil {
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
