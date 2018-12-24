package tsm1

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"
)

var (
	// ErrDigestManifestAlreadyRead is returned if the client attempts to read
	// a manifest from a digest more than once on the same reader.
	ErrDigestManifestAlreadyRead = errors.New("digest manifest already read")
)

type DigestReader struct {
	r            io.ReadCloser
	sr           *snappy.Reader
	manifestRead bool
}

func NewDigestReader(r io.ReadCloser) (*DigestReader, error) {
	return &DigestReader{r: r, sr: snappy.NewReader(r)}, nil
}

func (r *DigestReader) ReadManifest() (*DigestManifest, error) {
	if r.manifestRead {
		return nil, ErrDigestManifestAlreadyRead
	}

	var n uint32
	// Read manifest length.
	if err := binary.Read(r.sr, binary.BigEndian, &n); err != nil {
		return nil, err
	}

	lr := io.LimitReader(r.sr, int64(n))

	m := &DigestManifest{}
	if err := json.NewDecoder(lr).Decode(m); err != nil {
		return nil, err
	}

	r.manifestRead = true

	return m, nil
}

func (r *DigestReader) ReadTimeSpan() (string, *DigestTimeSpan, error) {
	if !r.manifestRead {
		if _, err := r.ReadManifest(); err != nil {
			return "", nil, err
		}
	}

	var n uint16
	if err := binary.Read(r.sr, binary.BigEndian, &n); err != nil {
		return "", nil, err
	}

	b := make([]byte, n)
	if _, err := io.ReadFull(r.sr, b); err != nil {
		return "", nil, err
	}

	var cnt uint32
	if err := binary.Read(r.sr, binary.BigEndian, &cnt); err != nil {
		return "", nil, err
	}

	ts := &DigestTimeSpan{}
	ts.Ranges = make([]DigestTimeRange, cnt)
	for i := 0; i < int(cnt); i++ {
		var buf [22]byte

		n, err := io.ReadFull(r.sr, buf[:])
		if err != nil {
			return "", nil, err
		} else if n != len(buf) {
			return "", nil, fmt.Errorf("read %d bytes, expected %d, data %v", n, len(buf), buf[:n])
		}

		ts.Ranges[i].Min = int64(binary.BigEndian.Uint64(buf[0:]))
		ts.Ranges[i].Max = int64(binary.BigEndian.Uint64(buf[8:]))
		ts.Ranges[i].CRC = binary.BigEndian.Uint32(buf[16:])
		ts.Ranges[i].N = int(binary.BigEndian.Uint16(buf[20:]))
	}

	return string(b), ts, nil
}

func (r *DigestReader) Close() error {
	return r.r.Close()
}
