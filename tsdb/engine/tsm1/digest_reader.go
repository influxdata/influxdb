package tsm1

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"io"
)

type DigestReader struct {
	r  io.ReadCloser
	gr *gzip.Reader
}

func NewDigestReader(r io.ReadCloser) (*DigestReader, error) {
	gr, err := gzip.NewReader(bufio.NewReader(r))
	if err != nil {
		return nil, err
	}
	return &DigestReader{r: r, gr: gr}, nil
}

func (w *DigestReader) ReadTimeSpan() (string, *DigestTimeSpan, error) {
	var n uint16
	if err := binary.Read(w.gr, binary.BigEndian, &n); err != nil {
		return "", nil, err
	}

	b := make([]byte, n)
	if _, err := io.ReadFull(w.gr, b); err != nil {
		return "", nil, err
	}

	var cnt uint32
	if err := binary.Read(w.gr, binary.BigEndian, &cnt); err != nil {
		return "", nil, err
	}

	ts := &DigestTimeSpan{}
	for i := 0; i < int(cnt); i++ {
		var min, max int64
		var crc uint32

		if err := binary.Read(w.gr, binary.BigEndian, &min); err != nil {
			return "", nil, err
		}

		if err := binary.Read(w.gr, binary.BigEndian, &max); err != nil {
			return "", nil, err
		}

		if err := binary.Read(w.gr, binary.BigEndian, &crc); err != nil {
			return "", nil, err
		}

		if err := binary.Read(w.gr, binary.BigEndian, &n); err != nil {
			return "", nil, err
		}
		ts.Add(min, max, int(n), crc)
	}

	return string(b), ts, nil
}

func (w *DigestReader) Close() error {
	if err := w.gr.Close(); err != nil {
		return err
	}
	return w.r.Close()
}
