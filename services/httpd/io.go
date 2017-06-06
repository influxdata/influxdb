package httpd

import (
	"errors"
	"io"
)

var (
	errTruncated = errors.New("Read: truncated")
)

// truncateReader returns a Reader that reads from r
// but stops with ErrTruncated after n bytes.
func truncateReader(r io.Reader, n int64) io.ReadCloser {
	tr := &truncatedReader{r: &io.LimitedReader{R: r, N: n + 1}}

	if rc, ok := r.(io.Closer); ok {
		tr.Closer = rc
	}

	return tr
}

// A truncatedReader limits the amount of data returned to a maximum of r.N bytes.
type truncatedReader struct {
	r *io.LimitedReader
	io.Closer
}

func (r *truncatedReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	if r.r.N <= 0 {
		return n, errTruncated
	}

	return n, err
}

func (r *truncatedReader) Close() error {
	if r.Closer != nil {
		return r.Closer.Close()
	}
	return nil
}
