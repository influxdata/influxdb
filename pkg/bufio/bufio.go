package bufio

import (
	"bufio"
	"errors"
	"io"
)

// Writer implements buffering for an io.WriteCloser object.
//
// Writer provides access to a buffered writer that can be closed. Closing the
// writer will result in all remaining data being flushed from the buffer before
// the underlying WriteCloser is closed.
type Writer struct {
	c  io.Closer
	bw *bufio.Writer
}

// NewWriter initialises a new Writer with the default buffer size.
func NewWriter(w io.WriteCloser) *Writer {
	return &Writer{
		c:  w,
		bw: bufio.NewWriter(w),
	}
}

// NewWriterSize initialises a new Writer with the provided buffer size.
func NewWriterSize(w io.WriteCloser, sz int) *Writer {
	return &Writer{
		c:  w,
		bw: bufio.NewWriterSize(w, sz),
	}
}

// Write writes the contents of p into the buffer. It returns the number of
// bytes written. If fewer than len(p) bytes are written, and error is returned
// explaining why.
func (w *Writer) Write(p []byte) (int, error) {
	return w.bw.Write(p)
}

// Close closes the Writer, flushing any remaining data from the buffer.
func (w *Writer) Close() error {
	if w.bw == nil || w.c == nil {
		return errors.New("cannot close nil Writer")
	}

	if err := w.bw.Flush(); err != nil {
		w.c.Close()
		return err
	}

	return w.c.Close()
}
