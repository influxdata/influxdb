package io

import (
	"errors"
	"io"
)

var ErrReadLimitExceeded = errors.New("read limit exceeded")

// LimitedReadCloser wraps an io.ReadCloser in limiting behavior using
// io.LimitedReader. It allows us to obtain the limit error at the time of close
// instead of just when writing.
type LimitedReadCloser struct {
	R             io.ReadCloser // underlying reader
	N             int64         // max bytes remaining
	err           error
	closed        bool
	limitExceeded bool
}

// NewLimitedReadCloser returns a new LimitedReadCloser.
func NewLimitedReadCloser(r io.ReadCloser, n int64) *LimitedReadCloser {
	return &LimitedReadCloser{
		R: r,
		N: n,
	}
}

func (l *LimitedReadCloser) Read(p []byte) (n int, err error) {
	if l.N <= 0 {
		l.limitExceeded = true
		return 0, io.EOF
	}
	if int64(len(p)) > l.N {
		p = p[0:l.N]
	}
	n, err = l.R.Read(p)
	l.N -= int64(n)
	return
}

// Close returns an ErrReadLimitExceeded when the wrapped reader exceeds the set
// limit for number of bytes.  This is safe to call more than once but not
// concurrently.
func (l *LimitedReadCloser) Close() (err error) {
	if l.limitExceeded {
		l.err = ErrReadLimitExceeded
	}
	if l.closed {
		// Close has already been called.
		return l.err
	}
	if err := l.R.Close(); err != nil && l.err == nil {
		l.err = err
	}
	// Prevent l.closer.Close from being called again.
	l.closed = true
	return l.err
}
