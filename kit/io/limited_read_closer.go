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
	*io.LimitedReader
	err   error
	close func() error
}

// NewLimitedReadCloser returns a new LimitedReadCloser.
func NewLimitedReadCloser(r io.ReadCloser, n int64) *LimitedReadCloser {
	// read up to max + 1 as limited reader just returns EOF when the limit is reached
	// or when there is nothing left to read. If we exceed the max batch size by one
	// then we know the limit has been passed.
	return &LimitedReadCloser{
		LimitedReader: &io.LimitedReader{R: r, N: n + 1},
		close:         r.Close,
	}
}

// Close returns an ErrReadLimitExceeded when the wrapped reader exceeds the set
// limit for number of bytes.  This is safe to call more than once but not
// concurrently.
func (l *LimitedReadCloser) Close() (err error) {
	defer func() {
		if cerr := l.close(); cerr != nil && err == nil {
			err = cerr
		}

		// only call close once
		l.close = func() error { return nil }
	}()

	if l.N < 1 {
		l.err = ErrReadLimitExceeded
	}

	return l.err
}
