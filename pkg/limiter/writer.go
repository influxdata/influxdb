package limiter

import (
	"context"
	"io"
	"os"
	"time"

	"golang.org/x/time/rate"
)

type Writer struct {
	w       io.WriteCloser
	limiter Rate
	ctx     context.Context
}

type Rate interface {
	WaitN(ctx context.Context, n int) error
	Burst() int
	Tokens() float64
	Limit() rate.Limit
}

func NewRate(bytesPerSec, burstLimit int) Rate {
	limiter := rate.NewLimiter(rate.Limit(bytesPerSec), burstLimit)
	limiter.AllowN(time.Now(), burstLimit) // spend initial burst
	return limiter
}

// NewWriter returns a writer that implements io.Writer with rate limiting.
// The limiter use a token bucket approach and limits the rate to bytesPerSec
// with a maximum burst of burstLimit.
func NewWriter(w io.WriteCloser, bytesPerSec, burstLimit int) *Writer {
	limiter := NewRate(bytesPerSec, burstLimit)

	return &Writer{
		w:       w,
		ctx:     context.Background(),
		limiter: limiter,
	}
}

// WithRate returns a Writer with the specified rate limiter.
func NewWriterWithRate(w io.WriteCloser, limiter Rate) *Writer {
	return &Writer{
		w:       w,
		ctx:     context.Background(),
		limiter: limiter,
	}
}

// Write writes bytes from b.
func (s *Writer) Write(b []byte) (int, error) {
	if s.limiter == nil {
		return s.w.Write(b)
	}

	var n int
	for n < len(b) {
		wantToWriteN := len(b[n:])
		if wantToWriteN > s.limiter.Burst() {
			wantToWriteN = s.limiter.Burst()
		}

		wroteN, err := s.w.Write(b[n : n+wantToWriteN])
		if err != nil {
			return n, err
		}
		n += wroteN

		if err := s.limiter.WaitN(s.ctx, wroteN); err != nil {
			return n, err
		}
	}

	return n, nil
}

func (s *Writer) Sync() error {
	if f, ok := s.w.(*os.File); ok {
		return f.Sync()
	}
	return nil
}

func (s *Writer) Name() string {
	if f, ok := s.w.(*os.File); ok {
		return f.Name()
	}
	return ""
}

func (s *Writer) Close() error {
	return s.w.Close()
}
