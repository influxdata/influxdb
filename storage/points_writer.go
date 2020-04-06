package storage

import (
	"context"

	"github.com/influxdata/influxdb/v2/models"
)

// PointsWriter describes the ability to write points into a storage engine.
type PointsWriter interface {
	WritePoints(context.Context, []models.Point) error
}

type BufferedPointsWriter struct {
	buf []models.Point
	n   int
	wr  PointsWriter
	err error
}

func NewBufferedPointsWriter(size int, pointswriter PointsWriter) *BufferedPointsWriter {
	return &BufferedPointsWriter{
		buf: make([]models.Point, size),
		wr:  pointswriter,
	}
}

// WritePoints writes the points to the underlying PointsWriter.
func (b *BufferedPointsWriter) WritePoints(ctx context.Context, p []models.Point) error {
	for len(p) > b.Available() && b.err == nil {
		if b.Buffered() == 0 {
			// Large write, empty buffer.
			// Write directly from p to avoid copy.
			b.err = b.wr.WritePoints(ctx, p)
			return b.err
		}
		n := copy(b.buf[b.n:], p)
		b.n += n
		b.err = b.Flush(ctx)
		p = p[n:]
	}
	if b.err != nil {
		return b.err
	}
	b.n += copy(b.buf[b.n:], p)
	return nil
}

// Available returns how many models.Points are unused in the buffer.
func (b *BufferedPointsWriter) Available() int { return len(b.buf) - b.n }

// Buffered returns the number of models.Points that have been written into the current buffer.
func (b *BufferedPointsWriter) Buffered() int { return b.n }

// Flush writes any buffered data to the underlying PointsWriter.
func (b *BufferedPointsWriter) Flush(ctx context.Context) error {
	if b.err != nil {
		return b.err
	}
	if b.n == 0 {
		return nil
	}

	b.err = b.wr.WritePoints(ctx, b.buf[:b.n])
	if b.err != nil {
		return b.err
	}
	b.n = 0
	return nil
}
