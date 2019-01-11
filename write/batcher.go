package write

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	platform "github.com/influxdata/influxdb"
)

const (
	// DefaultMaxBytes is 500KB; this is typically 250 to 500 lines.
	DefaultMaxBytes = 500000
	// DefaultInterval will flush every 10 seconds.
	DefaultInterval = 10 * time.Second
)

// batcher is a write service that batches for another write service.
var _ platform.WriteService = (*Batcher)(nil)

// Batcher batches line protocol for sends to output.
type Batcher struct {
	MaxFlushBytes    int                   // MaxFlushBytes is the maximum number of bytes to buffer before flushing
	MaxFlushInterval time.Duration         // MaxFlushInterval is the maximum amount of time to wait before flushing
	Service          platform.WriteService // Service receives batches flushed from Batcher.
}

// Write reads r in batches and sends to the output.
func (b *Batcher) Write(ctx context.Context, org, bucket platform.ID, r io.Reader) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if b.Service == nil {
		return fmt.Errorf("destination write service required")
	}

	lines := make(chan []byte)

	errC := make(chan error, 2)
	go b.write(ctx, org, bucket, lines, errC)
	go b.read(ctx, r, lines, errC)

	// we loop twice to check if both read and write have an error. if read exits
	// cleanly, then we still want to wait for write.
	for i := 0; i < 2; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errC:
			// onky if there is any error, exit immediately.
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// read will close the line channel when there is no more data, or an error occurs.
// it is possible for an io.Reader to block forever; Write's context can be
// used to cancel, but, it's possible there will be dangling read go routines.
func (b *Batcher) read(ctx context.Context, r io.Reader, lines chan<- []byte, errC chan<- error) {
	defer close(lines)
	scanner := bufio.NewScanner(r)
	scanner.Split(ScanLines)
	for scanner.Scan() {
		// exit early if the context is done
		select {
		case lines <- scanner.Bytes():
		case <-ctx.Done():
			errC <- ctx.Err()
			return
		}
	}
	errC <- scanner.Err()
}

// finishes when the lines channel is closed or context is done.
// if an error occurs while writing data to the write service, the error is send in the
// errC channel and the function returns.
func (b *Batcher) write(ctx context.Context, org, bucket platform.ID, lines <-chan []byte, errC chan<- error) {
	flushInterval := b.MaxFlushInterval
	if flushInterval == 0 {
		flushInterval = DefaultInterval
	}

	maxBytes := b.MaxFlushBytes
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytes
	}

	timer := time.NewTimer(flushInterval)
	defer func() { _ = timer.Stop() }()

	buf := make([]byte, 0, maxBytes)
	r := bytes.NewReader(buf)

	var line []byte
	var more = true
	// if read closes the channel normally, exit the loop
	for more {
		select {
		case line, more = <-lines:
			if more {
				buf = append(buf, line...)
			}
			// write if we exceed the max lines OR read routine has finished
			if len(buf) >= maxBytes || (!more && len(buf) > 0) {
				r.Reset(buf)
				timer.Reset(flushInterval)
				if err := b.Service.Write(ctx, org, bucket, r); err != nil {
					errC <- err
					return
				}
				buf = buf[:0]
			}
		case <-timer.C:
			if len(buf) > 0 {
				r.Reset(buf)
				timer.Reset(flushInterval)
				if err := b.Service.Write(ctx, org, bucket, r); err != nil {
					errC <- err
					return
				}
				buf = buf[:0]
			}
		case <-ctx.Done():
			errC <- ctx.Err()
			return
		}
	}

	errC <- nil
}

// ScanLines is used in bufio.Scanner.Split to split lines of line protocol.
func ScanLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		// We have a full newline-terminated line.
		return i + 1, data[0 : i+1], nil

	}

	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}

	// Request more data.
	return 0, nil, nil
}
