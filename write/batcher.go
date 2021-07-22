package write

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	platform "github.com/influxdata/influxdb/v2"
)

const (
	// DefaultMaxBytes is 500KB; this is typically 250 to 500 lines.
	DefaultMaxBytes = 500000
	// DefaultInterval will flush every 10 seconds.
	DefaultInterval = 10 * time.Second
)

var (
	// ErrLineTooLong is the error returned when reading a line that exceeds MaxLineLength.
	ErrLineTooLong = errors.New("batcher: line too long")
)

// batcher is a write service that batches for another write service.
var _ platform.WriteService = (*Batcher)(nil)

// Batcher batches line protocol for sends to output.
type Batcher struct {
	MaxFlushBytes    int                   // MaxFlushBytes is the maximum number of bytes to buffer before flushing
	MaxFlushInterval time.Duration         // MaxFlushInterval is the maximum amount of time to wait before flushing
	MaxLineLength    int                   // MaxLineLength specifies the maximum length of a single line
	Service          platform.WriteService // Service receives batches flushed from Batcher.
}

// WriteTo reads r in batches and writes to a target specified by filter.
func (b *Batcher) WriteTo(ctx context.Context, filter platform.BucketFilter, r io.Reader) error {
	return b.writeBytes(ctx, r, func(batch []byte) error {
		return b.Service.WriteTo(ctx, filter, bytes.NewReader(batch))
	})
}

func (b *Batcher) writeBytes(ctx context.Context, r io.Reader, writeFn func(batch []byte) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if b.Service == nil {
		return fmt.Errorf("destination write service required")
	}

	lines := make(chan []byte)

	errC := make(chan error, 2)
	go b.write(ctx, writeFn, lines, errC)
	go b.read(ctx, r, lines, errC)

	// we loop twice to check if both read and write have an error. if read exits
	// cleanly, then we still want to wait for write.
	for i := 0; i < 2; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errC:
			// only if there is any error, exit immediately.
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

	maxLineLength := bufio.MaxScanTokenSize
	if b.MaxLineLength > 0 {
		maxLineLength = b.MaxLineLength
	}
	scanner.Buffer(nil, maxLineLength)

	for scanner.Scan() {
		// exit early if the context is done
		select {
		// NOTE: We purposefully don't use scanner.Bytes() here because it returns a slice
		// pointing to an array which is reused / overwritten on every call to Scan().
		case lines <- []byte(scanner.Text()):
		case <-ctx.Done():
			errC <- ctx.Err()
			return
		}
	}
	err := scanner.Err()
	if errors.Is(err, bufio.ErrTooLong) {
		err = ErrLineTooLong
	}
	errC <- err
}

// finishes when the lines channel is closed or context is done.
// if an error occurs while writing data to the write service, the error is send in the
// errC channel and the function returns.
func (b *Batcher) write(ctx context.Context, writeFn func(batch []byte) error, lines <-chan []byte, errC chan<- error) {
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

	var line []byte
	var more = true
	// if read closes the channel normally, exit the loop
	for more {
		select {
		case line, more = <-lines:
			if more && string(line) != "\n" {
				buf = append(buf, line...)
			}
			// write if we exceed the max lines OR read routine has finished
			if len(buf) >= maxBytes || (!more && len(buf) > 0) {
				timer.Reset(flushInterval)
				if err := writeFn(buf); err != nil {
					errC <- err
					return
				}
				buf = buf[:0]
			}
		case <-timer.C:
			if len(buf) > 0 {
				timer.Reset(flushInterval)
				if err := writeFn(buf); err != nil {
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
