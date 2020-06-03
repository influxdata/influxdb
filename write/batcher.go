package write

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
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
	// WriteWorkers is a number of concurrent workers that write to service
	WriteWorkers int
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

	// a limited set of workers is used to write data
	workers := int(math.Max(float64(b.WriteWorkers), 1))

	// reuse buffers in a bufferPool
	var buffPoolMux sync.Mutex
	bufferPool := make([][]byte, 0, workers)
	newBuffer := func() []byte {
		buffPoolMux.Lock()
		defer buffPoolMux.Unlock()
		var buf []byte
		if len(bufferPool) > 0 {
			buf = bufferPool[len(bufferPool)-1]
			bufferPool = bufferPool[0 : len(bufferPool)-1]
		} else {
			buf = make([]byte, 0, maxBytes)
		}
		return buf
	}
	reuseBuffer := func(buf []byte) {
		buffPoolMux.Lock()
		bufferPool = append(bufferPool, buf[:0])
		buffPoolMux.Unlock()
	}

	// send data by multiple workers
	var workerGroup sync.WaitGroup
	writeDataC := make(chan []byte)
	workerErrorsC := make(chan error, workers)
	// start workers for jobs
	for i := 0; i < workers; i++ {
		workerGroup.Add(1)
		go func() {
			defer workerGroup.Done()
			for {
				buf, more := <-writeDataC
				if more {
					err := b.Service.Write(ctx, org, bucket, bytes.NewReader(buf))
					reuseBuffer(buf)
					if err != nil {
						workerErrorsC <- err
						return
					}
					continue
				}
				return
			}
		}()
	}

	buf := newBuffer()
	var line []byte
	var more = true
	var writeError error

	// pass return value through errC
	defer func() {
		// wait for workers to finish
		close(writeDataC)
		workerGroup.Wait()
		// use the last error, if any
		if writeError != nil {
			errC <- writeError
			return
		}
		select {
		// if a worker failed, report it
		case writeError = <-workerErrorsC:
			errC <- writeError
		// success
		default:
			errC <- nil
		}
	}()

	// if read closes the channel normally, exit the loop
	for more {
		select {
		case line, more = <-lines:
			if more {
				buf = append(buf, line...)
			}
			// write if we exceed the max lines OR read routine has finished
			if len(buf) >= maxBytes || (!more && len(buf) > 0) {
				timer.Reset(flushInterval)
				writeDataC <- buf
				buf = newBuffer()
			}
		case <-timer.C:
			if len(buf) > 0 {
				timer.Reset(flushInterval)
				writeDataC <- buf
				buf = newBuffer()
			}
		case <-ctx.Done():
			writeError = ctx.Err()
			return
		case writeError = <-workerErrorsC:
			return
		}
	}
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
