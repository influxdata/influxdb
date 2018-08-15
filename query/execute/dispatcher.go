package execute

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Dispatcher schedules work for a query.
// Each transformation submits work to be done to the dispatcher.
// Then the dispatcher schedules to work based on the available resources.
type Dispatcher interface {
	// Schedule fn to be executed
	Schedule(fn ScheduleFunc)
}

// ScheduleFunc is a function that represents work to do.
// The throughput is the maximum number of messages to process for this scheduling.
type ScheduleFunc func(throughput int)

// poolDispatcher implements Dispatcher using a pool of goroutines.
type poolDispatcher struct {
	work chan ScheduleFunc

	throughput int

	mu      sync.Mutex
	closed  bool
	closing chan struct{}
	wg      sync.WaitGroup
	err     error
	errC    chan error

	logger *zap.Logger
}

func newPoolDispatcher(throughput int, logger *zap.Logger) *poolDispatcher {
	return &poolDispatcher{
		throughput: throughput,
		work:       make(chan ScheduleFunc, 100),
		closing:    make(chan struct{}),
		errC:       make(chan error, 1),
		logger:     logger.With(zap.String("component", "dispatcher")),
	}
}

func (d *poolDispatcher) Schedule(fn ScheduleFunc) {
	select {
	case d.work <- fn:
	case <-d.closing:
	}
}

func (d *poolDispatcher) Start(n int, ctx context.Context) {
	d.wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer d.wg.Done()
			// Setup panic handling on the worker goroutines
			defer func() {
				if e := recover(); e != nil {
					var err error
					switch e := e.(type) {
					case error:
						err = e
					default:
						err = fmt.Errorf("%v", e)
					}
					d.setErr(fmt.Errorf("panic: %v\n%s", err, debug.Stack()))
					if entry := d.logger.Check(zapcore.InfoLevel, "Dispatcher panic"); entry != nil {
						entry.Stack = string(debug.Stack())
						entry.Write(zap.Error(err))
					}
				}
			}()
			d.run(ctx)
		}()
	}
}

// Err returns a channel with will produce an error if encountered.
func (d *poolDispatcher) Err() <-chan error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.errC
}

func (d *poolDispatcher) setErr(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// TODO(nathanielc): Collect all error information.
	if d.err == nil {
		d.err = err
		d.errC <- err
	}
}

//Stop the dispatcher.
func (d *poolDispatcher) Stop() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return d.err
	}
	d.closed = true
	close(d.closing)
	d.wg.Wait()
	return d.err
}

// run is the logic executed by each worker goroutine in the pool.
func (d *poolDispatcher) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// Immediately return, do not process any more work
			return
		case <-d.closing:
			// We are done, nothing left to do.
			return
		case fn := <-d.work:
			fn(d.throughput)
		}
	}
}
