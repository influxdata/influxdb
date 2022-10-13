// Package mock contains mock implementations of different task interfaces.
package mock

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/task/backend/executor"
	"github.com/influxdata/influxdb/v2/task/backend/scheduler"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
)

type promise struct {
	run        *taskmodel.Run
	hangingFor time.Duration

	done chan struct{}
	err  error

	ctx        context.Context
	cancelFunc context.CancelFunc
}

// ID is the id of the run that was created
func (p *promise) ID() platform.ID {
	return p.run.ID
}

// Cancel is used to cancel a executing query
func (p *promise) Cancel(ctx context.Context) {
	// call cancelfunc
	p.cancelFunc()

	// wait for ctx.Done or p.Done
	select {
	case <-p.Done():
	case <-ctx.Done():
	}
}

// Done provides a channel that closes on completion of a promise
func (p *promise) Done() <-chan struct{} {
	return p.done
}

// Error returns the error resulting from a run execution.
// If the execution is not complete error waits on Done().
func (p *promise) Error() error {
	<-p.done
	return p.err
}

func (e *Executor) createPromise(ctx context.Context, run *taskmodel.Run) (*promise, error) {
	ctx, cancel := context.WithCancel(ctx)
	p := &promise{
		run:        run,
		done:       make(chan struct{}),
		ctx:        ctx,
		cancelFunc: cancel,
		hangingFor: e.hangingFor,
	}

	go func() {
		time.Sleep(p.hangingFor)
		close(p.done)
	}()

	return p, nil
}

type Executor struct {
	mu         sync.Mutex
	wg         sync.WaitGroup
	hangingFor time.Duration

	// Forced error for next call to Execute.
	nextExecuteErr error

	ExecutedChan chan scheduler.ID
}

var _ scheduler.Executor = (*Executor)(nil)

func NewExecutor() *Executor {
	return &Executor{
		hangingFor:   time.Second,
		ExecutedChan: make(chan scheduler.ID, 10),
	}
}

func (e *Executor) Execute(ctx context.Context, id scheduler.ID, scheduledAt time.Time, runAt time.Time) error {

	select {
	case e.ExecutedChan <- scheduler.ID(id):
	default:
		return errors.New("could not add task ID to executedChan")
	}

	return nil
}

func (e *Executor) ManualRun(ctx context.Context, id platform.ID, runID platform.ID) (executor.Promise, error) {
	run := &taskmodel.Run{ID: runID, TaskID: id, StartedAt: time.Now().UTC()}
	p, err := e.createPromise(ctx, run)
	return p, err
}

func (e *Executor) ScheduleManualRun(ctx context.Context, id platform.ID, runID platform.ID) error {
	return nil
}

func (e *Executor) Wait() {
	e.wg.Wait()
}

func (e *Executor) Cancel(context.Context, platform.ID) error {
	return nil
}

// FailNextCallToExecute causes the next call to e.Execute to unconditionally return err.
func (e *Executor) FailNextCallToExecute(err error) {
	e.mu.Lock()
	e.nextExecuteErr = err
	e.mu.Unlock()
}
