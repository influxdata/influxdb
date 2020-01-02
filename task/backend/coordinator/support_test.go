package coordinator

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/backend/executor"
	"github.com/influxdata/influxdb/task/backend/scheduler"
)

var _ Executor = (*executorE)(nil)

type (
	executorE struct {
		calls []interface{}
	}

	manualRunCall struct {
		TaskID influxdb.ID
		RunID  influxdb.ID
	}

	cancelCallC struct {
		RunID influxdb.ID
	}
)

type (
	schedulerC struct {
		scheduler.Scheduler

		calls []interface{}
	}

	scheduleCall struct {
		Task scheduler.Schedulable
	}

	releaseCallC struct {
		TaskID scheduler.ID
	}
)

type (
	promise struct {
		run *influxdb.Run

		done chan struct{}
		err  error

		ctx        context.Context
		cancelFunc context.CancelFunc
	}
)

// ID is the id of the run that was created
func (p *promise) ID() influxdb.ID {
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

func (s *schedulerC) Schedule(task scheduler.Schedulable) error {
	s.calls = append(s.calls, scheduleCall{task})

	return nil
}

func (s *schedulerC) Release(taskID scheduler.ID) error {
	s.calls = append(s.calls, releaseCallC{taskID})

	return nil
}

func (e *executorE) ManualRun(ctx context.Context, id influxdb.ID, runID influxdb.ID) (executor.Promise, error) {
	e.calls = append(e.calls, manualRunCall{id, runID})
	ctx, cancel := context.WithCancel(ctx)
	p := promise{
		done:       make(chan struct{}),
		ctx:        ctx,
		cancelFunc: cancel,
	}
	close(p.done)

	err := p.Error()
	return &p, err
}

func (e *executorE) Cancel(ctx context.Context, runID influxdb.ID) error {
	e.calls = append(e.calls, cancelCallC{runID})
	return nil
}
