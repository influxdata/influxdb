package scheduler

import (
	"context"
	"time"

	"github.com/influxdata/influxdb"
)

type CompletionStore interface {
	// SetTime will allow us to set a latest completed time for an id.
	SetTime(ctx context.Context, id influxdb.ID, lastRun time.Time) error

	// Time retrieves the latest completed for this schedulable id.
	// If GetLatestCompleted is empty scheduler will use "now" as its latest completed
	Time(ctx context.Context, id influxdb.ID) (time.Time, error)
}

// Executor is a system used by the scheduler to actually execute the scheduleable item.
type Executor interface {
	// Execute is used to execute run's for any schedulable object.
	// the executor can go through manual runs, clean currently running, and then create a new run based on `now`.
	// if Now is zero we can just do the first 2 steps (This is how we would trigger manual runs).
	// Errors returned from the execute request imply that this attempt has failed and
	// should be put back in scheduler and re executed at a alter time. We will add scheduler specific errors
	// so the time can be configurable.
	Execute(ctx context.Context, id influxdb.ID, meta interface{}, now time.Time) (Execution, error)

	// CancelExecution is used to cancel a running execution
	CancelExecution(ctx context.Context, id, executionID influxdb.ID) error
}

// Execution is the currently executing element from the a call to Execute.
type Execution interface {
	// ID is a unique ID usable to look up or cancel a run
	ID() influxdb.ID

	// Status can be used to view what the current status of this execution is
	Status() string

	// Done() returns a read only channel that when closed indicates the execution is complete
	Done() <-chan struct{}

	// Error returns an error only when the execution is complete.
	// This is a hanging call until Done() is closed.
	Error() error
}

// Schedulable is a object that can be scheduled by the scheduler.
type Schedulable interface {
	// ID a unique ID we can to lookup a scheduler
	ID() influxdb.ID

	// Schedule is the schedule you want execution.
	Schedule() string // cron or every dx

	// Extra meta data to give to the executor on execution
	Meta() interface{}
}

// Scheduler is a example interface of a Scheduler.
// todo(lh): this probably wont be defined here but defined in the system that requires it.
type Scheduler interface {
	// Start allows the scheduler to Tick. A scheduler without start will do nothing
	Start(ctx context.Context)

	// Stop a scheduler from ticking.
	Stop()

	Now() time.Time

	// ClaimTask begins control of task execution in this scheduler.
	ClaimTask(authCtx context.Context, task Schedulable) error

	// UpdateTask will update the concurrency and the runners for a task
	UpdateTask(authCtx context.Context, task Schedulable) error

	// ReleaseTask immediately cancels any in-progress runs for the given task ID,
	// and releases any resources related to management of that task.
	ReleaseTask(taskID influxdb.ID) error

	// Cancel stops an executing run.
	CancelRun(ctx context.Context, taskID, runID influxdb.ID) error
}
