package scheduler

import (
	"context"
	"time"
)

// ID duplicates the influxdb ID so users of the scheduler don't have to
// import influxdb for the id.
// TODO(lh): maybe make this its own thing sometime in the future.
type ID uint64

// Executor is a system used by the scheduler to actually execute the scheduleable item.
type Executor interface {
	// Execute is used to execute run's for any schedulable object.
	// the executor can go through manual runs, clean currently running, and then create a new run based on `now`.
	// if Now is zero we can just do the first 2 steps (This is how we would trigger manual runs).
	// Errors returned from the execute request imply that this attempt has failed and
	// should be put back in scheduler and re executed at a alter time. We will add scheduler specific errors
	// so the time can be configurable.
	Execute(ctx context.Context, id ID, scheduledAt time.Time) error
}

// Schedulable is the interface that encapsulates the state that is required to schedule a job.
type Schedulable interface {
	// ID is the unique identifier for this Schedulable
	ID() ID

	// Schedule defines the frequency for which this Schedulable should be
	// queued for execution.
	Schedule() Schedule

	// Offset defines a negative or positive duration that should be added
	// to the scheduled time, resulting in the instance running earlier or later
	// than the scheduled time.
	Offset() time.Duration

	// LastScheduled specifies last time this Schedulable was queued
	// for execution.
	LastScheduled() time.Time
}

// SchedulableService encapsulates the work necessary to schedule a job
type SchedulableService interface {

	// UpdateLastScheduled notifies the instance that it was scheduled for
	// execution at the specified time
	UpdateLastScheduled(ctx context.Context, id ID, t time.Time) error
}

type Schedule struct {
}

// Scheduler is a example interface of a Scheduler.
// // todo(lh): remove this once we start building the actual scheduler
type Scheduler interface {

	// Schedule adds the specified task to the scheduler.
	Schedule(task Schedulable) error

	// Release removes the specified task from the scheduler.
	Release(taskID ID) error
}
