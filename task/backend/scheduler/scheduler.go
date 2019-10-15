package scheduler

import (
	"context"
	"time"

	"github.com/influxdata/cron"
)

// ID duplicates the influxdb ID so users of the scheduler don't have to
// import influxdb for the ID.
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

// Schedulable is the interface that encapsulates work that
// is to be executed on a specified schedule.
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

// NewSchedule takes a cron string
func NewSchedule(c string) (Schedule, error) {
	sch, err := cron.ParseUTC(c)
	return Schedule{cron: sch}, err
}

// Schedule is an object a valid schedule of runs
type Schedule struct {
	cron cron.Parsed
}

// Next returns the next time after from that a schedule should trigger on.
func (s Schedule) Next(from time.Time) (time.Time, error) {
	return cron.Parsed(s.cron).Next(from)
}

// ValidSchedule returns an error if the cron string is invalid.
func ValidateSchedule(c string) error {
	_, err := cron.ParseUTC(c)
	return err
}

// Scheduler is a example interface of a Scheduler.
// // todo(lh): remove this once we start building the actual scheduler
type Scheduler interface {

	// Schedule adds the specified task to the scheduler.
	Schedule(task Schedulable) error

	// Release removes the specified task from the scheduler.
	Release(taskID ID) error
}

type ErrUnrecoverable struct {
	error
}

func (e *ErrUnrecoverable) Error() string {
	if e.error != nil {
		return "error unrecoverable error on task run " + e.error.Error()
	}
	return "error unrecoverable error on task run"
}

func (e *ErrUnrecoverable) Unwrap() error {
	return e.error
}
