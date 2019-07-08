package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb"
)

// TaskControlService is a low-level controller interface, intended to be passed to
// task executors and schedulers, which allows creation, completion, and status updates of runs.
type TaskControlService interface {
	// CreateNextRun attempts to create a new run.
	// The new run's ScheduledFor is assigned the earliest possible time according to task's cron,
	// that is later than any in-progress run and LatestCompleted run.
	// If the run's ScheduledFor would be later than the passed-in now, CreateNextRun returns an ErrRunNotDueYet.
	CreateNextRun(ctx context.Context, taskID influxdb.ID, now int64) (RunCreation, error)

	// NextDueRun returns the Unix timestamp of when the next call to CreateNextRun will be ready.
	// The returned timestamp reflects the task's offset, so it does not necessarily exactly match the schedule time.
	NextDueRun(ctx context.Context, taskID influxdb.ID) (int64, error)

	// CreateRun creates a run with a schedule for time.
	// This differes from CreateNextRun in that it should not to use some scheduling system to determin when the run
	// should happen.
	// TODO(lh): remove comment once we no longer need create next run.
	CreateRun(ctx context.Context, taskID influxdb.ID, scheduledFor time.Time) (*influxdb.Run, error)

	CurrentlyRunning(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error)
	ManualRuns(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error)

	// StartManualRun pulls a manual run from the list and moves it to currently running.
	StartManualRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error)

	// FinishRun removes runID from the list of running tasks and if its `ScheduledFor` is later then last completed update it.
	FinishRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error)

	// UpdateRunState sets the run state at the respective time.
	UpdateRunState(ctx context.Context, taskID, runID influxdb.ID, when time.Time, state RunStatus) error

	// AddRunLog adds a log line to the run.
	AddRunLog(ctx context.Context, taskID, runID influxdb.ID, when time.Time, log string) error
}

type TaskStatus string

const (
	TaskActive   TaskStatus = "active"
	TaskInactive TaskStatus = "inactive"

	DefaultTaskStatus TaskStatus = TaskActive
)

type RunStatus int

const (
	RunStarted RunStatus = iota
	RunSuccess
	RunFail
	RunCanceled
	RunScheduled
)

func (r RunStatus) String() string {
	switch r {
	case RunStarted:
		return "started"
	case RunSuccess:
		return "success"
	case RunFail:
		return "failed"
	case RunCanceled:
		return "canceled"
	case RunScheduled:
		return "scheduled"
	}
	panic(fmt.Sprintf("unknown RunStatus: %d", r))
}

// RequestStillQueuedError is returned when attempting to retry a run which has not yet completed.
type RequestStillQueuedError struct {
	// Unix timestamps matching existing request's start and end.
	Start, End int64
}

const fmtRequestStillQueued = "previous retry for start=%s end=%s has not yet finished"

func (e RequestStillQueuedError) Error() string {
	return fmt.Sprintf(fmtRequestStillQueued,
		time.Unix(e.Start, 0).UTC().Format(time.RFC3339),
		time.Unix(e.End, 0).UTC().Format(time.RFC3339),
	)
}

// ParseRequestStillQueuedError attempts to parse a RequestStillQueuedError from msg.
// If msg is formatted correctly, the resultant error is returned; otherwise it returns nil.
func ParseRequestStillQueuedError(msg string) *RequestStillQueuedError {
	var s, e string
	n, err := fmt.Sscanf(msg, fmtRequestStillQueued, &s, &e)
	if err != nil || n != 2 {
		return nil
	}

	start, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}

	end, err := time.Parse(time.RFC3339, e)
	if err != nil {
		return nil
	}

	return &RequestStillQueuedError{Start: start.Unix(), End: end.Unix()}
}
