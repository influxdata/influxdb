package backend

import (
	"context"
	"time"

	"github.com/influxdata/influxdb"
)

// TaskControlService is a low-level controller interface, intended to be passed to
// task executors and schedulers, which allows creation, completion, and status updates of runs.
type TaskControlService interface {
	// CreateNextRun attempts to create a new run.
	// The new run's ScheduledFor is assigned the earliest possible time according to task's cron,
	// that is later than any in-progress run and LatestCompleted run.
	// If the run's ScheduledFor would be later than the passed-in now, CreateNextRun returns a RunNotYetDueError.
	CreateNextRun(ctx context.Context, taskID influxdb.ID, now int64) (RunCreation, error)

	// FinishRun removes runID from the list of running tasks and if its `ScheduledFor` is later then last completed update it.
	FinishRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error)

	// NextDueRun returns the Unix timestamp of when the next call to CreateNextRun will be ready.
	// The returned timestamp reflects the task's offset, so it does not necessarily exactly match the schedule time.
	NextDueRun(ctx context.Context, taskID influxdb.ID) (int64, error)

	// UpdateRunState sets the run state at the respective time.
	UpdateRunState(ctx context.Context, taskID, runID influxdb.ID, when time.Time, state RunStatus) error

	// AddRunLog adds a log line to the run.
	AddRunLog(ctx context.Context, taskID, runID influxdb.ID, when time.Time, log string) error
}
