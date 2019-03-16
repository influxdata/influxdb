package backend

import (
	"context"
	"time"

	"github.com/influxdata/influxdb"
)

// TaskControlService is a task service with control functions.
type TaskControlService interface {

	// CreateNextRun attempts to create a new run.
	// The new run's now is assigned the earliest possible time according to task's cron,
	// that is later than any in-progress run and LatestCompleted run.
	// If the run's now would be later than the passed-in now, CreateNextRun returns a RunNotYetDueError.
	CreateNextRun(ctx context.Context, taskID influxdb.ID, now int64) (RunCreation, error)

	// FinishRun removes runID from the list of running tasks and if its `now` is later then last completed update it.
	FinishRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error)

	// NextDueRun returns the Unix timestamp of when the next call to CreateNextRun will be ready.
	// The returned timestamp reflects the task's delay, so it does not necessarily exactly match the schedule time.
	NextDueRun() (int64, error)

	// UpdateRunState sets the run state and the respective time.
	UpdateRunState(ctx context.Context, taskID, runID influxdb.ID, when time.Time, state RunStatus) error

	// AddRunLog adds a log line to the run.
	AddRunLog(ctx context.Context, taskID, runID influxdb.ID, when time.Time, log string) error
}
