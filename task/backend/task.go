package backend

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2"
)

// TaskControlService is a low-level controller interface, intended to be passed to
// task executors and schedulers, which allows creation, completion, and status updates of runs.
type TaskControlService interface {

	// CreateRun creates a run with a scheduled for time.
	CreateRun(ctx context.Context, taskID influxdb.ID, scheduledFor time.Time, runAt time.Time) (*influxdb.Run, error)

	CurrentlyRunning(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error)
	ManualRuns(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error)

	// StartManualRun pulls a manual run from the list and moves it to currently running.
	StartManualRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error)

	// FinishRun removes runID from the list of running tasks and if its `ScheduledFor` is later then last completed update it.
	FinishRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error)

	// UpdateRunState sets the run state at the respective time.
	UpdateRunState(ctx context.Context, taskID, runID influxdb.ID, when time.Time, state influxdb.RunStatus) error

	// AddRunLog adds a log line to the run.
	AddRunLog(ctx context.Context, taskID, runID influxdb.ID, when time.Time, log string) error
}
