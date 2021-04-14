package backend

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
)

// TaskControlService is a low-level controller interface, intended to be passed to
// task executors and schedulers, which allows creation, completion, and status updates of runs.
type TaskControlService interface {

	// CreateRun creates a run with a scheduled for time.
	CreateRun(ctx context.Context, taskID platform.ID, scheduledFor time.Time, runAt time.Time) (*taskmodel.Run, error)

	CurrentlyRunning(ctx context.Context, taskID platform.ID) ([]*taskmodel.Run, error)
	ManualRuns(ctx context.Context, taskID platform.ID) ([]*taskmodel.Run, error)

	// StartManualRun pulls a manual run from the list and moves it to currently running.
	StartManualRun(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error)

	// FinishRun removes runID from the list of running tasks and if its `ScheduledFor` is later then last completed update it.
	FinishRun(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error)

	// UpdateRunState sets the run state at the respective time.
	UpdateRunState(ctx context.Context, taskID, runID platform.ID, when time.Time, state taskmodel.RunStatus) error

	// AddRunLog adds a log line to the run.
	AddRunLog(ctx context.Context, taskID, runID platform.ID, when time.Time, log string) error
}
