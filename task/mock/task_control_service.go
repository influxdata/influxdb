package mock

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/task/backend"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
)

var idgen = snowflake.NewDefaultIDGenerator()

// TaskControlService is a mock implementation of TaskControlService (used by NewScheduler).
type TaskControlService struct {
	mu sync.Mutex
	// Map of stringified task ID to last ID used for run.
	runs map[platform.ID]map[platform.ID]*taskmodel.Run

	// Map of stringified, concatenated task and platform ID, to runs that have been created.
	created map[string]*taskmodel.Run

	// Map of stringified task ID to task meta.
	tasks      map[platform.ID]*taskmodel.Task
	manualRuns []*taskmodel.Run
	// Map of task ID to total number of runs created for that task.
	totalRunsCreated map[platform.ID]int
	finishedRuns     map[platform.ID]*taskmodel.Run
}

var _ backend.TaskControlService = (*TaskControlService)(nil)

func NewTaskControlService() *TaskControlService {
	return &TaskControlService{
		runs:             make(map[platform.ID]map[platform.ID]*taskmodel.Run),
		finishedRuns:     make(map[platform.ID]*taskmodel.Run),
		tasks:            make(map[platform.ID]*taskmodel.Task),
		created:          make(map[string]*taskmodel.Run),
		totalRunsCreated: make(map[platform.ID]int),
	}
}

// SetTask sets the task.
// SetTask must be called before CreateNextRun, for a given task ID.
func (d *TaskControlService) SetTask(task *taskmodel.Task) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.tasks[task.ID] = task
}

func (d *TaskControlService) SetManualRuns(runs []*taskmodel.Run) {
	d.manualRuns = runs
}

func (t *TaskControlService) CreateRun(_ context.Context, taskID platform.ID, scheduledFor time.Time, runAt time.Time) (*taskmodel.Run, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	runID := idgen.ID()
	runs, ok := t.runs[taskID]
	if !ok {
		runs = make(map[platform.ID]*taskmodel.Run)
	}
	runs[runID] = &taskmodel.Run{
		ID:           runID,
		ScheduledFor: scheduledFor,
	}
	t.runs[taskID] = runs
	return runs[runID], nil
}

func (t *TaskControlService) StartManualRun(_ context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var run *taskmodel.Run
	for i, r := range t.manualRuns {
		if r.ID == runID {
			run = r
			t.manualRuns = append(t.manualRuns[:i], t.manualRuns[i+1:]...)
		}
	}
	if run == nil {
		return nil, taskmodel.ErrRunNotFound
	}
	return run, nil
}

func (d *TaskControlService) FinishRun(_ context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	tid := taskID
	rid := runID
	r := d.runs[tid][rid]
	delete(d.runs[tid], rid)
	t := d.tasks[tid]

	if r.ScheduledFor.After(t.LatestCompleted) {
		t.LatestCompleted = r.ScheduledFor
	}

	d.finishedRuns[rid] = r
	delete(d.created, tid.String()+rid.String())
	return r, nil
}

func (t *TaskControlService) CurrentlyRunning(ctx context.Context, taskID platform.ID) ([]*taskmodel.Run, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	rtn := []*taskmodel.Run{}
	for _, run := range t.runs[taskID] {
		rtn = append(rtn, run)
	}
	return rtn, nil
}

func (t *TaskControlService) ManualRuns(ctx context.Context, taskID platform.ID) ([]*taskmodel.Run, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.manualRuns != nil {
		return t.manualRuns, nil
	}
	return []*taskmodel.Run{}, nil
}

// UpdateRunState sets the run state at the respective time.
func (d *TaskControlService) UpdateRunState(ctx context.Context, taskID, runID platform.ID, when time.Time, state taskmodel.RunStatus) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	run, ok := d.runs[taskID][runID]
	if !ok {
		panic("run state called without a run")
	}
	switch state {
	case taskmodel.RunStarted:
		run.StartedAt = when
	case taskmodel.RunSuccess, taskmodel.RunFail, taskmodel.RunCanceled:
		run.FinishedAt = when
	case taskmodel.RunScheduled:
		// nothing
	default:
		panic("invalid status")
	}
	run.Status = state.String()
	return nil
}

// AddRunLog adds a log line to the run.
func (d *TaskControlService) AddRunLog(ctx context.Context, taskID, runID platform.ID, when time.Time, log string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	run := d.runs[taskID][runID]
	if run == nil {
		panic("cannot add a log to a non existent run")
	}
	run.Log = append(run.Log, taskmodel.Log{RunID: runID, Time: when.Format(time.RFC3339Nano), Message: log})
	return nil
}

func (d *TaskControlService) CreatedFor(taskID platform.ID) []*taskmodel.Run {
	d.mu.Lock()
	defer d.mu.Unlock()

	var qrs []*taskmodel.Run
	for _, qr := range d.created {
		if qr.TaskID == taskID {
			qrs = append(qrs, qr)
		}
	}

	return qrs
}

// TotalRunsCreatedForTask returns the number of runs created for taskID.
func (d *TaskControlService) TotalRunsCreatedForTask(taskID platform.ID) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.totalRunsCreated[taskID]
}

// PollForNumberCreated blocks for a small amount of time waiting for exactly the given count of created and unfinished runs for the given task ID.
// If the expected number isn't found in time, it returns an error.
//
// Because the scheduler and executor do a lot of state changes asynchronously, this is useful in test.
func (d *TaskControlService) PollForNumberCreated(taskID platform.ID, count int) ([]*taskmodel.Run, error) {
	const numAttempts = 50
	actualCount := 0
	var created []*taskmodel.Run
	for i := 0; i < numAttempts; i++ {
		time.Sleep(2 * time.Millisecond) // we sleep even on first so it becomes more likely that we catch when too many are produced.
		created = d.CreatedFor(taskID)
		actualCount = len(created)
		if actualCount == count {
			return created, nil
		}
	}
	return created, fmt.Errorf("did not see count of %d created run(s) for task with ID %s in time, instead saw %d", count, taskID, actualCount) // we return created anyways, to make it easier to debug
}

func (d *TaskControlService) FinishedRun(runID platform.ID) *taskmodel.Run {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.finishedRuns[runID]
}

func (d *TaskControlService) FinishedRuns() []*taskmodel.Run {
	rtn := []*taskmodel.Run{}
	for _, run := range d.finishedRuns {
		rtn = append(rtn, run)
	}

	sort.Slice(rtn, func(i, j int) bool { return rtn[i].ScheduledFor.Before(rtn[j].ScheduledFor) })
	return rtn
}
