package backend

import (
	"context"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/options"
)

// TaskControlService is a low-level controller interface, intended to be passed to
// task executors and schedulers, which allows creation, completion, and status updates of runs.
type TaskControlService interface {
	// CreateNextRun attempts to create a new run.
	// The new run's ScheduledFor is assigned the earliest possible time according to task's cron,
	// that is later than any in-progress run and LatestCompleted run.
	// If the run's ScheduledFor would be later than the passed-in now, CreateNextRun returns a RunNotYetDueError.
	CreateNextRun(ctx context.Context, taskID influxdb.ID, now int64) (RunCreation, error)

	CurrentlyRunning(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error)
	ManualRuns(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error)

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

// TaskControlAdaptor creates a TaskControlService for the older TaskStore system.
// TODO(lh): remove task control adaptor when we transition away from Store.
func TaskControlAdaptor(s Store, lw LogWriter, lr LogReader) TaskControlService {
	return &taskControlAdaptor{s, lw, lr}
}

// taskControlAdaptor adapts a Store and log readers and writers to implement the task control service.
type taskControlAdaptor struct {
	s  Store
	lw LogWriter
	lr LogReader
}

func (tcs *taskControlAdaptor) CreateNextRun(ctx context.Context, taskID influxdb.ID, now int64) (RunCreation, error) {
	return tcs.s.CreateNextRun(ctx, taskID, now)
}

func (tcs *taskControlAdaptor) FinishRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	// Once we completely switch over to the new system we can look at the returned run in the tests.
	task, err := tcs.s.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}
	tcs.lr.FindRunByID(ctx, task.Org, runID)

	return nil, tcs.s.FinishRun(ctx, taskID, runID)
}

func (tcs *taskControlAdaptor) CurrentlyRunning(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error) {
	t, m, err := tcs.s.FindTaskByIDWithMeta(ctx, taskID)
	if err != nil {
		return nil, err
	}

	var rtn = make([]*influxdb.Run, len(m.CurrentlyRunning))
	for i, cr := range m.CurrentlyRunning {
		rtn[i] = &influxdb.Run{
			ID:           influxdb.ID(cr.RunID),
			TaskID:       t.ID,
			ScheduledFor: time.Unix(cr.Now, 0).UTC().Format(time.RFC3339),
		}
		if cr.RequestedAt != 0 {
			rtn[i].RequestedAt = time.Unix(cr.RequestedAt, 0).UTC().Format(time.RFC3339)
		}
	}
	return rtn, nil
}

func (tcs *taskControlAdaptor) ManualRuns(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error) {
	t, m, err := tcs.s.FindTaskByIDWithMeta(ctx, taskID)
	if err != nil {
		return nil, err
	}

	var rtn = make([]*influxdb.Run, len(m.ManualRuns))
	for i, cr := range m.ManualRuns {
		rtn[i] = &influxdb.Run{
			ID:           influxdb.ID(cr.RunID),
			TaskID:       t.ID,
			ScheduledFor: time.Unix(cr.Start, 0).UTC().Format(time.RFC3339),
		}
		if cr.RequestedAt != 0 {
			rtn[i].RequestedAt = time.Unix(cr.RequestedAt, 0).Format(time.RFC3339)
		}
	}
	return rtn, nil
}

func (tcs *taskControlAdaptor) NextDueRun(ctx context.Context, taskID influxdb.ID) (int64, error) {
	m, err := tcs.s.FindTaskMetaByID(ctx, taskID)
	if err != nil {
		return 0, err
	}
	return m.NextDueRun()
}

func (tcs *taskControlAdaptor) UpdateRunState(ctx context.Context, taskID, runID influxdb.ID, when time.Time, state RunStatus) error {
	st, m, err := tcs.s.FindTaskByIDWithMeta(ctx, taskID)
	if err != nil {
		return err
	}
	var (
		schedFor, reqAt time.Time
	)
	// check the log store
	r, err := tcs.lr.FindRunByID(ctx, st.Org, runID)
	if err == nil && r != nil {
		schedFor, err = time.Parse(time.RFC3339, r.ScheduledFor)
		if err != nil {
			return err
		}
		if r.RequestedAt != "" {
			reqAt, err = time.Parse(time.RFC3339, r.RequestedAt)
			if err != nil {
				return err
			}
		}
	}

	// in the old system the log store may not have the run until after the first
	// state update, so we will need to pull the currently running.
	if schedFor.IsZero() {
		for _, cr := range m.CurrentlyRunning {
			if influxdb.ID(cr.RunID) == runID {
				schedFor = time.Unix(cr.Now, 0)
				if cr.RequestedAt != 0 {
					reqAt = time.Unix(cr.RequestedAt, 0)
				}
			}
		}
	}

	rlb := RunLogBase{
		Task:            st,
		RunID:           runID,
		RunScheduledFor: schedFor.Unix(),
	}
	if !reqAt.IsZero() {
		rlb.RequestedAt = reqAt.Unix()
	}
	if err := tcs.lw.UpdateRunState(ctx, rlb, when, state); err != nil {
		return err
	}
	return nil
}

func (tcs *taskControlAdaptor) AddRunLog(ctx context.Context, taskID, runID influxdb.ID, when time.Time, log string) error {
	st, m, err := tcs.s.FindTaskByIDWithMeta(ctx, taskID)
	if err != nil {
		return err
	}

	var (
		schedFor, reqAt time.Time
	)

	r, err := tcs.lr.FindRunByID(ctx, st.Org, runID)
	if err == nil && r != nil {
		schedFor, err = time.Parse(time.RFC3339, r.ScheduledFor)
		if err != nil {
			return err
		}
		if r.RequestedAt != "" {
			reqAt, err = time.Parse(time.RFC3339, r.RequestedAt)
			if err != nil {
				return err
			}
		}
	}

	// in the old system the log store may not have the run until after the first
	// state update, so we will need to pull the currently running.
	if schedFor.IsZero() {
		for _, cr := range m.CurrentlyRunning {
			if influxdb.ID(cr.RunID) == runID {
				schedFor = time.Unix(cr.Now, 0)
				if cr.RequestedAt != 0 {
					reqAt = time.Unix(cr.RequestedAt, 0)
				}
			}
		}
	}

	rlb := RunLogBase{
		Task:            st,
		RunID:           runID,
		RunScheduledFor: schedFor.Unix(),
	}
	if !reqAt.IsZero() {
		rlb.RequestedAt = reqAt.Unix()
	}
	return tcs.lw.AddRunLog(ctx, rlb, when, log)
}

// ToInfluxTask converts a backend tas and meta to a influxdb.Task
// TODO(lh): remove this when we no longer need the backend store.
func ToInfluxTask(t *StoreTask, m *StoreTaskMeta) (*influxdb.Task, error) {
	opts, err := options.FromScript(t.Script)
	if err != nil {
		return nil, err
	}

	pt := &influxdb.Task{
		ID:              t.ID,
		OrganizationID:  t.Org,
		Name:            t.Name,
		Flux:            t.Script,
		Cron:            opts.Cron,
		AuthorizationID: influxdb.ID(m.AuthorizationID),
	}
	if !opts.Every.IsZero() {
		pt.Every = opts.Every.String()
	}
	if opts.Offset != nil && !opts.Offset.IsZero() {
		pt.Offset = opts.Offset.String()
	}
	if m != nil {
		pt.Status = string(m.Status)
		pt.LatestCompleted = time.Unix(m.LatestCompleted, 0).UTC().Format(time.RFC3339)
		if m.CreatedAt != 0 {
			pt.CreatedAt = time.Unix(m.CreatedAt, 0).UTC().Format(time.RFC3339)
		}
		if m.UpdatedAt != 0 {
			pt.UpdatedAt = time.Unix(m.UpdatedAt, 0).UTC().Format(time.RFC3339)
		}
		pt.AuthorizationID = influxdb.ID(m.AuthorizationID)
	}
	return pt, nil
}
