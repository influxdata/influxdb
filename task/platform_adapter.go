package task

import (
	"context"
	"errors"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/influxdata/influxdb/task/options"
)

type RunController interface {
	CancelRun(ctx context.Context, taskID, runID platform.ID) error
	//TODO: add retry run to this.
}

// PlatformAdapter wraps a task.Store into the platform.TaskService interface.
func PlatformAdapter(s backend.Store, r backend.LogReader, rc RunController) platform.TaskService {
	return pAdapter{s: s, r: r, rc: rc}
}

type pAdapter struct {
	s  backend.Store
	rc RunController
	r  backend.LogReader
}

var _ platform.TaskService = pAdapter{}

func (p pAdapter) FindTaskByID(ctx context.Context, id platform.ID) (*platform.Task, error) {
	t, m, err := p.s.FindTaskByIDWithMeta(ctx, id)
	if err != nil {
		return nil, err
	}

	// The store interface specifies that a returned task is nil if the operation succeeded without a match.
	if t == nil {
		return nil, nil
	}

	return toPlatformTask(*t, m)
}

func (p pAdapter) FindTasks(ctx context.Context, filter platform.TaskFilter) ([]*platform.Task, int, error) {
	params := backend.TaskSearchParams{PageSize: filter.Limit}
	if filter.Organization != nil {
		params.Org = *filter.Organization
	}
	if filter.User != nil {
		params.User = *filter.User
	}
	if filter.After != nil {
		params.After = *filter.After
	}
	ts, err := p.s.ListTasks(ctx, params)
	if err != nil {
		return nil, 0, err
	}

	pts := make([]*platform.Task, len(ts))
	for i, t := range ts {
		pts[i], err = toPlatformTask(t.Task, &t.Meta)
		if err != nil {
			return nil, 0, err
		}
	}

	return pts, len(pts), nil
}

func (p pAdapter) CreateTask(ctx context.Context, t *platform.Task) error {
	opts, err := options.FromScript(t.Flux)
	if err != nil {
		return err
	}

	// TODO(mr): decide whether we allow user to configure scheduleAfter. https://github.com/influxdata/influxdb/issues/10884
	scheduleAfter := time.Now().Unix()

	if t.Status == "" {
		t.Status = string(backend.DefaultTaskStatus)
	}

	req := backend.CreateTaskRequest{
		Org:           t.OrganizationID,
		User:          t.Owner.ID,
		Script:        t.Flux,
		ScheduleAfter: scheduleAfter,
		Status:        backend.TaskStatus(t.Status),
	}

	id, err := p.s.CreateTask(ctx, req)
	if err != nil {
		return err
	}
	t.ID = id
	t.Cron = opts.Cron
	t.Name = opts.Name

	if opts.Every != 0 {
		t.Every = opts.Every.String()
	}
	if opts.Offset != 0 {
		t.Offset = opts.Offset.String()
	}

	return nil
}

func (p pAdapter) UpdateTask(ctx context.Context, id platform.ID, upd platform.TaskUpdate) (*platform.Task, error) {
	if err := upd.Validate(); err != nil {
		return nil, err
	}
	req := backend.UpdateTaskRequest{ID: id}
	if upd.Flux != nil {
		req.Script = *upd.Flux
	}
	if upd.Status != nil {
		req.Status = backend.TaskStatus(*upd.Status)
	}
	req.Options = upd.Options
	res, err := p.s.UpdateTask(ctx, req)
	if err != nil {
		return nil, err
	}
	if res.NewTask.Script == "" {
		return nil, errors.New("script not defined in the store")
	}
	opts, err := options.FromScript(res.NewTask.Script)
	if err != nil {
		return nil, err
	}

	task := &platform.Task{
		ID:     id,
		Name:   opts.Name,
		Status: res.NewMeta.Status,
		Owner:  platform.User{},
		Flux:   res.NewTask.Script,
		Every:  opts.Every.String(),
		Cron:   opts.Cron,
		Offset: opts.Offset.String(),
	}

	t, err := p.s.FindTaskByID(ctx, id)
	if err != nil {
		return nil, err
	}
	task.Owner.ID = t.User
	task.OrganizationID = t.Org

	return task, nil
}

func (p pAdapter) DeleteTask(ctx context.Context, id platform.ID) error {
	_, err := p.s.DeleteTask(ctx, id)
	// TODO(mr): Store.DeleteTask returns false, nil if ID didn't match; do we want to handle that case?
	return err
}

func (p pAdapter) FindLogs(ctx context.Context, filter platform.LogFilter) ([]*platform.Log, int, error) {
	logs, err := p.r.ListLogs(ctx, filter)
	logPointers := make([]*platform.Log, len(logs))
	for i := range logs {
		logPointers[i] = &logs[i]
	}
	return logPointers, len(logs), err
}

func (p pAdapter) FindRuns(ctx context.Context, filter platform.RunFilter) ([]*platform.Run, int, error) {
	runs, err := p.r.ListRuns(ctx, filter)
	return runs, len(runs), err
}

func (p pAdapter) FindRunByID(ctx context.Context, taskID, id platform.ID) (*platform.Run, error) {
	task, err := p.s.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}
	return p.r.FindRunByID(ctx, task.Org, id)
}

func (p pAdapter) RetryRun(ctx context.Context, taskID, id platform.ID) (*platform.Run, error) {
	task, err := p.s.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	run, err := p.r.FindRunByID(ctx, task.Org, id)
	if err != nil {
		return nil, err
	}
	if run.Status == backend.RunStarted.String() {
		return nil, backend.ErrRunNotFinished
	}

	scheduledTime, err := time.Parse(time.RFC3339, run.ScheduledFor)
	if err != nil {
		return nil, err
	}
	t := scheduledTime.UTC().Unix()
	requestedAt := time.Now().Unix()
	m, err := p.s.ManuallyRunTimeRange(ctx, run.TaskID, t, t, requestedAt)
	if err != nil {
		return nil, err
	}
	return &platform.Run{
		ID:           platform.ID(m.RunID),
		TaskID:       run.TaskID,
		RequestedAt:  time.Unix(requestedAt, 0).Format(time.RFC3339),
		Status:       backend.RunScheduled.String(),
		ScheduledFor: run.ScheduledFor,
	}, nil
}

func (p pAdapter) ForceRun(ctx context.Context, taskID platform.ID, scheduledFor int64) (*platform.Run, error) {
	requestedAt := time.Now()
	m, err := p.s.ManuallyRunTimeRange(ctx, taskID, scheduledFor, scheduledFor, requestedAt.Unix())
	if err != nil {
		return nil, err
	}
	return &platform.Run{
		ID:           platform.ID(m.RunID),
		TaskID:       taskID,
		RequestedAt:  requestedAt.UTC().Format(time.RFC3339),
		Status:       backend.RunScheduled.String(),
		ScheduledFor: time.Unix(scheduledFor, 0).UTC().Format(time.RFC3339),
	}, nil
}

func (p pAdapter) CancelRun(ctx context.Context, taskID, runID platform.ID) error {
	return p.rc.CancelRun(ctx, taskID, runID)
}

func toPlatformTask(t backend.StoreTask, m *backend.StoreTaskMeta) (*platform.Task, error) {
	opts, err := options.FromScript(t.Script)
	if err != nil {
		return nil, err
	}

	pt := &platform.Task{
		ID:             t.ID,
		OrganizationID: t.Org,
		Name:           t.Name,
		Owner: platform.User{
			ID:   t.User,
			Name: "", // TODO(mr): how to get owner name?
		},
		Flux: t.Script,
		Cron: opts.Cron,
	}
	if opts.Every != 0 {
		pt.Every = opts.Every.String()
	}
	if opts.Offset != 0 {
		pt.Offset = opts.Offset.String()
	}
	if m != nil {
		pt.Status = string(m.Status)
		pt.LatestCompleted = time.Unix(m.LatestCompleted, 0).Format(time.RFC3339)
	}
	return pt, nil
}
