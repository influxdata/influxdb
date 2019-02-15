package task

import (
	"context"
	"errors"
	"fmt"
	"time"

	platform "github.com/influxdata/influxdb"
	icontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/influxdata/influxdb/task/options"
)

type RunController interface {
	CancelRun(ctx context.Context, taskID, runID platform.ID) error
	//TODO: add retry run to this.
}

// PlatformAdapter wraps a task.Store into the platform.TaskService interface.
func PlatformAdapter(s backend.Store, r backend.LogReader, rc RunController, as platform.AuthorizationService, urm platform.UserResourceMappingService) platform.TaskService {
	return pAdapter{s: s, r: r, rc: rc, as: as, urm: urm}
}

type pAdapter struct {
	s  backend.Store
	rc RunController
	r  backend.LogReader

	// Needed to look up authorization ID from token during create.
	as  platform.AuthorizationService
	urm platform.UserResourceMappingService
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
		ownedTasks, _, err := p.urm.FindUserResourceMappings(
			ctx,
			platform.UserResourceMappingFilter{
				UserID:       *filter.User,
				UserType:     platform.Owner,
				ResourceType: platform.TasksResourceType,
			},
		)
		if err != nil {
			return nil, 0, err
		}

		var tasks []*platform.Task
		for _, ownedTask := range ownedTasks {
			storeTask, meta, err := p.s.FindTaskByIDWithMeta(ctx, ownedTask.ResourceID)
			if err != nil {
				return nil, 0, err
			}
			task, err := toPlatformTask(*storeTask, meta)
			if err != nil {
				return nil, 0, err
			}

			tasks = append(tasks, task)

		}
		return tasks, len(tasks), nil
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

func (p pAdapter) CreateTask(ctx context.Context, t platform.TaskCreate) (*platform.Task, error) {
	auth, err := icontext.GetAuthorizer(ctx)
	if err != nil {
		return nil, err
	}

	opts, err := options.FromScript(t.Flux)
	if err != nil {
		return nil, err
	}

	// TODO(mr): decide whether we allow user to configure scheduleAfter. https://github.com/influxdata/influxdb/issues/10884
	scheduleAfter := time.Now().Unix()

	if t.Status == "" {
		t.Status = string(backend.DefaultTaskStatus)
	}

	req := backend.CreateTaskRequest{
		Org:           t.OrganizationID,
		ScheduleAfter: scheduleAfter,
		Status:        backend.TaskStatus(t.Status),
		Script:        t.Flux,
		User:          auth.GetUserID(),
	}
	req.AuthorizationID, err = p.authorizationIDFromToken(ctx, t.Token)
	if err != nil {
		return nil, err
	}

	id, err := p.s.CreateTask(ctx, req)
	if err != nil {
		return nil, err
	}
	task := &platform.Task{
		ID:              id,
		Flux:            t.Flux,
		Cron:            opts.Cron,
		Name:            opts.Name,
		OrganizationID:  t.OrganizationID,
		Owner:           platform.User{ID: req.User},
		Status:          t.Status,
		AuthorizationID: req.AuthorizationID,
	}

	if opts.Every != 0 {
		task.Every = opts.Every.String()
	}
	if opts.Offset != 0 {
		task.Offset = opts.Offset.String()
	}

	mapping := &platform.UserResourceMapping{
		UserID:       auth.GetUserID(),
		UserType:     platform.Owner,
		ResourceType: platform.TasksResourceType,
		ResourceID:   task.ID,
	}

	if err := p.urm.CreateUserResourceMapping(ctx, mapping); err != nil {
		// clean up the task if we fail to map the user and resource
		// TODO(lh): Multi step creates could benefit from a service wide transactional request
		if derr := p.DeleteTask(ctx, task.ID); derr != nil {
			err = fmt.Errorf("%s: failed to clean up task: %s", err.Error(), derr.Error())
		}

		return nil, err
	}

	return task, nil
}

func (p pAdapter) UpdateTask(ctx context.Context, id platform.ID, upd platform.TaskUpdate) (*platform.Task, error) {
	err := upd.Validate()
	if err != nil {
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

	req.AuthorizationID, err = p.authorizationIDFromToken(ctx, upd.Token)
	if err != nil {
		return nil, err
	}

	res, err := p.s.UpdateTask(ctx, req)
	if err != nil {
		return nil, err
	}
	if res.NewTask.Script == "" {
		return nil, errors.New("script not defined in the store")
	}

	return p.FindTaskByID(ctx, id)
}

func (p pAdapter) DeleteTask(ctx context.Context, id platform.ID) error {
	_, err := p.s.DeleteTask(ctx, id)
	if err != nil {
		return err
	}
	// TODO(mr): Store.DeleteTask returns false, nil if ID didn't match; do we want to handle that case?

	// clean up resource maps for deleted task
	urms, _, err := p.urm.FindUserResourceMappings(ctx, platform.UserResourceMappingFilter{
		ResourceID:   id,
		ResourceType: platform.TasksResourceType,
	})

	if err != nil {
		return err
	} else {
		for _, m := range urms {
			if err := p.urm.DeleteUserResourceMapping(ctx, m.ResourceID, m.UserID); err != nil {
				return err
			}
		}
	}

	return nil
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

var errTokenUnreadable = errors.New("token invalid or unreadable by the current user")

// authorizationIDFromToken looks up the authorization ID from the given token,
// and returns that ID iff the authorizer on the context is allowed to view that authorization.
func (p pAdapter) authorizationIDFromToken(ctx context.Context, token string) (platform.ID, error) {
	authorizer, err := icontext.GetAuthorizer(ctx)
	if err != nil {
		return 0, err
	}

	if token == "" {
		// No explicit token. Use the authorization ID from the context's authorizer.
		k := authorizer.Kind()
		if k != platform.AuthorizationKind && k != platform.SessionAuthorizionKind {
			return 0, fmt.Errorf("unable to create task using authorization of kind %s", k)
		}

		return authorizer.Identifier(), nil
	}

	// Token was explicitly provided. Look it up.
	a, err := p.as.FindAuthorizationByToken(ctx, token)
	if err != nil {
		// TODO(mr): log the actual error.
		return 0, errTokenUnreadable
	}

	// It's a valid token. Is it our token?
	if a.GetUserID() != authorizer.GetUserID() {
		// The auth token isn't ours. Ensure we're allowed to read it.
		p, err := platform.NewPermissionAtID(a.ID, platform.ReadAction, platform.AuthorizationsResourceType, a.OrgID)
		if err != nil {
			// TODO(mr): log the actual error.
			return 0, errTokenUnreadable
		}
		if !authorizer.Allowed(*p) {
			return 0, errTokenUnreadable
		}
	}

	return a.ID, nil
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
		if m.CreatedAt != 0 {
			pt.CreatedAt = time.Unix(m.CreatedAt, 0).Format(time.RFC3339)
		}
		if m.UpdatedAt != 0 {
			pt.UpdatedAt = time.Unix(m.UpdatedAt, 0).Format(time.RFC3339)
		}
		pt.AuthorizationID = platform.ID(m.AuthorizationID)
	}
	return pt, nil
}
