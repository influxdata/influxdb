package task

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/flux"
	platform "github.com/influxdata/influxdb"
	platcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/query"
)

type authError struct {
	error
	perm platform.Permission
	auth platform.Authorizer
}

func (ae *authError) AuthzError() error {
	return fmt.Errorf("permission failed for auth (%s): %s", ae.auth.Identifier().String(), ae.perm.String())
}

var ErrFailedPermission = errors.New("unauthorized")

type taskServiceValidator struct {
	platform.TaskService
	preAuth query.PreAuthorizer
}

func NewValidator(ts platform.TaskService, bs platform.BucketService) platform.TaskService {
	return &taskServiceValidator{
		TaskService: ts,
		preAuth:     query.NewPreAuthorizer(bs),
	}
}
func (ts *taskServiceValidator) FindTaskByID(ctx context.Context, id platform.ID) (*platform.Task, error) {
	task, err := ts.TaskService.FindTaskByID(ctx, id)
	if err != nil {
		return nil, err
	}

	perm, err := platform.NewPermission(platform.ReadAction, platform.TasksResourceType, task.OrganizationID)
	if err != nil {
		return nil, err
	}

	if err := validatePermission(ctx, *perm); err != nil {
		return nil, err
	}

	return task, nil
}

func (ts *taskServiceValidator) FindTasks(ctx context.Context, filter platform.TaskFilter) ([]*platform.Task, int, error) {
	if filter.Organization != nil {
		perm, err := platform.NewPermission(platform.ReadAction, platform.TasksResourceType, *filter.Organization)
		if err != nil {
			return nil, -1, err
		}

		if err := validatePermission(ctx, *perm); err != nil {
			return nil, -1, err
		}

	}

	// TODO(lyon): If the user no longer has permission to the organization we might fail or filter here?
	return ts.TaskService.FindTasks(ctx, filter)
}

func (ts *taskServiceValidator) CreateTask(ctx context.Context, t *platform.Task) error {
	p, err := platform.NewPermission(platform.WriteAction, platform.TasksResourceType, t.OrganizationID)
	if err != nil {
		return err
	}

	if err := validatePermission(ctx, *p); err != nil {
		return err
	}

	if err := validateBucket(ctx, t.Flux, ts.preAuth); err != nil {
		return err
	}

	return ts.TaskService.CreateTask(ctx, t)
}

func (ts *taskServiceValidator) UpdateTask(ctx context.Context, id platform.ID, upd platform.TaskUpdate) (*platform.Task, error) {
	task, err := ts.TaskService.FindTaskByID(ctx, id)
	if err != nil {
		return nil, err
	}

	p, err := platform.NewPermission(platform.WriteAction, platform.TasksResourceType, task.OrganizationID)
	if err != nil {
		return nil, err
	}

	if err := validatePermission(ctx, *p); err != nil {
		return nil, err
	}

	if err := validateBucket(ctx, task.Flux, ts.preAuth); err != nil {
		return nil, err
	}

	return ts.TaskService.UpdateTask(ctx, id, upd)
}

func (ts *taskServiceValidator) DeleteTask(ctx context.Context, id platform.ID) error {
	task, err := ts.TaskService.FindTaskByID(ctx, id)
	if err != nil {
		return err
	}

	p, err := platform.NewPermission(platform.WriteAction, platform.TasksResourceType, task.OrganizationID)
	if err != nil {
		return err
	}

	if err := validatePermission(ctx, *p); err != nil {
		return err
	}

	return ts.TaskService.DeleteTask(ctx, id)
}

func (ts *taskServiceValidator) FindLogs(ctx context.Context, filter platform.LogFilter) ([]*platform.Log, int, error) {
	if filter.Org != nil {
		perm, err := platform.NewPermission(platform.ReadAction, platform.TasksResourceType, *filter.Org)
		if err != nil {
			return nil, -1, err
		}

		if err := validatePermission(ctx, *perm); err != nil {
			return nil, -1, err
		}

	}

	// TODO(lyon): If the user no longer has permission to the organization we might fail or filter here?
	return ts.TaskService.FindLogs(ctx, filter)
}

func (ts *taskServiceValidator) FindRuns(ctx context.Context, filter platform.RunFilter) ([]*platform.Run, int, error) {
	if filter.Org != nil {
		perm, err := platform.NewPermission(platform.ReadAction, platform.TasksResourceType, *filter.Org)
		if err != nil {
			return nil, -1, err
		}

		if err := validatePermission(ctx, *perm); err != nil {
			return nil, -1, err
		}

	}

	// TODO(lyon): If the user no longer has permission to the organization we might fail or filter here?
	return ts.TaskService.FindRuns(ctx, filter)
}

func (ts *taskServiceValidator) FindRunByID(ctx context.Context, taskID, runID platform.ID) (*platform.Run, error) {
	task, err := ts.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	p, err := platform.NewPermission(platform.ReadAction, platform.TasksResourceType, task.OrganizationID)
	if err != nil {
		return nil, err
	}

	if err := validatePermission(ctx, *p); err != nil {
		return nil, err
	}

	return ts.TaskService.FindRunByID(ctx, taskID, runID)
}

func (ts *taskServiceValidator) CancelRun(ctx context.Context, taskID, runID platform.ID) error {
	task, err := ts.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return err
	}

	p, err := platform.NewPermission(platform.WriteAction, platform.TasksResourceType, task.OrganizationID)
	if err != nil {
		return err
	}

	if err := validatePermission(ctx, *p); err != nil {
		return err
	}

	return ts.TaskService.CancelRun(ctx, taskID, runID)
}

func (ts *taskServiceValidator) RetryRun(ctx context.Context, taskID, runID platform.ID) (*platform.Run, error) {
	task, err := ts.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	p, err := platform.NewPermission(platform.WriteAction, platform.TasksResourceType, task.OrganizationID)
	if err != nil {
		return nil, err
	}

	if err := validatePermission(ctx, *p); err != nil {
		return nil, err
	}

	return ts.TaskService.RetryRun(ctx, taskID, runID)
}

func (ts *taskServiceValidator) ForceRun(ctx context.Context, taskID platform.ID, scheduledFor int64) (*platform.Run, error) {
	task, err := ts.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	p, err := platform.NewPermission(platform.WriteAction, platform.TasksResourceType, task.OrganizationID)
	if err != nil {
		return nil, err
	}

	if err := validatePermission(ctx, *p); err != nil {
		return nil, err
	}

	return ts.TaskService.ForceRun(ctx, taskID, scheduledFor)
}

func validatePermission(ctx context.Context, perm platform.Permission) error {
	auth, err := platcontext.GetAuthorizer(ctx)
	if err != nil {
		return err
	}

	if !auth.Allowed(perm) {
		return authError{error: ErrFailedPermission, perm: perm, auth: auth}
	}

	return nil
}

func validateBucket(ctx context.Context, script string, preAuth query.PreAuthorizer) error {
	auth, err := platcontext.GetAuthorizer(ctx)
	if err != nil {
		return err
	}

	spec, err := flux.Compile(ctx, script, time.Now())
	if err != nil {
		return platform.NewError(
			platform.WithErrorErr(err),
			platform.WithErrorMsg("Failed to compile flux script."),
			platform.WithErrorCode(platform.EInvalid))
	}

	if err := preAuth.PreAuthorize(ctx, spec, auth); err != nil {
		return platform.NewError(
			platform.WithErrorErr(err),
			platform.WithErrorMsg("Failed to authorize."),
			platform.WithErrorCode(platform.EInvalid))
	}

	return nil
}
