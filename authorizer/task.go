package authorizer

import (
	"context"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb"
	platform "github.com/influxdata/influxdb"
	platcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/task/backend"
	"go.uber.org/zap"
)

type authError struct {
	error
	perm platform.Permission
	auth platform.Authorizer
}

func (ae *authError) AuthzError() error {
	return fmt.Errorf("permission failed for auth (%s): %s", ae.auth.Identifier().String(), ae.perm.String())
}

var (
	ErrInactiveTask = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "inactive task",
	}

	ErrFailedPermission = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "unauthorized",
	}
)

type taskServiceValidator struct {
	platform.TaskService
	preAuth query.PreAuthorizer
	logger  *zap.Logger
}

// TaskService wraps ts and checks appropriate permissions before calling requested methods on ts.
// Authorization failures are logged to the logger.
func NewTaskService(logger *zap.Logger, ts platform.TaskService, bs platform.BucketService) platform.TaskService {
	return &taskServiceValidator{
		TaskService: ts,
		preAuth:     query.NewPreAuthorizer(bs),
		logger:      logger,
	}
}

func (ts *taskServiceValidator) FindTaskByID(ctx context.Context, id platform.ID) (*platform.Task, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, id)
	if err != nil {
		return nil, err
	}

	perm, err := platform.NewPermissionAtID(id, platform.ReadAction, platform.TasksResourceType, task.OrganizationID)
	if err != nil {
		return nil, err
	}

	if err := ts.validatePermission(ctx, *perm,
		zap.String("method", "FindTaskByID"), zap.Stringer("task_id", id),
	); err != nil {
		return nil, err
	}

	return task, nil
}

func (ts *taskServiceValidator) FindTasks(ctx context.Context, filter platform.TaskFilter) ([]*platform.Task, int, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Get the authorizer first.
	// We are getting a list of tasks that may be a superset of what the user is allowed to view.
	auth, err := platcontext.GetAuthorizer(ctx)
	if err != nil {
		ts.logger.Info("Failed to retrieve authorizer from context", zap.String("method", "FindTasks"))
		return nil, 0, err
	}

	// Get the tasks in the organization, without authentication.
	unauthenticatedTasks, _, err := ts.TaskService.FindTasks(ctx, filter)
	if err != nil {
		return nil, 0, err
	}

	// Then, filter down to what the user is allowed to see.
	tasks := make([]*platform.Task, 0, len(unauthenticatedTasks))
	for _, t := range unauthenticatedTasks {
		perm, err := platform.NewPermissionAtID(t.ID, platform.ReadAction, platform.TasksResourceType, t.OrganizationID)
		if err != nil {
			continue
		}

		// We don't want to log authorization errors on this one.
		if !auth.Allowed(*perm) {
			continue
		}

		// Allowed to read it.
		tasks = append(tasks, t)
	}

	return tasks, len(tasks), nil
}

func (ts *taskServiceValidator) CreateTask(ctx context.Context, t platform.TaskCreate) (*platform.Task, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if t.Token == "" {
		return nil, influxdb.ErrMissingToken
	}

	if t.Type == influxdb.TaskTypeWildcard {
		return nil, influxdb.ErrInvalidTaskType
	}

	p, err := platform.NewPermission(platform.WriteAction, platform.TasksResourceType, t.OrganizationID)
	if err != nil {
		return nil, err
	}

	loggerFields := []zap.Field{zap.String("method", "CreateTask")}
	if err := ts.validatePermission(ctx, *p, loggerFields...); err != nil {
		return nil, err
	}

	if err := ts.validateBucket(ctx, t.Flux, t.OrganizationID, loggerFields...); err != nil {
		return nil, err
	}

	return ts.TaskService.CreateTask(ctx, t)
}

func (ts *taskServiceValidator) UpdateTask(ctx context.Context, id platform.ID, upd platform.TaskUpdate) (*platform.Task, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, id)
	if err != nil {
		return nil, err
	}

	p, err := platform.NewPermissionAtID(id, platform.WriteAction, platform.TasksResourceType, task.OrganizationID)
	if err != nil {
		return nil, err
	}

	loggerFields := []zap.Field{zap.String("method", "UpdateTask"), zap.Stringer("task_id", id)}
	if err := ts.validatePermission(ctx, *p, loggerFields...); err != nil {
		return nil, err
	}

	// given an update to the task flux definition
	if upd.Flux != nil {
		if err := ts.validateBucket(ctx, *upd.Flux, task.OrganizationID, loggerFields...); err != nil {
			return nil, err
		}
	}

	return ts.TaskService.UpdateTask(ctx, id, upd)
}

func (ts *taskServiceValidator) DeleteTask(ctx context.Context, id platform.ID) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, id)
	if err != nil {
		return err
	}

	p, err := platform.NewPermissionAtID(id, platform.WriteAction, platform.TasksResourceType, task.OrganizationID)
	if err != nil {
		return err
	}

	if err := ts.validatePermission(ctx, *p,
		zap.String("method", "DeleteTask"), zap.Stringer("task_id", id),
	); err != nil {
		return err
	}

	return ts.TaskService.DeleteTask(ctx, id)
}

func (ts *taskServiceValidator) FindLogs(ctx context.Context, filter platform.LogFilter) ([]*platform.Log, int, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Look up the task first, through the validator, to ensure we have permission to view the task.
	if _, err := ts.FindTaskByID(ctx, filter.Task); err != nil {
		return nil, -1, err
	}

	// If we can find the task, we can read its logs.
	return ts.TaskService.FindLogs(ctx, filter)
}

func (ts *taskServiceValidator) FindRuns(ctx context.Context, filter platform.RunFilter) ([]*platform.Run, int, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Look up the task first, through the validator, to ensure we have permission to view the task.
	task, err := ts.FindTaskByID(ctx, filter.Task)
	if err != nil {
		return nil, -1, err
	}

	perm, err := platform.NewPermissionAtID(task.ID, platform.ReadAction, platform.TasksResourceType, task.OrganizationID)
	if err != nil {
		return nil, -1, err
	}

	if err := ts.validatePermission(ctx, *perm,
		zap.String("method", "FindRuns"), zap.Stringer("task_id", task.ID),
	); err != nil {
		return nil, -1, err
	}

	// TODO(lyon): If the user no longer has permission to the organization we might fail or filter here?
	return ts.TaskService.FindRuns(ctx, filter)
}

func (ts *taskServiceValidator) FindRunByID(ctx context.Context, taskID, runID platform.ID) (*platform.Run, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	p, err := platform.NewPermissionAtID(taskID, platform.ReadAction, platform.TasksResourceType, task.OrganizationID)
	if err != nil {
		return nil, err
	}

	if err := ts.validatePermission(ctx, *p,
		zap.String("method", "FindRunByID"), zap.Stringer("task_id", taskID), zap.Stringer("run_id", runID),
	); err != nil {
		return nil, err
	}

	return ts.TaskService.FindRunByID(ctx, taskID, runID)
}

func (ts *taskServiceValidator) CancelRun(ctx context.Context, taskID, runID platform.ID) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return err
	}

	p, err := platform.NewPermissionAtID(taskID, platform.WriteAction, platform.TasksResourceType, task.OrganizationID)
	if err != nil {
		return err
	}

	if err := ts.validatePermission(ctx, *p,
		zap.String("method", "CancelRun"), zap.Stringer("task_id", taskID), zap.Stringer("run_id", runID),
	); err != nil {
		return err
	}

	return ts.TaskService.CancelRun(ctx, taskID, runID)
}

func (ts *taskServiceValidator) RetryRun(ctx context.Context, taskID, runID platform.ID) (*platform.Run, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	if task.Status != string(backend.TaskActive) {
		return nil, ErrInactiveTask
	}

	p, err := platform.NewPermissionAtID(taskID, platform.WriteAction, platform.TasksResourceType, task.OrganizationID)
	if err != nil {
		return nil, err
	}

	if err := ts.validatePermission(ctx, *p,
		zap.String("method", "RetryRun"), zap.Stringer("task_id", taskID), zap.Stringer("run_id", runID),
	); err != nil {
		return nil, err
	}

	return ts.TaskService.RetryRun(ctx, taskID, runID)
}

func (ts *taskServiceValidator) ForceRun(ctx context.Context, taskID platform.ID, scheduledFor int64) (*platform.Run, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	if task.Status != string(backend.TaskActive) {
		return nil, ErrInactiveTask
	}

	p, err := platform.NewPermissionAtID(taskID, platform.WriteAction, platform.TasksResourceType, task.OrganizationID)
	if err != nil {
		return nil, err
	}

	if err := ts.validatePermission(ctx, *p,
		zap.String("method", "ForceRun"), zap.Stringer("task_id", taskID),
	); err != nil {
		return nil, err
	}

	return ts.TaskService.ForceRun(ctx, taskID, scheduledFor)
}

func (ts *taskServiceValidator) validatePermission(ctx context.Context, perm platform.Permission, loggerFields ...zap.Field) error {
	auth, err := platcontext.GetAuthorizer(ctx)
	if err != nil {
		ts.logger.With(loggerFields...).Info("Failed to retrieve authorizer from context")
		return err
	}

	if !auth.Allowed(perm) {
		ts.logger.With(loggerFields...).Info("Authorization failed",
			zap.String("user_id", auth.GetUserID().String()),
			zap.String("auth_kind", auth.Kind()),
			zap.String("auth_id", auth.Identifier().String()),
			zap.String("disallowed_permission", perm.String()),
		)
		return authError{error: ErrFailedPermission, perm: perm, auth: auth}
	}

	return nil
}

func (ts *taskServiceValidator) validateBucket(ctx context.Context, script string, orgID platform.ID, loggerFields ...zap.Field) error {
	auth, err := platcontext.GetAuthorizer(ctx)
	if err != nil {
		ts.logger.With(loggerFields...).Info("Failed to retrieve authorizer from context")
		return err
	}

	ast, err := flux.Parse(script)
	if err != nil {
		return platform.NewError(
			platform.WithErrorErr(err),
			platform.WithErrorMsg("Failed to compile flux script."),
			platform.WithErrorCode(platform.EInvalid))
	}

	if err := ts.preAuth.PreAuthorize(ctx, ast, auth, &orgID); err != nil {
		ts.logger.With(loggerFields...).Info("Task failed preauthorization check",
			zap.String("user_id", auth.GetUserID().String()),
			zap.String("org_id", orgID.String()),
			zap.String("auth_kind", auth.Kind()),
			zap.String("auth_id", auth.Identifier().String()),
		)

		// if error is already a platform error then return it
		if perr, ok := err.(*platform.Error); ok {
			return perr
		}

		return platform.NewError(
			platform.WithErrorErr(err),
			platform.WithErrorMsg("Failed to create task."),
			platform.WithErrorCode(platform.EUnauthorized))
	}

	return nil
}
