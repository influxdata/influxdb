package authorizer

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	platcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/task/backend"
	"go.uber.org/zap"
)

type authError struct {
	error
	perm influxdb.Permission
	auth influxdb.Authorizer
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
	influxdb.TaskService
	log *zap.Logger
}

// TaskService wraps ts and checks appropriate permissions before calling requested methods on ts.
// Authorization failures are logged to the logger.
func NewTaskService(log *zap.Logger, ts influxdb.TaskService) influxdb.TaskService {
	return &taskServiceValidator{
		TaskService: ts,
		log:         log,
	}
}

func (ts *taskServiceValidator) FindTaskByID(ctx context.Context, id influxdb.ID) (*influxdb.Task, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, id)
	if err != nil {
		return nil, err
	}

	perm, err := influxdb.NewPermissionAtID(id, influxdb.ReadAction, influxdb.TasksResourceType, task.OrganizationID)
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

func (ts *taskServiceValidator) FindTasks(ctx context.Context, filter influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Get the authorizer first.
	// We are getting a list of tasks that may be a superset of what the user is allowed to view.
	auth, err := platcontext.GetAuthorizer(ctx)
	if err != nil {
		ts.log.Info("Failed to retrieve authorizer from context", zap.String("method", "FindTasks"))
		return nil, 0, err
	}

	// Get the tasks in the organization, without authentication.
	unauthenticatedTasks, _, err := ts.TaskService.FindTasks(ctx, filter)
	if err != nil {
		return nil, 0, err
	}

	// Then, filter down to what the user is allowed to see.
	tasks := make([]*influxdb.Task, 0, len(unauthenticatedTasks))
	for _, t := range unauthenticatedTasks {
		perm, err := influxdb.NewPermissionAtID(t.ID, influxdb.ReadAction, influxdb.TasksResourceType, t.OrganizationID)
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

func (ts *taskServiceValidator) CreateTask(ctx context.Context, t influxdb.TaskCreate) (*influxdb.Task, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if !t.OwnerID.Valid() {
		return nil, influxdb.ErrInvalidOwnerID
	}

	p, err := influxdb.NewPermission(influxdb.WriteAction, influxdb.TasksResourceType, t.OrganizationID)
	if err != nil {
		return nil, err
	}

	loggerFields := []zap.Field{zap.String("method", "CreateTask")}
	if err := ts.validatePermission(ctx, *p, loggerFields...); err != nil {
		return nil, err
	}

	return ts.TaskService.CreateTask(ctx, t)
}

func (ts *taskServiceValidator) UpdateTask(ctx context.Context, id influxdb.ID, upd influxdb.TaskUpdate) (*influxdb.Task, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, id)
	if err != nil {
		return nil, err
	}

	p, err := influxdb.NewPermissionAtID(id, influxdb.WriteAction, influxdb.TasksResourceType, task.OrganizationID)
	if err != nil {
		return nil, err
	}

	loggerFields := []zap.Field{zap.String("method", "UpdateTask"), zap.Stringer("task_id", id)}
	if err := ts.validatePermission(ctx, *p, loggerFields...); err != nil {
		return nil, err
	}

	return ts.TaskService.UpdateTask(ctx, id, upd)
}

func (ts *taskServiceValidator) DeleteTask(ctx context.Context, id influxdb.ID) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, id)
	if err != nil {
		return err
	}

	p, err := influxdb.NewPermissionAtID(id, influxdb.WriteAction, influxdb.TasksResourceType, task.OrganizationID)
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

func (ts *taskServiceValidator) FindLogs(ctx context.Context, filter influxdb.LogFilter) ([]*influxdb.Log, int, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Look up the task first, through the validator, to ensure we have permission to view the task.
	if _, err := ts.FindTaskByID(ctx, filter.Task); err != nil {
		return nil, -1, err
	}

	// If we can find the task, we can read its logs.
	return ts.TaskService.FindLogs(ctx, filter)
}

func (ts *taskServiceValidator) FindRuns(ctx context.Context, filter influxdb.RunFilter) ([]*influxdb.Run, int, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Look up the task first, through the validator, to ensure we have permission to view the task.
	task, err := ts.FindTaskByID(ctx, filter.Task)
	if err != nil {
		return nil, -1, err
	}

	perm, err := influxdb.NewPermissionAtID(task.ID, influxdb.ReadAction, influxdb.TasksResourceType, task.OrganizationID)
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

func (ts *taskServiceValidator) FindRunByID(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	p, err := influxdb.NewPermissionAtID(taskID, influxdb.ReadAction, influxdb.TasksResourceType, task.OrganizationID)
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

func (ts *taskServiceValidator) CancelRun(ctx context.Context, taskID, runID influxdb.ID) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return err
	}

	p, err := influxdb.NewPermissionAtID(taskID, influxdb.WriteAction, influxdb.TasksResourceType, task.OrganizationID)
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

func (ts *taskServiceValidator) RetryRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
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

	p, err := influxdb.NewPermissionAtID(taskID, influxdb.WriteAction, influxdb.TasksResourceType, task.OrganizationID)
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

func (ts *taskServiceValidator) ForceRun(ctx context.Context, taskID influxdb.ID, scheduledFor int64) (*influxdb.Run, error) {
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

	p, err := influxdb.NewPermissionAtID(taskID, influxdb.WriteAction, influxdb.TasksResourceType, task.OrganizationID)
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

func (ts *taskServiceValidator) validatePermission(ctx context.Context, perm influxdb.Permission, loggerFields ...zap.Field) error {
	auth, err := platcontext.GetAuthorizer(ctx)
	if err != nil {
		ts.log.With(loggerFields...).Info("Failed to retrieve authorizer from context")
		return err
	}

	if !auth.Allowed(perm) {
		ts.log.With(loggerFields...).Info("Authorization failed",
			zap.String("user_id", auth.GetUserID().String()),
			zap.String("auth_kind", auth.Kind()),
			zap.String("auth_id", auth.Identifier().String()),
			zap.String("disallowed_permission", perm.String()),
		)
		return authError{error: ErrFailedPermission, perm: perm, auth: auth}
	}

	return nil
}
