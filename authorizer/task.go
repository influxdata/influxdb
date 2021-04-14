package authorizer

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
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
	ErrInactiveTask = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "inactive task",
	}

	ErrFailedPermission = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "unauthorized",
	}
)

type taskServiceValidator struct {
	taskmodel.TaskService
	log *zap.Logger
}

// TaskService wraps ts and checks appropriate permissions before calling requested methods on ts.
// Authorization failures are logged to the logger.
func NewTaskService(log *zap.Logger, ts taskmodel.TaskService) taskmodel.TaskService {
	return &taskServiceValidator{
		TaskService: ts,
		log:         log,
	}
}

func (ts *taskServiceValidator) processPermissionError(a influxdb.Authorizer, p influxdb.Permission, err error, loggerFields ...zap.Field) error {
	if errors.ErrorCode(err) == errors.EUnauthorized {
		ts.log.With(loggerFields...).Info("Authorization failed",
			zap.String("user_id", a.GetUserID().String()),
			zap.String("auth_kind", a.Kind()),
			zap.String("auth_id", a.Identifier().String()),
			zap.String("disallowed_permission", p.String()),
		)
		return authError{error: ErrFailedPermission, perm: p, auth: a}
	}
	return err
}

func (ts *taskServiceValidator) FindTaskByID(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, id)
	if err != nil {
		return nil, err
	}

	a, p, err := AuthorizeRead(ctx, influxdb.TasksResourceType, task.ID, task.OrganizationID)
	loggerFields := []zap.Field{zap.String("method", "FindTaskByID"), zap.Stringer("task_id", id)}
	if err := ts.processPermissionError(a, p, err, loggerFields...); err != nil {
		return nil, err
	}
	return task, nil
}

func (ts *taskServiceValidator) FindTasks(ctx context.Context, filter taskmodel.TaskFilter) ([]*taskmodel.Task, int, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	// Get the tasks in the organization, without authentication.
	unauthenticatedTasks, _, err := ts.TaskService.FindTasks(ctx, filter)
	if err != nil {
		return nil, 0, err
	}
	return AuthorizeFindTasks(ctx, unauthenticatedTasks)
}

func (ts *taskServiceValidator) CreateTask(ctx context.Context, t taskmodel.TaskCreate) (*taskmodel.Task, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if !t.OwnerID.Valid() {
		return nil, taskmodel.ErrInvalidOwnerID
	}

	a, p, err := AuthorizeCreate(ctx, influxdb.TasksResourceType, t.OrganizationID)
	loggerFields := []zap.Field{zap.String("method", "CreateTask")}
	if err := ts.processPermissionError(a, p, err, loggerFields...); err != nil {
		return nil, err
	}
	return ts.TaskService.CreateTask(ctx, t)
}

func (ts *taskServiceValidator) UpdateTask(ctx context.Context, id platform.ID, upd taskmodel.TaskUpdate) (*taskmodel.Task, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, id)
	if err != nil {
		return nil, err
	}

	a, p, err := AuthorizeWrite(ctx, influxdb.TasksResourceType, task.ID, task.OrganizationID)
	loggerFields := []zap.Field{zap.String("method", "UpdateTask"), zap.Stringer("task_id", id)}
	if err := ts.processPermissionError(a, p, err, loggerFields...); err != nil {
		return nil, err
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

	a, p, err := AuthorizeWrite(ctx, influxdb.TasksResourceType, task.ID, task.OrganizationID)
	loggerFields := []zap.Field{zap.String("method", "DeleteTask"), zap.Stringer("task_id", id)}
	if err := ts.processPermissionError(a, p, err, loggerFields...); err != nil {
		return err
	}
	return ts.TaskService.DeleteTask(ctx, id)
}

func (ts *taskServiceValidator) FindLogs(ctx context.Context, filter taskmodel.LogFilter) ([]*taskmodel.Log, int, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Look up the task first, through the validator, to ensure we have permission to view the task.
	if _, err := ts.FindTaskByID(ctx, filter.Task); err != nil {
		return nil, -1, err
	}

	// If we can find the task, we can read its logs.
	return ts.TaskService.FindLogs(ctx, filter)
}

func (ts *taskServiceValidator) FindRuns(ctx context.Context, filter taskmodel.RunFilter) ([]*taskmodel.Run, int, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Look up the task first, through the validator, to ensure we have permission to view the task.
	task, err := ts.FindTaskByID(ctx, filter.Task)
	if err != nil {
		return nil, -1, err
	}

	a, p, err := AuthorizeRead(ctx, influxdb.TasksResourceType, task.ID, task.OrganizationID)
	loggerFields := []zap.Field{zap.String("method", "FindRuns"), zap.Stringer("task_id", task.ID)}
	if err := ts.processPermissionError(a, p, err, loggerFields...); err != nil {
		return nil, -1, err
	}
	// TODO(lyon): If the user no longer has permission to the organization we might fail or filter here?
	return ts.TaskService.FindRuns(ctx, filter)
}

func (ts *taskServiceValidator) FindRunByID(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	a, p, err := AuthorizeRead(ctx, influxdb.TasksResourceType, task.ID, task.OrganizationID)
	loggerFields := []zap.Field{zap.String("method", "FindRunByID"), zap.Stringer("task_id", taskID), zap.Stringer("run_id", runID)}
	if err := ts.processPermissionError(a, p, err, loggerFields...); err != nil {
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

	a, p, err := AuthorizeWrite(ctx, influxdb.TasksResourceType, task.ID, task.OrganizationID)
	loggerFields := []zap.Field{zap.String("method", "CancelRun"), zap.Stringer("task_id", taskID), zap.Stringer("run_id", runID)}
	if err := ts.processPermissionError(a, p, err, loggerFields...); err != nil {
		return err
	}
	return ts.TaskService.CancelRun(ctx, taskID, runID)
}

func (ts *taskServiceValidator) RetryRun(ctx context.Context, taskID, runID platform.ID) (*taskmodel.Run, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	if task.Status != string(taskmodel.TaskActive) {
		return nil, ErrInactiveTask
	}

	a, p, err := AuthorizeWrite(ctx, influxdb.TasksResourceType, task.ID, task.OrganizationID)
	loggerFields := []zap.Field{zap.String("method", "RetryRun"), zap.Stringer("task_id", taskID), zap.Stringer("run_id", runID)}
	if err := ts.processPermissionError(a, p, err, loggerFields...); err != nil {
		return nil, err
	}
	return ts.TaskService.RetryRun(ctx, taskID, runID)
}

func (ts *taskServiceValidator) ForceRun(ctx context.Context, taskID platform.ID, scheduledFor int64) (*taskmodel.Run, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Unauthenticated task lookup, to identify the task's organization.
	task, err := ts.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	if task.Status != string(taskmodel.TaskActive) {
		return nil, ErrInactiveTask
	}

	a, p, err := AuthorizeWrite(ctx, influxdb.TasksResourceType, task.ID, task.OrganizationID)
	loggerFields := []zap.Field{zap.String("method", "ForceRun"), zap.Stringer("task_id", taskID)}
	if err := ts.processPermissionError(a, p, err, loggerFields...); err != nil {
		return nil, err
	}
	return ts.TaskService.ForceRun(ctx, taskID, scheduledFor)
}
