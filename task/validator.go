package task

import (
	"context"
	"errors"
	"fmt"

	"github.com/influxdata/platform"
	platcontext "github.com/influxdata/platform/context"
)

type authError struct {
	error
	perm platform.Permission
	auth *platform.Authorization
}

func (ae *authError) AuthzError() error {
	return fmt.Errorf("permission failed for auth (%s): %s", ae.auth.ID.String(), ae.perm.String())
}

var ErrFailedPermission = errors.New("unauthorized")

type taskServiceValidator struct {
	platform.TaskService
}

func NewValidator(ts platform.TaskService) platform.TaskService {
	return &taskServiceValidator{
		TaskService: ts,
	}
}

func (ts *taskServiceValidator) CreateTask(ctx context.Context, t *platform.Task) error {
	if err := validatePermission(ctx, platform.Permission{Action: platform.CreateAction, Resource: platform.TaskResource(t.Organization)}); err != nil {
		return err
	}

	return ts.TaskService.CreateTask(ctx, t)
}

// TODO(lh): add permission checking for the all the platform.TaskService functions.

func validatePermission(ctx context.Context, perm platform.Permission) error {
	auth, err := platcontext.GetAuthorization(ctx)
	if err != nil {
		return err
	}

	if !platform.Allowed(perm, auth) {
		return authError{error: ErrFailedPermission, perm: perm, auth: auth}
	}

	return nil
}
