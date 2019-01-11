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

func (ts *taskServiceValidator) CreateTask(ctx context.Context, t *platform.Task) error {
	p, err := platform.NewPermission(platform.WriteAction, platform.TasksResource, t.Organization)
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

// TODO(lh): add permission checking for the all the platform.TaskService functions.

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
		return err
	}

	if err := preAuth.PreAuthorize(ctx, spec, auth); err != nil {
		return err
	}

	return nil
}
