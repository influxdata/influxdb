package authorizer_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/influxdata/influxdb"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/authorizer"
	pctx "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/mock"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/pkg/errors"
	"go.uber.org/zap/zaptest"
)

func TestOnboardingValidation(t *testing.T) {
	svc := inmem.NewService()
	ts := authorizer.NewTaskService(zaptest.NewLogger(t), mockTaskService(3, 2, 1), svc)

	r, err := svc.Generate(context.Background(), &influxdb.OnboardingRequest{
		User:            "Setec Astronomy",
		Password:        "too many secrets",
		Org:             "thing",
		Bucket:          "holder",
		RetentionPeriod: 1,
	})

	if err != nil {
		t.Fatal(err)
	}

	ctx := pctx.SetAuthorizer(context.Background(), r.Auth)

	_, err = ts.CreateTask(ctx, influxdb.TaskCreate{
		OrganizationID: r.Org.ID,
		Token:          r.Auth.Token,
		Flux: `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func mockTaskService(orgID, taskID, runID influxdb.ID) influxdb.TaskService {
	task := influxdb.Task{
		ID:             taskID,
		OrganizationID: orgID,
		Name:           "cows",
		Status:         string(backend.TaskActive),
		Flux: `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`,
		Every: "1s",
	}

	log := influxdb.Log{Message: "howdy partner"}

	run := influxdb.Run{
		ID:           runID,
		TaskID:       taskID,
		Status:       "completed",
		ScheduledFor: "a while ago",
		StartedAt:    "not so long ago",
		FinishedAt:   "more recently",
		Log:          []influxdb.Log{log},
	}

	return &mock.TaskService{
		FindTaskByIDFn: func(context.Context, influxdb.ID) (*influxdb.Task, error) {
			return &task, nil
		},
		FindTasksFn: func(context.Context, influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
			return []*influxdb.Task{&task}, 1, nil
		},
		CreateTaskFn: func(_ context.Context, tc influxdb.TaskCreate) (*influxdb.Task, error) {
			taskCopy := task
			return &taskCopy, nil
		},
		UpdateTaskFn: func(context.Context, influxdb.ID, influxdb.TaskUpdate) (*influxdb.Task, error) {
			return &task, nil
		},
		DeleteTaskFn: func(context.Context, influxdb.ID) error {
			return nil
		},
		FindLogsFn: func(context.Context, influxdb.LogFilter) ([]*influxdb.Log, int, error) {
			return []*influxdb.Log{&log}, 1, nil
		},
		FindRunsFn: func(context.Context, influxdb.RunFilter) ([]*influxdb.Run, int, error) {
			return []*influxdb.Run{&run}, 1, nil
		},
		FindRunByIDFn: func(context.Context, influxdb.ID, influxdb.ID) (*influxdb.Run, error) {
			return &run, nil
		},
		CancelRunFn: func(context.Context, influxdb.ID, influxdb.ID) error {
			return nil
		},
		RetryRunFn: func(context.Context, influxdb.ID, influxdb.ID) (*influxdb.Run, error) {
			return &run, nil
		},
		ForceRunFn: func(context.Context, influxdb.ID, int64) (*influxdb.Run, error) {
			return &run, nil
		},
	}
}

func TestValidations(t *testing.T) {
	var (
		taskID   = influxdb.ID(0x7456)
		runID    = influxdb.ID(0x402)
		otherOrg = &influxdb.Organization{Name: "other_org"}
	)

	inmem := inmem.NewService()

	r, err := inmem.Generate(context.Background(), &influxdb.OnboardingRequest{
		User:            "Setec Astronomy",
		Password:        "too many secrets",
		Org:             "thing",
		Bucket:          "holder",
		RetentionPeriod: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := inmem.CreateOrganization(context.Background(), otherOrg); err != nil {
		t.Fatal(err)
	}

	otherBucket := &influxdb.Bucket{
		Name:  "other_bucket",
		OrgID: otherOrg.ID,
	}

	if err = inmem.CreateBucket(context.Background(), otherBucket); err != nil {
		t.Fatal(err)
	}

	var (
		orgID            = r.Org.ID
		validTaskService = authorizer.NewTaskService(zaptest.NewLogger(t), mockTaskService(orgID, taskID, runID), inmem)

		// Read all tasks in org.
		orgReadAllTaskPermissions = []influxdb.Permission{
			{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}},
		}

		// Read all tasks in some other org.
		wrongOrgReadAllTaskPermissions = []influxdb.Permission{
			{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &taskID}},
		}

		// Write all tasks in org, no specific bucket permissions.
		orgWriteAllTaskPermissions = []influxdb.Permission{
			{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}},
		}

		// Write all tasks in org, and read/write the onboarding bucket.
		orgWriteAllTaskBucketPermissions = []influxdb.Permission{
			{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}},
			{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.BucketsResourceType, OrgID: &orgID, ID: &r.Bucket.ID}},
			{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.BucketsResourceType, OrgID: &orgID, ID: &r.Bucket.ID}},
		}

		// Write the specific task, and read/write the onboarding bucket.
		orgWriteTaskBucketPermissions = []influxdb.Permission{
			{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID, ID: &taskID}},
			{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.BucketsResourceType, OrgID: &orgID, ID: &r.Bucket.ID}},
			{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.BucketsResourceType, OrgID: &orgID, ID: &r.Bucket.ID}},
		}

		// Permission only to specifically write the target task.
		orgWriteTaskPermissions = []influxdb.Permission{
			{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID, ID: &taskID}},
		}

		// Permission only to specifically read the target task.
		orgReadTaskPermissions = []influxdb.Permission{
			{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID, ID: &taskID}},
		}
	)

	tests := []struct {
		name  string
		check func(context.Context, influxdb.TaskService) error
		auth  *influxdb.Authorization
	}{
		{
			name: "create failure",
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.CreateTask(ctx, influxdb.TaskCreate{
					OrganizationID: r.Org.ID,
					Flux: `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`,
				})
				if err == nil {
					return errors.New("failed to error without permission")
				}
				return nil
			},
			auth: &influxdb.Authorization{},
		},
		{
			name: "create bad type",
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.CreateTask(ctx, influxdb.TaskCreate{
					OrganizationID: r.Org.ID,
					Token:          r.Auth.Token,
					Type:           influxdb.TaskTypeWildcard,
					Flux: `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`,
				})
				if err != influxdb.ErrInvalidTaskType {
					return errors.New("failed to error with invalid task type")
				}
				return nil
			},
			auth: &influxdb.Authorization{},
		}, {
			name: "create success",
			auth: r.Auth,
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.CreateTask(ctx, influxdb.TaskCreate{
					OrganizationID: r.Org.ID,
					Token:          r.Auth.Token,
					Flux: `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`,
				})
				return err
			},
		},
		{
			name: "create bad bucket",
			auth: r.Auth,
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				var (
					expMsg  = "Failed to create task."
					expCode = platform.EUnauthorized
					errfmt  = "expected %q, got %q"
					_, err  = svc.CreateTask(ctx, influxdb.TaskCreate{
						OrganizationID: r.Org.ID,
						Token:          r.Auth.Token,
						Flux: `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"bad") |> range(start:-5m) |> to(bucket:"bad", org:"thing")`,
					})
				)

				if err == nil {
					return errors.New("created task without bucket permission")
				}

				perr, ok := err.(*influxdb.Error)
				if !ok {
					return fmt.Errorf(errfmt, &platform.Error{}, err)
				}

				if perr.Code != expCode {
					return fmt.Errorf(errfmt, expCode, perr.Code)
				}

				if perr.Err == nil {
					return fmt.Errorf(errfmt, "platform.Error.Err to be present", perr.Err)
				}

				if !strings.Contains(perr.Err.Error(), "<not found> bucket \"bad\" not found") {
					return fmt.Errorf(errfmt, "to container bucket not found", perr.Err)
				}

				if perr.Msg != expMsg {
					return fmt.Errorf(errfmt, expMsg, perr.Msg)
				}

				return nil
			},
		},
		{
			name: "create missing org in to parameters",
			auth: r.Auth,
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				var (
					expMsg  = "Failed to compile flux script."
					expErr  = fmt.Errorf("error calling function \"to\": missing required keyword argument \"orgID\"")
					expCode = platform.EInvalid
					errfmt  = "expected %q, got %q"
					_, err  = svc.CreateTask(ctx, influxdb.TaskCreate{
						OrganizationID: r.Org.ID,
						Token:          r.Auth.Token,
						Flux: `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"bad") |> range(start:-5m) |> to(bucket:"bad")`,
					})
				)

				if err == nil {
					return errors.New("created task without bucket permission")
				}

				perr, ok := err.(*influxdb.Error)
				if !ok {
					return fmt.Errorf(errfmt, &platform.Error{}, err)
				}

				if perr.Code != expCode {
					return fmt.Errorf(errfmt, expCode, perr.Code)
				}

				if perr.Err == nil {
					return fmt.Errorf(errfmt, "platform.Error.Err to be present", perr.Err)
				}

				if perr.Err.Error() != expErr.Error() {
					return fmt.Errorf(errfmt, expErr, perr.Err)
				}

				if perr.Msg != expMsg {
					return fmt.Errorf(errfmt, expMsg, perr.Msg)
				}

				return nil
			},
		},
		{
			name: "FindTaskByID missing auth",
			auth: &influxdb.Authorization{Permissions: []influxdb.Permission{}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.FindTaskByID(ctx, taskID)
				if err == nil {
					return errors.New("returned without error without permission")
				}
				return nil
			},
		},
		{
			name: "FindTaskByID with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.FindTaskByID(ctx, taskID)
				return err
			},
		},
		{
			name: "FindTaskByID with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.FindTaskByID(ctx, taskID)
				return err
			},
		},
		{
			name: "FindTasks with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: wrongOrgReadAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				ts, _, err := svc.FindTasks(ctx, influxdb.TaskFilter{
					OrganizationID: &orgID,
				})
				if err == nil && len(ts) > 0 {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "FindTasks with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindTasks(ctx, influxdb.TaskFilter{
					OrganizationID: &orgID,
				})
				return err
			},
		},
		{
			name: "FindTasks with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindTasks(ctx, influxdb.TaskFilter{
					OrganizationID: &orgID,
				})
				return err
			},
		},
		{
			name: "FindTasks without org filter",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindTasks(ctx, influxdb.TaskFilter{})
				return err
			},
		},
		{
			name: "UpdateTask with readonly auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				flux := `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`
				_, err := svc.UpdateTask(ctx, taskID, influxdb.TaskUpdate{
					Flux: &flux,
				})
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "UpdateTask with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteAllTaskBucketPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				flux := `option task = {
		 name: "my_task",
		 every: 1s,
		}
		from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`
				_, err := svc.UpdateTask(ctx, taskID, influxdb.TaskUpdate{
					Flux: &flux,
				})
				return err
			},
		},
		{
			name: "UpdateTask with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteTaskBucketPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				flux := `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`
				_, err := svc.UpdateTask(ctx, taskID, influxdb.TaskUpdate{
					Flux: &flux,
				})
				return err
			},
		},
		{
			name: "UpdateTask with bad bucket",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				flux := `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"cows") |> range(start:-5m) |> to(bucket:"cows", org:"thing")`
				_, err := svc.UpdateTask(ctx, taskID, influxdb.TaskUpdate{
					Flux: &flux,
				})
				if err == nil {
					return errors.New("returned no error with unauthorized bucket")
				}
				return nil
			},
		},
		{
			name: "UpdateTask with bad org",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteAllTaskBucketPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				var (
					flux = `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"cows") |> range(start:-5m) |> to(bucket:"other_bucket", org:"other_org")`
					_, err = svc.UpdateTask(ctx, taskID, influxdb.TaskUpdate{
						Flux: &flux,
					})
				)

				perr, ok := err.(*influxdb.Error)
				if !ok {
					return fmt.Errorf("expected platform error, got %q of type %T", err, err)
				}

				if perr.Code != influxdb.EUnauthorized {
					return fmt.Errorf(`expected "unauthorized", got %q`, perr.Code)
				}

				if perr.Msg != "Failed to create task." {
					return fmt.Errorf(`expected "Failed to authorize.", got %q`, perr.Msg)
				}

				cerr, ok := errors.Cause(perr.Err).(*platform.Error)
				if !ok {
					return fmt.Errorf("expected platform error, got %q of type %T", perr.Err, perr.Err)
				}

				if cerr.Code != influxdb.ENotFound {
					return fmt.Errorf(`expected "not found", got %q`, perr.Code)
				}

				return nil
			},
		},
		{
			name: "DeleteTask missing auth",
			auth: &influxdb.Authorization{Permissions: []influxdb.Permission{}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				err := svc.DeleteTask(ctx, taskID)
				if err == nil {
					return errors.New("returned without error without permission")
				}
				return nil
			},
		},
		{
			name: "DeleteTask readonly auth",
			auth: &influxdb.Authorization{Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				err := svc.DeleteTask(ctx, taskID)
				if err == nil {
					return errors.New("returned without error without permission")
				}
				return nil
			},
		},
		{
			name: "DeleteTask with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				err := svc.DeleteTask(ctx, taskID)
				return err
			},
		},
		{
			name: "DeleteTask with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				err := svc.DeleteTask(ctx, taskID)
				return err
			},
		},
		{
			name: "FindLogs with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: wrongOrgReadAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindLogs(ctx, influxdb.LogFilter{
					Task: taskID,
				})
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "FindLogs with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindLogs(ctx, influxdb.LogFilter{
					Task: taskID,
				})
				return err
			},
		},
		{
			name: "FindLogs with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindLogs(ctx, influxdb.LogFilter{
					Task: taskID,
				})
				return err
			},
		},
		{
			name: "FindRuns with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: wrongOrgReadAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindRuns(ctx, influxdb.RunFilter{
					Task: taskID,
				})
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "FindRuns with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindRuns(ctx, influxdb.RunFilter{
					Task: taskID,
				})
				return err
			},
		},
		{
			name: "FindRuns with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindRuns(ctx, influxdb.RunFilter{
					Task: taskID,
				})
				return err
			},
		},
		{
			name: "FindRunByID missing auth",
			auth: &influxdb.Authorization{Permissions: []influxdb.Permission{}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.FindRunByID(ctx, taskID, 10)
				if err == nil {
					return errors.New("returned without error without permission")
				}
				return nil
			},
		},
		{
			name: "FindRunByID with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.FindRunByID(ctx, taskID, 10)
				return err
			},
		},
		{
			name: "FindRunByID with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgReadTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.FindRunByID(ctx, taskID, 10)
				return err
			},
		},
		{
			name: "CancelRun with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: wrongOrgReadAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				err := svc.CancelRun(ctx, taskID, 10)
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "CancelRun with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				err := svc.CancelRun(ctx, taskID, 10)
				return err
			},
		},
		{
			name: "CancelRun with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				err := svc.CancelRun(ctx, taskID, 10)
				return err
			},
		},
		{
			name: "RetryRun with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: wrongOrgReadAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.RetryRun(ctx, taskID, 10)
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "RetryRun with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.RetryRun(ctx, taskID, 10)
				return err
			},
		},
		{
			name: "RetryRun with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.RetryRun(ctx, taskID, 10)
				return err
			},
		},
		{
			name: "ForceRun with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: wrongOrgReadAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.ForceRun(ctx, taskID, 10000)
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "ForceRun with org auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteAllTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.ForceRun(ctx, taskID, 10000)
				return err
			},
		},
		{
			name: "ForceRun with task auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: orgWriteTaskPermissions},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.ForceRun(ctx, taskID, 10000)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := pctx.SetAuthorizer(context.Background(), test.auth)
			if err := test.check(ctx, validTaskService); err != nil {
				if aerr, ok := err.(http.AuthzError); ok {
					t.Error(aerr.AuthzError())
				}
				t.Error(err)
			}
		})
	}
}
