package task_test

import (
	"context"
	"errors"
	"testing"

	"github.com/influxdata/influxdb"
	pctx "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/task"
)

func TestOnboardingValidation(t *testing.T) {
	svc := inmem.NewService()
	validator := task.NewValidator(mockTaskService(), svc)

	r, err := svc.Generate(context.Background(), &influxdb.OnboardingRequest{
		User:            "dude",
		Password:        "secret",
		Org:             "thing",
		Bucket:          "holder",
		RetentionPeriod: 1,
	})

	if err != nil {
		t.Fatal(err)
	}

	ctx := pctx.SetAuthorizer(context.Background(), r.Auth)

	err = validator.CreateTask(ctx, &influxdb.Task{
		OrganizationID: r.Org.ID,
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

func mockTaskService() influxdb.TaskService {
	task := influxdb.Task{
		ID:             influxdb.ID(2),
		OrganizationID: influxdb.ID(1),
		Name:           "cows",
		Owner:          influxdb.User{ID: influxdb.ID(3), Name: "farmer"},
		Flux: `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`,
		Every: "1s",
	}

	log := influxdb.Log("howdy partner")

	run := influxdb.Run{
		ID:           influxdb.ID(10),
		TaskID:       influxdb.ID(2),
		Status:       "completed",
		ScheduledFor: "a while ago",
		StartedAt:    "not so long ago",
		FinishedAt:   "more recently",
		Log:          log,
	}

	return &mock.TaskService{
		FindTaskByIDFn: func(context.Context, influxdb.ID) (*influxdb.Task, error) {
			return &task, nil
		},
		FindTasksFn: func(context.Context, influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
			return []*influxdb.Task{&task}, 1, nil
		},
		CreateTaskFn: func(_ context.Context, t *influxdb.Task) error {
			t.ID = 1
			return nil
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
	inmem := inmem.NewService()
	validTaskService := task.NewValidator(mockTaskService(), inmem)

	r, err := inmem.Generate(context.Background(), &influxdb.OnboardingRequest{
		User:            "dude",
		Password:        "secret",
		Org:             "thing",
		Bucket:          "holder",
		RetentionPeriod: 1,
	})

	orgID := influxdb.ID(1)
	taskID := influxdb.ID(2)

	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		check func(context.Context, influxdb.TaskService) error
		auth  *influxdb.Authorization
	}{
		{
			name: "create failure",
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				err := svc.CreateTask(ctx, &influxdb.Task{
					OrganizationID: r.Org.ID,
					Name:           "cows",
					Owner:          influxdb.User{ID: influxdb.ID(2), Name: "farmer"},
					Flux: `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`,
					Every: "1s",
				})
				if err == nil {
					return errors.New("failed to error without permission")
				}
				return nil
			},
			auth: &influxdb.Authorization{},
		},
		{
			name: "create success",
			auth: r.Auth,
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				err := svc.CreateTask(ctx, &influxdb.Task{
					OrganizationID: r.Org.ID,
					Name:           "cows",
					Owner:          *r.User,
					Flux: `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`,
					Every: "1s",
				})
				return err
			},
		},
		{
			name: "create badbucket",
			auth: r.Auth,
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				err := svc.CreateTask(ctx, &influxdb.Task{
					OrganizationID: r.Org.ID,
					Name:           "cows",
					Owner:          *r.User,
					Flux: `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"bad") |> range(start:-5m) |> to(bucket:"bad", org:"thing")`,
					Every: "1s",
				})
				if err == nil {
					return errors.New("created task without bucket permission")
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
			name: "FindTaskByID with auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.FindTaskByID(ctx, taskID)
				return err
			},
		},
		{
			name: "FindTasks with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &taskID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindTasks(ctx, influxdb.TaskFilter{
					Organization: &orgID,
				})
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "FindTasks with auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindTasks(ctx, influxdb.TaskFilter{
					Organization: &orgID,
				})
				return err
			},
		},
		{
			name: "FindTasks without org",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindTasks(ctx, influxdb.TaskFilter{})
				return err
			},
		},
		{
			name: "UpdateTask with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				flux := `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`
				_, err := svc.UpdateTask(ctx, 2, influxdb.TaskUpdate{
					Flux: &flux,
				})
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "UpdateTask with auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{
				influxdb.Permission{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}},
				influxdb.Permission{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.BucketsResourceType, OrgID: &orgID, ID: &r.Bucket.ID}},
				influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.BucketsResourceType, OrgID: &orgID, ID: &r.Bucket.ID}},
			}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				flux := `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"holder") |> range(start:-5m) |> to(bucket:"holder", org:"thing")`
				_, err := svc.UpdateTask(ctx, 2, influxdb.TaskUpdate{
					Flux: &flux,
				})
				return err
			},
		},
		{
			name: "UpdateTask with bad bucket",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				flux := `option task = {
 name: "my_task",
 every: 1s,
}
from(bucket:"cows") |> range(start:-5m) |> to(bucket:"cows", org:"thing")`
				_, err := svc.UpdateTask(ctx, 2, influxdb.TaskUpdate{
					Flux: &flux,
				})
				if err == nil {
					return errors.New("returned no error with unauthorized bucket")
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
			auth: &influxdb.Authorization{Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				err := svc.DeleteTask(ctx, taskID)
				if err == nil {
					return errors.New("returned without error without permission")
				}
				return nil
			},
		},
		{
			name: "DeleteTask with auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				err := svc.DeleteTask(ctx, taskID)
				return err
			},
		},
		{
			name: "FindLogs with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.OrgsResourceType, OrgID: &taskID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindLogs(ctx, influxdb.LogFilter{
					Org: &orgID,
				})
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "FindLogs with auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindLogs(ctx, influxdb.LogFilter{
					Org: &orgID,
				})
				return err
			},
		},
		{
			name: "FindLogs without org",
			auth: &influxdb.Authorization{Status: "active"},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindLogs(ctx, influxdb.LogFilter{})
				return err
			},
		},
		{
			name: "FindRuns with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.OrgsResourceType, OrgID: &taskID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindRuns(ctx, influxdb.RunFilter{
					Org: &orgID,
				})
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "FindRuns with auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindRuns(ctx, influxdb.RunFilter{
					Org: &orgID,
				})
				return err
			},
		},
		{
			name: "FindRuns without org",
			auth: &influxdb.Authorization{Status: "active"},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, _, err := svc.FindRuns(ctx, influxdb.RunFilter{})
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
			name: "FindRunByID with auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.FindRunByID(ctx, taskID, 10)
				return err
			},
		},
		{
			name: "CancelRun with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.OrgsResourceType, OrgID: &taskID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				err := svc.CancelRun(ctx, 2, 10)
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "CancelRun with auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				err := svc.CancelRun(ctx, 2, 10)
				return err
			},
		},
		{
			name: "RetryRun with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.OrgsResourceType, OrgID: &taskID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.RetryRun(ctx, 2, 10)
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "RetryRun with auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.RetryRun(ctx, 2, 10)
				return err
			},
		},
		{
			name: "ForceRun with bad auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.OrgsResourceType, OrgID: &taskID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.ForceRun(ctx, 2, 10000)
				if err == nil {
					return errors.New("returned no error with a invalid auth")
				}
				return nil
			},
		},
		{
			name: "ForceRun with auth",
			auth: &influxdb.Authorization{Status: "active", Permissions: []influxdb.Permission{influxdb.Permission{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType, OrgID: &orgID}}}},
			check: func(ctx context.Context, svc influxdb.TaskService) error {
				_, err := svc.ForceRun(ctx, 2, 10000)
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
