// Package servicetest provides tests to ensure that implementations of
// platform/task/backend.Store and platform/task/backend.LogReader meet the requirements of influxdb.TaskService.
//
// Consumers of this package must import query/builtin.
// This package does not import it directly, to avoid requiring it too early.
package servicetest

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/kit/platform"
	influxdbmock "github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/task/backend"
	"github.com/influxdata/influxdb/v2/task/options"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// BackendComponentFactory is supplied by consumers of the adaptertest package,
// to provide the values required to constitute a PlatformAdapter.
// The provided context.CancelFunc is called after the test,
// and it is the implementer's responsibility to clean up after that is called.
//
// If creating the System fails, the implementer should call t.Fatal.
type BackendComponentFactory func(t *testing.T) (*System, context.CancelFunc)

// TestTaskService should be called by consumers of the servicetest package.
// This will call fn once to create a single influxdb.TaskService
// used across all subtests in TestTaskService.
func TestTaskService(t *testing.T, fn BackendComponentFactory, testCategory ...string) {
	sys, cancel := fn(t)
	defer cancel()

	if len(testCategory) == 0 {
		testCategory = []string{"transactional", "analytical"}
	}

	for _, category := range testCategory {
		switch category {
		case "transactional":
			t.Run("TransactionalTaskService", func(t *testing.T) {
				// We're running the subtests in parallel, but if we don't use this wrapper,
				// the defer cancel() call above would return before the parallel subtests completed.
				//
				// Running the subtests in parallel might make them slightly faster,
				// but more importantly, it should exercise concurrency to catch data races.

				t.Run("Task CRUD", func(t *testing.T) {
					t.Parallel()
					testTaskCRUD(t, sys)
				})

				t.Run("FindTasks paging", func(t *testing.T) {
					testTaskFindTasksPaging(t, sys)
				})

				t.Run("FindTasks after paging", func(t *testing.T) {
					testTaskFindTasksAfterPaging(t, sys)
				})

				t.Run("Task Update Options Full", func(t *testing.T) {
					t.Parallel()
					testTaskOptionsUpdateFull(t, sys)
				})

				t.Run("Task Runs", func(t *testing.T) {
					t.Parallel()
					testTaskRuns(t, sys)
				})

				t.Run("Task Concurrency", func(t *testing.T) {
					if testing.Short() {
						t.Skip("skipping in short mode")
					}
					t.Parallel()
					testTaskConcurrency(t, sys)
				})

				t.Run("Task Updates", func(t *testing.T) {
					t.Parallel()
					testUpdate(t, sys)
				})

				t.Run("Task Manual Run", func(t *testing.T) {
					t.Parallel()
					testManualRun(t, sys)
				})

				t.Run("Task Type", func(t *testing.T) {
					t.Parallel()
					testTaskType(t, sys)
				})

			})
		case "analytical":
			t.Run("AnalyticalTaskService", func(t *testing.T) {
				t.Run("Task Run Storage", func(t *testing.T) {
					t.Parallel()
					testRunStorage(t, sys)
				})
				t.Run("Task RetryRun", func(t *testing.T) {
					t.Parallel()
					testRetryAcrossStorage(t, sys)
				})
				t.Run("task Log Storage", func(t *testing.T) {
					t.Parallel()
					testLogsAcrossStorage(t, sys)
				})
			})
		}
	}

}

// TestCreds encapsulates credentials needed for a system to properly work with tasks.
type TestCreds struct {
	OrgID, UserID, AuthorizationID platform.ID
	Org                            string
	Token                          string
}

// Authorizer returns an authorizer for the credentials in the struct
func (tc TestCreds) Authorizer() influxdb.Authorizer {
	return &influxdb.Authorization{
		ID:     tc.AuthorizationID,
		OrgID:  tc.OrgID,
		UserID: tc.UserID,
		Token:  tc.Token,
	}
}

type OrganizationService interface {
	CreateOrganization(ctx context.Context, b *influxdb.Organization) error
}

type UserService interface {
	CreateUser(ctx context.Context, u *influxdb.User) error
}

type UserResourceMappingService interface {
	CreateUserResourceMapping(ctx context.Context, m *influxdb.UserResourceMapping) error
}

type AuthorizationService interface {
	CreateAuthorization(ctx context.Context, a *influxdb.Authorization) error
}

// System  as in "system under test" encapsulates the required parts of a influxdb.TaskAdapter
type System struct {
	TaskControlService backend.TaskControlService

	// Used in the Creds function to create valid organizations, users, tokens, etc.
	OrganizationService        OrganizationService
	UserService                UserService
	UserResourceMappingService UserResourceMappingService
	AuthorizationService       AuthorizationService

	// Set this context, to be used in tests, so that any spawned goroutines watching Ctx.Done()
	// will clean up after themselves.
	Ctx context.Context

	// TaskService is the task service we would like to test
	TaskService taskmodel.TaskService

	// Override for accessing credentials for an individual test.
	// Callers can leave this nil and the test will create its own random IDs for each test.
	// However, if the system needs to verify credentials,
	// the caller should set this value and return valid IDs and a valid token.
	// It is safe if this returns the same values every time it is called.
	CredsFunc func(*testing.T) (TestCreds, error)

	// Toggles behavior between KV and archive storage because FinishRun() deletes runs after completion
	CallFinishRun bool
}

func testTaskCRUD(t *testing.T, sys *System) {
	cr := creds(t, sys)

	// Create a task.
	tc := taskmodel.TaskCreate{
		OrganizationID: cr.OrgID,
		Flux:           fmt.Sprintf(scriptFmt, 0),
		OwnerID:        cr.UserID,
		Type:           taskmodel.TaskSystemType,
	}

	authorizedCtx := icontext.SetAuthorizer(sys.Ctx, cr.Authorizer())

	tsk, err := sys.TaskService.CreateTask(authorizedCtx, tc)
	if err != nil {
		t.Fatal(err)
	}
	if !tsk.ID.Valid() {
		t.Fatal("no task ID set")
	}

	findTask := func(tasks []*taskmodel.Task, id platform.ID) (*taskmodel.Task, error) {
		for _, t := range tasks {
			if t.ID == id {
				return t, nil
			}
		}
		return nil, fmt.Errorf("failed to find task by id %s", id)
	}

	findTasksByStatus := func(tasks []*taskmodel.Task, status string) []*taskmodel.Task {
		var foundTasks = []*taskmodel.Task{}
		for _, t := range tasks {
			if t.Status == status {
				foundTasks = append(foundTasks, t)
			}
		}
		return foundTasks
	}

	// TODO: replace with ErrMissingOwner test
	// // should not be able to create a task without a token
	// noToken := influxdb.TaskCreate{
	// 	OrganizationID: cr.OrgID,
	// 	Flux:           fmt.Sprintf(scriptFmt, 0),
	// 	// OwnerID:          cr.UserID, // should fail
	// }
	// _, err = sys.TaskService.CreateTask(authorizedCtx, noToken)

	// if err != influxdb.ErrMissingToken {
	// 	t.Fatalf("expected error missing token, got: %v", err)
	// }

	// Look up a task the different ways we can.
	// Map of method name to found task.
	found := map[string]*taskmodel.Task{
		"Created": tsk,
	}

	// Find by ID should return the right task.
	f, err := sys.TaskService.FindTaskByID(sys.Ctx, tsk.ID)
	if err != nil {
		t.Fatal(err)
	}
	found["FindTaskByID"] = f

	fs, _, err := sys.TaskService.FindTasks(sys.Ctx, taskmodel.TaskFilter{OrganizationID: &cr.OrgID})
	if err != nil {
		t.Fatal(err)
	}
	f, err = findTask(fs, tsk.ID)
	if err != nil {
		t.Fatal(err)
	}
	found["FindTasks with Organization filter"] = f

	fs, _, err = sys.TaskService.FindTasks(sys.Ctx, taskmodel.TaskFilter{Organization: cr.Org})
	if err != nil {
		t.Fatal(err)
	}
	f, err = findTask(fs, tsk.ID)
	if err != nil {
		t.Fatal(err)
	}
	found["FindTasks with Organization name filter"] = f

	fs, _, err = sys.TaskService.FindTasks(sys.Ctx, taskmodel.TaskFilter{User: &cr.UserID})
	if err != nil {
		t.Fatal(err)
	}
	f, err = findTask(fs, tsk.ID)
	if err != nil {
		t.Fatal(err)
	}
	found["FindTasks with User filter"] = f

	want := &taskmodel.Task{
		ID:              tsk.ID,
		CreatedAt:       tsk.CreatedAt,
		LatestCompleted: tsk.LatestCompleted,
		LatestScheduled: tsk.LatestScheduled,
		OrganizationID:  cr.OrgID,
		Organization:    cr.Org,
		OwnerID:         tsk.OwnerID,
		Name:            "task #0",
		Cron:            "* * * * *",
		Offset:          5 * time.Second,
		Status:          string(taskmodel.DefaultTaskStatus),
		Flux:            fmt.Sprintf(scriptFmt, 0),
		Type:            taskmodel.TaskSystemType,
	}

	for fn, f := range found {
		if diff := cmp.Diff(f, want); diff != "" {
			t.Logf("got: %+#v", f)
			t.Errorf("expected %s task to be consistant: -got/+want: %s", fn, diff)
		}
	}

	// Check limits
	tc2 := taskmodel.TaskCreate{
		OrganizationID: cr.OrgID,
		Flux:           fmt.Sprintf(scriptFmt, 1),
		OwnerID:        cr.UserID,
		Status:         string(taskmodel.TaskInactive),
	}

	if _, err := sys.TaskService.CreateTask(authorizedCtx, tc2); err != nil {
		t.Fatal(err)
	}
	if !tsk.ID.Valid() {
		t.Fatal("no task ID set")
	}
	tasks, _, err := sys.TaskService.FindTasks(sys.Ctx, taskmodel.TaskFilter{OrganizationID: &cr.OrgID, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) > 1 {
		t.Fatalf("failed to limit tasks: expected: 1, got : %d", len(tasks))
	}

	// Check after
	first := tasks[0]
	tasks, _, err = sys.TaskService.FindTasks(sys.Ctx, taskmodel.TaskFilter{OrganizationID: &cr.OrgID, After: &first.ID})
	if err != nil {
		t.Fatal(err)
	}
	// because this test runs concurrently we can only guarantee we at least 2 tasks
	// when using after we can check to make sure the after is not in the list
	if len(tasks) == 0 {
		t.Fatalf("expected at least 1 task: got 0")
	}
	for _, task := range tasks {
		if first.ID == task.ID {
			t.Fatalf("after task included in task list")
		}
	}

	// Check task status filter
	active := string(taskmodel.TaskActive)
	fs, _, err = sys.TaskService.FindTasks(sys.Ctx, taskmodel.TaskFilter{Status: &active})
	if err != nil {
		t.Fatal(err)
	}

	activeTasks := findTasksByStatus(fs, string(taskmodel.TaskActive))
	if len(fs) != len(activeTasks) {
		t.Fatalf("expected to find %d active tasks, found: %d", len(activeTasks), len(fs))
	}

	inactive := string(taskmodel.TaskInactive)
	fs, _, err = sys.TaskService.FindTasks(sys.Ctx, taskmodel.TaskFilter{Status: &inactive})
	if err != nil {
		t.Fatal(err)
	}

	inactiveTasks := findTasksByStatus(fs, string(taskmodel.TaskInactive))
	if len(fs) != len(inactiveTasks) {
		t.Fatalf("expected to find %d inactive tasks, found: %d", len(inactiveTasks), len(fs))
	}

	// Update task: script only.
	newFlux := fmt.Sprintf(scriptFmt, 99)
	origID := f.ID
	f, err = sys.TaskService.UpdateTask(authorizedCtx, origID, taskmodel.TaskUpdate{Flux: &newFlux})
	if err != nil {
		t.Fatal(err)
	}

	if origID != f.ID {
		t.Fatalf("task ID unexpectedly changed during update, from %s to %s", origID.String(), f.ID.String())
	}

	if f.Flux != newFlux {
		t.Fatalf("wrong flux from update; want %q, got %q", newFlux, f.Flux)
	}
	if f.Status != string(taskmodel.TaskActive) {
		t.Fatalf("expected task to be created active, got %q", f.Status)
	}

	// Update task: status only.
	newStatus := string(taskmodel.TaskInactive)
	f, err = sys.TaskService.UpdateTask(authorizedCtx, origID, taskmodel.TaskUpdate{Status: &newStatus})
	if err != nil {
		t.Fatal(err)
	}
	if f.Flux != newFlux {
		t.Fatalf("flux unexpected updated: %s", f.Flux)
	}
	if f.Status != newStatus {
		t.Fatalf("expected task status to be inactive, got %q", f.Status)
	}

	// Update task: reactivate status and update script.
	newStatus = string(taskmodel.TaskActive)
	newFlux = fmt.Sprintf(scriptFmt, 98)
	f, err = sys.TaskService.UpdateTask(authorizedCtx, origID, taskmodel.TaskUpdate{Flux: &newFlux, Status: &newStatus})
	if err != nil {
		t.Fatal(err)
	}
	if f.Flux != newFlux {
		t.Fatalf("flux unexpected updated: %s", f.Flux)
	}
	if f.Status != newStatus {
		t.Fatalf("expected task status to be inactive, got %q", f.Status)
	}

	// Update task: just update an option.
	newStatus = string(taskmodel.TaskActive)
	newFlux = "option task = {\n\tname: \"task-changed #98\",\n\tcron: \"* * * * *\",\n\toffset: 5s,\n\tconcurrency: 100,\n}\n\nfrom(bucket: \"b\")\n\t|> to(bucket: \"two\", orgID: \"000000000000000\")"
	f, err = sys.TaskService.UpdateTask(authorizedCtx, origID, taskmodel.TaskUpdate{Options: options.Options{Name: "task-changed #98"}})
	if err != nil {
		t.Fatal(err)
	}
	if f.Flux != newFlux {
		diff := cmp.Diff(f.Flux, newFlux)
		t.Fatalf("flux unexpected updated: %s", diff)
	}
	if f.Status != newStatus {
		t.Fatalf("expected task status to be active, got %q", f.Status)
	}

	// Update task: switch to every.
	newStatus = string(taskmodel.TaskActive)
	newFlux = "option task = {\n\tname: \"task-changed #98\",\n\tevery: 30s,\n\toffset: 5s,\n\tconcurrency: 100,\n}\n\nfrom(bucket: \"b\")\n\t|> to(bucket: \"two\", orgID: \"000000000000000\")"
	f, err = sys.TaskService.UpdateTask(authorizedCtx, origID, taskmodel.TaskUpdate{Options: options.Options{Every: *(options.MustParseDuration("30s"))}})
	if err != nil {
		t.Fatal(err)
	}
	if f.Flux != newFlux {
		diff := cmp.Diff(f.Flux, newFlux)
		t.Fatalf("flux unexpected updated: %s", diff)
	}
	if f.Status != newStatus {
		t.Fatalf("expected task status to be active, got %q", f.Status)
	}

	// Update task: just cron.
	newStatus = string(taskmodel.TaskActive)
	newFlux = fmt.Sprintf(scriptDifferentName, 98)
	f, err = sys.TaskService.UpdateTask(authorizedCtx, origID, taskmodel.TaskUpdate{Options: options.Options{Cron: "* * * * *"}})
	if err != nil {
		t.Fatal(err)
	}
	if f.Flux != newFlux {
		diff := cmp.Diff(f.Flux, newFlux)
		t.Fatalf("flux unexpected updated: %s", diff)
	}
	if f.Status != newStatus {
		t.Fatalf("expected task status to be active, got %q", f.Status)
	}

	// // Update task: use a new token on the context and modify some other option.
	// // Ensure the authorization doesn't change -- it did change at one time, which was bug https://github.com/influxdata/influxdb/issues/12218.
	// newAuthz := &influxdb.Authorization{OrgID: cr.OrgID, UserID: cr.UserID, Permissions: influxdb.OperPermissions()}
	// if err := sys.I.CreateAuthorization(sys.Ctx, newAuthz); err != nil {
	// 	t.Fatal(err)
	// }
	// newAuthorizedCtx := icontext.SetAuthorizer(sys.Ctx, newAuthz)
	// f, err = sys.TaskService.UpdateTask(newAuthorizedCtx, origID, influxdb.TaskUpdate{Options: options.Options{Name: "foo"}})
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// if f.Name != "foo" {
	// 	t.Fatalf("expected name to update to foo, got %s", f.Name)
	// }
	// if f.AuthorizationID != authzID {
	// 	t.Fatalf("expected authorization ID to remain %v, got %v", authzID, f.AuthorizationID)
	// }

	// // Now actually update to use the new token, from the original authorization context.
	// f, err = sys.TaskService.UpdateTask(authorizedCtx, origID, influxdb.TaskUpdate{Token: newAuthz.Token})
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// if f.AuthorizationID != newAuthz.ID {
	// 	t.Fatalf("expected authorization ID %v, got %v", newAuthz.ID, f.AuthorizationID)
	// }

	// Delete task.
	if err := sys.TaskService.DeleteTask(sys.Ctx, origID); err != nil {
		t.Fatal(err)
	}

	// Task should not be returned.
	if _, err := sys.TaskService.FindTaskByID(sys.Ctx, origID); err != taskmodel.ErrTaskNotFound {
		t.Fatalf("expected %v, got %v", taskmodel.ErrTaskNotFound, err)
	}
}

func testTaskFindTasksPaging(t *testing.T, sys *System) {
	script := `option task = {
	name: "Task %03d",
	cron: "* * * * *",
	concurrency: 100,
	offset: 10s,
}

from(bucket: "b")
	|> to(bucket: "two", orgID: "000000000000000")`

	cr := creds(t, sys)

	tc := taskmodel.TaskCreate{
		OrganizationID: cr.OrgID,
		OwnerID:        cr.UserID,
		Type:           taskmodel.TaskSystemType,
	}

	authorizedCtx := icontext.SetAuthorizer(sys.Ctx, cr.Authorizer())

	created := make([]*taskmodel.Task, 50)
	for i := 0; i < 50; i++ {
		tc.Flux = fmt.Sprintf(script, i/10)
		tsk, err := sys.TaskService.CreateTask(authorizedCtx, tc)
		if err != nil {
			t.Fatal(err)
		}
		if !tsk.ID.Valid() {
			t.Fatal("no task ID set")
		}

		created[i] = tsk
	}

	tasks, _, err := sys.TaskService.FindTasks(sys.Ctx, taskmodel.TaskFilter{Limit: 5})
	if err != nil {
		t.Fatalf("FindTasks: %v", err)
	}

	if got, exp := len(tasks), 5; got != exp {
		t.Fatalf("unexpected len(taksks), -got/+exp: %v", cmp.Diff(got, exp))
	}

	// find tasks using name which are after first 10
	name := "Task 004"
	tasks, _, err = sys.TaskService.FindTasks(sys.Ctx, taskmodel.TaskFilter{Limit: 5, Name: &name})
	if err != nil {
		t.Fatalf("FindTasks: %v", err)
	}

	if got, exp := len(tasks), 5; got != exp {
		t.Fatalf("unexpected len(taksks), -got/+exp: %v", cmp.Diff(got, exp))
	}
}

func testTaskFindTasksAfterPaging(t *testing.T, sys *System) {
	var (
		script = `option task = {
	name: "some-unique-task-name",
	cron: "* * * * *",
	concurrency: 100,
	offset: 10s,
}

from(bucket: "b")
	|> to(bucket: "two", orgID: "000000000000000")`
		cr = creds(t, sys)
		tc = taskmodel.TaskCreate{
			OrganizationID: cr.OrgID,
			OwnerID:        cr.UserID,
			Type:           taskmodel.TaskSystemType,
			Flux:           script,
		}
		authorizedCtx = icontext.SetAuthorizer(sys.Ctx, cr.Authorizer())
		created       = make([]*taskmodel.Task, 10)
		taskName      = "some-unique-task-name"
	)

	for i := 0; i < len(created); i++ {
		tsk, err := sys.TaskService.CreateTask(authorizedCtx, tc)
		if err != nil {
			t.Fatal(err)
		}
		if !tsk.ID.Valid() {
			t.Fatal("no task ID set")
		}

		created[i] = tsk
	}

	var (
		expected = [][]platform.ID{
			{created[0].ID, created[1].ID},
			{created[2].ID, created[3].ID},
			{created[4].ID, created[5].ID},
			{created[6].ID, created[7].ID},
			{created[8].ID, created[9].ID},
			// last page should be empty
			nil,
		}
		found = make([][]platform.ID, 0, 6)
		after *platform.ID
	)

	// one more than expected pages
	for i := 0; i < 6; i++ {
		tasks, _, err := sys.TaskService.FindTasks(sys.Ctx, taskmodel.TaskFilter{
			Limit: 2,
			After: after,
			Name:  &taskName,
		})
		if err != nil {
			t.Fatalf("FindTasks: %v", err)
		}

		var page []platform.ID
		for _, task := range tasks {
			page = append(page, task.ID)
		}

		found = append(found, page)

		if len(tasks) == 0 {
			break
		}

		after = &tasks[len(tasks)-1].ID
	}

	if !reflect.DeepEqual(expected, found) {
		t.Errorf("expected %#v, found %#v", expected, found)
	}
}

//Create a new task with a Cron and Offset option
//Update the task to remove the Offset option, and change Cron to Every
//Retrieve the task again to ensure the options are now Every, without Cron or Offset
func testTaskOptionsUpdateFull(t *testing.T, sys *System) {

	script := `option task = {
	name: "task-Options-Update",
	cron: "* * * * *",
	concurrency: 100,
	offset: 10s,
}

from(bucket: "b")
	|> to(bucket: "two", orgID: "000000000000000")`

	cr := creds(t, sys)

	ct := taskmodel.TaskCreate{
		OrganizationID: cr.OrgID,
		Flux:           script,
		OwnerID:        cr.UserID,
	}
	authorizedCtx := icontext.SetAuthorizer(sys.Ctx, cr.Authorizer())
	task, err := sys.TaskService.CreateTask(authorizedCtx, ct)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("update task and delete offset", func(t *testing.T) {
		expectedFlux := `option task = {name: "task-Options-Update", every: 10s, concurrency: 100}

from(bucket: "b")
	|> to(bucket: "two", orgID: "000000000000000")`
		f, err := sys.TaskService.UpdateTask(authorizedCtx, task.ID, taskmodel.TaskUpdate{Options: options.Options{Offset: &options.Duration{}, Every: *(options.MustParseDuration("10s"))}})
		if err != nil {
			t.Fatal(err)
		}
		savedTask, err := sys.TaskService.FindTaskByID(sys.Ctx, f.ID)
		if err != nil {
			t.Fatal(err)
		}
		if savedTask.Flux != expectedFlux {
			diff := cmp.Diff(savedTask.Flux, expectedFlux)
			t.Fatalf("flux unexpected updated: %s", diff)
		}
	})
	t.Run("update task with different offset option", func(t *testing.T) {
		expectedFlux := `option task = {
	name: "task-Options-Update",
	every: 10s,
	concurrency: 100,
	offset: 10s,
}

from(bucket: "b")
	|> to(bucket: "two", orgID: "000000000000000")`
		f, err := sys.TaskService.UpdateTask(authorizedCtx, task.ID, taskmodel.TaskUpdate{Options: options.Options{Offset: options.MustParseDuration("10s")}})
		if err != nil {
			t.Fatal(err)
		}
		savedTask, err := sys.TaskService.FindTaskByID(sys.Ctx, f.ID)
		if err != nil {
			t.Fatal(err)
		}
		if savedTask.Flux != expectedFlux {
			diff := cmp.Diff(savedTask.Flux, expectedFlux)
			t.Fatalf("flux unexpected updated: %s", diff)
		}

		withoutOffset := `option task = {
	name: "task-Options-Update",
	every: 10s,
	concurrency: 100,
}

from(bucket: "b")
	|> to(bucket: "two", orgID: "000000000000000")`
		fNoOffset, err := sys.TaskService.UpdateTask(authorizedCtx, task.ID, taskmodel.TaskUpdate{Flux: &withoutOffset})
		if err != nil {
			t.Fatal(err)
		}
		var zero time.Duration
		if fNoOffset.Offset != zero {
			t.Fatal("removing offset failed")
		}
	})

}

func testUpdate(t *testing.T, sys *System) {
	cr := creds(t, sys)

	now := time.Now()
	earliestCA := now.Add(-time.Second)

	ct := taskmodel.TaskCreate{
		OrganizationID: cr.OrgID,
		Flux:           fmt.Sprintf(scriptFmt, 0),
		OwnerID:        cr.UserID,
	}
	authorizedCtx := icontext.SetAuthorizer(sys.Ctx, cr.Authorizer())
	task, err := sys.TaskService.CreateTask(authorizedCtx, ct)
	if err != nil {
		t.Fatal(err)
	}

	if task.LatestScheduled.IsZero() {
		t.Fatal("expected a non-zero LatestScheduled on created task")
	}

	st, err := sys.TaskService.FindTaskByID(sys.Ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}

	after := time.Now()
	latestCA := after.Add(time.Second)

	ca := st.CreatedAt

	if earliestCA.After(ca) || latestCA.Before(ca) {
		t.Fatalf("createdAt not accurate, expected %s to be between %s and %s", ca, earliestCA, latestCA)
	}

	ti := st.LatestCompleted

	if now.Sub(ti) > 10*time.Second {
		t.Fatalf("latest completed not accurate, expected: ~%s, got %s", now, ti)
	}

	requestedAt := time.Now().Add(5 * time.Minute).UTC()

	rc, err := sys.TaskControlService.CreateRun(sys.Ctx, task.ID, requestedAt, requestedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}

	if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc.ID, time.Now(), taskmodel.RunStarted); err != nil {
		t.Fatal(err)
	}

	if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc.ID, time.Now(), taskmodel.RunSuccess); err != nil {
		t.Fatal(err)
	}

	if _, err := sys.TaskControlService.FinishRun(sys.Ctx, task.ID, rc.ID); err != nil {
		t.Fatal(err)
	}

	st2, err := sys.TaskService.FindTaskByID(sys.Ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}

	if st2.LatestCompleted.Before(st.LatestCompleted) {
		t.Fatalf("executed task has not updated latest complete: expected %s > %s", st2.LatestCompleted, st.LatestCompleted)
	}

	if st2.LastRunStatus != "success" {
		t.Fatal("executed task has not updated last run status")
	}

	if st2.LastRunError != "" {
		t.Fatal("executed task has updated last run error on success")
	}

	rc2, err := sys.TaskControlService.CreateRun(sys.Ctx, task.ID, requestedAt, requestedAt)
	if err != nil {
		t.Fatal(err)
	}

	if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc2.ID, time.Now(), taskmodel.RunStarted); err != nil {
		t.Fatal(err)
	}

	if err := sys.TaskControlService.AddRunLog(sys.Ctx, task.ID, rc2.ID, time.Now(), "error message"); err != nil {
		t.Fatal(err)
	}

	if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc2.ID, time.Now(), taskmodel.RunFail); err != nil {
		t.Fatal(err)
	}

	if err := sys.TaskControlService.AddRunLog(sys.Ctx, task.ID, rc2.ID, time.Now(), "last message"); err != nil {
		t.Fatal(err)
	}

	if _, err := sys.TaskControlService.FinishRun(sys.Ctx, task.ID, rc2.ID); err != nil {
		t.Fatal(err)
	}

	st3, err := sys.TaskService.FindTaskByID(sys.Ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}

	if st3.LatestCompleted.Before(st2.LatestCompleted) {
		t.Fatalf("executed task has not updated latest complete: expected %s > %s", st3.LatestCompleted, st2.LatestCompleted)
	}

	if st3.LastRunStatus != "failed" {
		t.Fatal("executed task has not updated last run status")
	}

	if st3.LastRunError != "error message" {
		t.Fatal("executed task has not updated last run error on failed")
	}

	now = time.Now()
	flux := fmt.Sprintf(scriptFmt, 1)
	task, err = sys.TaskService.UpdateTask(authorizedCtx, task.ID, taskmodel.TaskUpdate{Flux: &flux})
	if err != nil {
		t.Fatal(err)
	}
	after = time.Now()

	earliestUA := now.Add(-time.Second)
	latestUA := after.Add(time.Second)

	ua := task.UpdatedAt

	if earliestUA.After(ua) || latestUA.Before(ua) {
		t.Fatalf("updatedAt not accurate, expected %s to be between %s and %s", ua, earliestUA, latestUA)
	}

	st, err = sys.TaskService.FindTaskByID(sys.Ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}

	ua = st.UpdatedAt

	if earliestUA.After(ua) || latestUA.Before(ua) {
		t.Fatalf("updatedAt not accurate after pulling new task, expected %s to be between %s and %s", ua, earliestUA, latestUA)
	}

	ls := time.Now().Round(time.Second) // round to remove monotonic clock
	task, err = sys.TaskService.UpdateTask(authorizedCtx, task.ID, taskmodel.TaskUpdate{LatestScheduled: &ls})
	if err != nil {
		t.Fatal(err)
	}

	st, err = sys.TaskService.FindTaskByID(sys.Ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !st.LatestScheduled.Equal(ls) {
		t.Fatalf("expected latest scheduled to update, expected: %v, got: %v", ls, st.LatestScheduled)
	}

}

func testTaskRuns(t *testing.T, sys *System) {
	cr := creds(t, sys)

	t.Run("FindRuns and FindRunByID", func(t *testing.T) {
		t.Parallel()

		// Script is set to run every minute. The platform adapter is currently hardcoded to schedule after "now",
		// which makes timing of runs somewhat difficult.
		ct := taskmodel.TaskCreate{
			OrganizationID: cr.OrgID,
			Flux:           fmt.Sprintf(scriptFmt, 0),
			OwnerID:        cr.UserID,
		}
		task, err := sys.TaskService.CreateTask(icontext.SetAuthorizer(sys.Ctx, cr.Authorizer()), ct)
		if err != nil {
			t.Fatal(err)
		}

		// check run filter errors
		_, _, err0 := sys.TaskService.FindRuns(sys.Ctx, taskmodel.RunFilter{Task: task.ID, Limit: -1})
		if err0 != taskmodel.ErrOutOfBoundsLimit {
			t.Fatalf("failed to error with out of bounds run limit: %d", -1)
		}

		_, _, err1 := sys.TaskService.FindRuns(sys.Ctx, taskmodel.RunFilter{Task: task.ID, Limit: taskmodel.TaskMaxPageSize + 1})
		if err1 != taskmodel.ErrOutOfBoundsLimit {
			t.Fatalf("failed to error with out of bounds run limit: %d", taskmodel.TaskMaxPageSize+1)
		}

		requestedAt := time.Now().Add(time.Hour * -1).UTC() // This should guarantee we can make two runs.

		rc0, err := sys.TaskControlService.CreateRun(sys.Ctx, task.ID, requestedAt, requestedAt.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if rc0.TaskID != task.ID {
			t.Fatalf("wrong task ID on created task: got %s, want %s", rc0.TaskID, task.ID)
		}

		startedAt := time.Now().UTC()

		// Update the run state to Started; normally the scheduler would do this.
		if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc0.ID, startedAt, taskmodel.RunStarted); err != nil {
			t.Fatal(err)
		}

		rc1, err := sys.TaskControlService.CreateRun(sys.Ctx, task.ID, requestedAt, requestedAt.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if rc1.TaskID != task.ID {
			t.Fatalf("wrong task ID on created task run: got %s, want %s", rc1.TaskID, task.ID)
		}

		// Update the run state to Started; normally the scheduler would do this.
		if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc1.ID, startedAt, taskmodel.RunStarted); err != nil {
			t.Fatal(err)
		}

		runs, _, err := sys.TaskService.FindRuns(sys.Ctx, taskmodel.RunFilter{Task: task.ID, Limit: 1})
		if err != nil {
			t.Fatal(err)
		}

		if len(runs) != 1 {
			t.Fatalf("expected 1 run, got %#v", runs)
		}

		// Mark the second run finished.
		if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc1.ID, startedAt.Add(time.Second), taskmodel.RunSuccess); err != nil {
			t.Fatal(err)
		}

		if _, err := sys.TaskControlService.FinishRun(sys.Ctx, task.ID, rc1.ID); err != nil {
			t.Fatal(err)
		}

		// Limit 1 should only return the earlier run.
		runs, _, err = sys.TaskService.FindRuns(sys.Ctx, taskmodel.RunFilter{Task: task.ID, Limit: 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(runs) != 1 {
			t.Fatalf("expected 1 run, got %v", runs)
		}
		if runs[0].ID != rc0.ID {
			t.Fatalf("retrieved wrong run ID; want %s, got %s", rc0.ID, runs[0].ID)
		}
		if runs[0].StartedAt != startedAt {
			t.Fatalf("unexpectedStartedAt; want %s, got %s", startedAt, runs[0].StartedAt)
		}
		if runs[0].Status != taskmodel.RunStarted.String() {
			t.Fatalf("unexpected run status; want %s, got %s", taskmodel.RunStarted.String(), runs[0].Status)
		}

		if !runs[0].FinishedAt.IsZero() {
			t.Fatalf("expected empty FinishedAt, got %q", runs[0].FinishedAt)
		}

		// Look for a run that doesn't exist.
		_, err = sys.TaskService.FindRunByID(sys.Ctx, task.ID, platform.ID(math.MaxUint64))
		if err == nil {
			t.Fatalf("expected %s but got %s instead", taskmodel.ErrRunNotFound, err)
		}

		// look for a taskID that doesn't exist.
		_, err = sys.TaskService.FindRunByID(sys.Ctx, platform.ID(math.MaxUint64), runs[0].ID)
		if err == nil {
			t.Fatalf("expected %s but got %s instead", taskmodel.ErrRunNotFound, err)
		}

		foundRun0, err := sys.TaskService.FindRunByID(sys.Ctx, task.ID, runs[0].ID)
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(foundRun0, runs[0]); diff != "" {
			t.Fatalf("difference between listed run and found run: %s", diff)
		}
	})

	t.Run("FindRunsByTime", func(t *testing.T) {

		t.Parallel()
		ctx := icontext.SetAuthorizer(sys.Ctx, cr.Authorizer())
		ctx, err := feature.Annotate(ctx, influxdbmock.NewFlagger(map[feature.Flag]interface{}{
			feature.TimeFilterFlags(): true,
		}))
		require.NoError(t, err)

		// Script is set to run every minute. The platform adapter is currently hardcoded to schedule after "now",
		// which makes timing of runs somewhat difficult.
		ct := taskmodel.TaskCreate{
			OrganizationID: cr.OrgID,
			Flux:           fmt.Sprintf(scriptFmt, 0),
			OwnerID:        cr.UserID,
		}
		task, err := sys.TaskService.CreateTask(ctx, ct)
		if err != nil {
			t.Fatal(err)
		}

		// set to one hour before now because of bucket retention policy
		scheduledFor := time.Now().Add(time.Hour * -1).UTC()
		runs := make([]*taskmodel.Run, 0, 5)
		// create runs to put into Context
		for i := 5; i > 0; i-- {
			run, err := sys.TaskControlService.CreateRun(ctx, task.ID, scheduledFor.Add(time.Second*time.Duration(i)), scheduledFor.Add(time.Second*time.Duration(i)))
			if err != nil {
				t.Fatal(err)
			}
			err = sys.TaskControlService.UpdateRunState(ctx, task.ID, run.ID, scheduledFor.Add(time.Second*time.Duration(i+1)), taskmodel.RunStarted)
			if err != nil {
				t.Fatal(err)
			}
			err = sys.TaskControlService.UpdateRunState(ctx, task.ID, run.ID, scheduledFor.Add(time.Second*time.Duration(i+2)), taskmodel.RunSuccess)
			if err != nil {
				t.Fatal(err)
			}
			// setting run in memory to match the fields in Context
			run.StartedAt = scheduledFor.Add(time.Second * time.Duration(i+1))
			run.FinishedAt = scheduledFor.Add(time.Second * time.Duration(i+2))
			run.RunAt = scheduledFor.Add(time.Second * time.Duration(i))
			run.Status = taskmodel.RunSuccess.String()
			run.Log = nil

			if sys.CallFinishRun {
				run, err = sys.TaskControlService.FinishRun(ctx, task.ID, run.ID)
				if err != nil {
					t.Fatal(err)
				}
				// Analytical storage does not store run at
				run.RunAt = time.Time{}
			}

			runs = append(runs, run)
		}

		found, _, err := sys.TaskService.FindRuns(ctx,
			taskmodel.RunFilter{
				Task:       task.ID,
				Limit:      2,
				AfterTime:  scheduledFor.Add(time.Second * time.Duration(1)).Format(time.RFC3339),
				BeforeTime: scheduledFor.Add(time.Second * time.Duration(4)).Format(time.RFC3339),
			})
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, runs[2:4], found)

	})

	t.Run("ForceRun", func(t *testing.T) {
		t.Parallel()

		ct := taskmodel.TaskCreate{
			OrganizationID: cr.OrgID,
			Flux:           fmt.Sprintf(scriptFmt, 0),
			OwnerID:        cr.UserID,
		}
		task, err := sys.TaskService.CreateTask(icontext.SetAuthorizer(sys.Ctx, cr.Authorizer()), ct)
		if err != nil {
			t.Fatal(err)
		}

		const scheduledFor = 77
		r, err := sys.TaskService.ForceRun(sys.Ctx, task.ID, scheduledFor)
		if err != nil {
			t.Fatal(err)
		}
		exp, _ := time.Parse(time.RFC3339, "1970-01-01T00:01:17Z")
		if r.ScheduledFor != exp {
			t.Fatalf("expected: 1970-01-01T00:01:17Z, got %s", r.ScheduledFor)
		}

		// Forcing the same run before it's executed should be rejected.
		if _, err = sys.TaskService.ForceRun(sys.Ctx, task.ID, scheduledFor); err == nil {
			t.Fatalf("subsequent force should have been rejected; failed to error: %s", task.ID)
		}
	})

	t.Run("FindLogs", func(t *testing.T) {
		t.Parallel()

		ct := taskmodel.TaskCreate{
			OrganizationID: cr.OrgID,
			Flux:           fmt.Sprintf(scriptFmt, 0),
			OwnerID:        cr.UserID,
		}
		task, err := sys.TaskService.CreateTask(icontext.SetAuthorizer(sys.Ctx, cr.Authorizer()), ct)
		if err != nil {
			t.Fatal(err)
		}

		requestedAt := time.Now().Add(time.Hour * -1).UTC() // This should guarantee we can make a run.

		// Create two runs.
		rc1, err := sys.TaskControlService.CreateRun(sys.Ctx, task.ID, requestedAt, requestedAt.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc1.ID, time.Now(), taskmodel.RunStarted); err != nil {
			t.Fatal(err)
		}

		rc2, err := sys.TaskControlService.CreateRun(sys.Ctx, task.ID, requestedAt, requestedAt.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc2.ID, time.Now(), taskmodel.RunStarted); err != nil {
			t.Fatal(err)
		}
		// Add a log for the first run.
		log1Time := time.Now().UTC()
		if err := sys.TaskControlService.AddRunLog(sys.Ctx, task.ID, rc1.ID, log1Time, "entry 1"); err != nil {
			t.Fatal(err)
		}

		// Ensure it is returned when filtering logs by run ID.
		logs, _, err := sys.TaskService.FindLogs(sys.Ctx, taskmodel.LogFilter{
			Task: task.ID,
			Run:  &rc1.ID,
		})
		if err != nil {
			t.Fatal(err)
		}

		expLine1 := &taskmodel.Log{RunID: rc1.ID, Time: log1Time.Format(time.RFC3339Nano), Message: "entry 1"}
		exp := []*taskmodel.Log{expLine1}
		if diff := cmp.Diff(logs, exp); diff != "" {
			t.Fatalf("unexpected log: -got/+want: %s", diff)
		}

		// Add a log for the second run.
		log2Time := time.Now().UTC()
		if err := sys.TaskControlService.AddRunLog(sys.Ctx, task.ID, rc2.ID, log2Time, "entry 2"); err != nil {
			t.Fatal(err)
		}

		// Ensure both returned when filtering logs by task ID.
		logs, _, err = sys.TaskService.FindLogs(sys.Ctx, taskmodel.LogFilter{
			Task: task.ID,
		})
		if err != nil {
			t.Fatal(err)
		}
		expLine2 := &taskmodel.Log{RunID: rc2.ID, Time: log2Time.Format(time.RFC3339Nano), Message: "entry 2"}
		exp = []*taskmodel.Log{expLine1, expLine2}
		if diff := cmp.Diff(logs, exp); diff != "" {
			t.Fatalf("unexpected log: -got/+want: %s", diff)
		}
	})
}

func testTaskConcurrency(t *testing.T, sys *System) {
	cr := creds(t, sys)

	const numTasks = 450 // Arbitrarily chosen to get a reasonable count of concurrent creates and deletes.
	createTaskCh := make(chan taskmodel.TaskCreate, numTasks)

	// Since this test is run in parallel with other tests,
	// we need to keep a whitelist of IDs that are okay to delete.
	// This only matters when the creds function returns an identical user/org from another test.
	var idMu sync.Mutex
	taskIDs := make(map[platform.ID]struct{})

	var createWg sync.WaitGroup
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		createWg.Add(1)
		go func() {
			defer createWg.Done()
			aCtx := icontext.SetAuthorizer(sys.Ctx, cr.Authorizer())
			for ct := range createTaskCh {
				task, err := sys.TaskService.CreateTask(aCtx, ct)
				if err != nil {
					t.Errorf("error creating task: %v", err)
					continue
				}
				idMu.Lock()
				taskIDs[task.ID] = struct{}{}
				idMu.Unlock()
			}
		}()
	}

	// Signal for non-creator goroutines to stop.
	quitCh := make(chan struct{})
	go func() {
		createWg.Wait()
		close(quitCh)
	}()

	var extraWg sync.WaitGroup
	// Get all the tasks, and delete the first one we find.
	extraWg.Add(1)
	go func() {
		defer extraWg.Done()

		deleted := 0
		defer func() {
			t.Logf("Concurrently deleted %d tasks", deleted)
		}()
		for {
			// Check if we need to quit.
			select {
			case <-quitCh:
				return
			default:
			}

			// Get all the tasks.
			tasks, _, err := sys.TaskService.FindTasks(sys.Ctx, taskmodel.TaskFilter{OrganizationID: &cr.OrgID})
			if err != nil {
				t.Errorf("error finding tasks: %v", err)
				return
			}
			if len(tasks) == 0 {
				continue
			}

			// Check again if we need to quit.
			select {
			case <-quitCh:
				return
			default:
			}

			for _, tsk := range tasks {
				// Was the retrieved task an ID we're allowed to delete?
				idMu.Lock()
				_, ok := taskIDs[tsk.ID]
				idMu.Unlock()
				if !ok {
					continue
				}

				// Task was in whitelist. Delete it from the TaskService.
				// We could remove it from the taskIDs map, but this test is short-lived enough
				// that clearing out the map isn't really worth taking the lock again.
				if err := sys.TaskService.DeleteTask(sys.Ctx, tsk.ID); err != nil {
					t.Errorf("error deleting task: %v", err)
					return
				}
				deleted++

				// Wait just a tiny bit.
				time.Sleep(time.Millisecond)
				break
			}
		}
	}()

	extraWg.Add(1)
	go func() {
		defer extraWg.Done()

		runsCreated := 0
		defer func() {
			t.Logf("Concurrently created %d runs", runsCreated)
		}()
		for {
			// Check if we need to quit.
			select {
			case <-quitCh:
				return
			default:
			}

			// Get all the tasks.
			tasks, _, err := sys.TaskService.FindTasks(sys.Ctx, taskmodel.TaskFilter{OrganizationID: &cr.OrgID})
			if err != nil {
				t.Errorf("error finding tasks: %v", err)
				return
			}
			if len(tasks) == 0 {
				continue
			}

			// Check again if we need to quit.
			select {
			case <-quitCh:
				return
			default:
			}

			// Create a run for the last task we found.
			// The script should run every minute, so use max now.
			var tid platform.ID
			idMu.Lock()
			for i := len(tasks) - 1; i >= 0; i-- {
				_, ok := taskIDs[tasks[i].ID]
				if ok {
					tid = tasks[i].ID
					break
				}
			}
			idMu.Unlock()
			if !tid.Valid() {
				continue
			}
			if _, err := sys.TaskControlService.CreateRun(sys.Ctx, tid, time.Unix(253339232461, 0), time.Unix(253339232469, 1)); err != nil {
				// This may have errored due to the task being deleted. Check if the task still exists.

				if _, err2 := sys.TaskService.FindTaskByID(sys.Ctx, tid); err2 == taskmodel.ErrTaskNotFound {
					// It was deleted. Just continue.
					continue
				}
				// Otherwise, we were able to find the task, so something went wrong here.
				t.Errorf("error creating next run: %v", err)
				return
			}
			runsCreated++

			// Wait just a tiny bit.
			time.Sleep(time.Millisecond)
		}
	}()

	// Start adding tasks.
	for i := 0; i < numTasks; i++ {
		createTaskCh <- taskmodel.TaskCreate{
			OrganizationID: cr.OrgID,
			Flux:           fmt.Sprintf(scriptFmt, i),
			OwnerID:        cr.UserID,
		}
	}

	// Done adding. Wait for cleanup.
	close(createTaskCh)
	createWg.Wait()
	extraWg.Wait()
}

func testManualRun(t *testing.T, s *System) {
	cr := creds(t, s)

	// Create a task.
	tc := taskmodel.TaskCreate{
		OrganizationID: cr.OrgID,
		Flux:           fmt.Sprintf(scriptFmt, 0),
		OwnerID:        cr.UserID,
	}

	authorizedCtx := icontext.SetAuthorizer(s.Ctx, cr.Authorizer())

	tsk, err := s.TaskService.CreateTask(authorizedCtx, tc)
	if err != nil {
		t.Fatal(err)
	}
	if !tsk.ID.Valid() {
		t.Fatal("no task ID set")
	}

	scheduledFor := int64(77)
	run, err := s.TaskService.ForceRun(authorizedCtx, tsk.ID, scheduledFor)
	if err != nil {
		t.Fatal(err)
	}

	exp, _ := time.Parse(time.RFC3339, "1970-01-01T00:01:17Z")
	if run.ScheduledFor != exp {
		t.Fatalf("force run returned a different scheduled for time expected: %s, got %s", exp, run.ScheduledFor)
	}

	runs, err := s.TaskControlService.ManualRuns(authorizedCtx, tsk.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 manual run: got %d", len(runs))
	}
	if runs[0].ID != run.ID {
		diff := cmp.Diff(runs[0], run)
		t.Fatalf("manual run missmatch: %s", diff)
	}
}

func testRunStorage(t *testing.T, sys *System) {
	cr := creds(t, sys)

	// Script is set to run every minute. The platform adapter is currently hardcoded to schedule after "now",
	// which makes timing of runs somewhat difficult.
	ct := taskmodel.TaskCreate{
		OrganizationID: cr.OrgID,
		Flux:           fmt.Sprintf(scriptFmt, 0),
		OwnerID:        cr.UserID,
	}
	task, err := sys.TaskService.CreateTask(icontext.SetAuthorizer(sys.Ctx, cr.Authorizer()), ct)
	if err != nil {
		t.Fatal(err)
	}

	// check run filter errors
	_, _, err0 := sys.TaskService.FindRuns(sys.Ctx, taskmodel.RunFilter{Task: task.ID, Limit: -1})
	if err0 != taskmodel.ErrOutOfBoundsLimit {
		t.Fatalf("failed to error with out of bounds run limit: %d", -1)
	}

	_, _, err1 := sys.TaskService.FindRuns(sys.Ctx, taskmodel.RunFilter{Task: task.ID, Limit: taskmodel.TaskMaxPageSize + 1})
	if err1 != taskmodel.ErrOutOfBoundsLimit {
		t.Fatalf("failed to error with out of bounds run limit: %d", taskmodel.TaskMaxPageSize+1)
	}

	requestedAt := time.Now().Add(time.Hour * -1).UTC() // This should guarantee we can make two runs.

	rc0, err := sys.TaskControlService.CreateRun(sys.Ctx, task.ID, requestedAt, requestedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if rc0.TaskID != task.ID {
		t.Fatalf("wrong task ID on created task: got %s, want %s", rc0.TaskID, task.ID)
	}

	startedAt := time.Now().UTC().Add(time.Second * -10)

	// Update the run state to Started; normally the scheduler would do this.
	if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc0.ID, startedAt, taskmodel.RunStarted); err != nil {
		t.Fatal(err)
	}

	rc1, err := sys.TaskControlService.CreateRun(sys.Ctx, task.ID, requestedAt, requestedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if rc1.TaskID != task.ID {
		t.Fatalf("wrong task ID on created task run: got %s, want %s", rc1.TaskID, task.ID)
	}

	// Update the run state to Started; normally the scheduler would do this.
	if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc1.ID, startedAt.Add(time.Second), taskmodel.RunStarted); err != nil {
		t.Fatal(err)
	}

	// Mark the second run finished.
	if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc1.ID, startedAt.Add(time.Second*2), taskmodel.RunFail); err != nil {
		t.Fatal(err)
	}

	if _, err := sys.TaskControlService.FinishRun(sys.Ctx, task.ID, rc1.ID); err != nil {
		t.Fatal(err)
	}

	// Limit 1 should only return the earlier run.
	runs, _, err := sys.TaskService.FindRuns(sys.Ctx, taskmodel.RunFilter{Task: task.ID, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %v", runs)
	}
	if runs[0].ID != rc0.ID {
		t.Fatalf("retrieved wrong run ID; want %s, got %s", rc0.ID, runs[0].ID)
	}
	if runs[0].StartedAt != startedAt {
		t.Fatalf("unexpectedStartedAt; want %s, got %s", startedAt, runs[0].StartedAt)
	}
	if runs[0].Status != taskmodel.RunStarted.String() {
		t.Fatalf("unexpected run status; want %s, got %s", taskmodel.RunStarted.String(), runs[0].Status)
	}

	if !runs[0].FinishedAt.IsZero() {
		t.Fatalf("expected empty FinishedAt, got %q", runs[0].FinishedAt)
	}

	// Create 3rd run and test limiting to 2 runs
	rc2, err := sys.TaskControlService.CreateRun(sys.Ctx, task.ID, requestedAt, requestedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc2.ID, startedAt.Add(time.Second*3), taskmodel.RunStarted); err != nil {
		t.Fatal(err)
	}

	if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc2.ID, startedAt.Add(time.Second*4), taskmodel.RunSuccess); err != nil {
		t.Fatal(err)
	}
	if _, err := sys.TaskControlService.FinishRun(sys.Ctx, task.ID, rc2.ID); err != nil {
		t.Fatal(err)
	}

	runsLimit2, _, err := sys.TaskService.FindRuns(sys.Ctx, taskmodel.RunFilter{Task: task.ID, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(runsLimit2) != 2 {
		t.Fatalf("expected 2 runs, got %v", runsLimit2)
	}
	if runsLimit2[0].ID != rc0.ID {
		t.Fatalf("retrieved wrong run ID; want %s, got %s", rc0.ID, runs[0].ID)
	}

	// Unspecified limit returns all three runs, sorted by most recently scheduled first.
	runs, _, err = sys.TaskService.FindRuns(sys.Ctx, taskmodel.RunFilter{Task: task.ID})

	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 3 {
		t.Fatalf("expected 3 runs, got %v", runs)
	}
	if runs[0].ID != rc0.ID {
		t.Fatalf("retrieved wrong run ID; want %s, got %s", rc0.ID, runs[0].ID)
	}
	if runs[0].StartedAt != startedAt {
		t.Fatalf("unexpectedStartedAt; want %s, got %s", startedAt, runs[0].StartedAt)
	}
	if runs[0].Status != taskmodel.RunStarted.String() {
		t.Fatalf("unexpected run status; want %s, got %s", taskmodel.RunStarted.String(), runs[0].Status)
	}
	// TODO (al): handle empty finishedAt
	// if runs[0].FinishedAt != "" {
	// 	t.Fatalf("expected empty FinishedAt, got %q", runs[0].FinishedAt)
	// }

	if runs[2].ID != rc1.ID {
		t.Fatalf("retrieved wrong run ID; want %s, got %s", rc1.ID, runs[2].ID)
	}

	if exp := startedAt.Add(time.Second); runs[2].StartedAt != exp {
		t.Fatalf("unexpected StartedAt; want %s, got %s", exp, runs[2].StartedAt)
	}
	if runs[2].Status != taskmodel.RunFail.String() {
		t.Fatalf("unexpected run status; want %s, got %s", taskmodel.RunSuccess.String(), runs[2].Status)
	}
	if exp := startedAt.Add(time.Second * 2); runs[2].FinishedAt != exp {
		t.Fatalf("unexpected FinishedAt; want %s, got %s", exp, runs[2].FinishedAt)
	}

	// Look for a run that doesn't exist.
	_, err = sys.TaskService.FindRunByID(sys.Ctx, task.ID, platform.ID(math.MaxUint64))
	if err == nil {
		t.Fatalf("expected %s but got %s instead", taskmodel.ErrRunNotFound, err)
	}

	// look for a taskID that doesn't exist.
	_, err = sys.TaskService.FindRunByID(sys.Ctx, platform.ID(math.MaxUint64), runs[0].ID)
	if err == nil {
		t.Fatalf("expected %s but got %s instead", taskmodel.ErrRunNotFound, err)
	}

	foundRun0, err := sys.TaskService.FindRunByID(sys.Ctx, task.ID, runs[0].ID)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(foundRun0, runs[0]); diff != "" {
		t.Fatalf("difference between listed run and found run: %s", diff)
	}

	foundRun1, err := sys.TaskService.FindRunByID(sys.Ctx, task.ID, runs[1].ID)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(foundRun1, runs[1]); diff != "" {
		t.Fatalf("difference between listed run and found run: %s", diff)
	}
}

func testRetryAcrossStorage(t *testing.T, sys *System) {
	cr := creds(t, sys)

	// Script is set to run every minute.
	ct := taskmodel.TaskCreate{
		OrganizationID: cr.OrgID,
		Flux:           fmt.Sprintf(scriptFmt, 0),
		OwnerID:        cr.UserID,
	}
	task, err := sys.TaskService.CreateTask(icontext.SetAuthorizer(sys.Ctx, cr.Authorizer()), ct)
	if err != nil {
		t.Fatal(err)
	}
	// Non-existent ID should return the right error.
	_, err = sys.TaskService.RetryRun(sys.Ctx, task.ID, platform.ID(math.MaxUint64))
	if !strings.Contains(err.Error(), "run not found") {
		t.Errorf("expected retrying run that doesn't exist to return %v, got %v", taskmodel.ErrRunNotFound, err)
	}

	requestedAt := time.Now().Add(time.Hour * -1).UTC() // This should guarantee we can make a run.

	rc, err := sys.TaskControlService.CreateRun(sys.Ctx, task.ID, requestedAt, requestedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if rc.TaskID != task.ID {
		t.Fatalf("wrong task ID on created task: got %s, want %s", rc.TaskID, task.ID)
	}

	startedAt := time.Now().UTC()

	// Update the run state to Started then Failed; normally the scheduler would do this.
	if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc.ID, startedAt, taskmodel.RunStarted); err != nil {
		t.Fatal(err)
	}
	if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc.ID, startedAt.Add(time.Second), taskmodel.RunFail); err != nil {
		t.Fatal(err)
	}
	if _, err := sys.TaskControlService.FinishRun(sys.Ctx, task.ID, rc.ID); err != nil {
		t.Fatal(err)
	}

	// Now retry the run.
	m, err := sys.TaskService.RetryRun(sys.Ctx, task.ID, rc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if m.TaskID != task.ID {
		t.Fatalf("wrong task ID on retried run: got %s, want %s", m.TaskID, task.ID)
	}
	if m.Status != "scheduled" {
		t.Fatal("expected new retried run to have status of scheduled")
	}

	if m.ScheduledFor != rc.ScheduledFor {
		t.Fatalf("wrong scheduledFor on task: got %s, want %s", m.ScheduledFor, rc.ScheduledFor)
	}

	exp := taskmodel.RequestStillQueuedError{Start: rc.ScheduledFor.Unix(), End: rc.ScheduledFor.Unix()}

	// Retrying a run which has been queued but not started, should be rejected.
	if _, err = sys.TaskService.RetryRun(sys.Ctx, task.ID, rc.ID); err != exp && err.Error() != "run already queued" {
		t.Fatalf("subsequent retry should have been rejected with %v; got %v", exp, err)
	}
}

func testLogsAcrossStorage(t *testing.T, sys *System) {
	cr := creds(t, sys)

	// Script is set to run every minute. The platform adapter is currently hardcoded to schedule after "now",
	// which makes timing of runs somewhat difficult.
	ct := taskmodel.TaskCreate{
		OrganizationID: cr.OrgID,
		Flux:           fmt.Sprintf(scriptFmt, 0),
		OwnerID:        cr.UserID,
	}
	task, err := sys.TaskService.CreateTask(icontext.SetAuthorizer(sys.Ctx, cr.Authorizer()), ct)
	if err != nil {
		t.Fatal(err)
	}

	requestedAt := time.Now().Add(time.Hour * -1).UTC() // This should guarantee we can make two runs.

	rc0, err := sys.TaskControlService.CreateRun(sys.Ctx, task.ID, requestedAt, requestedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if rc0.TaskID != task.ID {
		t.Fatalf("wrong task ID on created task: got %s, want %s", rc0.TaskID, task.ID)
	}

	startedAt := time.Now().UTC()

	// Update the run state to Started; normally the scheduler would do this.
	if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc0.ID, startedAt, taskmodel.RunStarted); err != nil {
		t.Fatal(err)
	}

	rc1, err := sys.TaskControlService.CreateRun(sys.Ctx, task.ID, requestedAt, requestedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if rc1.TaskID != task.ID {
		t.Fatalf("wrong task ID on created task run: got %s, want %s", rc1.TaskID, task.ID)
	}

	// Update the run state to Started; normally the scheduler would do this.
	if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc1.ID, startedAt, taskmodel.RunStarted); err != nil {
		t.Fatal(err)
	}

	// Mark the second run finished.
	if err := sys.TaskControlService.UpdateRunState(sys.Ctx, task.ID, rc1.ID, startedAt.Add(time.Second), taskmodel.RunSuccess); err != nil {
		t.Fatal(err)
	}

	// Create several run logs in both rc0 and rc1
	// We can then finalize rc1 and ensure that both the transactional (currently running logs) can be found with analytical (completed) logs.
	sys.TaskControlService.AddRunLog(sys.Ctx, task.ID, rc0.ID, time.Now(), "0-0")
	sys.TaskControlService.AddRunLog(sys.Ctx, task.ID, rc0.ID, time.Now(), "0-1")
	sys.TaskControlService.AddRunLog(sys.Ctx, task.ID, rc0.ID, time.Now(), "0-2")
	sys.TaskControlService.AddRunLog(sys.Ctx, task.ID, rc1.ID, time.Now(), "1-0")
	sys.TaskControlService.AddRunLog(sys.Ctx, task.ID, rc1.ID, time.Now(), "1-1")
	sys.TaskControlService.AddRunLog(sys.Ctx, task.ID, rc1.ID, time.Now(), "1-2")
	sys.TaskControlService.AddRunLog(sys.Ctx, task.ID, rc1.ID, time.Now(), "1-3")
	if _, err := sys.TaskControlService.FinishRun(sys.Ctx, task.ID, rc1.ID); err != nil {
		t.Fatal(err)
	}

	logs, _, err := sys.TaskService.FindLogs(sys.Ctx, taskmodel.LogFilter{Task: task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 7 {
		for _, log := range logs {
			t.Logf("log: %+v\n", log)
		}
		t.Fatalf("failed to get all logs: expected: 7 got: %d", len(logs))
	}
	smash := func(logs []*taskmodel.Log) string {
		smashed := ""
		for _, log := range logs {
			smashed = smashed + log.Message
		}
		return smashed
	}
	if smash(logs) != "0-00-10-21-01-11-21-3" {
		t.Fatalf("log contents not acceptable, expected: %q, got: %q", "0-00-10-21-01-11-21-3", smash(logs))
	}

	logs, _, err = sys.TaskService.FindLogs(sys.Ctx, taskmodel.LogFilter{Task: task.ID, Run: &rc1.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 4 {
		t.Fatalf("failed to get all logs: expected: 4 got: %d", len(logs))
	}

	if smash(logs) != "1-01-11-21-3" {
		t.Fatalf("log contents not acceptable, expected: %q, got: %q", "1-01-11-21-3", smash(logs))
	}

	logs, _, err = sys.TaskService.FindLogs(sys.Ctx, taskmodel.LogFilter{Task: task.ID, Run: &rc0.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 3 {
		t.Fatalf("failed to get all logs: expected: 3 got: %d", len(logs))
	}

	if smash(logs) != "0-00-10-2" {
		t.Fatalf("log contents not acceptable, expected: %q, got: %q", "0-00-10-2", smash(logs))
	}

}

func creds(t *testing.T, s *System) TestCreds {
	// t.Helper()

	if s.CredsFunc == nil {
		u := &influxdb.User{Name: t.Name() + "-user"}
		if err := s.UserService.CreateUser(s.Ctx, u); err != nil {
			t.Fatal(err)
		}
		o := &influxdb.Organization{Name: t.Name() + "-org"}
		if err := s.OrganizationService.CreateOrganization(s.Ctx, o); err != nil {
			t.Fatal(err)
		}

		if err := s.UserResourceMappingService.CreateUserResourceMapping(s.Ctx, &influxdb.UserResourceMapping{
			ResourceType: influxdb.OrgsResourceType,
			ResourceID:   o.ID,
			UserID:       u.ID,
			UserType:     influxdb.Owner,
		}); err != nil {
			t.Fatal(err)
		}

		authz := influxdb.Authorization{
			OrgID:       o.ID,
			UserID:      u.ID,
			Permissions: influxdb.OperPermissions(),
		}
		if err := s.AuthorizationService.CreateAuthorization(context.Background(), &authz); err != nil {
			t.Fatal(err)
		}
		return TestCreds{
			OrgID:           o.ID,
			Org:             o.Name,
			UserID:          u.ID,
			AuthorizationID: authz.ID,
			Token:           authz.Token,
		}
	}

	c, err := s.CredsFunc(t)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

const (
	scriptFmt = `option task = {
	name: "task #%d",
	cron: "* * * * *",
	offset: 5s,
	concurrency: 100,
}

from(bucket: "b")
	|> to(bucket: "two", orgID: "000000000000000")`

	scriptDifferentName = `option task = {
	name: "task-changed #%d",
	cron: "* * * * *",
	offset: 5s,
	concurrency: 100,
}

from(bucket: "b")
	|> to(bucket: "two", orgID: "000000000000000")`
)

func testTaskType(t *testing.T, sys *System) {
	cr := creds(t, sys)
	authorizedCtx := icontext.SetAuthorizer(sys.Ctx, cr.Authorizer())

	// Create a tasks
	ts := taskmodel.TaskCreate{
		OrganizationID: cr.OrgID,
		Flux:           fmt.Sprintf(scriptFmt, 0),
		OwnerID:        cr.UserID,
	}

	tsk, err := sys.TaskService.CreateTask(authorizedCtx, ts)
	if err != nil {
		t.Fatal(err)
	}
	if !tsk.ID.Valid() {
		t.Fatal("no task ID set")
	}

	tc := taskmodel.TaskCreate{
		Type:           "cows",
		OrganizationID: cr.OrgID,
		Flux:           fmt.Sprintf(scriptFmt, 0),
		OwnerID:        cr.UserID,
	}

	tskCow, err := sys.TaskService.CreateTask(authorizedCtx, tc)
	if err != nil {
		t.Fatal(err)
	}
	if !tskCow.ID.Valid() {
		t.Fatal("no task ID set")
	}

	tp := taskmodel.TaskCreate{
		Type:           "pigs",
		OrganizationID: cr.OrgID,
		Flux:           fmt.Sprintf(scriptFmt, 0),
		OwnerID:        cr.UserID,
	}

	tskPig, err := sys.TaskService.CreateTask(authorizedCtx, tp)
	if err != nil {
		t.Fatal(err)
	}
	if !tskPig.ID.Valid() {
		t.Fatal("no task ID set")
	}

	// get system tasks (or task's with no type)
	tasks, _, err := sys.TaskService.FindTasks(sys.Ctx, taskmodel.TaskFilter{OrganizationID: &cr.OrgID, Type: &taskmodel.TaskSystemType})
	if err != nil {
		t.Fatal(err)
	}

	for _, task := range tasks {
		if task.Type != "" && task.Type != taskmodel.TaskSystemType {
			t.Fatal("received a task with a type when sending no type restriction")
		}
	}

	// get filtered tasks
	tasks, _, err = sys.TaskService.FindTasks(sys.Ctx, taskmodel.TaskFilter{OrganizationID: &cr.OrgID, Type: &tc.Type})
	if err != nil {
		t.Fatal(err)
	}

	if len(tasks) != 1 {
		fmt.Printf("tasks: %+v\n", tasks)
		t.Fatalf("failed to return tasks by type, expected 1, got %d", len(tasks))
	}

	// get all tasks
	tasks, _, err = sys.TaskService.FindTasks(sys.Ctx, taskmodel.TaskFilter{OrganizationID: &cr.OrgID})
	if err != nil {
		t.Fatal(err)
	}

	if len(tasks) != 3 {
		t.Fatalf("failed to return tasks with wildcard, expected 3, got %d", len(tasks))
	}
}
