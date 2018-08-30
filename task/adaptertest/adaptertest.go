// Package adaptertest provides tests to ensure that implementations of
// platform/task/backend.Store and platform/task/backend.LogReader meet the requirements of platform.TaskService.
//
// Consumers of this package must import query/builtin.
// This package does not import it directly, to avoid requiring it too early.
package adaptertest

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/snowflake"
	"github.com/influxdata/platform/task"
	"github.com/influxdata/platform/task/backend"
	"github.com/influxdata/platform/task/options"
)

func init() {
	// TODO(mr): remove as part of https://github.com/influxdata/platform/issues/484.
	options.EnableScriptCacheForTest()
}

// BackendComponentFactory is supplied by consumers of the adaptertestpackage,
// to provide the values required to constitute a PlatformAdapter.
// The provided context.CancelFunc is called after the test,
// and it is the implementer's responsibility to clean up after that is called.
//
// If creating the System fails, the implementer should call t.Fatal.
type BackendComponentFactory func(t *testing.T) (*System, context.CancelFunc)

// TestTaskService should be called by consumers of the adaptertest package.
// This will call fn once to create a single platform.TaskService
// used across all subtests in TestTaskService.
func TestTaskService(t *testing.T, fn BackendComponentFactory) {
	sys, cancel := fn(t)
	defer cancel()
	sys.ts = task.PlatformAdapter(sys.S, sys.LR)

	t.Run("TaskService", func(t *testing.T) {
		// We're running the subtests in parallel, but if we don't use this wrapper,
		// the defer cancel() call above would return before the parallel subtests completed.
		//
		// Running the subtests in parallel might make them slightly faster,
		// but more importantly, it should exercise concurrency to catch data races.

		t.Run("Task CRUD", func(t *testing.T) {
			t.Parallel()
			testTaskCRUD(t, sys)
		})

		t.Run("Task Runs", func(t *testing.T) {
			t.Parallel()
			testTaskRuns(t, sys)
		})

		t.Run("Task Concurrency", func(t *testing.T) {
			t.Parallel()
			testTaskConcurrency(t, sys)
		})
	})
}

// System, as in "system under test", encapsulates the required parts of a platform.TaskAdapter
// (the underlying Store, LogReader, and LogWriter) for low-level operations.
type System struct {
	S  backend.Store
	LR backend.LogReader
	LW backend.LogWriter

	// Set this context, to be used in tests, so that any spawned goroutines watching Ctx.Done()
	// will clean up after themselves.
	Ctx context.Context

	ts platform.TaskService
}

func testTaskCRUD(t *testing.T, sys *System) {
	orgID := idGen.ID()
	userID := idGen.ID()

	// Create a task.
	task := &platform.Task{Organization: orgID, Owner: platform.User{ID: userID}, Flux: fmt.Sprintf(scriptFmt, 0)}
	if err := sys.ts.CreateTask(sys.Ctx, task); err != nil {
		t.Fatal(err)
	}
	if task.ID == nil {
		t.Fatal("no task ID set")
	}

	// Look up a task the different ways we can.
	// Map of method name to found task.
	found := make(map[string]*platform.Task)

	// Find by ID should return the right task.
	f, err := sys.ts.FindTaskByID(sys.Ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	found["FindTaskByID"] = f

	fs, _, err := sys.ts.FindTasks(sys.Ctx, platform.TaskFilter{Organization: &orgID})
	if err != nil {
		t.Fatal(err)
	}
	if len(fs) != 1 {
		t.Fatalf("expected 1 task returned, got %d: %#v", len(fs), fs)
	}
	found["FindTasks with Organization filter"] = fs[0]

	fs, _, err = sys.ts.FindTasks(sys.Ctx, platform.TaskFilter{User: &userID})
	if err != nil {
		t.Fatal(err)
	}
	if len(fs) != 1 {
		t.Fatalf("expected 1 task returned, got %d: %#v", len(fs), fs)
	}
	found["FindTasks with User filter"] = fs[0]

	for fn, f := range found {
		if !bytes.Equal(f.Organization, orgID) {
			t.Fatalf("%s: wrong organization returned; want %s, got %s", fn, orgID.String(), f.Organization.String())
		}
		if !bytes.Equal(f.Owner.ID, userID) {
			t.Fatalf("%s: wrong user returned; want %s, got %s", fn, userID.String(), f.Owner.ID.String())
		}

		if f.Name != "task #0" {
			t.Fatalf(`%s: wrong name returned; want "task #0", got %q`, fn, f.Name)
		}
		if f.Cron != "* * * * *" {
			t.Fatalf(`%s: wrong cron returned; want "* * * * *", got %q`, fn, f.Cron)
		}
		if f.Every != "" {
			t.Fatalf(`%s: wrong every returned; want "", got %q`, fn, f.Every)
		}
	}

	// Update task.
	newFlux := fmt.Sprintf(scriptFmt, 99)
	origID := append(platform.ID(nil), f.ID...)
	f, err = sys.ts.UpdateTask(sys.Ctx, origID, platform.TaskUpdate{Flux: &newFlux})
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(origID, f.ID) {
		t.Fatalf("task ID unexpectedly changed during update, from %s to %s", origID.String(), f.ID.String())
	}
	if f.Flux != newFlux {
		t.Fatalf("wrong flux from update; want %q, got %q", newFlux, f.Flux)
	}

	// Delete task.
	if err := sys.ts.DeleteTask(sys.Ctx, origID); err != nil {
		t.Fatal(err)
	}

	// Task should not be returned.
	if _, err := sys.ts.FindTaskByID(sys.Ctx, origID); err != nil {
		t.Fatal(err)
	}
}

func testTaskRuns(t *testing.T, sys *System) {
	orgID := idGen.ID()
	userID := idGen.ID()

	task := &platform.Task{Organization: orgID, Owner: platform.User{ID: userID}, Flux: fmt.Sprintf(scriptFmt, 0)}
	if err := sys.ts.CreateTask(sys.Ctx, task); err != nil {
		t.Fatal(err)
	}

	const requestedAtUnix = 1000
	if err := sys.S.ManuallyRunTimeRange(sys.Ctx, task.ID, 60, 300, requestedAtUnix); err != nil {
		t.Fatal(err)
	}

	// Create a run.
	rc, err := sys.S.CreateNextRun(sys.Ctx, task.ID, requestedAtUnix+1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rc.Created.TaskID, task.ID) {
		t.Fatalf("unexpected created run: got %s, want %s", rc.Created.TaskID.String(), task.ID.String())
	}
	runID := rc.Created.RunID

	// Set the run state to started.
	st, err := sys.S.FindTaskByID(sys.Ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Now()
	if err := sys.LW.UpdateRunState(sys.Ctx, st, runID, startedAt, backend.RunStarted); err != nil {
		t.Fatal(err)
	}

	// Find runs, to see the started run.
	runs, n, err := sys.ts.FindRuns(sys.Ctx, platform.RunFilter{Org: &orgID, Task: &task.ID})
	if err != nil {
		t.Fatal(err)
	}
	if n != len(runs) {
		t.Fatalf("expected n=%d, got %d", len(runs), n)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 run returned, got %d", len(runs))
	}

	r := runs[0]
	if !bytes.Equal(r.ID, runID) {
		t.Fatalf("expected to find run with ID %s, got %s", runID.String(), r.ID.String())
	}
	if !bytes.Equal(r.TaskID, task.ID) {
		t.Fatalf("expected run to have task ID %s, got %s", task.ID.String(), r.TaskID.String())
	}
	if want := startedAt.UTC().Format(time.RFC3339); r.StartedAt != want {
		t.Fatalf("expected run to be started at %q, got %q", want, r.StartedAt)
	}
	if want := time.Unix(rc.Created.Now, 0).UTC().Format(time.RFC3339); r.ScheduledFor != want {
		// Not yet expected to match. Change to t.Fatalf as part of addressing https://github.com/influxdata/platform/issues/753.
		t.Logf("TODO(#753): expected run to be scheduled for %q, got %q", want, r.ScheduledFor)
	}
	if want := time.Unix(requestedAtUnix, 0).UTC().Format(time.RFC3339); r.RequestedAt != want {
		// Not yet expected to match. Change to t.Fatalf as part of addressing https://github.com/influxdata/platform/issues/753.
		t.Logf("TODO(#753): expected run to be requested at %q, got %q", want, r.RequestedAt)
	}
	if r.FinishedAt != "" {
		t.Fatalf("expected run not be finished, got %q", r.FinishedAt)
	}
}

func testTaskConcurrency(t *testing.T, sys *System) {
	orgID := idGen.ID()
	userID := idGen.ID()

	const numTasks = 300
	taskCh := make(chan *platform.Task, numTasks)

	var createWg sync.WaitGroup
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		createWg.Add(1)
		go func() {
			defer createWg.Done()
			for task := range taskCh {
				if err := sys.ts.CreateTask(sys.Ctx, task); err != nil {
					t.Errorf("error creating task: %v", err)
				}
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
			tasks, _, err := sys.ts.FindTasks(sys.Ctx, platform.TaskFilter{Organization: &orgID})
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

			// Delete the first task we found.
			if err := sys.ts.DeleteTask(sys.Ctx, tasks[0].ID); err != nil {
				t.Errorf("error deleting task: %v", err)
				return
			}
			deleted++

			// Wait just a tiny bit.
			time.Sleep(time.Millisecond)
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
			tasks, _, err := sys.ts.FindTasks(sys.Ctx, platform.TaskFilter{Organization: &orgID})
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
			tid := tasks[len(tasks)-1].ID
			if _, err := sys.S.CreateNextRun(sys.Ctx, tid, math.MaxInt64); err != nil {
				// This may have errored due to the task being deleted. Check if the task still exists.
				if t, err2 := sys.S.FindTaskByID(sys.Ctx, tid); err2 == nil && t == nil {
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
		taskCh <- &platform.Task{
			Organization: orgID,
			Owner:        platform.User{ID: userID},
			Flux:         fmt.Sprintf(scriptFmt, i),
		}
	}

	// Done adding. Wait for cleanup.
	close(taskCh)
	createWg.Wait()
	extraWg.Wait()
}

const scriptFmt = `option task = {
	name: "task #%d",
	cron: "* * * * *",
	concurrency: 100,
}
from(bucket:"b") |> toHTTP(url:"http://example.com")`

var idGen = snowflake.NewIDGenerator()
