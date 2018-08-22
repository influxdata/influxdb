package storetest

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/task/backend"
)

type CreateStoreFunc func(*testing.T) backend.Store
type DestroyStoreFunc func(*testing.T, backend.Store)
type TestFunc func(*testing.T, CreateStoreFunc, DestroyStoreFunc)

// NewStoreTest creates a test function for a given store.
func NewStoreTest(name string, cf CreateStoreFunc, df DestroyStoreFunc, funcNames ...string) func(t *testing.T) {
	if len(funcNames) == 0 {
		funcNames = []string{
			"CreateTask",
			"ModifyTask",
			"ListTasks",
			"FindTask",
			"FindMeta",
			"EnableDisableTask",
			"DeleteTask",
			"CreateNextRun",
			"FinishRun",
			"ManuallyRunTimeRange",
		}
	}
	availableFuncs := map[string]TestFunc{
		"CreateTask":           testStoreCreate,
		"ModifyTask":           testStoreModify,
		"ListTasks":            testStoreListTasks,
		"FindTask":             testStoreFindTask,
		"FindMeta":             testStoreFindMeta,
		"EnableDisableTask":    testStoreTaskEnableDisable,
		"DeleteTask":           testStoreDelete,
		"CreateNextRun":        testStoreCreateNextRun,
		"FinishRun":            testStoreFinishRun,
		"ManuallyRunTimeRange": testStoreManuallyRunTimeRange,
		"DeleteOrg":            testStoreDeleteOrg,
		"DeleteUser":           testStoreDeleteUser,
	}

	return func(t *testing.T) {
		fn := func(funcName string, tf TestFunc) {
			t.Run(funcName, func(t *testing.T) {
				tf(t, cf, df)
			})
		}
		t.Run(name, func(t *testing.T) {
			for _, funcName := range funcNames {
				fn(funcName, availableFuncs[funcName])
			}
		})
	}
}

func testStoreCreate(t *testing.T, create CreateStoreFunc, destroy DestroyStoreFunc) {
	const script = `option task = {
		name: "a task",
		cron: "* * * * *",
	}

from(bucket:"test") |> range(start:-1h)`
	const scriptNoName = `option task = {
	cron: "* * * * *",
}

from(bucket:"test") |> range(start:-1h)`
	s := create(t)
	defer destroy(t, s)

	for _, args := range []struct {
		caseName     string
		org, user    platform.ID
		name, script string
		noerr        bool
	}{
		{caseName: "happy path", org: []byte{1}, user: []byte{2}, script: script, noerr: true},
		{caseName: "missing org", org: nil, user: []byte{2}, script: script},
		{caseName: "missing user", org: []byte{1}, user: nil, script: script},
		{caseName: "missing name", org: []byte{1}, user: []byte{2}, script: scriptNoName},
		{caseName: "missing script", org: []byte{1}, user: []byte{2}, script: ""},
		{caseName: "repeated name and org", org: []byte{1}, user: []byte{3}, script: script},
		{caseName: "repeated name and user", org: []byte{3}, user: []byte{2}, script: script},
		{caseName: "repeated name, org, and user", org: []byte{1}, user: []byte{2}, script: script},
	} {
		t.Run(args.caseName, func(t *testing.T) {
			_, err := s.CreateTask(context.Background(), args.org, args.user, args.script, 0)
			if args.noerr && err != nil {
				t.Fatalf("expected err!=nil but got nil instead")
			} else if err == nil && !args.noerr {
				t.Fatalf("expected nil error but got %v", err)
			}
		})
	}
}

func testStoreModify(t *testing.T, create CreateStoreFunc, destroy DestroyStoreFunc) {
	const script = `option task = {
		name: "a task",
		cron: "* * * * *",
	}

from(bucket:"x") |> range(start:-1h)`

	const script2 = `option task = {
		name: "a task2",
		cron: "* * * * *",
	}

from(bucket:"y") |> range(start:-1h)`
	const script3 = `option task = {
	name: "a task3",
	cron: "* * * * *",
}

from(bucket:"y") |> range(start:-1h)`
	const scriptNoName = `option task = {
	cron: "* * * * *",
}

from(bucket:"y") |> range(start:-1h)`

	t.Run("happy path", func(t *testing.T) {
		s := create(t)
		defer destroy(t, s)

		id, err := s.CreateTask(context.Background(), []byte{1}, []byte{2}, script, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.ModifyTask(context.Background(), id, script2); err != nil {
			t.Fatal(err)
		}

		task, err := s.FindTaskByID(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if task.Script != script2 {
			t.Fatalf("Task didnt update: %s", task.Script)
		}
		if task.Name != "a task2" {
			t.Fatalf("Task didn't update name, expected 'a task2' but got '%s' for task %v", task.Name, task)
		}
	})

	for _, args := range []struct {
		caseName string
		id       platform.ID
		script   string
	}{
		{caseName: "missing id", id: nil, script: script},
		{caseName: "not found", id: []byte{7, 1, 2, 3}, script: script},
		{caseName: "missing script", id: []byte{1}, script: ""},
		{caseName: "missing name", id: []byte{1}, script: scriptNoName},
	} {
		t.Run(args.caseName, func(t *testing.T) {
			s := create(t)
			defer destroy(t, s)

			if err := s.ModifyTask(context.Background(), args.id, args.script); err == nil {
				t.Fatal("expected error but did not receive one")
			}
		})
	}
	t.Run("name repetition", func(t *testing.T) {
		s := create(t)
		defer destroy(t, s)
		id1, err := s.CreateTask(context.Background(), []byte{1}, []byte{2}, script, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.CreateTask(context.Background(), []byte{1}, []byte{2}, script2, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.ModifyTask(context.Background(), id1, script2); err != backend.ErrTaskNameTaken {
			t.Fatal("expected ErrTaskNameTaken but did not receive one")
		}
	})
}

func testStoreListTasks(t *testing.T, create CreateStoreFunc, destroy DestroyStoreFunc) {
	const scriptFmt = `option task = {
		name: "testStoreListTasks %d",
		cron: "* * * * *",
	}

from(bucket:"test") |> range(start:-1h)`
	t.Run("happy path", func(t *testing.T) {
		s := create(t)
		defer destroy(t, s)

		orgID := []byte{1}
		userID := []byte{2}

		id, err := s.CreateTask(context.Background(), orgID, userID, fmt.Sprintf(scriptFmt, 0), 0)
		if err != nil {
			t.Fatal(err)
		}

		ts, err := s.ListTasks(context.Background(), backend.TaskSearchParams{Org: orgID})
		if err != nil {
			t.Fatal(err)
		}
		if len(ts) != 1 {
			t.Fatalf("expected 1 result, got %d", len(ts))
		}
		if !bytes.Equal(ts[0].ID, id) {
			t.Fatalf("got task ID %v, exp %v", ts[0].ID, id)
		}

		ts, err = s.ListTasks(context.Background(), backend.TaskSearchParams{User: userID})
		if err != nil {
			t.Fatal(err)
		}
		if len(ts) != 1 {
			t.Fatalf("expected 1 result, got %d", len(ts))
		}
		if !bytes.Equal(ts[0].ID, id) {
			t.Fatalf("got task ID %v, exp %v", ts[0].ID, id)
		}

		ts, err = s.ListTasks(context.Background(), backend.TaskSearchParams{Org: []byte{1, 2, 3}})
		if err != nil {
			t.Fatal(err)
		}
		if len(ts) > 0 {
			t.Fatalf("expected no results for bad org ID, got %d result(s)", len(ts))
		}

		ts, err = s.ListTasks(context.Background(), backend.TaskSearchParams{User: []byte{1, 2, 3}})
		if err != nil {
			t.Fatal(err)
		}
		if len(ts) > 0 {
			t.Fatalf("expected no results for bad user ID, got %d result(s)", len(ts))
		}

		newID, err := s.CreateTask(context.Background(), orgID, userID, fmt.Sprintf(scriptFmt, 1), 0)
		if err != nil {
			t.Fatal(err)
		}

		ts, err = s.ListTasks(context.Background(), backend.TaskSearchParams{After: id})
		if err != nil {
			t.Fatal(err)
		}
		if len(ts) != 1 {
			t.Fatalf("expected 1 result, got %d", len(ts))
		}
		if !bytes.Equal(ts[0].ID, newID) {
			t.Fatalf("got task ID %v, exp %v", ts[0].ID, newID)
		}
	})

	t.Run("multiple, large pages", func(t *testing.T) {
		if os.Getenv("JENKINS_URL") != "" {
			t.Skip("Skipping test that parses a lot of Flux on Jenkins. Unskip when https://github.com/influxdata/platform/issues/484 is fixed.")
		}
		if testing.Short() {
			t.Skip("Skipping test in short mode.")
		}
		s := create(t)
		defer destroy(t, s)

		orgID := []byte{1}
		userID := []byte{2}

		type createdTask struct {
			id           platform.ID
			name, script string
		}

		tasks := make([]createdTask, 150)
		const script = `option task = {
			name: "my_bucket_%d",
			cron: "* * * * *",
		}
	from(bucket:"my_bucket_%d") |> range(start:-1h)`

		for i := range tasks {
			tasks[i].name = fmt.Sprintf("my_bucket_%d", i)
			tasks[i].script = fmt.Sprintf(script, i, i)

			id, err := s.CreateTask(context.Background(), orgID, userID, tasks[i].script, 0)
			if err != nil {
				t.Fatalf("failed to create task %d: %v", i, err)
			}
			tasks[i].id = id
		}

		for _, p := range []backend.TaskSearchParams{
			{Org: orgID, PageSize: 100},
			{User: userID, PageSize: 100},
		} {
			got, err := s.ListTasks(context.Background(), p)
			if err != nil {
				t.Fatalf("failed to list tasks with search param %v: %v", p, err)
			}

			if len(got) != 100 {
				t.Fatalf("expected 100 returned tasks, got %d", len(got))
			}

			for i, g := range got {
				if !bytes.Equal(tasks[i].id, g.ID) {
					t.Fatalf("task ID mismatch at index %d: got %x, expected %x", i, g.ID, tasks[i].id)
				}

				if !bytes.Equal(orgID, g.Org) {
					t.Fatalf("task org mismatch at index %d: got %x, expected %x", i, g.Org, orgID)
				}

				if !bytes.Equal(userID, g.User) {
					t.Fatalf("task user mismatch at index %d: got %x, expected %x", i, g.User, userID)
				}

				if tasks[i].name != g.Name {
					t.Fatalf("task name mismatch at index %d: got %q, expected %q", i, g.Name, tasks[i].name)
				}

				if tasks[i].script != g.Script {
					t.Fatalf("task script mismatch at index %d: got %q, expected %q", i, g.Script, tasks[i].script)
				}
			}
		}
	})

	t.Run("invalid params", func(t *testing.T) {
		s := create(t)
		defer destroy(t, s)

		if _, err := s.ListTasks(context.Background(), backend.TaskSearchParams{PageSize: -1}); err == nil {
			t.Fatal("expected error for negative page size but got nil")
		}

		if _, err := s.ListTasks(context.Background(), backend.TaskSearchParams{PageSize: math.MaxInt32}); err == nil {
			t.Fatal("expected error for huge page size but got nil")
		}

		if _, err := s.ListTasks(context.Background(), backend.TaskSearchParams{Org: []byte{1}, User: []byte{2}}); err == nil {
			t.Fatal("expected error when specifying both org and user, but got nil")
		}
	})
}

func testStoreFindTask(t *testing.T, create CreateStoreFunc, destroy DestroyStoreFunc) {
	const script = `option task = {
		name: "a task",
		cron: "* * * * *",
	}

from(bucket:"test") |> range(start:-1h)`

	t.Run("happy path", func(t *testing.T) {
		s := create(t)
		defer destroy(t, s)

		org := []byte{1}
		user := []byte{2}

		id, err := s.CreateTask(context.Background(), org, user, script, 0)
		if err != nil {
			t.Fatal(err)
		}

		task, err := s.FindTaskByID(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(task.ID, id) {
			t.Fatalf("unexpected ID: got %v, exp %v", task.ID, id)
		}
		if !bytes.Equal(task.Org, org) {
			t.Fatalf("unexpected org: got %v, exp %v", task.Org, org)
		}
		if !bytes.Equal(task.User, user) {
			t.Fatalf("unexpected user: got %v, exp %v", task.User, user)
		}
		if task.Name != "a task" {
			t.Fatalf("unexpected name %q", task.Name)
		}
		if task.Script != script {
			t.Fatalf("unexpected script %q", task.Script)
		}

		badID := append([]byte(nil), id...)
		badID[len(badID)-1]++

		task, err = s.FindTaskByID(context.Background(), badID)
		if err != nil {
			t.Fatalf("expected no error when finding nonexistent ID, got %v", err)
		}
		if task != nil {
			t.Fatalf("expected nil task when finding nonexistent ID, got %#v", task)
		}
	})
}

func testStoreFindMeta(t *testing.T, create CreateStoreFunc, destroy DestroyStoreFunc) {
	const script = `option task = {
		name: "a task",
		cron: "* * * * *",
		concurrency: 3,
		delay: 5s,
	}

from(bucket:"test") |> range(start:-1h)`

	s := create(t)
	defer destroy(t, s)

	org := []byte{1}
	user := []byte{2}

	id, err := s.CreateTask(context.Background(), org, user, script, 6000)
	if err != nil {
		t.Fatal(err)
	}

	meta, err := s.FindTaskMetaByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}

	if meta.MaxConcurrency != 3 {
		t.Fatal("failed to set max concurrency")
	}

	if meta.LatestCompleted != 6000 {
		t.Fatalf("LatestCompleted should have been set to 6000, got %d", meta.LatestCompleted)
	}

	if meta.EffectiveCron != "* * * * *" {
		t.Fatalf("unexpected cron stored in meta: %q", meta.EffectiveCron)
	}

	if time.Duration(meta.Delay)*time.Second != 5*time.Second {
		t.Fatalf("unexpected delay stored in meta: %v", meta.Delay)
	}

	badID := []byte("bad")
	meta, err = s.FindTaskMetaByID(context.Background(), badID)
	if err == nil {
		t.Fatalf("failed to error on bad taskID")
	}
	if meta != nil {
		t.Fatalf("expected nil meta when finding nonexistent ID, got %#v", meta)
	}

	rc, err := s.CreateNextRun(context.Background(), id, 6065)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.CreateNextRun(context.Background(), id, 6125)
	if err != nil {
		t.Fatal(err)
	}

	err = s.FinishRun(context.Background(), id, rc.Created.RunID)
	if err != nil {
		t.Fatal(err)
	}

	meta, err = s.FindTaskMetaByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}

	if len(meta.CurrentlyRunning) != 1 {
		t.Fatal("creating and finishing runs doesn't work")
	}

	if meta.LatestCompleted != 6060 {
		t.Fatalf("expected LatestCompleted to be updated by finished run, but it wasn't; LatestCompleted=%d", meta.LatestCompleted)
	}
}

func testStoreTaskEnableDisable(t *testing.T, create CreateStoreFunc, destroy DestroyStoreFunc) {
	const script = `option task = {
		name: "a task",
		cron: "* * * * *",
	}

	from(bucket:"test") |> range(start:-1h)`

	s := create(t)
	defer destroy(t, s)

	org := []byte{1}
	user := []byte{2}

	id, err := s.CreateTask(context.Background(), org, user, script, 0)
	if err != nil {
		t.Fatal(err)
	}

	meta, err := s.FindTaskMetaByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}

	if meta.Status != string(backend.TaskEnabled) {
		t.Fatal("task status not set to enabled on create")
	}

	if err := s.DisableTask(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	meta, err = s.FindTaskMetaByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}

	if meta.Status != string(backend.TaskDisabled) {
		t.Fatal("task status not set to enabled on create")
	}

	if err := s.EnableTask(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	meta, err = s.FindTaskMetaByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}

	if meta.Status != string(backend.TaskEnabled) {
		t.Fatal("task status not set to enabled on create")
	}
}

func testStoreDelete(t *testing.T, create CreateStoreFunc, destroy DestroyStoreFunc) {
	const script = `option task = {
		name: "a task",
		cron: "* * * * *",
	}

from(bucket:"test") |> range(start:-1h)`

	t.Run("happy path", func(t *testing.T) {
		s := create(t)
		defer destroy(t, s)

		id, err := s.CreateTask(context.Background(), []byte{1}, []byte{2}, script, 0)
		if err != nil {
			t.Fatal(err)
		}

		deleted, err := s.DeleteTask(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !deleted {
			t.Fatal("stored task not deleted")
		}

		// Deleting a nonexistent ID should return false, nil.
		deleted, err = s.DeleteTask(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if deleted {
			t.Fatal("previously deleted task reported as deleted")
		}

		// The deleted task should not be found.
		task, err := s.FindTaskByID(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if task != nil {
			t.Fatalf("expected nil task when finding nonexistent ID, got %#v", task)
		}
	})
}

func testStoreCreateNextRun(t *testing.T, create CreateStoreFunc, destroy DestroyStoreFunc) {
	const script = `option task = {
		name: "a task",
		cron: "* * * * *",
		delay: 5s,
		concurrency: 2,
	}

from(bucket:"test") |> range(start:-1h)`

	s := create(t)
	defer destroy(t, s)

	t.Run("no queue", func(t *testing.T) {
		taskID, err := s.CreateTask(context.Background(), []byte{1}, []byte{2}, script, 30)
		if err != nil {
			t.Fatal(err)
		}

		badID := append([]byte(nil), taskID...)
		badID[len(badID)-1]++
		if _, err := s.CreateNextRun(context.Background(), badID, 999); err == nil {
			t.Fatal("expected error for CreateNextRun with bad ID, got none")
		}

		_, err = s.CreateNextRun(context.Background(), taskID, 64)
		if e, ok := err.(backend.RunNotYetDueError); !ok {
			t.Fatalf("expected RunNotYetDueError, got %v (%T)", err, err)
		} else if e.DueAt != 65 {
			t.Fatalf("expected run due at 65, got %d", e.DueAt)
		}

		rc, err := s.CreateNextRun(context.Background(), taskID, 65)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(rc.Created.TaskID, taskID) {
			t.Fatalf("bad created task ID; exp %x got %x", taskID, rc.Created.TaskID)
		}
		if rc.Created.Now != 60 {
			t.Fatalf("unexpected time for created run: %d", rc.Created.Now)
		}
		if rc.NextDue != 125 {
			t.Fatalf("unexpected next due time: %d", rc.NextDue)
		}

		rc, err = s.CreateNextRun(context.Background(), taskID, 125)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(rc.Created.TaskID, taskID) {
			t.Fatalf("bad created task ID; exp %x got %x", taskID, rc.Created.TaskID)
		}
		if rc.Created.Now != 120 {
			t.Fatalf("unexpected time for created run: %d", rc.Created.Now)
		}
		if rc.NextDue != 185 {
			t.Fatalf("unexpected next due time: %d", rc.NextDue)
		}
	})

	t.Run("with a queue", func(t *testing.T) {
		const script = `option task = {
			name: "a task",
			cron: "* * * * *",
			delay: 5s,
			concurrency: 9,
		}

	from(bucket:"test") |> range(start:-1h)`
		taskID, err := s.CreateTask(context.Background(), []byte{5}, []byte{6}, script, 2999)
		if err != nil {
			t.Fatal(err)
		}

		// Task is set to every minute. Should schedule once on 0 and once on 60.
		if err := s.ManuallyRunTimeRange(context.Background(), taskID, 0, 60, 3000); err != nil {
			t.Fatal(err)
		}

		// Should schedule once exactly on 180.
		if err := s.ManuallyRunTimeRange(context.Background(), taskID, 180, 180, 3001); err != nil {
			t.Fatal(err)
		}

		rc, err := s.CreateNextRun(context.Background(), taskID, 3005)
		if err != nil {
			t.Fatal(err)
		}

		if rc.Created.Now != 3000 {
			t.Fatalf("expected run to be created with time 3000, got %d", rc.Created.Now)
		}
		if rc.NextDue != 3065 {
			t.Fatalf("expected next due run to be 3065, got %d", rc.NextDue)
		}
		if !rc.HasQueue {
			t.Fatal("expected run to have queue but it didn't")
		}

		// Queue: 0
		rc, err = s.CreateNextRun(context.Background(), taskID, 3005)
		if err != nil {
			t.Fatal(err)
		}
		if rc.Created.Now != 0 {
			t.Fatalf("expected run to be scheduled for 0, got %d", rc.Created.Now)
		}
		if rc.NextDue != 3065 {
			// NextDue doesn't change with queue.
			t.Fatalf("expected next due run to be 3065, got %d", rc.NextDue)
		}
		if !rc.HasQueue {
			t.Fatal("expected run to have queue but it didn't")
		}

		// Queue: 60
		rc, err = s.CreateNextRun(context.Background(), taskID, 3005)
		if err != nil {
			t.Fatal(err)
		}
		if rc.Created.Now != 60 {
			t.Fatalf("expected run to be scheduled for 0, got %d", rc.Created.Now)
		}
		if rc.NextDue != 3065 {
			// NextDue doesn't change with queue.
			t.Fatalf("expected next due run to be 3065, got %d", rc.NextDue)
		}
		if !rc.HasQueue {
			t.Fatal("expected run to have queue but it didn't")
		}

		// Queue: 180
		rc, err = s.CreateNextRun(context.Background(), taskID, 3005)
		if err != nil {
			t.Fatal(err)
		}
		if rc.Created.Now != 180 {
			t.Fatalf("expected run to be scheduled for 0, got %d", rc.Created.Now)
		}
		if rc.NextDue != 3065 {
			// NextDue doesn't change with queue.
			t.Fatalf("expected next due run to be 3065, got %d", rc.NextDue)
		}
		if rc.HasQueue {
			t.Fatal("expected run to have empty queue but it didn't")
		}
	})
}

func testStoreFinishRun(t *testing.T, create CreateStoreFunc, destroy DestroyStoreFunc) {
	const script = `option task = {
		name: "a task",
		cron: "* * * * *",
	}

from(bucket:"test") |> range(start:-1h)`
	s := create(t)
	defer destroy(t, s)

	task, err := s.CreateTask(context.Background(), []byte{1}, []byte{2}, script, 0)
	if err != nil {
		t.Fatal(err)
	}

	rc, err := s.CreateNextRun(context.Background(), task, 60)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.FinishRun(context.Background(), task, rc.Created.RunID); err != nil {
		t.Fatal(err)
	}

	if err := s.FinishRun(context.Background(), task, rc.Created.RunID); err == nil {
		t.Fatal("expected failure when removing run that doesnt exist")
	}
}

func testStoreManuallyRunTimeRange(t *testing.T, create CreateStoreFunc, destroy DestroyStoreFunc) {
	const script = `option task = {
		name: "a task",
		cron: "* * * * *",
	}

from(bucket:"test") |> range(start:-1h)`
	s := create(t)
	defer destroy(t, s)

	taskID, err := s.CreateTask(context.Background(), []byte{1}, []byte{2}, script, 0)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.ManuallyRunTimeRange(context.Background(), taskID, 1, 10, 0); err != nil {
		t.Fatal(err)
	}

	meta, err := s.FindTaskMetaByID(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.ManualRuns) != 1 {
		t.Fatalf("expected 1 manual run to be created, got %d", len(meta.ManualRuns))
	}

	rc, err := s.CreateNextRun(context.Background(), taskID, 9999)
	if err != nil {
		t.Fatal(err)
	}

	if !rc.HasQueue {
		t.Fatal("CreateNextRun should have reported that there is a queue")
	}
}

func testStoreDeleteUser(t *testing.T, create CreateStoreFunc, destroy DestroyStoreFunc) {
	s := create(t)
	defer destroy(t, s)
	ids := createABunchOFTasks(t, s,
		func(u, _ uint64) bool {
			return u == 0
		},
	)
	user := make(platform.ID, 8)
	err := s.DeleteUser(context.Background(), user)
	if err != nil {
		t.Fatal(err)
	}
	for i := range ids {
		task, err := s.FindTaskByID(context.Background(), ids[i])
		if err != nil {
			t.Fatal(err)
		}
		if task != nil {
			t.Fatal("expected task to be deleted but it was not")
		}
	}
}

func testStoreDeleteOrg(t *testing.T, create CreateStoreFunc, destroy DestroyStoreFunc) {
	s := create(t)
	defer destroy(t, s)
	ids := createABunchOFTasks(t, s,
		func(_, o uint64) bool {
			return o == 0
		},
	)
	org := make(platform.ID, 8)
	err := s.DeleteOrg(context.Background(), org)
	if err != nil {
		t.Fatal(err)
	}
	for i := range ids {
		task, err := s.FindTaskByID(context.Background(), ids[i])
		if err != nil {
			t.Fatal(err)
		}
		if task != nil {
			t.Fatal("expected task to be deleted but it was not")
		}
	}
}

func createABunchOFTasks(t *testing.T, s backend.Store, filter func(user, org uint64) bool) []platform.ID {
	const script = `option task = {
		name: "a task",
		cron: "* * * * *",
	}

from(bucket:"test") |> range(start:-1h)`
	var id platform.ID
	var ids []platform.ID
	var err error
	for i := 0; i < 15; i++ {
		for userInt := uint64(0); userInt < 4; userInt++ {
			for orgInt := uint64(0); orgInt < 3; orgInt++ {
				org := make(platform.ID, 8)
				user := make(platform.ID, 8)
				binary.BigEndian.PutUint64(org, orgInt)
				binary.BigEndian.PutUint64(user, userInt)
				if id, err = s.CreateTask(context.Background(), org, user, script, 0); err != nil {
					t.Fatal(err)
				}
				if filter(userInt, orgInt) {
					ids = append(ids, id)
				}
			}
		}
	}
	return ids
}
