package storetest

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb"
	pcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/task/backend"
	platformtesting "github.com/influxdata/influxdb/testing"
)

// MakeNewAuthorizationFunc is a function that creates a new authorization associated with a valid org and user.
// The permissions on the authorization should be allowed to do everything (see influxdb.OperPermissions).
type MakeNewAuthorizationFunc func(context.Context, *testing.T) *platform.Authorization

// CreateRunStoreFunc returns a new LogWriter and LogReader.
// If the writer and reader are associated with a backend that validates authorizations,
// it must return a valid MakeNewAuthorizationFunc; otherwise the returned MakeNewAuthorizationFunc may be nil,
// in which case the tests will use authorizations associated with a random org and user ID.
type CreateRunStoreFunc func(*testing.T) (backend.LogWriter, backend.LogReader, MakeNewAuthorizationFunc)
type DestroyRunStoreFunc func(*testing.T, backend.LogWriter, backend.LogReader)

func NewRunStoreTest(name string, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			t.Run("UpdateRunState", func(t *testing.T) {
				t.Parallel()
				updateRunState(t, crf, drf)
			})
			t.Run("RunLog", func(t *testing.T) {
				t.Parallel()
				runLogTest(t, crf, drf)
			})
			t.Run("ListRuns", func(t *testing.T) {
				if testing.Short() {
					t.Skip("Skipping test in short mode.")
				}
				t.Parallel()
				listRunsTest(t, crf, drf)
			})
			t.Run("FindRunByID", func(t *testing.T) {
				t.Parallel()
				findRunByIDTest(t, crf, drf)
			})
			t.Run("ListLogs", func(t *testing.T) {
				t.Parallel()
				listLogsTest(t, crf, drf)
			})
		})
	}
}

func updateRunState(t *testing.T, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) {
	writer, reader, makeAuthz := crf(t)
	defer drf(t, writer, reader)

	now := time.Now().UTC()

	task := &backend.StoreTask{
		ID:  platformtesting.MustIDBase16("ab01ab01ab01ab01"),
		Org: platformtesting.MustIDBase16("ab01ab01ab01ab05"),
	}
	scheduledFor := now.Add(-3 * time.Second)
	run := platform.Run{
		ID:           platformtesting.MustIDBase16("2c20766972747573"),
		TaskID:       task.ID,
		Status:       "started",
		ScheduledFor: scheduledFor.Format(time.RFC3339),
	}
	rlb := backend.RunLogBase{
		Task:            task,
		RunID:           run.ID,
		RunScheduledFor: scheduledFor.Unix(),
	}

	ctx := context.Background()
	ctx = pcontext.SetAuthorizer(ctx, makeNewAuthorization(ctx, t, makeAuthz))

	startAt := now.Add(-2 * time.Second)
	if err := writer.UpdateRunState(ctx, rlb, startAt, backend.RunStarted); err != nil {
		t.Fatal(err)
	}
	run.StartedAt = startAt.Format(time.RFC3339Nano)
	run.Status = "started"

	returnedRun, err := reader.FindRunByID(ctx, task.Org, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(run, *returnedRun); diff != "" {
		t.Fatalf("unexpected run found: -want/+got: %s", diff)
	}

	endAt := now.Add(-1 * time.Second)
	if err := writer.UpdateRunState(ctx, rlb, endAt, backend.RunSuccess); err != nil {
		t.Fatal(err)
	}
	run.FinishedAt = endAt.Format(time.RFC3339Nano)
	run.Status = "success"

	returnedRun, err = reader.FindRunByID(ctx, task.Org, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(run, *returnedRun); diff != "" {
		t.Fatalf("unexpected run found: -want/+got: %s", diff)
	}
}

func runLogTest(t *testing.T, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) {
	writer, reader, makeAuthz := crf(t)
	defer drf(t, writer, reader)

	task := &backend.StoreTask{
		ID:  platformtesting.MustIDBase16("ab01ab01ab01ab01"),
		Org: platformtesting.MustIDBase16("ab01ab01ab01ab05"),
	}

	sf := time.Now().UTC()
	sa := sf.Add(-10 * time.Second)
	run := platform.Run{
		ID:           platformtesting.MustIDBase16("2c20766972747573"),
		TaskID:       task.ID,
		Status:       "started",
		ScheduledFor: sf.Format(time.RFC3339),
		StartedAt:    sa.Format(time.RFC3339Nano),
	}
	rlb := backend.RunLogBase{
		Task:            task,
		RunID:           run.ID,
		RunScheduledFor: sf.Unix(),
	}

	ctx := context.Background()
	ctx = pcontext.SetAuthorizer(ctx, makeNewAuthorization(ctx, t, makeAuthz))

	if err := writer.UpdateRunState(ctx, rlb, sa, backend.RunStarted); err != nil {
		t.Fatal(err)
	}

	if err := writer.AddRunLog(ctx, rlb, sa.Add(time.Second), "first"); err != nil {
		t.Fatal(err)
	}
	if err := writer.AddRunLog(ctx, rlb, sa.Add(2*time.Second), "second"); err != nil {
		t.Fatal(err)
	}
	if err := writer.AddRunLog(ctx, rlb, sa.Add(3*time.Second), "third"); err != nil {
		t.Fatal(err)
	}

	run.Log = []platform.Log{
		platform.Log{Time: sa.Add(time.Second).Format(time.RFC3339Nano), Message: "first"},
		platform.Log{Time: sa.Add(2 * time.Second).Format(time.RFC3339Nano), Message: "second"},
		platform.Log{Time: sa.Add(3 * time.Second).Format(time.RFC3339Nano), Message: "third"},
	}
	returnedRun, err := reader.FindRunByID(ctx, task.Org, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(run, *returnedRun); diff != "" {
		t.Fatalf("unexpected run found: -want/+got: %s", diff)
	}
}

func listRunsTest(t *testing.T, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) {
	writer, reader, makeAuthz := crf(t)
	defer drf(t, writer, reader)

	task := &backend.StoreTask{
		ID:  platformtesting.MustIDBase16("ab01ab01ab01ab01"),
		Org: platformtesting.MustIDBase16("ab01ab01ab01ab05"),
	}

	ctx := context.Background()
	ctx = pcontext.SetAuthorizer(ctx, makeNewAuthorization(ctx, t, makeAuthz))

	{
		r, err := reader.ListRuns(ctx, task.ID, platform.RunFilter{Task: task.ID})
		if err != nil {
			// TODO(lh): We may get an error here in the future when the system is more aggressive at checking orgID's
			t.Fatalf("got an error with bad orgID when we should have returned a empty list: %v", err)
		}
		if len(r) != 0 {
			t.Fatalf("expected 0 runs, got: %d", len(r))
		}
	}
	{
		r, err := reader.ListRuns(ctx, task.Org, platform.RunFilter{Task: task.Org})
		if err != nil {
			t.Fatalf("got an error with bad taskID when we should have returned a empty list: %v", err)
		}
		if len(r) != 0 {
			t.Fatalf("expected 0 runs, got: %d", len(r))
		}
	}

	now := time.Now().UTC()
	const nRuns = 150
	runs := make([]platform.Run, nRuns)
	for i := 0; i < len(runs); i++ {
		// Scheduled for times ascending with IDs.
		scheduledFor := now.Add(time.Duration(-2*(nRuns-i)) * time.Second)
		id := platform.ID(i + 1)
		runs[i] = platform.Run{
			ID:           id,
			Status:       "started",
			ScheduledFor: scheduledFor.Format(time.RFC3339),
		}
		rlb := backend.RunLogBase{
			Task:            task,
			RunID:           runs[i].ID,
			RunScheduledFor: scheduledFor.Unix(),
		}

		err := writer.UpdateRunState(ctx, rlb, scheduledFor.Add(time.Second), backend.RunStarted)
		if err != nil {
			t.Fatal(err)
		}
	}

	if _, err := reader.ListRuns(ctx, task.Org, platform.RunFilter{}); err == nil {
		t.Fatal("failed to error with invalid task ID")
	}
	{
		r, err := reader.ListRuns(ctx, 9999999, platform.RunFilter{Task: task.ID})
		if err != nil {
			t.Fatalf("got an error with a bad orgID when we should have returned a empty list: %v", err)
		}
		if len(r) != 0 {
			t.Fatalf("expected 0 runs, got: %d", len(r))
		}
	}

	listRuns, err := reader.ListRuns(ctx, task.Org, platform.RunFilter{
		Task:  task.ID,
		Limit: 2 * nRuns,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != len(runs) {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), len(runs))
	}

	const afterIDIdx = 20
	listRuns, err = reader.ListRuns(ctx, task.Org, platform.RunFilter{
		Task:  task.ID,
		After: &runs[afterIDIdx].ID,
		Limit: 2 * nRuns,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != len(runs)-(afterIDIdx+1) {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), len(runs)-(afterIDIdx+1))
	}

	listRuns, err = reader.ListRuns(ctx, task.Org, platform.RunFilter{
		Task:  task.ID,
		Limit: 30,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != 30 {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), 30)
	}

	const afterTimeIdx = 34
	scheduledFor, _ := time.Parse(time.RFC3339, runs[afterTimeIdx].ScheduledFor)
	listRuns, err = reader.ListRuns(ctx, task.Org, platform.RunFilter{
		Task:      task.ID,
		AfterTime: scheduledFor.Format(time.RFC3339),
		Limit:     2 * nRuns,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != len(runs)-(afterTimeIdx+1) {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), len(runs)-(afterTimeIdx+1))
	}

	const beforeTimeIdx = 34
	scheduledFor, _ = time.Parse(time.RFC3339, runs[beforeTimeIdx].ScheduledFor)
	listRuns, err = reader.ListRuns(ctx, task.Org, platform.RunFilter{
		Task:       task.ID,
		BeforeTime: scheduledFor.Add(time.Millisecond).Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != beforeTimeIdx {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), beforeTimeIdx)
	}

	// add a run and now list again but this time with a requested at
	scheduledFor = now.Add(time.Duration(-2*(nRuns-len(runs))) * time.Second)
	run := platform.Run{
		ID:           platform.ID(len(runs) + 1),
		Status:       "started",
		ScheduledFor: scheduledFor.Format(time.RFC3339),
		RequestedAt:  scheduledFor.Format(time.RFC3339),
	}
	runs = append(runs, run)
	rlb := backend.RunLogBase{
		Task:            task,
		RunID:           run.ID,
		RunScheduledFor: scheduledFor.Unix(),
		RequestedAt:     scheduledFor.Unix(),
	}

	if err := writer.UpdateRunState(ctx, rlb, scheduledFor.Add(time.Second), backend.RunStarted); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	listRuns, err = reader.ListRuns(ctx, task.Org, platform.RunFilter{
		Task:  task.ID,
		Limit: 2 * nRuns,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listRuns) != len(runs) {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), len(runs))
	}
}

func findRunByIDTest(t *testing.T, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) {
	writer, reader, makeAuthz := crf(t)
	defer drf(t, writer, reader)

	if _, err := reader.FindRunByID(context.Background(), platform.InvalidID(), platform.InvalidID()); err == nil {
		t.Fatal("failed to error with bad id")
	}

	task := &backend.StoreTask{
		ID:  platformtesting.MustIDBase16("ab01ab01ab01ab01"),
		Org: platformtesting.MustIDBase16("ab01ab01ab01ab05"),
	}
	sf := time.Now().UTC().Add(-10 * time.Second)
	sa := sf.Add(time.Second)

	run := platform.Run{
		ID:           platformtesting.MustIDBase16("2c20766972747573"),
		TaskID:       task.ID,
		Status:       "started",
		ScheduledFor: sf.Format(time.RFC3339),
		StartedAt:    sa.Format(time.RFC3339Nano),
	}
	rlb := backend.RunLogBase{
		Task:            task,
		RunID:           run.ID,
		RunScheduledFor: sf.Unix(),
	}

	ctx := context.Background()
	ctx = pcontext.SetAuthorizer(ctx, makeNewAuthorization(ctx, t, makeAuthz))
	if err := writer.UpdateRunState(ctx, rlb, sa, backend.RunStarted); err != nil {
		t.Fatal(err)
	}

	returnedRun, err := reader.FindRunByID(ctx, task.Org, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(run, *returnedRun) {
		t.Fatalf("expected:\n%#v, got: \n%#v", run, *returnedRun)
	}

	returnedRun.Log = []platform.Log{platform.Log{Message: "cows"}}

	rr2, err := reader.FindRunByID(ctx, task.Org, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if reflect.DeepEqual(returnedRun, rr2) {
		t.Fatalf("updateing returned run modified RunStore data")
	}

	_, err = reader.FindRunByID(ctx, task.Org, 0xccc)
	if err != backend.ErrRunNotFound {
		t.Fatalf("expected finding run with invalid ID to return %v, got %v", backend.ErrRunNotFound, err)
	}
}

func listLogsTest(t *testing.T, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) {
	writer, reader, makeAuthz := crf(t)
	defer drf(t, writer, reader)

	task := &backend.StoreTask{
		ID:  platformtesting.MustIDBase16("ab01ab01ab01ab01"),
		Org: platformtesting.MustIDBase16("ab01ab01ab01ab05"),
	}

	ctx := context.Background()
	ctx = pcontext.SetAuthorizer(ctx, makeNewAuthorization(ctx, t, makeAuthz))

	if _, err := reader.ListLogs(ctx, task.Org, platform.LogFilter{}); err == nil {
		t.Fatalf("expected error when task ID missing, but got nil")
	}
	r, err := reader.ListLogs(ctx, 9999999, platform.LogFilter{Task: task.ID})
	if err != nil {
		t.Fatalf("with bad org ID, expected no error: %v", err)
	}
	if len(r) != 0 {
		t.Fatalf("with bad org id expected no runs, got: %d", len(r))
	}

	now := time.Now().UTC()
	const nRuns = 20
	runs := make([]platform.Run, nRuns)
	for i := 0; i < len(runs); i++ {
		sf := now.Add(time.Duration(i-nRuns) * time.Second)
		id := platform.ID(i + 1)
		runs[i] = platform.Run{
			ID:           id,
			Status:       "started",
			ScheduledFor: sf.UTC().Format(time.RFC3339),
		}
		rlb := backend.RunLogBase{
			Task:            task,
			RunID:           runs[i].ID,
			RunScheduledFor: sf.Unix(),
		}

		err := writer.UpdateRunState(ctx, rlb, sf.Add(time.Millisecond), backend.RunStarted)
		if err != nil {
			t.Fatal(err)
		}

		writer.AddRunLog(ctx, rlb, sf.Add(2*time.Millisecond), fmt.Sprintf("log%d", i))
	}

	const targetRun = 4
	logs, err := reader.ListLogs(ctx, task.Org, platform.LogFilter{Task: task.ID, Run: &runs[targetRun].ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}

	fmtTimelog := now.Add(time.Duration(targetRun-nRuns)*time.Second + 2*time.Millisecond).Format(time.RFC3339Nano)
	if logs[0].Time != fmtTimelog {
		t.Fatalf("expected: %q, got: %q", fmtTimelog, logs[0].Time)
	}
	if "log4" != logs[0].Message {
		t.Fatalf("expected: %q, got: %q", "log4", logs[0].Message)
	}

	logs, err = reader.ListLogs(ctx, task.Org, platform.LogFilter{Task: task.ID})
	if err != nil {
		t.Fatal(err)
	}

	if len(logs) != len(runs) {
		t.Fatal("not all logs retrieved")
	}
}

func makeNewAuthorization(ctx context.Context, t *testing.T, makeAuthz MakeNewAuthorizationFunc) *platform.Authorization {
	if makeAuthz != nil {
		return makeAuthz(ctx, t)
	}

	return &platform.Authorization{
		ID:          platformtesting.MustIDBase16("ab01ab01ab01ab01"),
		UserID:      platformtesting.MustIDBase16("ab01ab01ab01ab01"),
		OrgID:       platformtesting.MustIDBase16("ab01ab01ab01ab05"),
		Permissions: platform.OperPermissions(),
	}
}
