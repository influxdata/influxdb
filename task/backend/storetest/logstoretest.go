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

type CreateRunStoreFunc func(*testing.T) (backend.LogWriter, backend.LogReader)
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
	writer, reader := crf(t)
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

	ctx := pcontext.SetAuthorizer(context.Background(), makeNewAuthorization())

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
	writer, reader := crf(t)
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

	ctx := pcontext.SetAuthorizer(context.Background(), makeNewAuthorization())

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

	run.Log = platform.Log(fmt.Sprintf(
		"%s: first\n%s: second\n%s: third",
		sa.Add(time.Second).Format(time.RFC3339Nano),
		sa.Add(2*time.Second).Format(time.RFC3339Nano),
		sa.Add(3*time.Second).Format(time.RFC3339Nano),
	))
	returnedRun, err := reader.FindRunByID(ctx, task.Org, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(run, *returnedRun); diff != "" {
		t.Fatalf("unexpected run found: -want/+got: %s", diff)
	}
}

func listRunsTest(t *testing.T, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) {
	writer, reader := crf(t)
	defer drf(t, writer, reader)

	task := &backend.StoreTask{
		ID:  platformtesting.MustIDBase16("ab01ab01ab01ab01"),
		Org: platformtesting.MustIDBase16("ab01ab01ab01ab05"),
	}

	ctx := pcontext.SetAuthorizer(context.Background(), makeNewAuthorization())

	if _, err := reader.ListRuns(ctx, platform.RunFilter{Task: &task.ID}); err == nil {
		t.Fatal("failed to error on bad id")
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

	if _, err := reader.ListRuns(ctx, platform.RunFilter{}); err == nil {
		t.Fatal("failed to error without any filter")
	}

	listRuns, err := reader.ListRuns(ctx, platform.RunFilter{
		Task:  &task.ID,
		Org:   &task.Org,
		Limit: 2 * nRuns,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != len(runs) {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), len(runs))
	}

	const afterIDIdx = 20
	listRuns, err = reader.ListRuns(ctx, platform.RunFilter{
		Task:  &task.ID,
		Org:   &task.Org,
		After: &runs[afterIDIdx].ID,
		Limit: 2 * nRuns,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != len(runs)-(afterIDIdx+1) {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), len(runs)-(afterIDIdx+1))
	}

	listRuns, err = reader.ListRuns(ctx, platform.RunFilter{
		Task:  &task.ID,
		Org:   &task.Org,
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
	listRuns, err = reader.ListRuns(ctx, platform.RunFilter{
		Task:      &task.ID,
		Org:       &task.Org,
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
	listRuns, err = reader.ListRuns(ctx, platform.RunFilter{
		Task:       &task.ID,
		Org:        &task.Org,
		BeforeTime: scheduledFor.Add(time.Millisecond).Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != beforeTimeIdx {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), beforeTimeIdx)
	}
}

func findRunByIDTest(t *testing.T, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) {
	writer, reader := crf(t)
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

	ctx := pcontext.SetAuthorizer(context.Background(), makeNewAuthorization())
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

	returnedRun.Log = "cows"

	rr2, err := reader.FindRunByID(ctx, task.Org, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if reflect.DeepEqual(returnedRun, rr2) {
		t.Fatalf("updateing returned run modified RunStore data")
	}
}

func listLogsTest(t *testing.T, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) {
	writer, reader := crf(t)
	defer drf(t, writer, reader)

	task := &backend.StoreTask{
		ID:  platformtesting.MustIDBase16("ab01ab01ab01ab01"),
		Org: platformtesting.MustIDBase16("ab01ab01ab01ab05"),
	}

	ctx := pcontext.SetAuthorizer(context.Background(), makeNewAuthorization())

	if _, err := reader.ListLogs(ctx, platform.LogFilter{}); err == nil {
		t.Fatal("failed to error with no filter")
	}
	if _, err := reader.ListLogs(ctx, platform.LogFilter{Run: &task.ID}); err == nil {
		t.Fatal("failed to error with a non-run-ID")
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
	logs, err := reader.ListLogs(ctx, platform.LogFilter{Run: &runs[targetRun].ID, Org: &task.Org})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}

	fmtTimelog := now.Add(time.Duration(targetRun-nRuns)*time.Second + 2*time.Millisecond).Format(time.RFC3339Nano)
	if fmtTimelog+": log4" != string(logs[0]) {
		t.Fatalf("expected: %q, got: %q", fmtTimelog+": log4", string(logs[0]))
	}

	logs, err = reader.ListLogs(ctx, platform.LogFilter{Task: &task.ID, Org: &task.Org})
	if err != nil {
		t.Fatal(err)
	}

	if len(logs) != len(runs) {
		t.Fatal("not all logs retrieved")
	}
}

func makeNewAuthorization() *platform.Authorization {
	return &platform.Authorization{
		ID:          platformtesting.MustIDBase16("ab01ab01ab01ab01"),
		UserID:      platformtesting.MustIDBase16("ab01ab01ab01ab01"),
		OrgID:       platformtesting.MustIDBase16("ab01ab01ab01ab05"),
		Permissions: platform.OperPermissions(),
	}
}
