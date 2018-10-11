package storetest

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/task/backend"
	platformtesting "github.com/influxdata/platform/testing"
)

type CreateRunStoreFunc func(*testing.T) (backend.LogWriter, backend.LogReader)
type DestroyRunStoreFunc func(*testing.T, backend.LogWriter, backend.LogReader)

func NewRunStoreTest(name string, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run(name, func(t *testing.T) {
			t.Run("UpdateRunState", func(t *testing.T) {
				updateRunState(t, crf, drf)
			})
			t.Run("RunLog", func(t *testing.T) {
				runLogTest(t, crf, drf)
			})
			t.Run("ListRuns", func(t *testing.T) {
				listRunsTest(t, crf, drf)
			})
			t.Run("FindRunByID", func(t *testing.T) {
				findRunByIDTest(t, crf, drf)
			})
			t.Run("ListLogs", func(t *testing.T) {
				listLogsTest(t, crf, drf)
			})
		})
	}
}

func updateRunState(t *testing.T, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) {
	writer, reader := crf(t)
	defer drf(t, writer, reader)

	task := &backend.StoreTask{
		ID:  platformtesting.MustIDBase16("ab01ab01ab01ab01"),
		Org: platformtesting.MustIDBase16("ab01ab01ab01ab05"),
	}
	scheduledFor := time.Unix(1, 0).UTC()
	run := platform.Run{
		ID:           platformtesting.MustIDBase16("2c20766972747573"),
		TaskID:       task.ID,
		Status:       "started",
		ScheduledFor: scheduledFor.Format(time.RFC3339),
	}
	rlb := backend.RunLogBase{
		Task:            task,
		RunID:           run.ID,
		RunScheduledFor: 1,
	}

	startAt := time.Unix(2, 0).UTC()
	if err := writer.UpdateRunState(context.Background(), rlb, startAt, backend.RunStarted); err != nil {
		t.Fatal(err)
	}
	run.StartedAt = startAt.Format(time.RFC3339Nano)
	run.Status = "started"

	returnedRun, err := reader.FindRunByID(context.Background(), task.Org, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(run, *returnedRun) {
		t.Fatalf("expected: %+v, got: %+v", run, returnedRun)
	}

	endAt := time.Unix(3, 0).UTC()
	if err := writer.UpdateRunState(context.Background(), rlb, endAt, backend.RunSuccess); err != nil {
		t.Fatal(err)
	}
	run.FinishedAt = endAt.Format(time.RFC3339Nano)
	run.Status = "success"

	returnedRun, err = reader.FindRunByID(context.Background(), task.Org, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(run, *returnedRun) {
		t.Fatalf("expected: %+v, got: %+v, \n diff: %+v", run, *returnedRun, cmp.Diff(run, *returnedRun))
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
	sa := sf.Add(time.Second)
	run := platform.Run{
		ID:           platformtesting.MustIDBase16("2c20766972747573"),
		TaskID:       task.ID,
		Status:       "started",
		ScheduledFor: sf.Format(time.RFC3339),
		StartedAt:    sa.Format(time.RFC3339),
	}
	rlb := backend.RunLogBase{
		Task:            task,
		RunID:           run.ID,
		RunScheduledFor: sf.Unix(),
	}

	logTime := time.Now().UTC()

	if err := writer.AddRunLog(context.Background(), rlb, logTime, "bad"); err == nil {
		t.Fatal("shouldn't be able to log against non existing run")
	}

	err := writer.UpdateRunState(context.Background(), rlb, sa, backend.RunStarted)
	if err != nil {
		t.Fatal(err)
	}

	if err := writer.AddRunLog(context.Background(), rlb, logTime, "first"); err != nil {
		t.Fatal(err)
	}

	if err := writer.AddRunLog(context.Background(), rlb, logTime, "second"); err != nil {
		t.Fatal(err)
	}
	if err := writer.AddRunLog(context.Background(), rlb, logTime, "third"); err != nil {
		t.Fatal(err)
	}

	fmtLogTime := logTime.UTC().Format(time.RFC3339)
	run.Log = platform.Log(fmt.Sprintf("%s: first\n%s: second\n%s: third", fmtLogTime, fmtLogTime, fmtLogTime))
	returnedRun, err := reader.FindRunByID(context.Background(), task.Org, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(run, *returnedRun) {
		t.Fatalf("expected: %+v, got: %+v,\n\ndiff: %+v", run, *returnedRun, cmp.Diff(run, *returnedRun))
	}
}

func listRunsTest(t *testing.T, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) {
	writer, reader := crf(t)
	defer drf(t, writer, reader)

	task := &backend.StoreTask{
		ID:  platformtesting.MustIDBase16("ab01ab01ab01ab01"),
		Org: platformtesting.MustIDBase16("ab01ab01ab01ab05"),
	}

	if _, err := reader.ListRuns(context.Background(), platform.RunFilter{Task: &task.ID}); err == nil {
		t.Fatal("failed to error on bad id")
	}

	runs := make([]platform.Run, 200)
	for i := 0; i < len(runs); i++ {
		scheduledFor := time.Unix(int64(i), 0).UTC()
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

		err := writer.UpdateRunState(context.Background(), rlb, scheduledFor.Add(time.Second), backend.RunStarted)
		if err != nil {
			t.Fatal(err)
		}
	}

	_, err := reader.ListRuns(context.Background(), platform.RunFilter{})
	if err == nil {
		t.Fatal("failed to error without any filter")
	}

	listRuns, err := reader.ListRuns(context.Background(), platform.RunFilter{
		Task: &task.ID,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != len(runs) {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), len(runs))
	}

	listRuns, err = reader.ListRuns(context.Background(), platform.RunFilter{
		Task:  &task.ID,
		After: &runs[20].ID,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != len(runs)-20 {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), len(runs)-20)
	}

	listRuns, err = reader.ListRuns(context.Background(), platform.RunFilter{
		Task:  &task.ID,
		Limit: 30,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != 30 {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), 30)
	}

	scheduledFor, _ := time.Parse(time.RFC3339, runs[34].ScheduledFor)
	listRuns, err = reader.ListRuns(context.Background(), platform.RunFilter{
		Task:      &task.ID,
		AfterTime: scheduledFor.Add(-time.Second).Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != len(runs)-34 {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), len(runs)-34)
	}

	scheduledFor, _ = time.Parse(time.RFC3339, runs[34].ScheduledFor)
	listRuns, err = reader.ListRuns(context.Background(), platform.RunFilter{
		Task:       &task.ID,
		BeforeTime: scheduledFor.Add(time.Nanosecond).Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != 35 {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), 34)
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
	sf := time.Now().UTC()
	sa := sf.Add(time.Second)

	run := platform.Run{
		ID:           platformtesting.MustIDBase16("2c20766972747573"),
		TaskID:       task.ID,
		Status:       "started",
		ScheduledFor: sf.Format(time.RFC3339),
		StartedAt:    sa.Format(time.RFC3339),
	}
	rlb := backend.RunLogBase{
		Task:            task,
		RunID:           run.ID,
		RunScheduledFor: sf.Unix(),
	}

	if err := writer.UpdateRunState(context.Background(), rlb, sa, backend.RunStarted); err != nil {
		t.Fatal(err)
	}

	returnedRun, err := reader.FindRunByID(context.Background(), task.Org, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(run, *returnedRun) {
		t.Fatalf("expected:\n%#v, got: \n%#v", run, *returnedRun)
	}

	returnedRun.Log = "cows"

	rr2, err := reader.FindRunByID(context.Background(), task.Org, run.ID)
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

	if _, err := reader.ListLogs(context.Background(), platform.LogFilter{}); err == nil {
		t.Fatal("failed to error with no filter")
	}
	if _, err := reader.ListLogs(context.Background(), platform.LogFilter{Run: &task.ID}); err == nil {
		t.Fatal("failed to error with no filter")
	}

	runs := make([]platform.Run, 20)
	for i := 0; i < len(runs); i++ {
		sf := time.Unix(int64(i), 0)
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

		err := writer.UpdateRunState(context.Background(), rlb, time.Now(), backend.RunStarted)
		if err != nil {
			t.Fatal(err)
		}

		writer.AddRunLog(context.Background(), rlb, time.Unix(int64(i), 0), fmt.Sprintf("log%d", i))
	}

	logs, err := reader.ListLogs(context.Background(), platform.LogFilter{Run: &runs[4].ID})
	if err != nil {
		t.Fatal(err)
	}

	fmtTimelog := time.Unix(int64(4), 0).Format(time.RFC3339)
	if fmtTimelog+": log4" != string(logs[0]) {
		t.Fatalf("expected: %+v, got: %+v", fmtTimelog+": log4", string(logs[0]))
	}

	logs, err = reader.ListLogs(context.Background(), platform.LogFilter{Task: &task.ID})
	if err != nil {
		t.Fatal(err)
	}

	if len(logs) != len(runs) {
		t.Fatal("not all logs retrieved")
	}
}
