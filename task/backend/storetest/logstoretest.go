package storetest

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/task/backend"
)

type CreateRunStoreFunc func(*testing.T) (backend.LogWriter, backend.LogReader)
type DestroyRunStoreFunc func(*testing.T, backend.LogWriter, backend.LogReader)

func NewRunStoreTest(name string, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run(name, func(t *testing.T) {
			t.Run("SetRunScheduled", func(t *testing.T) {
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
		ID:  platform.ID([]byte("ab01ab01ab01ab01")),
		Org: platform.ID([]byte("ab01ab01ab01ab05")),
	}
	scheduledFor := time.Unix(1, 0).UTC()
	run := platform.Run{
		ID:           platform.ID([]byte("run")),
		TaskID:       task.ID,
		Status:       "scheduled",
		ScheduledFor: scheduledFor.Format(time.RFC3339),
	}

	err := writer.UpdateRunState(context.Background(), task, run.ID, scheduledFor, backend.RunScheduled)
	if err != nil {
		t.Fatal(err)
	}

	returnedRun, err := reader.FindRunByID(context.Background(), task.Org, task.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(run, *returnedRun) {
		t.Fatalf("expected: %+v, got: %+v", run, returnedRun)
	}

	startAt := time.Unix(2, 0).UTC()
	if err := writer.UpdateRunState(context.Background(), task, run.ID, startAt, backend.RunStarted); err != nil {
		t.Fatal(err)
	}
	run.StartedAt = startAt.Format(time.RFC3339Nano)
	run.Status = "started"

	returnedRun, err = reader.FindRunByID(context.Background(), task.Org, task.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(run, *returnedRun) {
		t.Fatalf("expected: %+v, got: %+v", run, returnedRun)
	}

	endAt := time.Unix(3, 0).UTC()
	if err := writer.UpdateRunState(context.Background(), task, run.ID, endAt, backend.RunSuccess); err != nil {
		t.Fatal(err)
	}
	run.FinishedAt = endAt.Format(time.RFC3339Nano)
	run.Status = "success"

	returnedRun, err = reader.FindRunByID(context.Background(), task.Org, task.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(run, *returnedRun) {
		t.Fatalf("expected: %+v, got: %+v", run, returnedRun)
	}
}

func runLogTest(t *testing.T, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) {
	writer, reader := crf(t)
	defer drf(t, writer, reader)

	task := &backend.StoreTask{
		ID:  platform.ID([]byte("ab01ab01ab01ab01")),
		Org: platform.ID([]byte("ab01ab01ab01ab05")),
	}

	run := platform.Run{
		ID:           platform.ID([]byte("run")),
		TaskID:       task.ID,
		Status:       "scheduled",
		ScheduledFor: time.Now().UTC().Format(time.RFC3339),
	}

	logTime := time.Now().UTC()

	if err := writer.AddRunLog(context.Background(), task, run.ID, logTime, "bad"); err == nil {
		t.Fatal("shouldn't be able to log against non existing run")
	}

	err := writer.UpdateRunState(context.Background(), task, run.ID, time.Now(), backend.RunScheduled)
	if err != nil {
		t.Fatal(err)
	}

	if err := writer.AddRunLog(context.Background(), task, run.ID, logTime, "first"); err != nil {
		t.Fatal(err)
	}

	if err := writer.AddRunLog(context.Background(), task, run.ID, logTime, "second"); err != nil {
		t.Fatal(err)
	}
	if err := writer.AddRunLog(context.Background(), task, run.ID, logTime, "third"); err != nil {
		t.Fatal(err)
	}

	fmtLogTime := logTime.UTC().Format(time.RFC3339)
	run.Log = platform.Log(fmt.Sprintf("%s: first\n%s: second\n%s: third", fmtLogTime, fmtLogTime, fmtLogTime))
	returnedRun, err := reader.FindRunByID(context.Background(), task.Org, task.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(run, *returnedRun) {
		t.Fatalf("expected: %+v, got: %+v", run, returnedRun)
	}
}

func listRunsTest(t *testing.T, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) {
	writer, reader := crf(t)
	defer drf(t, writer, reader)

	task := &backend.StoreTask{
		ID:  platform.ID([]byte("ab01ab01ab01ab01")),
		Org: platform.ID([]byte("ab01ab01ab01ab05")),
	}

	if _, err := reader.ListRuns(context.Background(), platform.RunFilter{Task: &task.ID}); err == nil {
		t.Fatal("failed to error on bad id")
	}

	runs := make([]platform.Run, 200)
	for i := 0; i < len(runs); i++ {
		scheduledFor := time.Unix(int64(i), 0).UTC()
		runs[i] = platform.Run{
			ID:           platform.ID([]byte(fmt.Sprintf("run%d", i))),
			Status:       "scheduled",
			ScheduledFor: scheduledFor.Format(time.RFC3339),
		}

		err := writer.UpdateRunState(context.Background(), task, runs[i].ID, scheduledFor, backend.RunScheduled)
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

	if _, err := reader.FindRunByID(context.Background(), platform.ID([]byte("fat")), platform.ID([]byte("ugly")), platform.ID([]byte("bad"))); err == nil {
		t.Fatal("failed to error with bad id")
	}

	task := &backend.StoreTask{
		ID:  platform.ID([]byte("ab01ab01ab01ab01")),
		Org: platform.ID([]byte("ab01ab01ab01ab05")),
	}
	run := platform.Run{
		ID:           platform.ID([]byte("run")),
		TaskID:       task.ID,
		Status:       "scheduled",
		ScheduledFor: time.Now().UTC().Format(time.RFC3339),
	}

	if err := writer.UpdateRunState(context.Background(), task, run.ID, time.Now(), backend.RunScheduled); err != nil {
		t.Fatal(err)
	}

	returnedRun, err := reader.FindRunByID(context.Background(), task.Org, task.ID, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(run, *returnedRun) {
		t.Fatalf("expected:\n%#v, got: \n%#v", run, *returnedRun)
	}

	returnedRun.Log = "cows"

	rr2, err := reader.FindRunByID(context.Background(), task.Org, task.ID, run.ID)
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
		ID:  platform.ID([]byte("ab01ab01ab01ab01")),
		Org: platform.ID([]byte("ab01ab01ab01ab05")),
	}

	if _, err := reader.ListLogs(context.Background(), platform.LogFilter{}); err == nil {
		t.Fatal("failed to error with no filter")
	}
	if _, err := reader.ListLogs(context.Background(), platform.LogFilter{Run: &task.ID}); err == nil {
		t.Fatal("failed to error with no filter")
	}

	runs := make([]platform.Run, 20)
	for i := 0; i < len(runs); i++ {
		runs[i] = platform.Run{
			ID:           platform.ID([]byte(fmt.Sprintf("run%d", i))),
			Status:       "started",
			ScheduledFor: time.Unix(int64(i), 0).Format(time.RFC3339),
		}

		err := writer.UpdateRunState(context.Background(), task, runs[i].ID, time.Now(), backend.RunScheduled)
		if err != nil {
			t.Fatal(err)
		}

		writer.AddRunLog(context.Background(), task, runs[i].ID, time.Unix(int64(i), 0), fmt.Sprintf("log%d", i))
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
