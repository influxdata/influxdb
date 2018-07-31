package storetest

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/task/backend"
	platformtesting "github.com/influxdata/platform/testing"
)

type CreateRunStoreFunc func(*testing.T) (backend.LogWriter, backend.LogReader)
type DestroyRunStoreFunc func(*testing.T, backend.LogWriter, backend.LogReader)

func NewRunStoreTest(name string, crf CreateRunStoreFunc, drf DestroyRunStoreFunc) func(*testing.T) {
	return func(t *testing.T) {
		t.Run(name, func(t *testing.T) {
			t.Run("SetRunQueued", func(t *testing.T) {
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

	taskID := platformtesting.MustIDFromString("ab01ab01ab01ab01")
	queuedAt := time.Unix(1, 0)
	run := platform.Run{
		ID:       platformtesting.MustIDFromString("ab02ab02ab02ab02"),
		Status:   "queued",
		QueuedAt: queuedAt.Format(time.RFC3339),
	}

	err := writer.UpdateRunState(context.Background(), taskID, run.ID, queuedAt, backend.RunQueued)
	if err != nil {
		t.Fatal(err)
	}

	returnedRun, err := reader.FindRunByID(context.Background(), taskID, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(run, *returnedRun) {
		t.Fatalf("expected: %+v, got: %+v", run, returnedRun)
	}

	startAt := time.Unix(2, 0)
	if err := writer.UpdateRunState(context.Background(), taskID, run.ID, startAt, backend.RunStarted); err != nil {
		t.Fatal(err)
	}
	run.StartTime = startAt.Format(time.RFC3339)
	run.Status = "started"

	returnedRun, err = reader.FindRunByID(context.Background(), taskID, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(run, *returnedRun) {
		t.Fatalf("expected: %+v, got: %+v", run, returnedRun)
	}

	endAt := time.Unix(3, 0)
	if err := writer.UpdateRunState(context.Background(), taskID, run.ID, endAt, backend.RunSuccess); err != nil {
		t.Fatal(err)
	}
	run.EndTime = endAt.Format(time.RFC3339)
	run.Status = "success"

	returnedRun, err = reader.FindRunByID(context.Background(), taskID, run.ID)
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

	taskID := platformtesting.MustIDFromString("ab01ab01ab01ab01")
	run := platform.Run{
		ID:       platformtesting.MustIDFromString("ab02ab02ab02ab02"),
		Status:   "queued",
		QueuedAt: time.Now().Format(time.RFC3339),
	}

	logTime := time.Now()

	if err := writer.AddRunLog(context.Background(), taskID, run.ID, logTime, "bad"); err == nil {
		t.Fatal("shouldn't be able to log against non existing run")
	}

	err := writer.UpdateRunState(context.Background(), taskID, run.ID, time.Now(), backend.RunQueued)
	if err != nil {
		t.Fatal(err)
	}

	if err := writer.AddRunLog(context.Background(), taskID, run.ID, logTime, "first"); err != nil {
		t.Fatal(err)
	}

	if err := writer.AddRunLog(context.Background(), taskID, run.ID, logTime, "second"); err != nil {
		t.Fatal(err)
	}
	if err := writer.AddRunLog(context.Background(), taskID, run.ID, logTime, "third"); err != nil {
		t.Fatal(err)
	}

	fmtLogTime := logTime.Format(time.RFC3339)
	run.Log = platform.Log(fmt.Sprintf("%s: first\n%s: second\n%s: third", fmtLogTime, fmtLogTime, fmtLogTime))
	returnedRun, err := reader.FindRunByID(context.Background(), taskID, run.ID)
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

	taskID := platformtesting.MustIDFromString("ab01ab01ab01ab01")

	if _, err := reader.ListRuns(context.Background(), platform.RunFilter{Task: &taskID}); err == nil {
		t.Fatal("failed to error on bad id")
	}

	runs := make([]platform.Run, 200)
	for i := 0; i < len(runs); i++ {
		queuedAt := time.Unix(int64(i), 0)
		runs[i] = platform.Run{
			ID:       platform.ID(i),
			Status:   "queued",
			QueuedAt: queuedAt.Format(time.RFC3339),
		}

		err := writer.UpdateRunState(context.Background(), taskID, runs[i].ID, queuedAt, backend.RunQueued)
		if err != nil {
			t.Fatal(err)
		}
	}

	_, err := reader.ListRuns(context.Background(), platform.RunFilter{})
	if err == nil {
		t.Fatal("failed to error without any filter")
	}

	listRuns, err := reader.ListRuns(context.Background(), platform.RunFilter{
		Task: &taskID,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != len(runs) {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), len(runs))
	}

	listRuns, err = reader.ListRuns(context.Background(), platform.RunFilter{
		Task:  &taskID,
		After: &runs[20].ID,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != len(runs)-20 {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), len(runs)-20)
	}

	listRuns, err = reader.ListRuns(context.Background(), platform.RunFilter{
		Task:  &taskID,
		Limit: 30,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != 30 {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), 30)
	}

	queuedAt, _ := time.Parse(time.RFC3339, runs[34].QueuedAt)
	listRuns, err = reader.ListRuns(context.Background(), platform.RunFilter{
		Task:      &taskID,
		AfterTime: queuedAt.Add(-1 * time.Nanosecond).Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(listRuns) != len(runs)-34 {
		t.Fatalf("retrieved: %d, expected: %d", len(listRuns), len(runs)-34)
	}

	queuedAt, _ = time.Parse(time.RFC3339, runs[34].QueuedAt)
	listRuns, err = reader.ListRuns(context.Background(), platform.RunFilter{
		Task:       &taskID,
		BeforeTime: queuedAt.Add(time.Nanosecond).Format(time.RFC3339),
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

	var emptyID platform.ID

	if _, err := reader.FindRunByID(context.Background(), emptyID, emptyID); err == nil {
		t.Fatal("failed to error with bad id")
	}

	taskID := platformtesting.MustIDFromString("ab01ab01ab01ab01")
	run := platform.Run{
		ID:       platformtesting.MustIDFromString("ab02ab02ab02ab02"),
		Status:   "queued",
		QueuedAt: time.Now().Format(time.RFC3339),
	}

	if err := writer.UpdateRunState(context.Background(), taskID, run.ID, time.Now(), backend.RunQueued); err != nil {
		t.Fatal(err)
	}

	returnedRun, err := reader.FindRunByID(context.Background(), taskID, run.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(run, *returnedRun) {
		t.Fatalf("expected:\n%#v, got: \n%#v", run, *returnedRun)
	}

	returnedRun.Log = "cows"

	rr2, err := reader.FindRunByID(context.Background(), taskID, run.ID)
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

	taskID := platformtesting.MustIDFromString("ab01ab01ab01ab01")

	if _, err := reader.ListLogs(context.Background(), platform.LogFilter{}); err == nil {
		t.Fatal("failed to error with no filter")
	}
	if _, err := reader.ListLogs(context.Background(), platform.LogFilter{Run: &taskID}); err == nil {
		t.Fatal("failed to error with no filter")
	}

	runs := make([]platform.Run, 20)
	for i := 0; i < len(runs); i++ {
		runs[i] = platform.Run{
			ID:       platform.ID(i),
			Status:   "started",
			QueuedAt: time.Unix(int64(i), 0).Format(time.RFC3339),
		}

		err := writer.UpdateRunState(context.Background(), taskID, runs[i].ID, time.Now(), backend.RunQueued)
		if err != nil {
			t.Fatal(err)
		}

		writer.AddRunLog(context.Background(), taskID, runs[i].ID, time.Unix(int64(i), 0), fmt.Sprintf("log%d", i))
	}

	logs, err := reader.ListLogs(context.Background(), platform.LogFilter{Run: &runs[4].ID})
	if err != nil {
		t.Fatal(err)
	}

	fmtTimelog := time.Unix(int64(4), 0).Format(time.RFC3339)
	if fmtTimelog+": log4" != string(logs[0]) {
		t.Fatalf("expected: %+v, got: %+v", fmtTimelog+": log4", string(logs[0]))
	}

	logs, err = reader.ListLogs(context.Background(), platform.LogFilter{Task: &taskID})
	if err != nil {
		t.Fatal(err)
	}

	if len(logs) != len(runs) {
		t.Fatal("not all logs retrieved")
	}
}
