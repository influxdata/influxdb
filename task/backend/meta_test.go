package backend_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/snowflake"
	"github.com/influxdata/platform/task/backend"
)

var idGen = snowflake.NewIDGenerator()

func makeID() (platform.ID, error) {
	return idGen.ID(), nil
}

func TestMeta_CreateNextRun(t *testing.T) {
	good := backend.StoreTaskMeta{
		MaxConcurrency:  2,
		Status:          "enabled",
		EffectiveCron:   "* * * * *", // Every minute.
		LatestCompleted: 60,          // It has run once for the first minute.
	}

	_, err := good.CreateNextRun(59, makeID)
	if e, ok := err.(backend.RunNotYetDueError); !ok {
		t.Fatalf("expected RunNotYetDueError, got %v (%T)", err, err)
	} else if e.DueAt != 120 {
		t.Fatalf("expected run due at 120, got %d", e.DueAt)
	}

	bad := new(backend.StoreTaskMeta)
	*bad = good
	bad.MaxConcurrency = 0
	if _, err := bad.CreateNextRun(120, makeID); err == nil || !strings.Contains(err.Error(), "max concurrency") {
		t.Fatalf("expected error about max concurrency, got %v", err)
	}

	*bad = good
	bad.EffectiveCron = "not a cron"
	if _, err := bad.CreateNextRun(120, makeID); err == nil {
		t.Fatal("expected error with bad cron")
	}

	idErr := errors.New("error making ID")
	*bad = good
	if _, err := bad.CreateNextRun(120, func() (platform.ID, error) { return nil, idErr }); err != idErr {
		t.Fatalf("expected id creation error, got %v", err)
	}

	rc, err := good.CreateNextRun(300, makeID)
	if err != nil {
		t.Fatal(err)
	}
	if rc.Created.TaskID != nil {
		t.Fatalf("CreateNextRun should not have set task ID; got %v", rc.Created.TaskID)
	}
	if rc.Created.RunID == nil {
		t.Fatal("CreateNextRun should have set run ID but didn't")
	}
	if rc.Created.Now != 120 {
		t.Fatalf("expected created run to have time 120, got %d", rc.Created.Now)
	}
	if rc.NextDue != 180 {
		t.Fatalf("unexpected next run time: %d", rc.NextDue)
	}

	rc, err = good.CreateNextRun(300, makeID)
	if err != nil {
		t.Fatal(err)
	}
	if rc.Created.TaskID != nil {
		t.Fatalf("CreateNextRun should not have set task ID; got %v", rc.Created.TaskID)
	}
	if rc.Created.RunID == nil {
		t.Fatal("CreateNextRun should have set run ID but didn't")
	}
	if rc.Created.Now != 180 {
		t.Fatalf("expected created run to have time 180, got %d", rc.Created.Now)
	}
	if rc.NextDue != 240 {
		t.Fatalf("unexpected next run time: %d", rc.NextDue)
	}

	if _, err := good.CreateNextRun(300, makeID); err == nil || !strings.Contains(err.Error(), "max concurrency") {
		t.Fatalf("expected error about max concurrency, got %v", err)
	}
}

func TestMeta_CreateNextRun_Queue(t *testing.T) {
	stm := backend.StoreTaskMeta{
		MaxConcurrency:  9,
		Status:          "enabled",
		EffectiveCron:   "* * * * *", // Every minute.
		LatestCompleted: 3000,        // It has run once for the first minute.
	}

	// Should run on 0, 60, and 120.
	if err := stm.ManuallyRunTimeRange(0, 120, 3005); err != nil {
		t.Fatal(err)
	}
	// Should run once: 240.
	if err := stm.ManuallyRunTimeRange(240, 240, 3005); err != nil {
		t.Fatal(err)
	}

	// Run once on the next natural schedule.
	rc, err := stm.CreateNextRun(3060, makeID)
	if err != nil {
		t.Fatal(err)
	}
	if rc.Created.Now != 3060 {
		t.Fatalf("expected created now of 3060, got %d", rc.Created.Now)
	}
	if rc.NextDue != 3120 {
		t.Fatalf("expected NextDue = 3120, got %d", rc.NextDue)
	}
	if !rc.HasQueue {
		t.Fatal("expected to have queue but didn't")
	}

	// 0 from queue.
	rc, err = stm.CreateNextRun(3060, makeID)
	if err != nil {
		t.Fatal(err)
	}
	if rc.Created.Now != 0 {
		t.Fatalf("expected created now of 0, got %d", rc.Created.Now)
	}
	if rc.NextDue != 3120 {
		t.Fatalf("expected NextDue = 3120, got %d", rc.NextDue)
	}
	if !rc.HasQueue {
		t.Fatal("expected to have queue but didn't")
	}

	// 60 from queue.
	rc, err = stm.CreateNextRun(3060, makeID)
	if err != nil {
		t.Fatal(err)
	}
	if rc.Created.Now != 60 {
		t.Fatalf("expected created now of 60, got %d", rc.Created.Now)
	}
	if rc.NextDue != 3120 {
		t.Fatalf("expected NextDue = 3120, got %d", rc.NextDue)
	}
	if !rc.HasQueue {
		t.Fatal("expected to have queue but didn't")
	}

	// 120 from queue.
	rc, err = stm.CreateNextRun(3060, makeID)
	if err != nil {
		t.Fatal(err)
	}
	if rc.Created.Now != 120 {
		t.Fatalf("expected created now of 120, got %d", rc.Created.Now)
	}
	if rc.NextDue != 3120 {
		t.Fatalf("expected NextDue = 3120, got %d", rc.NextDue)
	}
	if !rc.HasQueue {
		t.Fatal("expected to have queue but didn't")
	}

	// 240 from queue.
	rc, err = stm.CreateNextRun(3060, makeID)
	if err != nil {
		t.Fatal(err)
	}
	if rc.Created.Now != 240 {
		t.Fatalf("expected created now of 240, got %d", rc.Created.Now)
	}
	if rc.NextDue != 3120 {
		t.Fatalf("expected NextDue = 3120, got %d", rc.NextDue)
	}
	if rc.HasQueue {
		t.Fatal("expected to have empty queue but didn't")
	}
}

func TestMeta_CreateNextRun_Delay(t *testing.T) {
	stm := backend.StoreTaskMeta{
		MaxConcurrency:  2,
		Status:          "enabled",
		EffectiveCron:   "* * * * *", // Every minute.
		Delay:           5,
		LatestCompleted: 30, // Arbitrary non-overlap starting point.
	}

	_, err := stm.CreateNextRun(61, makeID)
	if e, ok := err.(backend.RunNotYetDueError); !ok {
		t.Fatalf("expected RunNotYetDueError, got %v (%T)", err, err)
	} else if e.DueAt != 65 {
		t.Fatalf("expected run due at 65, got %d", e.DueAt)
	}

	rc, err := stm.CreateNextRun(300, makeID)
	if err != nil {
		t.Fatal(err)
	}
	if rc.Created.Now != 60 {
		t.Fatalf("expected created run to have time 60, got %d", rc.Created.Now)
	}
	if rc.NextDue != 125 {
		t.Fatalf("unexpected next run time: %d", rc.NextDue)
	}
}

func TestMeta_ManuallyRunTimeRange(t *testing.T) {
	now := time.Now().Unix()
	stm := backend.StoreTaskMeta{
		MaxConcurrency:  2,
		Status:          "enabled",
		EffectiveCron:   "* * * * *", // Every minute.
		Delay:           5,
		LatestCompleted: 30, // Arbitrary non-overlap starting point.
	}

	// Constant defined in (*StoreTaskMeta).ManuallyRunTimeRange.
	const maxQueueSize = 32

	for i := int64(0); i < maxQueueSize; i++ {
		j := i * 10
		if err := stm.ManuallyRunTimeRange(j, j+5, j+now); err != nil {
			t.Fatal(err)
		}
		if int64(len(stm.ManualRuns)) != i+1 {
			t.Fatalf("expected %d runs queued, got %d", i+1, len(stm.ManualRuns))
		}

		run := stm.ManualRuns[len(stm.ManualRuns)-1]
		if run.Start != j {
			t.Fatalf("expected start %d, got %d", j, run.Start)
		}
		if run.End != j+5 {
			t.Fatalf("expected end %d, got %d", j+5, run.End)
		}
		if run.LatestCompleted != j-1 {
			t.Fatalf("expected LatestCompleted %d, got %d", j-1, run.LatestCompleted)
		}
		if run.RequestedAt != j+now {
			t.Fatalf("expected RequestedAt %d, got %d", j+now, run.RequestedAt)
		}
	}

	// One more should cause ErrManualQueueFull.
	if err := stm.ManuallyRunTimeRange(maxQueueSize*100, maxQueueSize*200, maxQueueSize+now); err != backend.ErrManualQueueFull {
		t.Fatalf("expected ErrManualQueueFull, got %v", err)
	}
	if len(stm.ManualRuns) != maxQueueSize {
		t.Fatalf("expected to be unable to exceed queue size of %d; got %d", maxQueueSize, len(stm.ManualRuns))
	}
}
