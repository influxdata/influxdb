package backend_test

import (
	"errors"
	"strings"
	"testing"

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
		MaxConcurrency: 2,
		Status:         "enabled",
		EffectiveCron:  "* * * * *", // Every minute.
		LastCompleted:  60,          // It has run once for the first minute.
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

func TestMeta_CreateNextRun_Delay(t *testing.T) {
	stm := backend.StoreTaskMeta{
		MaxConcurrency: 2,
		Status:         "enabled",
		EffectiveCron:  "* * * * *", // Every minute.
		Delay:          5,
		LastCompleted:  30, // Arbitrary non-overlap starting point.
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
