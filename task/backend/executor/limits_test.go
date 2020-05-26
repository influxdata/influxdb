package executor

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
)

var (
	taskWith1Concurrency  = &influxdb.Task{ID: 1, Flux: `option task = {concurrency: 1, name:"x", every:1m} from(bucket:"b-src") |> range(start:-1m) |> to(bucket:"b-dst", org:"o")`}
	taskWith10Concurrency = &influxdb.Task{ID: 1, Flux: `option task = {concurrency: 10, name:"x", every:1m} from(bucket:"b-src") |> range(start:-1m) |> to(bucket:"b-dst", org:"o")`}
)

func TestTaskConcurrency(t *testing.T) {
	tes := taskExecutorSystem(t)
	te := tes.ex
	r1, err := te.tcs.CreateRun(context.Background(), taskWith1Concurrency.ID, time.Now().Add(-4*time.Second), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	r2, err := te.tcs.CreateRun(context.Background(), taskWith1Concurrency.ID, time.Now().Add(-3*time.Second), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	r3, err := te.tcs.CreateRun(context.Background(), taskWith1Concurrency.ID, time.Now().Add(-2*time.Second), time.Now())
	if err != nil {
		t.Fatal(err)
	}

	r4 := &influxdb.Run{
		ID:           3,
		ScheduledFor: time.Now(),
	}

	clFunc := ConcurrencyLimit(te)
	if err := clFunc(taskWith1Concurrency, r1); err != nil {
		t.Fatal(err)
	}
	if err := clFunc(taskWith1Concurrency, r2); err == nil {
		t.Fatal("failed to error when exceeding limit by 1")
	}
	if err := clFunc(taskWith1Concurrency, r3); err == nil {
		t.Fatal("failed to error when exceeding limit by 2")
	}
	if err := clFunc(taskWith1Concurrency, r4); err == nil {
		t.Fatal("failed to error when exceeding limit before saving run")
	}

	if err := clFunc(taskWith10Concurrency, r4); err != nil {
		t.Fatal(err)
	}

	// TODO(lh): add testing around infinite concurrency once the task options
	// are not setting a default concurrency to 1.
}
