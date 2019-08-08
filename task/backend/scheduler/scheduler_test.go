package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/task/backend"

	"github.com/influxdata/influxdb/task/backend/scheduler"
)

type mockExecutor struct {
	sync.Mutex
}

func (e *mockExecutor) Execute(ctx context.Context, id scheduler.ID, scheduledAt time.Time) (backend.RunPromise, error) {
	select {
	case <-ctx.Done():
	}
}

func TestSchedule_Next(t *testing.T) {
	now := time.Now().Add(-20 * time.Second)
	exe := mockExecutor{}
	sch, err := scheduler.NewScheduler(exe.Execute)
	if err != nil {
		t.Fatal(err)
	}
	sch.Schedule(1, "* * * * * * *", 10*time.Second, now.Add(20*time.Second))
}
