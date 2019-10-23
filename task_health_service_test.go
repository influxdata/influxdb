package influxdb_test

import (
	"context"
	"testing"
	"time"

	influxdb "github.com/influxdata/influxdb"
)

var now string = time.Now().Format(time.RFC3339Nano)

type MockFindTasksSerivce struct {
	FindTasksFn func(context.Context, influxdb.TaskFilter) ([]*influxdb.Task, int, error)
}

func (s MockFindTasksSerivce) FindTasks(ctx context.Context, filter influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
	return s.FindTasksFn(ctx, filter)
}

func TestTaskHealthService(t *testing.T) {
	mockService := MockFindTasksSerivce{
		FindTasksFn: func(context.Context, influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
			ts := []*influxdb.Task{&influxdb.Task{LatestCompleted: now, Status: "active"}}
			return ts, 1, nil
		},
	}
	d := 5 * time.Second
	healthService := influxdb.NewTaskHealthService(context.Background(), mockService, d)

	_, err := healthService.PollActiveTasks()
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err.Error())
	}
}
