package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
)

var now = time.Now()

type MockFindTasksSerivce struct {
	FindTasksFn func(context.Context, influxdb.TaskFilter) ([]*influxdb.Task, int, error)
}

func (s MockFindTasksSerivce) FindTasks(ctx context.Context, filter influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
	return s.FindTasksFn(ctx, filter)
}

func TestInmemTaskService(t *testing.T) {
	mockService := MockFindTasksSerivce{
		FindTasksFn: func(context.Context, influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
			ts := []*influxdb.Task{&influxdb.Task{LatestScheduled: now, Status: "active"}}
			return ts, 1, nil
		},
	}

	ctx := context.Background()
	taskLatency := newTaskLatency(ctx, mockService, nil)
	taskLatency.Update(ctx)
}
