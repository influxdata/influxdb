package backend

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/task/backend/scheduler"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
)

var (
	mockTaskID  = platform.ID(1)
	mockTimeNow = time.Now()
)

func (m MockTaskService) UpdateTask(_ context.Context, id platform.ID, _ taskmodel.TaskUpdate) (*taskmodel.Task, error) {
	return &taskmodel.Task{ID: id, UpdatedAt: mockTimeNow}, nil
}

type MockTaskService struct{}

func Test_Schedulable_Task_Service(t *testing.T) {

	for _, test := range []struct {
		name string
		task *taskmodel.Task
	}{
		{
			name: "Create New Schedulable Task Service",
			task: taskOne,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ts := MockTaskService{}

			schedulableService := NewSchedulableTaskService(ts)

			err := schedulableService.UpdateLastScheduled(context.Background(), scheduler.ID(mockTaskID), mockTimeNow)
			if err != nil {
				t.Fatalf("expected nil error, got: %v", err)
			}
		})
	}
}
