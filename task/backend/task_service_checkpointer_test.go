package backend_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/task/backend"
)

func TestCheckpoint(t *testing.T) {
	mockService := &mock.TaskService{
		UpdateTaskFn: func(context.Context, influxdb.ID, influxdb.TaskUpdate) (*influxdb.Task, error) { return nil, nil },
	}

	cp := backend.NewTaskServiceCheckpointer(mockService)
	err := cp.Checkpoint(context.Background(), influxdb.ID(1), time.Now())
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointError(t *testing.T) {
	mockService := &mock.TaskService{
		UpdateTaskFn: func(context.Context, influxdb.ID, influxdb.TaskUpdate) (*influxdb.Task, error) {
			return nil, fmt.Errorf("error!")
		},
	}

	cp := backend.NewTaskServiceCheckpointer(mockService)
	err := cp.Checkpoint(context.Background(), influxdb.ID(1), time.Now())
	if err == nil {
		t.Fatalf("Expected error")
	}
}

func TestLast(t *testing.T) {
	want := time.Now()
	s := want.Format(time.RFC3339Nano)
	mockService := &mock.TaskService{
		FindTaskByIDFn: func(context.Context, influxdb.ID) (*influxdb.Task, error) {
			return &influxdb.Task{LatestCompleted: s}, nil
		},
	}

	cp := backend.NewTaskServiceCheckpointer(mockService)
	got, err := cp.Last(context.Background(), influxdb.ID(1))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(want) {
		t.Fatalf("wrong time, wanted: %v, got: %v", want, got)
	}
}

func TestLastFetchError(t *testing.T) {
	mockService := &mock.TaskService{
		FindTaskByIDFn: func(context.Context, influxdb.ID) (*influxdb.Task, error) {
			return nil, fmt.Errorf("error 1")
		},
	}

	cp := backend.NewTaskServiceCheckpointer(mockService)
	_, err := cp.Last(context.Background(), influxdb.ID(1))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestLastParseError(t *testing.T) {
	mockService := &mock.TaskService{
		FindTaskByIDFn: func(context.Context, influxdb.ID) (*influxdb.Task, error) {
			return &influxdb.Task{LatestCompleted: "howdy"}, nil
		},
	}

	cp := backend.NewTaskServiceCheckpointer(mockService)
	_, err := cp.Last(context.Background(), influxdb.ID(1))
	if err == nil {
		t.Fatalf("expected error parsing invalid time: %v", err)
	}
}
