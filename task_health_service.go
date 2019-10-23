package influxdb

import (
	"context"
	"errors"
	"time"
)

// FindTasksService implements the FindTasks method of a TaskService for the TaskHealthService to use
type FindTasksService interface {
	FindTasks(ctx context.Context, filter TaskFilter) ([]*Task, int, error)
}

// TaskLatencyMetricsService is responsible for monitoring the latency of active tasks
type TaskLatencyMetricsService struct {
	fts           FindTasksService
	pollingPeriod time.Duration
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewTaskLatencyMetricsService takes a TaskLatencyMetricsService and creates a new TaskLatencyMetricsService
func NewTaskLatencyMetricsService(ctx context.Context, fts FindTasksService, p time.Duration) *TaskLatencyMetricsService {
	ctxWithCancel, cancel := context.WithCancel(ctx)

	return &TaskLatencyMetricsService{
		fts:           fts,
		ctx:           ctxWithCancel,
		cancel:        cancel,
		pollingPeriod: p,
	}
}

func (p *TaskLatencyMetricsService) Open() {
	ticker := time.NewTicker(p.pollingPeriod)

	go func() {
		for {
			select {
			case <-ticker.C:
				p.PollActiveTasks()
			case <-p.ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (p *TaskLatencyMetricsService) Close() {
	p.cancel()
}

// PollActiveTasks checks the KV store for Active Tasks on a regular interval scheduled by pollingPeriod
func (p *TaskLatencyMetricsService) PollActiveTasks() ([]*Task, error) {
	active := true
	filter := TaskFilter{Active: &active}

	tasks, _, err := p.fts.FindTasks(p.ctx, filter)
	if err != nil {
		return nil, errors.New("TaskLatencyMetricsService could not find tasks")
	}

	return tasks, nil
}
