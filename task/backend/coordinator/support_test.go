package coordinator

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/backend"
)

type (
	scheduler struct {
		backend.Scheduler

		claimErr,
		updateErr,
		releaseErr error

		calls []interface{}
	}

	claimCall struct {
		Task *influxdb.Task
	}

	updateCall struct {
		Task *influxdb.Task
	}

	releaseCall struct {
		ID influxdb.ID
	}

	cancelCall struct {
		TaskID, RunID influxdb.ID
	}
)

func (s *scheduler) ClaimTask(_ context.Context, task *influxdb.Task) error {
	s.calls = append(s.calls, claimCall{task})

	return s.claimErr
}

func (s *scheduler) UpdateTask(_ context.Context, task *influxdb.Task) error {
	s.calls = append(s.calls, updateCall{task})

	return s.updateErr
}

func (s *scheduler) ReleaseTask(taskID influxdb.ID) error {
	s.calls = append(s.calls, releaseCall{taskID})

	return s.releaseErr
}

func (s *scheduler) CancelRun(_ context.Context, taskID influxdb.ID, runID influxdb.ID) error {
	s.calls = append(s.calls, cancelCall{taskID, runID})

	return nil
}
