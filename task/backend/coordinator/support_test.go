package coordinator

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/task/backend"
)

type (
	schedulerS struct {
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

func (s *schedulerS) ClaimTask(_ context.Context, task *influxdb.Task) error {
	s.calls = append(s.calls, claimCall{task})

	return s.claimErr
}

func (s *schedulerS) UpdateTask(_ context.Context, task *influxdb.Task) error {
	s.calls = append(s.calls, updateCall{task})

	return s.updateErr
}

func (s *schedulerS) ReleaseTask(taskID influxdb.ID) error {
	s.calls = append(s.calls, releaseCall{taskID})

	return s.releaseErr
}

func (s *schedulerS) CancelRun(_ context.Context, taskID influxdb.ID, runID influxdb.ID) error {
	s.calls = append(s.calls, cancelCall{taskID, runID})

	return nil
}
