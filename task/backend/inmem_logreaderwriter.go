package backend

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/platform"
)

var ErrRunNotFound error = errors.New("run not found")

type runReaderWriter struct {
	mu       sync.RWMutex
	byTaskID map[string][]*platform.Run
	byRunID  map[string]*platform.Run
}

func NewInMemRunReaderWriter() *runReaderWriter {
	return &runReaderWriter{byRunID: map[string]*platform.Run{}, byTaskID: map[string][]*platform.Run{}}
}

func (r *runReaderWriter) UpdateRunState(ctx context.Context, task *StoreTask, runID platform.ID, when time.Time, status RunStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	timeSetter := func(r *platform.Run) {
		whenStr := when.Format(time.RFC3339)
		switch status {
		case RunScheduled:
			r.ScheduledFor = whenStr
		case RunStarted:
			r.StartedAt = whenStr
		case RunFail, RunSuccess, RunCanceled:
			r.FinishedAt = whenStr
		}
	}

	existingRun, ok := r.byRunID[runID.String()]
	if !ok {
		run := &platform.Run{ID: runID, Status: status.String()}
		timeSetter(run)
		r.byRunID[runID.String()] = run
		r.byTaskID[task.ID.String()] = append(r.byTaskID[task.ID.String()], run)
		return nil
	}

	timeSetter(existingRun)
	existingRun.Status = status.String()
	return nil
}

func (r *runReaderWriter) AddRunLog(ctx context.Context, task *StoreTask, runID platform.ID, when time.Time, log string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	log = fmt.Sprintf("%s: %s", when.Format(time.RFC3339), log)
	existingRun, ok := r.byRunID[runID.String()]
	if !ok {
		return ErrRunNotFound
	}
	if existingRun.Log != "" {
		existingRun.Log = existingRun.Log + "\n"
	}
	existingRun.Log = platform.Log(string(existingRun.Log) + log)
	return nil
}

func (r *runReaderWriter) ListRuns(ctx context.Context, runFilter platform.RunFilter) ([]*platform.Run, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if runFilter.Task == nil {
		return nil, errors.New("task is required")
	}

	_, ok := r.byTaskID[runFilter.Task.String()]
	if !ok {
		return nil, ErrRunNotFound
	}

	runs := make([]*platform.Run, len(r.byTaskID[runFilter.Task.String()]))
	copy(runs, r.byTaskID[runFilter.Task.String()])

	sort.Slice(runs, func(i int, j int) bool {
		return runs[i].ScheduledFor < runs[j].ScheduledFor
	})

	beforeCheck := runFilter.BeforeTime != ""
	afterCheck := runFilter.AfterTime != ""

	afterIndex := 0
	beforeIndex := len(runs)

	for i, run := range runs {
		if afterIndex == 0 && runFilter.After != nil && runFilter.After.String() == run.ID.String() {
			afterIndex = i
		}

		if run.ScheduledFor != "" {
			if afterCheck && afterIndex == 0 && run.ScheduledFor > runFilter.AfterTime {
				afterIndex = i
			}

			if beforeCheck && beforeIndex == len(runs) && runFilter.BeforeTime < run.ScheduledFor {
				beforeIndex = i
				break
			}
		}

	}

	if runFilter.Limit != 0 && beforeIndex-afterIndex > runFilter.Limit {
		beforeIndex = afterIndex + runFilter.Limit
	}

	return runs[afterIndex:beforeIndex], nil
}

func (r *runReaderWriter) FindRunByID(ctx context.Context, taskID, runID platform.ID) (*platform.Run, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	run, ok := r.byRunID[runID.String()]
	if !ok {
		return nil, ErrRunNotFound
	}

	var rtnRun platform.Run
	rtnRun = *run
	return &rtnRun, nil
}

func (r *runReaderWriter) ListLogs(ctx context.Context, logFilter platform.LogFilter) ([]platform.Log, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if logFilter.Task == nil && logFilter.Run == nil {
		return nil, errors.New("task or run is required")
	}

	if logFilter.Run != nil {
		run, ok := r.byRunID[logFilter.Run.String()]
		if !ok {
			return nil, ErrRunNotFound
		}
		return []platform.Log{run.Log}, nil
	}

	logs := []platform.Log{}
	for _, run := range r.byTaskID[logFilter.Task.String()] {
		logs = append(logs, run.Log)
	}
	return logs, nil
}
