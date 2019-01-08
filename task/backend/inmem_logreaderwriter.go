package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	platform "github.com/influxdata/influxdb"
)

type runReaderWriter struct {
	mu       sync.RWMutex
	byTaskID map[string][]*platform.Run
	byRunID  map[string]*platform.Run
}

func NewInMemRunReaderWriter() *runReaderWriter {
	return &runReaderWriter{byRunID: map[string]*platform.Run{}, byTaskID: map[string][]*platform.Run{}}
}

func (r *runReaderWriter) UpdateRunState(ctx context.Context, rlb RunLogBase, when time.Time, status RunStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	timeSetter := func(r *platform.Run) {
		whenStr := when.UTC().Format(time.RFC3339Nano)
		switch status {
		case RunStarted:
			r.StartedAt = whenStr
		case RunFail, RunSuccess, RunCanceled:
			r.FinishedAt = whenStr
		}
	}

	ridStr := rlb.RunID.String()
	existingRun, ok := r.byRunID[ridStr]
	if !ok {
		sf := time.Unix(rlb.RunScheduledFor, 0).UTC()
		run := &platform.Run{
			ID:           rlb.RunID,
			TaskID:       rlb.Task.ID,
			Status:       status.String(),
			ScheduledFor: sf.Format(time.RFC3339),
		}
		if rlb.RequestedAt != 0 {
			run.RequestedAt = time.Unix(rlb.RequestedAt, 0).UTC().Format(time.RFC3339)
		}
		timeSetter(run)
		r.byRunID[ridStr] = run
		tidStr := rlb.Task.ID.String()
		r.byTaskID[tidStr] = append(r.byTaskID[tidStr], run)
		return nil
	}

	timeSetter(existingRun)
	existingRun.Status = status.String()
	return nil
}

func (r *runReaderWriter) AddRunLog(ctx context.Context, rlb RunLogBase, when time.Time, log string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	log = fmt.Sprintf("%s: %s", when.Format(time.RFC3339Nano), log)
	ridStr := rlb.RunID.String()
	existingRun, ok := r.byRunID[ridStr]
	if !ok {
		return ErrRunNotFound
	}
	sep := ""
	if existingRun.Log != "" {
		sep = "\n"
	}
	existingRun.Log = platform.Log(string(existingRun.Log) + sep + log)
	return nil
}

func (r *runReaderWriter) ListRuns(ctx context.Context, runFilter platform.RunFilter) ([]*platform.Run, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if runFilter.Task == nil {
		return nil, errors.New("task is required")
	}

	ex, ok := r.byTaskID[runFilter.Task.String()]
	if !ok {
		return nil, ErrRunNotFound
	}

	afterID := ""
	if runFilter.After != nil {
		afterID = runFilter.After.String()
	}
	runs := make([]*platform.Run, 0, len(ex))
	for _, r := range ex {
		// Skip this entry if we would be filtering it out.
		if runFilter.BeforeTime != "" && runFilter.BeforeTime <= r.ScheduledFor {
			continue
		}
		if runFilter.AfterTime != "" && runFilter.AfterTime >= r.ScheduledFor {
			continue
		}
		if r.ID.String() <= afterID {
			continue
		}

		// Copy the element, to avoid a data race if the original Run is modified in UpdateRunState or AddRunLog.
		r := *r
		runs = append(runs, &r)

		if runFilter.Limit > 0 && len(runs) >= runFilter.Limit {
			break
		}
	}

	return runs, nil
}

func (r *runReaderWriter) FindRunByID(ctx context.Context, orgID, runID platform.ID) (*platform.Run, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	run, ok := r.byRunID[runID.String()]
	if !ok {
		return nil, ErrRunNotFound
	}

	rtnRun := *run
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
