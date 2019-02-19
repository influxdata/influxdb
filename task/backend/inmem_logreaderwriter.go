package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	platform "github.com/influxdata/influxdb"
)

// orgtask is used as a key for storing runs by org and task ID.
// This is only relevant for the in-memory run store.
type orgtask struct {
	o, t platform.ID
}

type runReaderWriter struct {
	mu        sync.RWMutex
	byOrgTask map[orgtask][]*platform.Run
	byRunID   map[string]*platform.Run
}

func NewInMemRunReaderWriter() *runReaderWriter {
	return &runReaderWriter{byRunID: map[string]*platform.Run{}, byOrgTask: map[orgtask][]*platform.Run{}}
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
		ot := orgtask{o: rlb.Task.Org, t: rlb.Task.ID}
		r.byOrgTask[ot] = append(r.byOrgTask[ot], run)
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

func (r *runReaderWriter) ListRuns(ctx context.Context, orgID platform.ID, runFilter platform.RunFilter) ([]*platform.Run, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !runFilter.Task.Valid() {
		return nil, errors.New("task is required")
	}

	ex, ok := r.byOrgTask[orgtask{o: orgID, t: runFilter.Task}]
	if !ok {
		return nil, ErrNoRunsFound
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

func (r *runReaderWriter) ListLogs(ctx context.Context, orgID platform.ID, logFilter platform.LogFilter) ([]platform.Log, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !logFilter.Task.Valid() {
		return nil, errors.New("task ID required")
	}

	if logFilter.Run != nil {
		run, ok := r.byRunID[logFilter.Run.String()]
		if !ok {
			return nil, ErrRunNotFound
		}
		// TODO(mr): validate that task ID matches, if task is also set. Needs test.
		return []platform.Log{run.Log}, nil
	}

	var logs []platform.Log
	ot := orgtask{o: orgID, t: logFilter.Task}
	for _, run := range r.byOrgTask[ot] {
		logs = append(logs, run.Log)
	}

	if len(logs) == 0 {
		return nil, ErrNoRunsFound
	}

	return logs, nil
}
