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
		whenStr := when.UTC().Format(time.RFC3339)
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
	log = fmt.Sprintf("%s: %s", when.Format(time.RFC3339), log)
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

	runs = runs[afterIndex:beforeIndex]
	for i := range runs {
		// Copy every element, to avoid a data race if the original Run is modified in UpdateRunState or AddRunLog.
		r := *runs[i]
		runs[i] = &r
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
