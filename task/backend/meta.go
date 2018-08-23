package backend

import (
	"bytes"
	"errors"
	"math"
	"time"

	"github.com/influxdata/platform"
	cron "gopkg.in/robfig/cron.v2"
)

// This file contains helper methods for the StoreTaskMeta type defined in protobuf.

// FinishRun removes the run matching runID from m's CurrentlyRunning slice,
// and if that run's Now value is greater than m's LatestCompleted value,
// updates the value of LatestCompleted to the run's Now value.
//
// If runID matched a run, FinishRun returns true. Otherwise it returns false.
func (stm *StoreTaskMeta) FinishRun(runID platform.ID) bool {
	for i, runner := range stm.CurrentlyRunning {
		if !bytes.Equal(runner.RunID, runID) {
			continue
		}

		stm.CurrentlyRunning = append(stm.CurrentlyRunning[:i], stm.CurrentlyRunning[i+1:]...)

		rs, re, ra := runner.RangeStart, runner.RangeEnd, runner.RequestedAt
		if rs == 0 && re == 0 && ra == 0 {
			// It must be a naturally scheduled run.
			if runner.Now > stm.LatestCompleted {
				stm.LatestCompleted = runner.Now
			}
		} else {
			// It was a requested run. Check if we need to update a latest completed.
			for _, q := range stm.ManualRuns {
				if q.Start == rs && q.End == re && q.RequestedAt == ra {
					// Match.
					if runner.Now > q.LatestCompleted {
						q.LatestCompleted = runner.Now
					}
					break
				}
			}
		}

		return true
	}
	return false
}

// CreateNextRun attempts to update stm's CurrentlyRunning slice with a new run.
// The new run's now is assigned the earliest possible time according to stm.EffectiveCron,
// that is later than any in-progress run and stm's LatestCompleted timestamp.
// If the run's now would be later than the passed-in now, CreateNextRun returns a RunNotYetDueError.
//
// makeID is a function provided by the caller to create an ID, in case we can create a run.
// Because a StoreTaskMeta doesn't know the ID of the task it belongs to, it never sets RunCreation.Created.TaskID.
func (stm *StoreTaskMeta) CreateNextRun(now int64, makeID func() (platform.ID, error)) (RunCreation, error) {
	if len(stm.CurrentlyRunning) >= int(stm.MaxConcurrency) {
		return RunCreation{}, errors.New("cannot create next run when max concurrency already reached")
	}

	// Not calling stm.DueAt here because we reuse sch.
	// We can definitely optimize (minimize) cron parsing at a later point in time.
	sch, err := cron.Parse(stm.EffectiveCron)
	if err != nil {
		return RunCreation{}, err
	}

	latest := stm.LatestCompleted
	for _, cr := range stm.CurrentlyRunning {
		if cr.Now > latest {
			latest = cr.Now
		}
	}

	nextScheduled := sch.Next(time.Unix(latest, 0))
	nextScheduledUnix := nextScheduled.Unix()
	if dueAt := nextScheduledUnix + int64(stm.Delay); dueAt > now {
		// Can't schedule yet.
		if len(stm.ManualRuns) > 0 {
			return stm.createNextRunFromQueue(now, dueAt, sch, makeID)
		}
		return RunCreation{}, RunNotYetDueError{DueAt: dueAt}
	}

	id, err := makeID()
	if err != nil {
		return RunCreation{}, err
	}

	stm.CurrentlyRunning = append(stm.CurrentlyRunning, &StoreTaskMetaRun{
		Now:   nextScheduledUnix,
		Try:   1,
		RunID: id,
	})

	return RunCreation{
		Created: QueuedRun{
			RunID: id,
			Now:   nextScheduledUnix,
		},
		NextDue:  sch.Next(nextScheduled).Unix() + int64(stm.Delay),
		HasQueue: len(stm.ManualRuns) > 0,
	}, nil
}

// createNextRunFromQueue creates the next run from a queue.
// This should only be called when the queue is not empty.
func (stm *StoreTaskMeta) createNextRunFromQueue(now, nextDue int64, sch cron.Schedule, makeID func() (platform.ID, error)) (RunCreation, error) {
	if len(stm.ManualRuns) == 0 {
		return RunCreation{}, errors.New("cannot create run from empty queue")
	}

	q := stm.ManualRuns[0]
	latest := q.LatestCompleted
	for _, r := range stm.CurrentlyRunning {
		if r.RangeStart != q.Start || r.RangeEnd != q.End || r.RequestedAt != q.RequestedAt {
			// Doesn't match our queue.
			continue
		}
		if r.Now > latest {
			latest = r.Now
		}
	}

	runNow := sch.Next(time.Unix(latest, 0)).Unix()

	// Already validated that we have room to create another run, in CreateNextRun.
	id, err := makeID()
	if err != nil {
		return RunCreation{}, err
	}
	stm.CurrentlyRunning = append(stm.CurrentlyRunning, &StoreTaskMetaRun{
		Now:   runNow,
		Try:   1,
		RunID: id,

		RangeStart:  q.Start,
		RangeEnd:    q.End,
		RequestedAt: q.RequestedAt,
	})

	if runNow >= q.End {
		// Drop the queue.
		stm.ManualRuns = append(stm.ManualRuns[:0], stm.ManualRuns[1:]...)
	}

	return RunCreation{
		Created: QueuedRun{
			RunID: id,
			Now:   runNow,
		},
		NextDue:  nextDue,
		HasQueue: len(stm.ManualRuns) > 0,
	}, nil
}

// NextDueRun returns the Unix timestamp of when the next call to CreateNextRun will be ready.
// The returned timestamp reflects the task's delay, so it does not necessarily exactly match the schedule time.
func (stm *StoreTaskMeta) NextDueRun() (int64, error) {
	sch, err := cron.Parse(stm.EffectiveCron)
	if err != nil {
		return 0, err
	}

	latest := stm.LatestCompleted
	for _, cr := range stm.CurrentlyRunning {
		if cr.Now > latest {
			latest = cr.Now
		}
	}

	return sch.Next(time.Unix(latest, 0)).Unix() + int64(stm.Delay), nil
}

// ManuallyRunTimeRange requests a manual run covering the approximate range specified by the Unix timestamps start and end.
// More specifically, it requests runs scheduled no earlier than start, but possibly later than start,
// if start does not land on the task's schedule; and as late as, but not necessarily equal to, end.
// requestedAt is the Unix timestamp indicating when this run range was requested.
//
// If adding the range would exceed the queue size, ManuallyRunTimeRange returns ErrManualQueueFull.
func (stm *StoreTaskMeta) ManuallyRunTimeRange(start, end, requestedAt int64) error {
	// Arbitrarily chosen upper limit that seems unlikely to be reached except in pathological cases.
	const maxQueueSize = 32
	if len(stm.ManualRuns) >= maxQueueSize {
		return ErrManualQueueFull
	}

	lc := start - 1
	if start == math.MinInt64 {
		// Don't roll over in pathological case of starting at minimum int64.
		lc = start
	}
	run := &StoreTaskMetaManualRun{
		Start:           start,
		End:             end,
		LatestCompleted: lc,
		RequestedAt:     requestedAt,
	}
	stm.ManualRuns = append(stm.ManualRuns, run)

	return nil
}
