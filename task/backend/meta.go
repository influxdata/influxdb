package backend

import (
	"bytes"
	"errors"
	"time"

	"github.com/influxdata/platform"
	cron "gopkg.in/robfig/cron.v2"
)

// This file contains helper methods for the StoreTaskMeta type defined in protobuf.

// FinishRun removes the run matching runID from m's CurrentlyRunning slice,
// and if that run's Now value is greater than m's LastCompleted value,
// updates the value of LastCompleted to the run's Now value.
//
// If runID matched a run, FinishRun returns true. Otherwise it returns false.
func (stm *StoreTaskMeta) FinishRun(runID platform.ID) bool {
	for i, runner := range stm.CurrentlyRunning {
		if bytes.Equal(runner.RunID, runID) {
			stm.CurrentlyRunning = append(stm.CurrentlyRunning[:i], stm.CurrentlyRunning[i+1:]...)
			if runner.Now > stm.LastCompleted {
				stm.LastCompleted = runner.Now
				return true
			}
		}
	}
	return false
}

// CreateNextRun attempts to update stm's CurrentlyRunning slice with a new run
// whose now time is no later than the given now.
// makeID is a function provided by the caller to create an ID, in case we can create a run.
// Because a StoreTaskMeta doesn't know the ID of the task it belongs to, it never sets RunCreation.Created.TaskID.
func (stm *StoreTaskMeta) CreateNextRun(now int64, makeID func() (platform.ID, error)) (RunCreation, error) {
	if len(stm.CurrentlyRunning) >= int(stm.MaxConcurrency) {
		return RunCreation{}, errors.New("cannot create next run when max concurrency already reached")
	}

	sch, err := cron.Parse(stm.EffectiveCron)
	if err != nil {
		return RunCreation{}, err
	}

	latest := stm.LastCompleted
	for _, cr := range stm.CurrentlyRunning {
		if cr.Now > latest {
			latest = cr.Now
		}
	}

	nextScheduled := sch.Next(time.Unix(latest, 0))
	nextScheduledUnix := nextScheduled.Unix()
	if dueAt := nextScheduledUnix + int64(stm.Delay); dueAt > now {
		// Can't schedule yet.
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
		NextDue: sch.Next(nextScheduled).Unix() + int64(stm.Delay),
	}, nil
}
