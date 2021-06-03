package executor

import (
	"context"
	"sort"

	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/task/options"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
)

// ConcurrencyLimit creates a concurrency limit func that uses the executor to determine
// if the task has exceeded the concurrency limit.
func ConcurrencyLimit(exec *Executor, lang fluxlang.FluxLanguageService) LimitFunc {
	return func(t *taskmodel.Task, r *taskmodel.Run) error {
		o, err := options.FromScriptAST(lang, t.Flux)
		if err != nil {
			return err
		}
		if o.Concurrency == nil {
			return nil
		}

		runs, err := exec.tcs.CurrentlyRunning(context.Background(), t.ID)
		if err != nil {
			return err
		}

		// sort by scheduledFor time because we want to make sure older scheduled for times
		// are higher priority
		sort.SliceStable(runs, func(i, j int) bool {
			runi := runs[i].ScheduledFor

			runj := runs[j].ScheduledFor

			return runi.Before(runj)
		})

		if len(runs) > int(*o.Concurrency) {
			for i, run := range runs {
				if run.ID == r.ID {
					if i >= int(*o.Concurrency) {
						return taskmodel.ErrTaskConcurrencyLimitReached(i - int(*o.Concurrency))
					}
					return nil // no need to keep looping.
				}
			}
			// this run isn't currently running. but we have more run's then the concurrency allows
			return taskmodel.ErrTaskConcurrencyLimitReached(len(runs) - int(*o.Concurrency))
		}
		return nil
	}
}
