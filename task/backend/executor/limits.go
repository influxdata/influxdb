package executor

import (
	"context"
	"sort"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/options"
)

// ConcurrencyLimit creates a concurrency limit func that uses the executor to determine
// if the task has exceeded the concurrency limit.
func ConcurrencyLimit(exec *Executor) LimitFunc {
	return func(t *influxdb.Task, r *influxdb.Run) error {
		o, err := options.FromScript(t.Flux)
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
						return influxdb.ErrTaskConcurrencyLimitReached(i - int(*o.Concurrency))
					}
					return nil // no need to keep looping.
				}
			}
			// this run isn't currently running. but we have more run's then the concurrency allows
			return influxdb.ErrTaskConcurrencyLimitReached(len(runs) - int(*o.Concurrency))
		}
		return nil
	}
}
