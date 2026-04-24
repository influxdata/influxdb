package run

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2/kit/check"
)

const (
	// TaskSchedulerCheckName is the name surfaced on /health for the task
	// scheduler pulse check.
	TaskSchedulerCheckName = "task-scheduler"

	// DefaultSchedulerPulseThreshold is the default wall-clock lag above
	// which the scheduler is considered stalled. Picked to absorb a GC
	// pause or a cold dispatch without being so long that operators miss
	// real wedges.
	DefaultSchedulerPulseThreshold = 30 * time.Second

	msgSchedulerStalledFmt = "scheduler stalled: next run due %s ago"
)

// NextRunScheduled is implemented by task schedulers that expose the time at
// which their next run is due. A zero time means nothing is scheduled.
type NextRunScheduled interface {
	When() time.Time
}

// SchedulerPulseCheck is a health check that reports StatusFail when the
// task scheduler's next-run timestamp has fallen behind wall time by more
// than threshold — which indicates the scheduler's main loop is stalled
// (timer fired but process() never ran).
type SchedulerPulseCheck struct {
	sched     NextRunScheduled
	threshold time.Duration
	now       func() time.Time
}

// NewSchedulerPulseCheck returns a check that fails when sched.When()
// returns a non-zero time older than threshold relative to now.
func NewSchedulerPulseCheck(sched NextRunScheduled, threshold time.Duration) *SchedulerPulseCheck {
	return &SchedulerPulseCheck{
		sched:     sched,
		threshold: threshold,
		now:       time.Now,
	}
}

// CheckName reports the check name used when registered with check.Check.
func (*SchedulerPulseCheck) CheckName() string { return TaskSchedulerCheckName }

// Check returns StatusPass when the scheduler has no pending work or its
// next-run timestamp is in the future / within threshold; StatusFail when
// the next-run timestamp is in the past by more than threshold.
func (c *SchedulerPulseCheck) Check(_ context.Context) check.Response {
	w := c.sched.When()
	if w.IsZero() {
		return check.Pass()
	}
	lag := c.now().Sub(w)
	if lag > c.threshold {
		return check.Response{
			Status:  check.StatusFail,
			Message: fmt.Sprintf(msgSchedulerStalledFmt, lag.Round(time.Second)),
		}
	}
	return check.Pass()
}
