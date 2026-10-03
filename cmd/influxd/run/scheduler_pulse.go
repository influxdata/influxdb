package run

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2/kit/check"
)

const (
	// DefaultSchedulerPulseThreshold is the default wall-clock lag above
	// which the scheduler is considered stalled. Picked to absorb a GC
	// pause or a cold dispatch without being so long that operators miss
	// real wedges.
	DefaultSchedulerPulseThreshold = 30 * time.Second

	msgSchedulerStalledFmt = "scheduler stalled: next run due %s ago"
	msgSchedulerIdle       = "scheduler idle: no scheduled runs"
	msgSchedulerNextRunFmt = "next run in %s"
	msgSchedulerOnTimeFmt  = "on time, dispatch lag %s"

	// Every non-idle state reports one duration under measureGroupDispatch,
	// in seconds: measureKeyLag when the next run is due or overdue,
	// measureKeyNextRunIn while it is still in the future.
	measureGroupDispatch = "dispatch"
	measureKeyLag        = "lag"
	measureKeyNextRunIn  = "next_run_in"
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

// Check returns StatusPass when the scheduler has no pending work or its
// next-run timestamp is in the future / within threshold; StatusFail when
// the next-run timestamp is in the past by more than threshold. Pass
// responses carry an Info message distinguishing idle, waiting-on-timer,
// and recently-dispatched states.
func (c *SchedulerPulseCheck) Check(_ context.Context) check.Response {
	w := c.sched.When()
	if w.IsZero() {
		return check.Info(msgSchedulerIdle)
	}
	now := c.now()
	deadline := w.Add(c.threshold)

	if now.After(deadline) {
		lag := now.Sub(w)
		return check.Fail(fmt.Sprintf(msgSchedulerStalledFmt, lag.Round(time.Second))).
			WithMeasure(measureGroupDispatch, dispatchMeasure(measureKeyLag, lag))
	}
	if now.Before(w) {
		until := w.Sub(now)
		return check.Info(msgSchedulerNextRunFmt, until.Round(time.Second)).
			WithMeasure(measureGroupDispatch, dispatchMeasure(measureKeyNextRunIn, until))
	}
	lag := now.Sub(w)
	return check.Info(msgSchedulerOnTimeFmt, lag.Round(time.Second)).
		WithMeasure(measureGroupDispatch, dispatchMeasure(measureKeyLag, lag))
}

// dispatchMeasure reports d, unrounded, under key in seconds.
func dispatchMeasure(key string, d time.Duration) check.Measure {
	return check.Measure{Unit: check.UnitSeconds, Values: map[string]float64{key: d.Seconds()}}
}
