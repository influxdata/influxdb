package run

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/stretchr/testify/require"
)

type fakeScheduler struct{ when time.Time }

func (f fakeScheduler) When() time.Time { return f.when }

func TestSchedulerPulseCheck_ZeroWhenPasses(t *testing.T) {
	c := NewSchedulerPulseCheck(fakeScheduler{}, DefaultSchedulerPulseThreshold)
	resp := c.Check(context.Background())
	require.Equal(t, check.StatusPass, resp.Status())
	require.Equal(t, msgSchedulerIdle, resp.Message())
	require.Nil(t, resp.Measures())
}

func TestSchedulerPulseCheck_FutureWhenPasses(t *testing.T) {
	const until = DefaultSchedulerPulseThreshold / 3
	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	c := NewSchedulerPulseCheck(fakeScheduler{when: now.Add(until)}, DefaultSchedulerPulseThreshold)
	c.now = func() time.Time { return now }

	resp := c.Check(context.Background())
	require.Equal(t, check.StatusPass, resp.Status())
	require.Equal(t, fmt.Sprintf(msgSchedulerNextRunFmt, until.Round(time.Second)), resp.Message())
	requireDispatch(t, resp, measureKeyNextRunIn, until)
}

func TestSchedulerPulseCheck_SmallLagPasses(t *testing.T) {
	const lag = DefaultSchedulerPulseThreshold / 30
	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	c := NewSchedulerPulseCheck(fakeScheduler{when: now.Add(-lag)}, DefaultSchedulerPulseThreshold)
	c.now = func() time.Time { return now }

	resp := c.Check(context.Background())
	require.Equal(t, check.StatusPass, resp.Status())
	require.Equal(t, fmt.Sprintf(msgSchedulerOnTimeFmt, lag.Round(time.Second)), resp.Message())
	requireDispatch(t, resp, measureKeyLag, lag)
}

func TestSchedulerPulseCheck_AtThresholdPasses(t *testing.T) {
	// lag == threshold → pass. Only strictly greater than threshold fails.
	const lag = DefaultSchedulerPulseThreshold
	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	c := NewSchedulerPulseCheck(fakeScheduler{when: now.Add(-lag)}, DefaultSchedulerPulseThreshold)
	c.now = func() time.Time { return now }

	resp := c.Check(context.Background())
	require.Equal(t, check.StatusPass, resp.Status())
	require.Equal(t, fmt.Sprintf(msgSchedulerOnTimeFmt, lag.Round(time.Second)), resp.Message())
	requireDispatch(t, resp, measureKeyLag, lag)
}

func TestSchedulerPulseCheck_OverThresholdFails(t *testing.T) {
	const lag = 2 * DefaultSchedulerPulseThreshold
	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	c := NewSchedulerPulseCheck(fakeScheduler{when: now.Add(-lag)}, DefaultSchedulerPulseThreshold)
	c.now = func() time.Time { return now }

	resp := c.Check(context.Background())
	require.Equal(t, check.StatusFail, resp.Status())
	require.Equal(t, fmt.Sprintf(msgSchedulerStalledFmt, lag.Round(time.Second)), resp.Message())
	requireDispatch(t, resp, measureKeyLag, lag)
}

// requireDispatch checks that resp reports d, unrounded, as its only dispatch
// value under key.
func requireDispatch(t *testing.T, resp check.Response, key string, d time.Duration) {
	t.Helper()
	require.Equal(t, check.Measures{measureGroupDispatch: {
		Unit:   check.UnitSeconds,
		Values: map[string]float64{key: d.Seconds()},
	}}, resp.Measures())
}

// TestSchedulerPulseCheck_MeasureIsUnrounded pins that the number is the
// exact duration, while the message keeps its whole-second rounding.
func TestSchedulerPulseCheck_MeasureIsUnrounded(t *testing.T) {
	const lag = 1500*time.Millisecond + 250*time.Microsecond
	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	c := NewSchedulerPulseCheck(fakeScheduler{when: now.Add(-lag)}, DefaultSchedulerPulseThreshold)
	c.now = func() time.Time { return now }

	resp := c.Check(context.Background())
	require.Equal(t, fmt.Sprintf(msgSchedulerOnTimeFmt, 2*time.Second), resp.Message())
	requireDispatch(t, resp, measureKeyLag, lag)
	require.InDelta(t, 1.50025, resp.Measures()[measureGroupDispatch].Values[measureKeyLag], 1e-12)
}
