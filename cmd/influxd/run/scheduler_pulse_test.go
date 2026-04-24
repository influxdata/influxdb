package run

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeScheduler struct{ when time.Time }

func (f fakeScheduler) When() time.Time { return f.when }

func TestSchedulerPulseCheck_ZeroWhenPasses(t *testing.T) {
	c := NewSchedulerPulseCheck(fakeScheduler{}, DefaultSchedulerPulseThreshold)
	resp := c.Check(context.Background())
	require.Equal(t, check.StatusPass, resp.Status)
	assert.Empty(t, resp.Message)
}

func TestSchedulerPulseCheck_FutureWhenPasses(t *testing.T) {
	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	c := NewSchedulerPulseCheck(fakeScheduler{when: now.Add(DefaultSchedulerPulseThreshold / 3)}, DefaultSchedulerPulseThreshold)
	c.now = func() time.Time { return now }

	require.Equal(t, check.StatusPass, c.Check(context.Background()).Status)
}

func TestSchedulerPulseCheck_SmallLagPasses(t *testing.T) {
	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	c := NewSchedulerPulseCheck(fakeScheduler{when: now.Add(-DefaultSchedulerPulseThreshold / 30)}, DefaultSchedulerPulseThreshold)
	c.now = func() time.Time { return now }

	require.Equal(t, check.StatusPass, c.Check(context.Background()).Status)
}

func TestSchedulerPulseCheck_AtThresholdPasses(t *testing.T) {
	// lag == threshold → pass. Only strictly greater than threshold fails.
	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	c := NewSchedulerPulseCheck(fakeScheduler{when: now.Add(-DefaultSchedulerPulseThreshold)}, DefaultSchedulerPulseThreshold)
	c.now = func() time.Time { return now }

	require.Equal(t, check.StatusPass, c.Check(context.Background()).Status)
}

func TestSchedulerPulseCheck_OverThresholdFails(t *testing.T) {
	const lag = 2 * DefaultSchedulerPulseThreshold
	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	c := NewSchedulerPulseCheck(fakeScheduler{when: now.Add(-lag)}, DefaultSchedulerPulseThreshold)
	c.now = func() time.Time { return now }

	resp := c.Check(context.Background())
	require.Equal(t, check.StatusFail, resp.Status)
	require.Equal(t, fmt.Sprintf(msgSchedulerStalledFmt, lag.Round(time.Second)), resp.Message)
}

func TestSchedulerPulseCheck_CheckName(t *testing.T) {
	require.Equal(t, TaskSchedulerCheckName, (&SchedulerPulseCheck{}).CheckName())
}
