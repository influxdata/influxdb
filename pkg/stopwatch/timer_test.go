package stopwatch_test

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/pkg/stopwatch"
	"github.com/stretchr/testify/assert"
)

func TestTimer(t *testing.T) {
	var sw stopwatch.Timer
	assert.Equal(t, time.Duration(0), sw.Elapsed())

	sw.Start()
	assert.True(t, sw.IsStarted())

	// multiple calls to start are safe
	sw.Start()
	assert.True(t, sw.IsStarted())

	time.Sleep(5 * time.Millisecond)
	sw.Stop()
	assert.False(t, sw.IsStarted())

	// multiple calls to Stop are safe
	sw.Stop()
	assert.False(t, sw.IsStarted())
	assert.GreaterOrEqual(t, int64(sw.Elapsed()), int64(5*time.Millisecond))

	sw.Start()
	time.Sleep(5 * time.Millisecond)
	sw.Stop()
	assert.GreaterOrEqual(t, int64(sw.Elapsed()), int64(10*time.Millisecond))

	sw.Measure(func() {
		assert.True(t, sw.IsStarted())
		time.Sleep(5 * time.Millisecond)
	})
	assert.False(t, sw.IsStarted())
	assert.GreaterOrEqual(t, int64(sw.Elapsed()), int64(15*time.Millisecond))

}
