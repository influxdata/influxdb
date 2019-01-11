package metrics

import (
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
)

func TestRegistry_MustRegisterCounter(t *testing.T) {
	r := NewRegistry()
	id := r.MustRegisterCounter("counter")
	assert.Equal(t, id, ID(0), "invalid id")
}

func TestRegistry_MustRegisterCounter_Panics(t *testing.T) {
	r := NewRegistry()
	r.MustRegisterCounter("counter")
	assert.PanicsWithValue(t, "metric name 'counter' already in use", func() {
		r.MustRegisterCounter("counter")
	})
}

func TestRegistry_NewGroup_CounterIsZero(t *testing.T) {
	r := NewRegistry()
	id := r.MustRegisterCounter("counter")

	c := r.NewGroup(DefaultGroup).GetCounter(id)
	c.Add(1)
	assert.Equal(t, int64(1), c.Value())

	c = r.NewGroup(DefaultGroup).GetCounter(id)
	assert.Equal(t, int64(0), c.Value())
}

func TestRegistry_MustRegisterTimer(t *testing.T) {
	r := NewRegistry()
	id := r.MustRegisterTimer("timer")
	assert.Equal(t, ID(0), id, "invalid id")
}

func TestRegistry_MustRegisterTimer_Panics(t *testing.T) {
	r := NewRegistry()
	r.MustRegisterCounter("timer")
	assert.PanicsWithValue(t, "metric name 'timer' already in use", func() {
		r.MustRegisterCounter("timer")
	})
}

func TestRegistry_MustRegisterMultiple(t *testing.T) {
	r := NewRegistry()
	cnt := r.MustRegisterCounter("counter")
	tmr := r.MustRegisterTimer("timer")
	assert.Equal(t, ID(0), cnt, "invalid id")
	assert.Equal(t, ID(0), tmr, "invalid id")
}

func TestRegistry_MustRegister_Panics_Across_Measurements(t *testing.T) {
	r := NewRegistry()
	r.MustRegisterCounter("foo")
	assert.PanicsWithValue(t, "metric name 'foo' already in use", func() {
		r.MustRegisterCounter("foo")
	})
}
