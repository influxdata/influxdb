package gen

import (
	"math/rand"
	"time"

	"github.com/influxdata/influxdb/tsdb/cursors"
)

type FloatRandomValuesSequence struct {
	buf   FloatArray
	vals  FloatArray
	n     int
	t     int64
	state struct {
		n     int
		t     int64
		d     int64
		scale float64
	}
}

func NewFloatRandomValuesSequence(n int, start time.Time, delta time.Duration, scale float64) *FloatRandomValuesSequence {
	g := &FloatRandomValuesSequence{
		buf: *NewFloatArrayLen(cursors.DefaultMaxPointsPerBlock),
	}
	g.state.n = n
	g.state.t = start.UnixNano()
	g.state.d = int64(delta)
	g.state.scale = scale
	g.Reset()
	return g
}

func (g *FloatRandomValuesSequence) Reset() {
	g.n = g.state.n
	g.t = g.state.t
}

func (g *FloatRandomValuesSequence) Next() bool {
	if g.n == 0 {
		return false
	}

	c := min(g.n, cursors.DefaultMaxPointsPerBlock)
	g.n -= c
	g.vals.Timestamps = g.buf.Timestamps[:0]
	g.vals.Values = g.buf.Values[:0]

	for i := 0; i < c; i++ {
		g.vals.Timestamps = append(g.vals.Timestamps, g.t)
		g.vals.Values = append(g.vals.Values, rand.Float64()*g.state.scale)
		g.t += g.state.d
	}
	return true
}

func (g *FloatRandomValuesSequence) Values() Values {
	return &g.vals
}
