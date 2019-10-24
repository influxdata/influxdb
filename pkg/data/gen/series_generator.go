package gen

import (
	"math"
	"time"

	"github.com/influxdata/influxdb/models"
)

type SeriesGenerator interface {
	// Next advances the series generator to the next series key.
	Next() bool

	// Key returns the series key.
	// The returned value may be cached.
	Key() []byte

	// Name returns the name of the measurement.
	// The returned value may be modified by a subsequent call to Next.
	Name() []byte

	// Tags returns the tag set.
	// The returned value may be modified by a subsequent call to Next.
	Tags() models.Tags

	// Field returns the name of the field.
	// The returned value may be modified by a subsequent call to Next.
	Field() []byte

	// TimeValuesGenerator returns a values sequence for the current series.
	TimeValuesGenerator() TimeValuesSequence
}

type TimeSequenceSpec struct {
	// Count specifies the maximum number of values to generate.
	Count int

	// Start specifies the starting time for the values.
	Start time.Time

	// Delta specifies the interval between time stamps.
	Delta time.Duration

	// Precision specifies the precision of timestamp intervals
	Precision time.Duration
}

func (ts TimeSequenceSpec) ForTimeRange(tr TimeRange) TimeSequenceSpec {
	// Truncate time range
	if ts.Delta > 0 {
		tr = tr.Truncate(ts.Delta)
	} else {
		tr = tr.Truncate(ts.Precision)
	}

	ts.Start = tr.Start

	if ts.Delta > 0 {
		intervals := int(tr.End.Sub(tr.Start) / ts.Delta)
		if intervals > ts.Count {
			// if the number of intervals in the specified time range exceeds
			// the maximum count, move the start forward to limit the number of values
			ts.Start = tr.End.Add(-time.Duration(ts.Count) * ts.Delta)
		} else {
			ts.Count = intervals
		}
	} else {
		ts.Delta = tr.End.Sub(tr.Start) / time.Duration(ts.Count)
		if ts.Delta < ts.Precision {
			// count is too high for the range of time and precision
			ts.Count = int(tr.End.Sub(tr.Start) / ts.Precision)
			ts.Delta = ts.Precision
		} else {
			ts.Delta = ts.Delta.Round(ts.Precision)
		}
		ts.Precision = 0
	}

	return ts
}

type TimeRange struct {
	Start time.Time
	End   time.Time
}

func (t TimeRange) Truncate(d time.Duration) TimeRange {
	return TimeRange{
		Start: t.Start.Truncate(d),
		End:   t.End.Truncate(d),
	}
}

type TimeValuesSequence interface {
	Reset()
	Next() bool
	Values() Values
}

type Values interface {
	MinTime() int64
	MaxTime() int64
	Encode([]byte) ([]byte, error)
}

type cache struct {
	key  []byte
	tags models.Tags
}

type seriesGenerator struct {
	name  []byte
	tags  TagsSequence
	field []byte
	vg    TimeValuesSequence
	n     int64

	c cache
}

func NewSeriesGenerator(name []byte, field []byte, vg TimeValuesSequence, tags TagsSequence) SeriesGenerator {
	return NewSeriesGeneratorLimit(name, field, vg, tags, math.MaxInt64)
}

func NewSeriesGeneratorLimit(name []byte, field []byte, vg TimeValuesSequence, tags TagsSequence, n int64) SeriesGenerator {
	return &seriesGenerator{
		name:  name,
		field: field,
		tags:  tags,
		vg:    vg,
		n:     n,
	}
}

func (g *seriesGenerator) Next() bool {
	if g.n > 0 {
		g.n--
		if g.tags.Next() {
			g.c = cache{}
			g.vg.Reset()
			return true
		}
		g.n = 0
	}

	return false
}

func (g *seriesGenerator) Key() []byte {
	if len(g.c.key) == 0 {
		g.c.key = models.MakeKey(g.name, g.tags.Value())
	}
	return g.c.key
}

func (g *seriesGenerator) Name() []byte {
	return g.name
}

func (g *seriesGenerator) Tags() models.Tags {
	if len(g.c.tags) == 0 {
		g.c.tags = g.tags.Value().Clone()
	}
	return g.c.tags
}

func (g *seriesGenerator) Field() []byte {
	return g.field
}

func (g *seriesGenerator) TimeValuesGenerator() TimeValuesSequence {
	return g.vg
}
