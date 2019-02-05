package gen

import (
	"math"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/platform/pkg/data/gen"
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
	// Count specifies the number of values to generate.
	Count int

	// Start specifies the starting time for the values.
	Start time.Time

	// Delta specifies the interval between time stamps.
	Delta time.Duration

	// Precision specifies the precision of timestamp intervals
	Precision time.Duration
}

type TimeRange struct {
	Start time.Time
	End   time.Time
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
	tags  gen.TagsSequence
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
