package exec

import (
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
	// The returned value may be cached.
	Name() []byte

	// Tags returns the tag set.
	// The returned value may be cached.
	Tags() models.Tags

	// Field returns the name of the field.
	// The returned value may be cached.
	Field() []byte

	// ValuesGenerator returns a values sequence for the current series.
	ValuesGenerator() gen.ValuesSequence
}

type cache struct {
	key  []byte
	tags models.Tags
}

type seriesGenerator struct {
	name  []byte
	tags  gen.TagsSequence
	field []byte
	vg    gen.ValuesSequence

	c cache
}

func NewSeriesGenerator(name []byte, field []byte, vg gen.ValuesSequence, tags gen.TagsSequence) SeriesGenerator {
	return &seriesGenerator{
		name:  name,
		field: field,
		vg:    vg,
		tags:  tags,
	}
}

func (g *seriesGenerator) Next() bool {
	if g.tags.Next() {
		g.c = cache{}
		g.vg.Reset()
		return true
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

func (g *seriesGenerator) ValuesGenerator() gen.ValuesSequence {
	return g.vg
}
