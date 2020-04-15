package mock

import (
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/data/gen"
	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

type GeneratorResultSet struct {
	sg  gen.SeriesGenerator
	f   floatTimeValuesGeneratorCursor
	i   integerTimeValuesGeneratorCursor
	u   unsignedTimeValuesGeneratorCursor
	s   stringTimeValuesGeneratorCursor
	b   booleanTimeValuesGeneratorCursor
	cur cursors.Cursor
}

var _ reads.ResultSet = (*GeneratorResultSet)(nil)

// NewResultSetFromSeriesGenerator transforms a SeriesGenerator into a ResultSet,
// which is useful for mocking data when a client requires a ResultSet.
func NewResultSetFromSeriesGenerator(sg gen.SeriesGenerator) *GeneratorResultSet {
	return &GeneratorResultSet{sg: sg}
}

func (g *GeneratorResultSet) Next() bool {
	return g.sg.Next()
}

func (g *GeneratorResultSet) Cursor() cursors.Cursor {
	switch g.sg.FieldType() {
	case models.Float:
		g.f.tv = g.sg.TimeValuesGenerator()
		g.cur = &g.f
	case models.Integer:
		g.i.tv = g.sg.TimeValuesGenerator()
		g.cur = &g.i
	case models.Unsigned:
		g.u.tv = g.sg.TimeValuesGenerator()
		g.cur = &g.u
	case models.String:
		g.s.tv = g.sg.TimeValuesGenerator()
		g.cur = &g.s
	case models.Boolean:
		g.b.tv = g.sg.TimeValuesGenerator()
		g.cur = &g.b
	default:
		panic("unreachable")
	}

	return g.cur
}

func (g *GeneratorResultSet) Tags() models.Tags { return g.sg.Tags() }
func (g *GeneratorResultSet) Close()            {}
func (g *GeneratorResultSet) Err() error        { return nil }

func (g *GeneratorResultSet) Stats() cursors.CursorStats {
	var stats cursors.CursorStats
	stats.Add(g.f.Stats())
	stats.Add(g.i.Stats())
	stats.Add(g.u.Stats())
	stats.Add(g.s.Stats())
	stats.Add(g.b.Stats())
	return stats
}

// cursors

type timeValuesGeneratorCursor struct {
	tv    gen.TimeValuesSequence
	stats cursors.CursorStats
}

func (t timeValuesGeneratorCursor) Close()                     {}
func (t timeValuesGeneratorCursor) Err() error                 { return nil }
func (t timeValuesGeneratorCursor) Stats() cursors.CursorStats { return t.stats }

type floatTimeValuesGeneratorCursor struct {
	timeValuesGeneratorCursor
	a cursors.FloatArray
}

func (c *floatTimeValuesGeneratorCursor) Next() *cursors.FloatArray {
	if c.tv.Next() {
		c.tv.Values().(gen.FloatValues).Copy(&c.a)
		c.stats.ScannedBytes += len(c.a.Values) * 8
		c.stats.ScannedValues += c.a.Len()
	} else {
		c.a.Timestamps = c.a.Timestamps[:0]
		c.a.Values = c.a.Values[:0]
	}
	return &c.a
}

type integerTimeValuesGeneratorCursor struct {
	timeValuesGeneratorCursor
	a cursors.IntegerArray
}

func (c *integerTimeValuesGeneratorCursor) Next() *cursors.IntegerArray {
	if c.tv.Next() {
		c.tv.Values().(gen.IntegerValues).Copy(&c.a)
		c.stats.ScannedBytes += len(c.a.Values) * 8
		c.stats.ScannedValues += c.a.Len()
	} else {
		c.a.Timestamps = c.a.Timestamps[:0]
		c.a.Values = c.a.Values[:0]
	}
	return &c.a
}

type unsignedTimeValuesGeneratorCursor struct {
	timeValuesGeneratorCursor
	a cursors.UnsignedArray
}

func (c *unsignedTimeValuesGeneratorCursor) Next() *cursors.UnsignedArray {
	if c.tv.Next() {
		c.tv.Values().(gen.UnsignedValues).Copy(&c.a)
		c.stats.ScannedBytes += len(c.a.Values) * 8
		c.stats.ScannedValues += c.a.Len()
	} else {
		c.a.Timestamps = c.a.Timestamps[:0]
		c.a.Values = c.a.Values[:0]
	}
	return &c.a
}

type stringTimeValuesGeneratorCursor struct {
	timeValuesGeneratorCursor
	a cursors.StringArray
}

func (c *stringTimeValuesGeneratorCursor) Next() *cursors.StringArray {
	if c.tv.Next() {
		c.tv.Values().(gen.StringValues).Copy(&c.a)
		for _, v := range c.a.Values {
			c.stats.ScannedBytes += len(v)
		}
		c.stats.ScannedValues += c.a.Len()
	} else {
		c.a.Timestamps = c.a.Timestamps[:0]
		c.a.Values = c.a.Values[:0]
	}
	return &c.a
}

type booleanTimeValuesGeneratorCursor struct {
	timeValuesGeneratorCursor
	a cursors.BooleanArray
}

func (c *booleanTimeValuesGeneratorCursor) Next() *cursors.BooleanArray {
	if c.tv.Next() {
		c.tv.Values().(gen.BooleanValues).Copy(&c.a)
		c.stats.ScannedBytes += len(c.a.Values)
		c.stats.ScannedValues += c.a.Len()
	} else {
		c.a.Timestamps = c.a.Timestamps[:0]
		c.a.Values = c.a.Values[:0]
	}
	return &c.a
}
