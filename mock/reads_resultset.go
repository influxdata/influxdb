package mock

import (
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/data/gen"
	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

type GeneratorResultSet struct {
	sg    gen.SeriesGenerator
	max   int
	count int
	f     floatTimeValuesGeneratorCursor
	i     integerTimeValuesGeneratorCursor
	u     unsignedTimeValuesGeneratorCursor
	s     stringTimeValuesGeneratorCursor
	b     booleanTimeValuesGeneratorCursor
	cur   cursors.Cursor
}

var _ reads.ResultSet = (*GeneratorResultSet)(nil)

type GeneratorOptionFn func(*GeneratorResultSet)

// WithGeneratorMaxValues limits the number of values
// produced by GeneratorResultSet to n.
func WithGeneratorMaxValues(n int) GeneratorOptionFn {
	return func(g *GeneratorResultSet) {
		g.max = n
	}
}

// NewResultSetFromSeriesGenerator transforms a SeriesGenerator into a ResultSet,
// and therefore may be used anywhere a ResultSet is required.
func NewResultSetFromSeriesGenerator(sg gen.SeriesGenerator, opts ...GeneratorOptionFn) *GeneratorResultSet {
	s := &GeneratorResultSet{sg: sg}

	for _, opt := range opts {
		opt(s)
	}

	s.f.max = s.max
	s.i.max = s.max
	s.u.max = s.max
	s.s.max = s.max
	s.b.max = s.max
	s.f.count = &s.count
	s.i.count = &s.count
	s.u.count = &s.count
	s.s.count = &s.count
	s.b.count = &s.count

	return s
}

func (g *GeneratorResultSet) Next() bool {
	remain := g.max - g.count
	return g.sg.Next() && (g.max == 0 || remain > 0)
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
	max   int
	count *int
	stats cursors.CursorStats
}

func (t timeValuesGeneratorCursor) Close()                     {}
func (t timeValuesGeneratorCursor) Err() error                 { return nil }
func (t timeValuesGeneratorCursor) Stats() cursors.CursorStats { return t.stats }
func (t *timeValuesGeneratorCursor) add(n int)                 { *t.count += n }
func (t *timeValuesGeneratorCursor) checkCount() bool          { return t.max == 0 || *t.count < t.max }
func (t *timeValuesGeneratorCursor) remain() int               { return t.max - *t.count }

type floatTimeValuesGeneratorCursor struct {
	timeValuesGeneratorCursor
	a cursors.FloatArray
}

func (c *floatTimeValuesGeneratorCursor) Next() *cursors.FloatArray {
	if c.checkCount() && c.tv.Next() {
		c.tv.Values().(gen.FloatValues).Copy(&c.a)
		if remain := c.remain(); c.max > 0 && remain < c.a.Len() {
			c.a.Timestamps = c.a.Timestamps[:remain]
			c.a.Values = c.a.Values[:remain]
		}
		c.stats.ScannedBytes += len(c.a.Values) * 8
		c.stats.ScannedValues += c.a.Len()
		c.add(c.a.Len())
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
	if c.checkCount() && c.tv.Next() {
		c.tv.Values().(gen.IntegerValues).Copy(&c.a)
		if remain := c.remain(); c.max > 0 && remain < c.a.Len() {
			c.a.Timestamps = c.a.Timestamps[:remain]
			c.a.Values = c.a.Values[:remain]
		}
		c.stats.ScannedBytes += len(c.a.Values) * 8
		c.stats.ScannedValues += c.a.Len()
		c.add(c.a.Len())
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
	if c.checkCount() && c.tv.Next() {
		c.tv.Values().(gen.UnsignedValues).Copy(&c.a)
		if remain := c.remain(); c.max > 0 && remain < c.a.Len() {
			c.a.Timestamps = c.a.Timestamps[:remain]
			c.a.Values = c.a.Values[:remain]
		}
		c.stats.ScannedBytes += len(c.a.Values) * 8
		c.stats.ScannedValues += c.a.Len()
		c.add(c.a.Len())
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
	if c.checkCount() && c.tv.Next() {
		c.tv.Values().(gen.StringValues).Copy(&c.a)
		if remain := c.remain(); c.max > 0 && remain < c.a.Len() {
			c.a.Timestamps = c.a.Timestamps[:remain]
			c.a.Values = c.a.Values[:remain]
		}
		for _, v := range c.a.Values {
			c.stats.ScannedBytes += len(v)
		}
		c.stats.ScannedValues += c.a.Len()
		c.add(c.a.Len())
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
	if c.checkCount() && c.tv.Next() {
		c.tv.Values().(gen.BooleanValues).Copy(&c.a)
		if remain := c.remain(); c.max > 0 && remain < c.a.Len() {
			c.a.Timestamps = c.a.Timestamps[:remain]
			c.a.Values = c.a.Values[:remain]
		}
		c.stats.ScannedBytes += len(c.a.Values)
		c.stats.ScannedValues += c.a.Len()
		c.add(c.a.Len())
	} else {
		c.a.Timestamps = c.a.Timestamps[:0]
		c.a.Values = c.a.Values[:0]
	}
	return &c.a
}
