package reads_test

import (
	"context"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/data/gen"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

type seriesGeneratorCursorIterator struct {
	g   gen.SeriesGenerator
	f   floatTimeValuesGeneratorCursor
	i   integerTimeValuesGeneratorCursor
	u   unsignedTimeValuesGeneratorCursor
	s   stringTimeValuesGeneratorCursor
	b   booleanTimeValuesGeneratorCursor
	cur cursors.Cursor
}

func (ci *seriesGeneratorCursorIterator) Next(ctx context.Context, r *cursors.CursorRequest) (cursors.Cursor, error) {
	switch ci.g.FieldType() {
	case models.Float:
		ci.f.tv = ci.g.TimeValuesGenerator()
		ci.cur = &ci.f
	case models.Integer:
		ci.i.tv = ci.g.TimeValuesGenerator()
		ci.cur = &ci.i
	case models.Unsigned:
		ci.u.tv = ci.g.TimeValuesGenerator()
		ci.cur = &ci.u
	case models.String:
		ci.s.tv = ci.g.TimeValuesGenerator()
		ci.cur = &ci.s
	case models.Boolean:
		ci.b.tv = ci.g.TimeValuesGenerator()
		ci.cur = &ci.b
	default:
		panic("unreachable")
	}

	return ci.cur, nil
}

func (ci *seriesGeneratorCursorIterator) Stats() cursors.CursorStats {
	return ci.cur.Stats()
}

type seriesGeneratorSeriesCursor struct {
	ci seriesGeneratorCursorIterator
	r  reads.SeriesRow
}

func newSeriesGeneratorSeriesCursor(g gen.SeriesGenerator) *seriesGeneratorSeriesCursor {
	s := &seriesGeneratorSeriesCursor{}
	s.ci.g = g
	s.r.Query = &s.ci
	return s
}

func (s *seriesGeneratorSeriesCursor) Close()     {}
func (s *seriesGeneratorSeriesCursor) Err() error { return nil }

func (s *seriesGeneratorSeriesCursor) Next() *reads.SeriesRow {
	if s.ci.g.Next() {
		s.r.SeriesTags = s.ci.g.Tags()
		s.r.Tags = s.ci.g.Tags()
		return &s.r
	}
	return nil
}

type timeValuesGeneratorCursor struct {
	tv    gen.TimeValuesSequence
	stats cursors.CursorStats
}

func (t timeValuesGeneratorCursor) Close()                     {}
func (t timeValuesGeneratorCursor) Err() error                 { return nil }
func (t timeValuesGeneratorCursor) Stats() cursors.CursorStats { return t.stats }

type floatTimeValuesGeneratorCursor struct {
	timeValuesGeneratorCursor
	a tsdb.FloatArray
}

func (c *floatTimeValuesGeneratorCursor) Next() *cursors.FloatArray {
	if c.tv.Next() {
		c.tv.Values().(gen.FloatValues).Copy(&c.a)
	} else {
		c.a.Timestamps = c.a.Timestamps[:0]
		c.a.Values = c.a.Values[:0]
	}
	c.stats.ScannedBytes += len(c.a.Values) * 8
	c.stats.ScannedValues += c.a.Len()
	return &c.a
}

type integerTimeValuesGeneratorCursor struct {
	timeValuesGeneratorCursor
	a tsdb.IntegerArray
}

func (c *integerTimeValuesGeneratorCursor) Next() *cursors.IntegerArray {
	if c.tv.Next() {
		c.tv.Values().(gen.IntegerValues).Copy(&c.a)
	} else {
		c.a.Timestamps = c.a.Timestamps[:0]
		c.a.Values = c.a.Values[:0]
	}
	c.stats.ScannedBytes += len(c.a.Values) * 8
	c.stats.ScannedValues += c.a.Len()
	return &c.a
}

type unsignedTimeValuesGeneratorCursor struct {
	timeValuesGeneratorCursor
	a tsdb.UnsignedArray
}

func (c *unsignedTimeValuesGeneratorCursor) Next() *cursors.UnsignedArray {
	if c.tv.Next() {
		c.tv.Values().(gen.UnsignedValues).Copy(&c.a)
	} else {
		c.a.Timestamps = c.a.Timestamps[:0]
		c.a.Values = c.a.Values[:0]
	}
	c.stats.ScannedBytes += len(c.a.Values) * 8
	c.stats.ScannedValues += c.a.Len()
	return &c.a
}

type stringTimeValuesGeneratorCursor struct {
	timeValuesGeneratorCursor
	a tsdb.StringArray
}

func (c *stringTimeValuesGeneratorCursor) Next() *cursors.StringArray {
	if c.tv.Next() {
		c.tv.Values().(gen.StringValues).Copy(&c.a)
	} else {
		c.a.Timestamps = c.a.Timestamps[:0]
		c.a.Values = c.a.Values[:0]
	}
	for _, v := range c.a.Values {
		c.stats.ScannedBytes += len(v)
	}
	c.stats.ScannedValues += c.a.Len()
	return &c.a
}

type booleanTimeValuesGeneratorCursor struct {
	timeValuesGeneratorCursor
	a tsdb.BooleanArray
}

func (c *booleanTimeValuesGeneratorCursor) Next() *cursors.BooleanArray {
	if c.tv.Next() {
		c.tv.Values().(gen.BooleanValues).Copy(&c.a)
	} else {
		c.a.Timestamps = c.a.Timestamps[:0]
		c.a.Values = c.a.Values[:0]
	}
	c.stats.ScannedBytes += len(c.a.Values)
	c.stats.ScannedValues += c.a.Len()
	return &c.a
}
