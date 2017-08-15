package iterators // import "github.com/influxdata/influxdb/query/internal/iterators"

//go:generate tmpl -data=@tmpldata iterators.gen.go.tmpl

import (
	"fmt"

	"github.com/influxdata/influxdb/influxql"
)

type zipIterator struct {
	e     *influxql.Emitter
	itrs  []influxql.Iterator
	point influxql.FloatPoint
}

// Zip creates a new zip iterator. Each of the iterators
// will be read into the auxiliary fields for each point.
func Zip(itrs []influxql.Iterator, ascending bool) influxql.Iterator {
	e := influxql.NewEmitter(itrs, ascending, 0)
	e.OmitTime = true
	return &zipIterator{
		e:    e,
		itrs: itrs,
		point: influxql.FloatPoint{
			Aux: make([]interface{}, len(itrs)),
		},
	}
}

func (itr *zipIterator) Next() (*influxql.FloatPoint, error) {
	t, name, tags, err := itr.e.LoadBuf()
	if err != nil || t == influxql.ZeroTime {
		return nil, err
	}
	itr.point.Time = t
	itr.point.Name = name
	itr.point.Tags = tags

	itr.e.ReadInto(t, name, tags, itr.point.Aux)
	return &itr.point, nil
}

func (itr *zipIterator) Stats() influxql.IteratorStats {
	stats := influxql.IteratorStats{}
	for _, itr := range itr.itrs {
		stats.Add(itr.Stats())
	}
	return stats
}

func (itr *zipIterator) Close() error {
	return itr.e.Close()
}

func Filter(input influxql.Iterator, fields map[string]influxql.IteratorMap, cond influxql.Expr) influxql.Iterator {
	switch input := input.(type) {
	case influxql.FloatIterator:
		return newFloatFilterIterator(input, fields, cond)
	case influxql.IntegerIterator:
		return newIntegerFilterIterator(input, fields, cond)
	case influxql.StringIterator:
		return newStringFilterIterator(input, fields, cond)
	case influxql.BooleanIterator:
		return newBooleanFilterIterator(input, fields, cond)
	default:
		panic(fmt.Sprintf("unsupported filter iterator type: %T", input))
	}
}

func Map(input influxql.Iterator, drivers []influxql.IteratorMap, dimensions []string) influxql.IteratorMapper {
	return influxql.NewIteratorMapper(input, drivers, dimensions)
}
