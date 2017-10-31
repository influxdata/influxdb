package storage

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
)

func TestPlannerCondition(t *testing.T) {
	sitr := &floatIterator{
		Points: []query.FloatPoint{
			{Aux: []interface{}{"cpu,host=host1"}},
			{Aux: []interface{}{"mem,host=host1"}},
		},
	}

	cond, err := influxql.ParseExpr(`(_name = 'cpu' AND (_field = 'user' OR _field = 'system')) OR (_name = 'mem' AND "$" = 0)`)
	if err != nil {
		t.Fatal("ParseExpr", err)
	}

	p := &indexSeriesCursor{
		sitr:            sitr,
		fields:          []string{"user", "system", "val"},
		cond:            cond,
		measurementCond: influxql.Reduce(RewriteExprRemoveFieldValue(influxql.CloneExpr(cond)), nil),
	}

	keys := []string{}
	row := p.Next()
	for row != nil {
		keys = append(keys, row.key+" "+row.field)
		row = p.Next()
	}

	exp := []string{"cpu,host=host1 user", "cpu,host=host1 system", "mem,host=host1 user", "mem,host=host1 system", "mem,host=host1 val"}
	if !cmp.Equal(exp, keys) {
		t.Errorf("unexpected, %s", cmp.Diff(exp, keys))
	}
}

// FloatIterator is a represents an iterator that reads from a slice.
type floatIterator struct {
	Points []query.FloatPoint
	stats  query.IteratorStats
}

func (itr *floatIterator) Stats() query.IteratorStats { return itr.stats }
func (itr *floatIterator) Close() error               { return nil }

// Next returns the next value and shifts it off the beginning of the points slice.
func (itr *floatIterator) Next() (*query.FloatPoint, error) {
	if len(itr.Points) == 0 {
		return nil, nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]
	return v, nil
}
