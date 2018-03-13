package storage

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

func TestPlannerCondition(t *testing.T) {
	sqry := &floatIterator{
		Points: []tsdb.SeriesCursorRow{
			{Name: []byte("cpu"), Tags: models.ParseTags([]byte("cpu,host=host1"))},
			{Name: []byte("mem"), Tags: models.ParseTags([]byte("mem,host=host1"))},
		},
	}

	cond, err := influxql.ParseExpr(`(_name = 'cpu' AND (_field = 'user' OR _field = 'system')) OR (_name = 'mem' AND "$" = 0)`)
	if err != nil {
		t.Fatal("ParseExpr", err)
	}

	p := &indexSeriesCursor{
		sqry:            sqry,
		fields:          []string{"user", "system", "val"},
		cond:            cond,
		measurementCond: influxql.Reduce(RewriteExprRemoveFieldValue(influxql.CloneExpr(cond)), nil),
	}

	var keys []string
	row := p.Next()
	for row != nil {
		keys = append(keys, string(models.MakeKey(row.name, row.stags))+" "+row.field)
		row = p.Next()
	}

	exp := []string{"cpu,host=host1 user", "cpu,host=host1 system", "mem,host=host1 user", "mem,host=host1 system", "mem,host=host1 val"}
	if !cmp.Equal(exp, keys) {
		t.Errorf("unexpected, %s", cmp.Diff(exp, keys))
	}
}

// floatIterator is a represents an iterator that reads from a slice.
type floatIterator struct {
	Points []tsdb.SeriesCursorRow
}

func (itr *floatIterator) Close() error {
	return nil
}

func (itr *floatIterator) Next() (*tsdb.SeriesCursorRow, error) {
	if len(itr.Points) == 0 {
		return nil, nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]
	return v, nil
}
