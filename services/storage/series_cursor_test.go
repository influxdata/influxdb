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

	makeFields := func(s ...string) []field {
		f := make([]field, len(s))
		for i := range s {
			f[i].n = s[i]
			f[i].nb = []byte(s[i])
		}
		return f
	}

	p := &indexSeriesCursor{
		sqry:            sqry,
		fields:          measurementFields{"cpu": makeFields("system", "user"), "mem": makeFields("val")},
		cond:            cond,
		measurementCond: influxql.Reduce(RewriteExprRemoveFieldValue(influxql.CloneExpr(cond)), nil),
	}

	var keys []string
	row := p.Next()
	for row != nil {
		keys = append(keys, string(models.MakeKey(row.Name, row.SeriesTags))+" "+row.Field)
		row = p.Next()
	}

	exp := []string{"cpu,host=host1 system", "cpu,host=host1 user", "mem,host=host1 val"}
	if !cmp.Equal(keys, exp) {
		t.Errorf("unexpected -got/+want\n%s", cmp.Diff(keys, exp))
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
