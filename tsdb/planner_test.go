package tsdb_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/tsdb"
)

// Ensure a simple SELECT statement can be executed.
func TestSelectStatementExecutor_Execute(t *testing.T) {
	e := NewSelectStatementExecutor(mustParseSelectStatement(`SELECT min(value) FROM cpu`))
	defer e.Close()

	e.IteratorCreator.CreateIteratorFn = func(name string, start, end time.Time) (tsdb.Iterator, error) {
		if name != "value" {
			t.Fatalf("unexpected ref name: %s", name)
		}
		return &FloatIterator{Values: []tsdb.FloatValue{
			{Time: time.Unix(0, 0).UTC(), Value: 15},
			{Time: time.Unix(5, 0).UTC(), Value: 10},
		}}, nil
	}

	ch := e.Execute()
	if row := <-ch; !reflect.DeepEqual(row, nil) {
		t.Fatalf("unexpected row: %s", spew.Sdump(row))
	}
}

// SelectStatementExecutor is a test wrapper for tsdb.SelectStatementExecutor.
type SelectStatementExecutor struct {
	*tsdb.SelectStatementExecutor
	IteratorCreator IteratorCreator
}

// NewSelectStatementExecutor returns a new instance of SelectStatementExecutor.
func NewSelectStatementExecutor(stmt *influxql.SelectStatement) *SelectStatementExecutor {
	e := &SelectStatementExecutor{
		SelectStatementExecutor: tsdb.NewSelectStatementExecutor(stmt),
	}
	e.SelectStatementExecutor.IteratorCreator = &e.IteratorCreator
	return e
}

// IteratorCreator is a mockable implementation of SelectStatementExecutor.IteratorCreator.
type IteratorCreator struct {
	CreateIteratorFn func(name string, start, end time.Time) (tsdb.Iterator, error)
}

func (ic *IteratorCreator) CreateIterator(name string, start, end time.Time) (tsdb.Iterator, error) {
	return ic.CreateIteratorFn(name, start, end)
}
