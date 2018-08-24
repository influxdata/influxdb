package executetest

import (
	"github.com/influxdata/platform/query"
)

type Result struct {
	Nm   string
	Tbls []*Table
	Err  error
}

func NewResult(tables []*Table) *Result {
	return &Result{Tbls: tables}
}

func (r *Result) Name() string {
	return r.Nm
}

func (r *Result) Tables() query.TableIterator {
	return &TableIterator{
		r.Tbls,
		r.Err,
	}
}

func (r *Result) Normalize() {
	NormalizeTables(r.Tbls)
}

type TableIterator struct {
	tables []*Table
	err    error
}

func (ti *TableIterator) Do(f func(query.Table) error) error {
	if ti.err != nil {
		return ti.err
	}
	for _, t := range ti.tables {
		if err := f(t); err != nil {
			return err
		}
	}
	return nil
}
