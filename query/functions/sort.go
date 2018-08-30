package functions

import (
	"fmt"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/interpreter"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
)

const SortKind = "sort"

type SortOpSpec struct {
	Cols []string `json:"cols"`
	Desc bool     `json:"desc"`
}

var sortSignature = query.DefaultFunctionSignature()

func init() {
	sortSignature.Params["cols"] = semantic.NewArrayType(semantic.String)
	sortSignature.Params["desc"] = semantic.Bool

	query.RegisterFunction(SortKind, createSortOpSpec, sortSignature)
	query.RegisterOpSpec(SortKind, newSortOp)
	plan.RegisterProcedureSpec(SortKind, newSortProcedure, SortKind)
	execute.RegisterTransformation(SortKind, createSortTransformation)
}

func createSortOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(SortOpSpec)

	if array, ok, err := args.GetArray("cols", semantic.String); err != nil {
		return nil, err
	} else if ok {
		spec.Cols, err = interpreter.ToStringArray(array)
		if err != nil {
			return nil, err
		}
	} else {
		//Default behavior to sort by value
		spec.Cols = []string{execute.DefaultValueColLabel}
	}

	if desc, ok, err := args.GetBool("desc"); err != nil {
		return nil, err
	} else if ok {
		spec.Desc = desc
	}

	return spec, nil
}

func newSortOp() query.OperationSpec {
	return new(SortOpSpec)
}

func (s *SortOpSpec) Kind() query.OperationKind {
	return SortKind
}

type SortProcedureSpec struct {
	Cols []string
	Desc bool
}

func newSortProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*SortOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &SortProcedureSpec{
		Cols: spec.Cols,
		Desc: spec.Desc,
	}, nil
}

func (s *SortProcedureSpec) Kind() plan.ProcedureKind {
	return SortKind
}
func (s *SortProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(SortProcedureSpec)

	ns.Cols = make([]string, len(s.Cols))
	copy(ns.Cols, s.Cols)

	ns.Desc = s.Desc
	return ns
}

func createSortTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*SortProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewSortTransformation(d, cache, s)
	return t, d, nil
}

type sortTransformation struct {
	d     execute.Dataset
	cache execute.TableBuilderCache

	cols []string
	desc bool
}

func NewSortTransformation(d execute.Dataset, cache execute.TableBuilderCache, spec *SortProcedureSpec) *sortTransformation {
	return &sortTransformation{
		d:     d,
		cache: cache,
		cols:  spec.Cols,
		desc:  spec.Desc,
	}
}

func (t *sortTransformation) RetractTable(id execute.DatasetID, key query.GroupKey) error {
	return t.d.RetractTable(key)
}

func (t *sortTransformation) Process(id execute.DatasetID, tbl query.Table) error {
	key := tbl.Key()
	for _, label := range t.cols {
		if key.HasCol(label) {
			key = t.sortedKey(key)
			break
		}
	}

	builder, created := t.cache.TableBuilder(key)
	if !created {
		return fmt.Errorf("sort found duplicate table with key: %v", tbl.Key())
	}
	execute.AddTableCols(tbl, builder)
	execute.AppendTable(tbl, builder)

	builder.Sort(t.cols, t.desc)
	return nil
}

func (t *sortTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *sortTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *sortTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}

func (t *sortTransformation) sortedKey(key query.GroupKey) query.GroupKey {
	cols := make([]query.ColMeta, len(key.Cols()))
	vs := make([]values.Value, len(key.Cols()))
	j := 0
	for _, label := range t.cols {
		idx := execute.ColIdx(label, key.Cols())
		if idx >= 0 {
			cols[j] = key.Cols()[idx]
			vs[j] = key.Value(idx)
			j++
		}
	}
	for idx, c := range key.Cols() {
		if !execute.ContainsStr(t.cols, c.Label) {
			cols[j] = c
			vs[j] = key.Value(idx)
			j++
		}
	}
	return execute.NewGroupKey(cols, vs)
}
