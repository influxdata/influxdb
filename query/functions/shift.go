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

const ShiftKind = "shift"

type ShiftOpSpec struct {
	Shift   query.Duration `json:"shift"`
	Columns []string       `json:"columns"`
}

var shiftSignature = query.DefaultFunctionSignature()

func init() {
	shiftSignature.Params["shift"] = semantic.Duration

	query.RegisterFunction(ShiftKind, createShiftOpSpec, shiftSignature)
	query.RegisterOpSpec(ShiftKind, newShiftOp)
	plan.RegisterProcedureSpec(ShiftKind, newShiftProcedure, ShiftKind)
	execute.RegisterTransformation(ShiftKind, createShiftTransformation)
}

func createShiftOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(ShiftOpSpec)

	if shift, err := args.GetRequiredDuration("shift"); err != nil {
		return nil, err
	} else {
		spec.Shift = shift
	}

	if cols, ok, err := args.GetArray("columns", semantic.String); err != nil {
		return nil, err
	} else if ok {
		columns, err := interpreter.ToStringArray(cols)
		if err != nil {
			return nil, err
		}
		spec.Columns = columns
	} else {
		spec.Columns = []string{
			execute.DefaultTimeColLabel,
			execute.DefaultStopColLabel,
			execute.DefaultStartColLabel,
		}
	}
	return spec, nil
}

func newShiftOp() query.OperationSpec {
	return new(ShiftOpSpec)
}

func (s *ShiftOpSpec) Kind() query.OperationKind {
	return ShiftKind
}

type ShiftProcedureSpec struct {
	Shift   query.Duration
	Columns []string
}

func newShiftProcedure(qs query.OperationSpec, _ plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*ShiftOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &ShiftProcedureSpec{
		Shift:   spec.Shift,
		Columns: spec.Columns,
	}, nil
}

func (s *ShiftProcedureSpec) Kind() plan.ProcedureKind {
	return ShiftKind
}

func (s *ShiftProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(ShiftProcedureSpec)
	*ns = *s

	if s.Columns != nil {
		ns.Columns = make([]string, len(s.Columns))
		copy(ns.Columns, s.Columns)
	}
	return ns
}

func createShiftTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*ShiftProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewShiftTransformation(d, cache, s)
	return t, d, nil
}

type shiftTransformation struct {
	d       execute.Dataset
	cache   execute.TableBuilderCache
	shift   execute.Duration
	columns []string
}

func NewShiftTransformation(d execute.Dataset, cache execute.TableBuilderCache, spec *ShiftProcedureSpec) *shiftTransformation {
	return &shiftTransformation{
		d:       d,
		cache:   cache,
		shift:   execute.Duration(spec.Shift),
		columns: spec.Columns,
	}
}

func (t *shiftTransformation) RetractTable(id execute.DatasetID, key query.GroupKey) error {
	return t.d.RetractTable(key)
}

func (t *shiftTransformation) Process(id execute.DatasetID, tbl query.Table) error {
	key := tbl.Key()
	// Update key
	cols := make([]query.ColMeta, len(key.Cols()))
	vs := make([]values.Value, len(key.Cols()))
	for j, c := range key.Cols() {
		if execute.ContainsStr(t.columns, c.Label) {
			if c.Type != query.TTime {
				return fmt.Errorf("column %q is not of type time", c.Label)
			}
			cols[j] = c
			vs[j] = values.NewTimeValue(key.ValueTime(j).Add(t.shift))
		} else {
			cols[j] = c
			vs[j] = key.Value(j)
		}
	}
	key = execute.NewGroupKey(cols, vs)

	builder, created := t.cache.TableBuilder(key)
	if !created {
		return fmt.Errorf("shift found duplicate table with key: %v", tbl.Key())
	}
	execute.AddTableCols(tbl, builder)

	return tbl.Do(func(cr query.ColReader) error {
		for j, c := range cr.Cols() {
			if execute.ContainsStr(t.columns, c.Label) {
				l := cr.Len()
				for i := 0; i < l; i++ {
					builder.AppendTime(j, cr.Times(j)[i].Add(t.shift))
				}
			} else {
				execute.AppendCol(j, j, cr, builder)
			}
		}
		return nil
	})
}

func (t *shiftTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}

func (t *shiftTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}

func (t *shiftTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}

func (t *shiftTransformation) SetParents(ids []execute.DatasetID) {}
