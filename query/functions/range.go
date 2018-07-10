package functions

import (
	"fmt"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
)

const RangeKind = "range"

type RangeOpSpec struct {
	Start query.Time `json:"start"`
	Stop  query.Time `json:"stop"`
}

var rangeSignature = query.DefaultFunctionSignature()

func init() {
	rangeSignature.Params["start"] = semantic.Time
	rangeSignature.Params["stop"] = semantic.Time

	query.RegisterFunction(RangeKind, createRangeOpSpec, rangeSignature)
	query.RegisterOpSpec(RangeKind, newRangeOp)
	plan.RegisterProcedureSpec(RangeKind, newRangeProcedure, RangeKind)
	// TODO register a range transformation. Currently range is only supported if it is pushed down into a select procedure.
	execute.RegisterTransformation(RangeKind, createRangeTransformation)
}

func createRangeOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}
	start, err := args.GetRequiredTime("start")
	if err != nil {
		return nil, err
	}
	spec := &RangeOpSpec{
		Start: start,
	}

	if stop, ok, err := args.GetTime("stop"); err != nil {
		return nil, err
	} else if ok {
		spec.Stop = stop
	} else {
		// Make stop time implicit "now"
		spec.Stop.IsRelative = true
	}

	return spec, nil
}

func newRangeOp() query.OperationSpec {
	return new(RangeOpSpec)
}

func (s *RangeOpSpec) Kind() query.OperationKind {
	return RangeKind
}

type RangeProcedureSpec struct {
	Bounds plan.BoundsSpec
}

func newRangeProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*RangeOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	return &RangeProcedureSpec{
		Bounds: plan.BoundsSpec{
			Start: spec.Start,
			Stop:  spec.Stop,
		},
	}, nil
}

func (s *RangeProcedureSpec) Kind() plan.ProcedureKind {
	return RangeKind
}
func (s *RangeProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(RangeProcedureSpec)
	ns.Bounds = s.Bounds
	return ns
}

func (s *RangeProcedureSpec) PushDownRules() []plan.PushDownRule {
	return []plan.PushDownRule{{
		Root:    FromKind,
		Through: []plan.ProcedureKind{GroupKind, LimitKind, FilterKind},
	}}
}
func (s *RangeProcedureSpec) PushDown(root *plan.Procedure, dup func() *plan.Procedure) {
	selectSpec := root.Spec.(*FromProcedureSpec)
	if selectSpec.BoundsSet {
		// Example case where this matters
		//    var data = select(database: "mydb")
		//    var past = data.range(start:-2d,stop:-1d)
		//    var current = data.range(start:-1d,stop:now)
		root = dup()
		selectSpec = root.Spec.(*FromProcedureSpec)
		selectSpec.BoundsSet = false
		selectSpec.Bounds = plan.BoundsSpec{}
		return
	}
	selectSpec.BoundsSet = true
	selectSpec.Bounds = s.Bounds
}

func (s *RangeProcedureSpec) TimeBounds() plan.BoundsSpec {
	return s.Bounds
}

func createRangeTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*RangeProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t, err := NewRangeTransformation(d, cache, s)
	if err != nil {
		return nil, nil, err
	}
	return t, d, nil
}

type rangeTransformation struct {
	d     execute.Dataset
	cache execute.TableBuilderCache

	Start query.Time
	Stop  query.Time
}

func NewRangeTransformation(d execute.Dataset, cache execute.TableBuilderCache, spec *RangeProcedureSpec) (*rangeTransformation, error) {
	return &rangeTransformation{
		d:     d,
		cache: cache,
		Start: spec.Bounds.Start,
		Stop:  spec.Bounds.Stop,
	}, nil
}

func (t *rangeTransformation) RetractTable(id execute.DatasetID, key query.GroupKey) error {
	return t.d.RetractTable(key)
}

func (t *rangeTransformation) Process(id execute.DatasetID, b query.Table) error {
	builder, created := t.cache.TableBuilder(b.Key())
	if !created {
		return fmt.Errorf("range found duplicate table with key: %v", b.Key())
	}
	execute.AddTableCols(b, builder)
	cols := make([]int, len(b.Cols()))
	for i := range cols {
		cols[i] = i
	}
	execute.AppendTable(b, builder, cols)

	return nil
}

func (t *rangeTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *rangeTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *rangeTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
