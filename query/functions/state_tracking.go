package functions

import (
	"fmt"
	"log"
	"time"

	"github.com/influxdata/platform/query/interpreter"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
	"github.com/pkg/errors"
)

const StateTrackingKind = "stateTracking"

type StateTrackingOpSpec struct {
	Fn            *semantic.FunctionExpression `json:"fn"`
	CountLabel    string                       `json:"count_label"`
	DurationLabel string                       `json:"duration_label"`
	DurationUnit  query.Duration               `json:"duration_unit"`
	TimeCol       string                       `json:"time_col"`
}

var stateTrackingSignature = query.DefaultFunctionSignature()

func init() {
	stateTrackingSignature.Params["fn"] = semantic.Function
	stateTrackingSignature.Params["countLabel"] = semantic.String
	stateTrackingSignature.Params["durationLabel"] = semantic.String
	stateTrackingSignature.Params["durationUnit"] = semantic.Duration

	query.RegisterFunction(StateTrackingKind, createStateTrackingOpSpec, stateTrackingSignature)
	query.RegisterBuiltIn("state-tracking", stateTrackingBuiltin)
	query.RegisterOpSpec(StateTrackingKind, newStateTrackingOp)
	plan.RegisterProcedureSpec(StateTrackingKind, newStateTrackingProcedure, StateTrackingKind)
	execute.RegisterTransformation(StateTrackingKind, createStateTrackingTransformation)
}

var stateTrackingBuiltin = `
// stateCount computes the number of consecutive records in a given state.
// The state is defined via the function fn. For each consecutive point for
// which the expression evaluates as true, the state count will be incremented
// When a point evaluates as false, the state count is reset.
//
// The state count will be added as an additional column to each record. If the
// expression evaluates as false, the value will be -1. If the expression
// generates an error during evaluation, the point is discarded, and does not
// affect the state count.
stateCount = (fn, label="stateCount", table=<-) =>
	stateTracking(table:table, countLabel:label, fn:fn)

// stateDuration computes the duration of a given state.
// The state is defined via the function fn. For each consecutive point for
// which the expression evaluates as true, the state duration will be
// incremented by the duration between points. When a point evaluates as false,
// the state duration is reset.
//
// The state duration will be added as an additional column to each record. If the
// expression evaluates as false, the value will be -1. If the expression
// generates an error during evaluation, the point is discarded, and does not
// affect the state duration.
//
// Note that as the first point in the given state has no previous point, its
// state duration will be 0.
//
// The duration is represented as an integer in the units specified.
stateDuration = (fn, label="stateDuration", unit=1s, table=<-) =>
	stateTracking(table:table, durationLabel:label, fn:fn, durationUnit:unit)
`

func createStateTrackingOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	f, err := args.GetRequiredFunction("fn")
	if err != nil {
		return nil, err
	}

	fn, err := interpreter.ResolveFunction(f)
	if err != nil {
		return nil, err
	}

	spec := &StateTrackingOpSpec{
		Fn:           fn,
		DurationUnit: query.Duration(time.Second),
	}

	if label, ok, err := args.GetString("countLabel"); err != nil {
		return nil, err
	} else if ok {
		spec.CountLabel = label
	}
	if label, ok, err := args.GetString("durationLabel"); err != nil {
		return nil, err
	} else if ok {
		spec.DurationLabel = label
	}
	if unit, ok, err := args.GetDuration("durationUnit"); err != nil {
		return nil, err
	} else if ok {
		spec.DurationUnit = unit
	}
	if label, ok, err := args.GetString("timeCol"); err != nil {
		return nil, err
	} else if ok {
		spec.TimeCol = label
	} else {
		spec.TimeCol = execute.DefaultTimeColLabel
	}

	if spec.DurationLabel != "" && spec.DurationUnit <= 0 {
		return nil, errors.New("state tracking duration unit must be greater than zero")
	}
	return spec, nil
}

func newStateTrackingOp() query.OperationSpec {
	return new(StateTrackingOpSpec)
}

func (s *StateTrackingOpSpec) Kind() query.OperationKind {
	return StateTrackingKind
}

type StateTrackingProcedureSpec struct {
	Fn *semantic.FunctionExpression
	CountLabel,
	DurationLabel string
	DurationUnit query.Duration
	TimeCol      string
}

func newStateTrackingProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*StateTrackingOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &StateTrackingProcedureSpec{
		Fn:            spec.Fn,
		CountLabel:    spec.CountLabel,
		DurationLabel: spec.DurationLabel,
		DurationUnit:  spec.DurationUnit,
		TimeCol:       spec.TimeCol,
	}, nil
}

func (s *StateTrackingProcedureSpec) Kind() plan.ProcedureKind {
	return StateTrackingKind
}
func (s *StateTrackingProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(StateTrackingProcedureSpec)
	*ns = *s

	ns.Fn = s.Fn.Copy().(*semantic.FunctionExpression)

	return ns
}

func createStateTrackingTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*StateTrackingProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewBlockBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t, err := NewStateTrackingTransformation(d, cache, s)
	if err != nil {
		return nil, nil, err
	}
	return t, d, nil
}

type stateTrackingTransformation struct {
	d     execute.Dataset
	cache execute.BlockBuilderCache

	fn *execute.RowPredicateFn

	timeCol,
	countLabel,
	durationLabel string

	durationUnit int64
}

func NewStateTrackingTransformation(d execute.Dataset, cache execute.BlockBuilderCache, spec *StateTrackingProcedureSpec) (*stateTrackingTransformation, error) {
	fn, err := execute.NewRowPredicateFn(spec.Fn)
	if err != nil {
		return nil, err
	}
	return &stateTrackingTransformation{
		d:             d,
		cache:         cache,
		fn:            fn,
		countLabel:    spec.CountLabel,
		durationLabel: spec.DurationLabel,
		durationUnit:  int64(spec.DurationUnit),
		timeCol:       spec.TimeCol,
	}, nil
}

func (t *stateTrackingTransformation) RetractBlock(id execute.DatasetID, key execute.PartitionKey) error {
	return t.d.RetractBlock(key)
}

func (t *stateTrackingTransformation) Process(id execute.DatasetID, b execute.Block) error {
	builder, created := t.cache.BlockBuilder(b.Key())
	if !created {
		return fmt.Errorf("found duplicate block with key: %v", b.Key())
	}
	execute.AddBlockCols(b, builder)

	// Prepare the functions for the column types.
	cols := b.Cols()
	err := t.fn.Prepare(cols)
	if err != nil {
		// TODO(nathanielc): Should we not fail the query for failed compilation?
		return err
	}

	var countCol, durationCol = -1, -1

	// Add new value columns
	if t.countLabel != "" {
		countCol = builder.AddCol(execute.ColMeta{
			Label: t.countLabel,
			Type:  execute.TInt,
		})
	}
	if t.durationLabel != "" {
		durationCol = builder.AddCol(execute.ColMeta{
			Label: t.durationLabel,
			Type:  execute.TInt,
		})
	}

	var (
		startTime execute.Time
		count,
		duration int64
		inState bool
	)

	timeIdx := execute.ColIdx(t.timeCol, b.Cols())
	if timeIdx < 0 {
		return fmt.Errorf("no column %q exists", t.timeCol)
	}
	// Append modified rows
	return b.Do(func(cr execute.ColReader) error {
		l := cr.Len()
		for i := 0; i < l; i++ {
			tm := cr.Times(timeIdx)[i]
			match, err := t.fn.Eval(i, cr)
			if err != nil {
				log.Printf("failed to evaluate state count expression: %v", err)
				continue
			}
			if !match {
				count = -1
				duration = -1
				inState = false
			} else {
				if !inState {
					startTime = tm
					duration = 0
					count = 0
					inState = true
				}
				if t.durationUnit > 0 {
					duration = int64(tm-startTime) / t.durationUnit
				}
				count++
			}
			execute.AppendRecordForCols(i, cr, builder, cr.Cols())
			if countCol > 0 {
				builder.AppendInt(countCol, count)
			}
			if durationCol > 0 {
				builder.AppendInt(durationCol, duration)
			}
		}
		return nil
	})
}

func (t *stateTrackingTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *stateTrackingTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *stateTrackingTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
