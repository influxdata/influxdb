package functions

import (
	"fmt"
	"math"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
)

const StddevKind = "stddev"

type StddevOpSpec struct {
	execute.AggregateConfig
}

var stddevSignature = query.DefaultFunctionSignature()

func init() {
	query.RegisterFunction(StddevKind, createStddevOpSpec, stddevSignature)
	query.RegisterOpSpec(StddevKind, newStddevOp)
	plan.RegisterProcedureSpec(StddevKind, newStddevProcedure, StddevKind)
	execute.RegisterTransformation(StddevKind, createStddevTransformation)
}
func createStddevOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}
	s := new(StddevOpSpec)
	if err := s.AggregateConfig.ReadArgs(args); err != nil {
		return s, err
	}
	return s, nil
}

func newStddevOp() query.OperationSpec {
	return new(StddevOpSpec)
}

func (s *StddevOpSpec) Kind() query.OperationKind {
	return StddevKind
}

type StddevProcedureSpec struct {
	execute.AggregateConfig
}

func newStddevProcedure(qs query.OperationSpec, a plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*StddevOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	return &StddevProcedureSpec{
		AggregateConfig: spec.AggregateConfig,
	}, nil
}

func (s *StddevProcedureSpec) Kind() plan.ProcedureKind {
	return StddevKind
}
func (s *StddevProcedureSpec) Copy() plan.ProcedureSpec {
	return &StddevProcedureSpec{
		AggregateConfig: s.AggregateConfig,
	}
}

type StddevAgg struct {
	n, m2, mean float64
}

func createStddevTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*StddevProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	t, d := execute.NewAggregateTransformationAndDataset(id, mode, new(StddevAgg), s.AggregateConfig, a.Allocator())
	return t, d, nil
}

func (a *StddevAgg) NewBoolAgg() execute.DoBoolAgg {
	return nil
}

func (a *StddevAgg) NewIntAgg() execute.DoIntAgg {
	return new(StddevAgg)
}

func (a *StddevAgg) NewUIntAgg() execute.DoUIntAgg {
	return new(StddevAgg)
}

func (a *StddevAgg) NewFloatAgg() execute.DoFloatAgg {
	return new(StddevAgg)
}

func (a *StddevAgg) NewStringAgg() execute.DoStringAgg {
	return nil
}
func (a *StddevAgg) DoInt(vs []int64) {
	var delta, delta2 float64
	for _, v := range vs {
		a.n++
		// TODO handle overflow
		delta = float64(v) - a.mean
		a.mean += delta / a.n
		delta2 = float64(v) - a.mean
		a.m2 += delta * delta2
	}
}
func (a *StddevAgg) DoUInt(vs []uint64) {
	var delta, delta2 float64
	for _, v := range vs {
		a.n++
		// TODO handle overflow
		delta = float64(v) - a.mean
		a.mean += delta / a.n
		delta2 = float64(v) - a.mean
		a.m2 += delta * delta2
	}
}
func (a *StddevAgg) DoFloat(vs []float64) {
	var delta, delta2 float64
	for _, v := range vs {
		a.n++
		delta = v - a.mean
		a.mean += delta / a.n
		delta2 = v - a.mean
		a.m2 += delta * delta2
	}
}
func (a *StddevAgg) Type() query.DataType {
	return query.TFloat
}
func (a *StddevAgg) ValueFloat() float64 {
	if a.n < 2 {
		return math.NaN()
	}
	return math.Sqrt(a.m2 / (a.n - 1))
}
