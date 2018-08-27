package functions

import (
	"fmt"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
)

const SumKind = "sum"

type SumOpSpec struct {
	execute.AggregateConfig
}

var sumSignature = execute.DefaultAggregateSignature()

func init() {
	query.RegisterFunction(SumKind, createSumOpSpec, sumSignature)
	query.RegisterOpSpec(SumKind, newSumOp)
	plan.RegisterProcedureSpec(SumKind, newSumProcedure, SumKind)
	execute.RegisterTransformation(SumKind, createSumTransformation)
}

func createSumOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}
	s := new(SumOpSpec)
	if err := s.AggregateConfig.ReadArgs(args); err != nil {
		return s, err
	}
	return s, nil
}

func newSumOp() query.OperationSpec {
	return new(SumOpSpec)
}

func (s *SumOpSpec) Kind() query.OperationKind {
	return SumKind
}

type SumProcedureSpec struct {
	execute.AggregateConfig
}

func newSumProcedure(qs query.OperationSpec, a plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*SumOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	return &SumProcedureSpec{
		AggregateConfig: spec.AggregateConfig,
	}, nil
}

func (s *SumProcedureSpec) Kind() plan.ProcedureKind {
	return SumKind
}

func (s *SumProcedureSpec) Copy() plan.ProcedureSpec {
	return &SumProcedureSpec{
		AggregateConfig: s.AggregateConfig,
	}
}

func (s *SumProcedureSpec) AggregateMethod() string {
	return SumKind
}
func (s *SumProcedureSpec) ReAggregateSpec() plan.ProcedureSpec {
	return new(SumProcedureSpec)
}

func (s *SumProcedureSpec) PushDownRules() []plan.PushDownRule {
	return []plan.PushDownRule{{
		Root:    FromKind,
		Through: nil,
		Match: func(spec plan.ProcedureSpec) bool {
			selectSpec := spec.(*FromProcedureSpec)
			return !selectSpec.GroupingSet
		},
	}}
}
func (s *SumProcedureSpec) PushDown(root *plan.Procedure, dup func() *plan.Procedure) {
	selectSpec := root.Spec.(*FromProcedureSpec)
	if selectSpec.AggregateSet {
		root = dup()
		selectSpec = root.Spec.(*FromProcedureSpec)
		selectSpec.AggregateSet = false
		selectSpec.AggregateMethod = ""
		return
	}
	selectSpec.AggregateSet = true
	selectSpec.AggregateMethod = s.AggregateMethod()
}

type SumAgg struct{}

func createSumTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*SumProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}

	t, d := execute.NewAggregateTransformationAndDataset(id, mode, new(SumAgg), s.AggregateConfig, a.Allocator())
	return t, d, nil
}

func (a *SumAgg) NewBoolAgg() execute.DoBoolAgg {
	return nil
}
func (a *SumAgg) NewIntAgg() execute.DoIntAgg {
	return new(SumIntAgg)
}
func (a *SumAgg) NewUIntAgg() execute.DoUIntAgg {
	return new(SumUIntAgg)
}
func (a *SumAgg) NewFloatAgg() execute.DoFloatAgg {
	return new(SumFloatAgg)
}
func (a *SumAgg) NewStringAgg() execute.DoStringAgg {
	return nil
}

type SumIntAgg struct {
	sum int64
}

func (a *SumIntAgg) DoInt(vs []int64) {
	for _, v := range vs {
		a.sum += v
	}
}
func (a *SumIntAgg) Type() query.DataType {
	return query.TInt
}
func (a *SumIntAgg) ValueInt() int64 {
	return a.sum
}

type SumUIntAgg struct {
	sum uint64
}

func (a *SumUIntAgg) DoUInt(vs []uint64) {
	for _, v := range vs {
		a.sum += v
	}
}
func (a *SumUIntAgg) Type() query.DataType {
	return query.TUInt
}
func (a *SumUIntAgg) ValueUInt() uint64 {
	return a.sum
}

type SumFloatAgg struct {
	sum float64
}

func (a *SumFloatAgg) DoFloat(vs []float64) {
	for _, v := range vs {
		a.sum += v
	}
}
func (a *SumFloatAgg) Type() query.DataType {
	return query.TFloat
}
func (a *SumFloatAgg) ValueFloat() float64 {
	return a.sum
}
