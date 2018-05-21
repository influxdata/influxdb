package functions

import (
	"fmt"

	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/plan"
)

const CountKind = "count"

type CountOpSpec struct {
	execute.AggregateConfig
}

var countSignature = query.DefaultFunctionSignature()

func init() {
	query.RegisterFunction(CountKind, createCountOpSpec, countSignature)
	query.RegisterOpSpec(CountKind, newCountOp)
	plan.RegisterProcedureSpec(CountKind, newCountProcedure, CountKind)
	execute.RegisterTransformation(CountKind, createCountTransformation)
}

func createCountOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}
	s := new(CountOpSpec)
	if err := s.AggregateConfig.ReadArgs(args); err != nil {
		return nil, err
	}
	return s, nil
}

func newCountOp() query.OperationSpec {
	return new(CountOpSpec)
}

func (s *CountOpSpec) Kind() query.OperationKind {
	return CountKind
}

type CountProcedureSpec struct {
	execute.AggregateConfig
}

func newCountProcedure(qs query.OperationSpec, a plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*CountOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	return &CountProcedureSpec{
		AggregateConfig: spec.AggregateConfig,
	}, nil
}

func (s *CountProcedureSpec) Kind() plan.ProcedureKind {
	return CountKind
}

func (s *CountProcedureSpec) Copy() plan.ProcedureSpec {
	return &CountProcedureSpec{
		AggregateConfig: s.AggregateConfig,
	}
}

func (s *CountProcedureSpec) AggregateMethod() string {
	return CountKind
}
func (s *CountProcedureSpec) ReAggregateSpec() plan.ProcedureSpec {
	return new(SumProcedureSpec)
}

func (s *CountProcedureSpec) PushDownRules() []plan.PushDownRule {
	return []plan.PushDownRule{{
		Root:    FromKind,
		Through: nil,
		Match: func(spec plan.ProcedureSpec) bool {
			selectSpec := spec.(*FromProcedureSpec)
			return !selectSpec.GroupingSet
		},
	}}
}

func (s *CountProcedureSpec) PushDown(root *plan.Procedure, dup func() *plan.Procedure) {
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

type CountAgg struct {
	count int64
}

func createCountTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*CountProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}

	t, d := execute.NewAggregateTransformationAndDataset(id, mode, new(CountAgg), s.AggregateConfig, a.Allocator())
	return t, d, nil
}

func (a *CountAgg) NewBoolAgg() execute.DoBoolAgg {
	return new(CountAgg)
}
func (a *CountAgg) NewIntAgg() execute.DoIntAgg {
	return new(CountAgg)
}
func (a *CountAgg) NewUIntAgg() execute.DoUIntAgg {
	return new(CountAgg)
}
func (a *CountAgg) NewFloatAgg() execute.DoFloatAgg {
	return new(CountAgg)
}
func (a *CountAgg) NewStringAgg() execute.DoStringAgg {
	return new(CountAgg)
}

func (a *CountAgg) DoBool(vs []bool) {
	a.count += int64(len(vs))
}
func (a *CountAgg) DoUInt(vs []uint64) {
	a.count += int64(len(vs))
}
func (a *CountAgg) DoInt(vs []int64) {
	a.count += int64(len(vs))
}
func (a *CountAgg) DoFloat(vs []float64) {
	a.count += int64(len(vs))
}
func (a *CountAgg) DoString(vs []string) {
	a.count += int64(len(vs))
}

func (a *CountAgg) Type() execute.DataType {
	return execute.TInt
}
func (a *CountAgg) ValueInt() int64 {
	return a.count
}
