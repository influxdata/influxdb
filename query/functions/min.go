package functions

import (
	"fmt"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
)

const MinKind = "min"

type MinOpSpec struct {
	execute.SelectorConfig
}

var minSignature = query.DefaultFunctionSignature()

func init() {
	minSignature.Params["column"] = semantic.String
	minSignature.Params["useRowTime"] = semantic.Bool

	query.RegisterFunction(MinKind, createMinOpSpec, minSignature)
	query.RegisterOpSpec(MinKind, newMinOp)
	plan.RegisterProcedureSpec(MinKind, newMinProcedure, MinKind)
	execute.RegisterTransformation(MinKind, createMinTransformation)
}

func createMinOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(MinOpSpec)
	if err := spec.SelectorConfig.ReadArgs(args); err != nil {
		return nil, err
	}

	return spec, nil
}

func newMinOp() query.OperationSpec {
	return new(MinOpSpec)
}

func (s *MinOpSpec) Kind() query.OperationKind {
	return MinKind
}

type MinProcedureSpec struct {
	execute.SelectorConfig
}

func newMinProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*MinOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	return &MinProcedureSpec{
		SelectorConfig: spec.SelectorConfig,
	}, nil
}

func (s *MinProcedureSpec) Kind() plan.ProcedureKind {
	return MinKind
}
func (s *MinProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(MinProcedureSpec)
	ns.SelectorConfig = s.SelectorConfig
	return ns
}

type MinSelector struct {
	set  bool
	rows []execute.Row
}

func createMinTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	ps, ok := spec.(*MinProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", ps)
	}
	t, d := execute.NewRowSelectorTransformationAndDataset(id, mode, new(MinSelector), ps.SelectorConfig, a.Allocator())
	return t, d, nil
}

type MinIntSelector struct {
	MinSelector
	min int64
}
type MinUIntSelector struct {
	MinSelector
	min uint64
}
type MinFloatSelector struct {
	MinSelector
	min float64
}

func (s *MinSelector) NewBoolSelector() execute.DoBoolRowSelector {
	return nil
}

func (s *MinSelector) NewIntSelector() execute.DoIntRowSelector {
	return new(MinIntSelector)
}

func (s *MinSelector) NewUIntSelector() execute.DoUIntRowSelector {
	return new(MinUIntSelector)
}

func (s *MinSelector) NewFloatSelector() execute.DoFloatRowSelector {
	return new(MinFloatSelector)
}

func (s *MinSelector) NewStringSelector() execute.DoStringRowSelector {
	return nil
}

func (s *MinSelector) Rows() []execute.Row {
	if !s.set {
		return nil
	}
	return s.rows
}

func (s *MinSelector) selectRow(idx int, cr execute.ColReader) {
	// Capture row
	if idx >= 0 {
		s.rows = []execute.Row{execute.ReadRow(idx, cr)}
	}
}

func (s *MinIntSelector) DoInt(vs []int64, cr execute.ColReader) {
	minIdx := -1
	for i, v := range vs {
		if !s.set || v < s.min {
			s.set = true
			s.min = v
			minIdx = i
		}
	}
	s.selectRow(minIdx, cr)
}
func (s *MinUIntSelector) DoUInt(vs []uint64, cr execute.ColReader) {
	minIdx := -1
	for i, v := range vs {
		if !s.set || v < s.min {
			s.set = true
			s.min = v
			minIdx = i
		}
	}
	s.selectRow(minIdx, cr)
}
func (s *MinFloatSelector) DoFloat(vs []float64, cr execute.ColReader) {
	minIdx := -1
	for i, v := range vs {
		if !s.set || v < s.min {
			s.set = true
			s.min = v
			minIdx = i
		}
	}
	s.selectRow(minIdx, cr)
}
