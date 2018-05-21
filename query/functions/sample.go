package functions

import (
	"fmt"

	"math/rand"

	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/plan"
	"github.com/influxdata/ifql/semantic"
)

const SampleKind = "sample"

type SampleOpSpec struct {
	N   int64 `json:"n"`
	Pos int64 `json:"pos"`
	execute.SelectorConfig
}

var sampleSignature = query.DefaultFunctionSignature()

func init() {
	sampleSignature.Params["column"] = semantic.String
	sampleSignature.Params["useRowTime"] = semantic.Bool

	query.RegisterFunction(SampleKind, createSampleOpSpec, sampleSignature)
	query.RegisterOpSpec(SampleKind, newSampleOp)
	plan.RegisterProcedureSpec(SampleKind, newSampleProcedure, SampleKind)
	execute.RegisterTransformation(SampleKind, createSampleTransformation)
}

func createSampleOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(SampleOpSpec)

	n, err := args.GetRequiredInt("n")
	if err != nil {
		return nil, err
	}
	spec.N = n

	if pos, ok, err := args.GetInt("pos"); err != nil {
		return nil, err
	} else if ok {
		spec.Pos = pos
	} else {
		spec.Pos = -1
	}

	if err := spec.SelectorConfig.ReadArgs(args); err != nil {
		return nil, err
	}

	return spec, nil
}

func newSampleOp() query.OperationSpec {
	return new(SampleOpSpec)
}

func (s *SampleOpSpec) Kind() query.OperationKind {
	return SampleKind
}

type SampleProcedureSpec struct {
	N   int64
	Pos int64
	execute.SelectorConfig
}

func newSampleProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*SampleOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	return &SampleProcedureSpec{
		N:              spec.N,
		Pos:            spec.Pos,
		SelectorConfig: spec.SelectorConfig,
	}, nil
}

func (s *SampleProcedureSpec) Kind() plan.ProcedureKind {
	return SampleKind
}
func (s *SampleProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(SampleProcedureSpec)
	ns.N = s.N
	ns.Pos = s.Pos
	ns.SelectorConfig = s.SelectorConfig
	return ns
}

type SampleSelector struct {
	N   int
	Pos int

	offset   int
	selected []int
}

func createSampleTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	ps, ok := spec.(*SampleProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", ps)
	}

	ss := &SampleSelector{
		N:   int(ps.N),
		Pos: int(ps.Pos),
	}
	t, d := execute.NewIndexSelectorTransformationAndDataset(id, mode, ss, ps.SelectorConfig, a.Allocator())
	return t, d, nil
}

func (s *SampleSelector) reset() {
	pos := s.Pos
	if pos < 0 {
		pos = rand.Intn(s.N)
	}
	s.offset = pos
}

func (s *SampleSelector) NewBoolSelector() execute.DoBoolIndexSelector {
	s.reset()
	return s
}

func (s *SampleSelector) NewIntSelector() execute.DoIntIndexSelector {
	s.reset()
	return s
}

func (s *SampleSelector) NewUIntSelector() execute.DoUIntIndexSelector {
	s.reset()
	return s
}

func (s *SampleSelector) NewFloatSelector() execute.DoFloatIndexSelector {
	s.reset()
	return s
}

func (s *SampleSelector) NewStringSelector() execute.DoStringIndexSelector {
	s.reset()
	return s
}

func (s *SampleSelector) selectSample(l int) []int {
	var i int
	s.selected = s.selected[0:0]
	for i = s.offset; i < l; i += s.N {
		s.selected = append(s.selected, i)
	}
	s.offset = i - l
	return s.selected
}

func (s *SampleSelector) DoBool(vs []bool) []int {
	return s.selectSample(len(vs))
}
func (s *SampleSelector) DoInt(vs []int64) []int {
	return s.selectSample(len(vs))
}
func (s *SampleSelector) DoUInt(vs []uint64) []int {
	return s.selectSample(len(vs))
}
func (s *SampleSelector) DoFloat(vs []float64) []int {
	return s.selectSample(len(vs))
}
func (s *SampleSelector) DoString(vs []string) []int {
	return s.selectSample(len(vs))
}
