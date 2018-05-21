package functions

import (
	"fmt"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
	"github.com/pkg/errors"
)

const LimitKind = "limit"

// LimitOpSpec limits the number of rows returned per block.
// Currently offset is not supported.
type LimitOpSpec struct {
	N int64 `json:"n"`
	//Offset int64 `json:"offset"`
}

var limitSignature = query.DefaultFunctionSignature()

func init() {
	limitSignature.Params["n"] = semantic.Int

	query.RegisterFunction(LimitKind, createLimitOpSpec, limitSignature)
	query.RegisterOpSpec(LimitKind, newLimitOp)
	plan.RegisterProcedureSpec(LimitKind, newLimitProcedure, LimitKind)
	// TODO register a range transformation. Currently range is only supported if it is pushed down into a select procedure.
	execute.RegisterTransformation(LimitKind, createLimitTransformation)
}

func createLimitOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(LimitOpSpec)

	n, err := args.GetRequiredInt("n")
	if err != nil {
		return nil, err
	}
	spec.N = n

	return spec, nil
}

func newLimitOp() query.OperationSpec {
	return new(LimitOpSpec)
}

func (s *LimitOpSpec) Kind() query.OperationKind {
	return LimitKind
}

type LimitProcedureSpec struct {
	N int64 `json:"n"`
	//Offset int64 `json:"offset"`
}

func newLimitProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*LimitOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	return &LimitProcedureSpec{
		N: spec.N,
		//Offset: spec.Offset,
	}, nil
}

func (s *LimitProcedureSpec) Kind() plan.ProcedureKind {
	return LimitKind
}
func (s *LimitProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(LimitProcedureSpec)
	ns.N = s.N
	//ns.Offset = s.Offset
	return ns
}

func (s *LimitProcedureSpec) PushDownRules() []plan.PushDownRule {
	return []plan.PushDownRule{{
		Root:    FromKind,
		Through: []plan.ProcedureKind{GroupKind, RangeKind, FilterKind},
	}}
}
func (s *LimitProcedureSpec) PushDown(root *plan.Procedure, dup func() *plan.Procedure) {
	selectSpec := root.Spec.(*FromProcedureSpec)
	if selectSpec.LimitSet {
		root = dup()
		selectSpec = root.Spec.(*FromProcedureSpec)
		selectSpec.LimitSet = false
		selectSpec.PointsLimit = 0
		selectSpec.SeriesLimit = 0
		selectSpec.SeriesOffset = 0
		return
	}
	selectSpec.LimitSet = true
	selectSpec.PointsLimit = s.N
	selectSpec.SeriesLimit = 0
	selectSpec.SeriesOffset = 0
}

func createLimitTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*LimitProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewBlockBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewLimitTransformation(d, cache, s)
	return t, d, nil
}

type limitTransformation struct {
	d     execute.Dataset
	cache execute.BlockBuilderCache

	n int

	colMap []int
}

func NewLimitTransformation(d execute.Dataset, cache execute.BlockBuilderCache, spec *LimitProcedureSpec) *limitTransformation {
	return &limitTransformation{
		d:     d,
		cache: cache,
		n:     int(spec.N),
	}
}

func (t *limitTransformation) RetractBlock(id execute.DatasetID, key query.PartitionKey) error {
	return t.d.RetractBlock(key)
}

func (t *limitTransformation) Process(id execute.DatasetID, b query.Block) error {
	builder, created := t.cache.BlockBuilder(b.Key())
	if !created {
		return fmt.Errorf("limit found duplicate block with key: %v", b.Key())
	}
	execute.AddBlockCols(b, builder)

	ncols := builder.NCols()
	if cap(t.colMap) < ncols {
		t.colMap = make([]int, ncols)
		for j := range t.colMap {
			t.colMap[j] = j
		}
	} else {
		t.colMap = t.colMap[:ncols]
	}

	// AppendBlock with limit
	n := t.n
	b.Do(func(cr query.ColReader) error {
		if n <= 0 {
			// Returning an error terminates iteration
			return errors.New("finished")
		}
		l := cr.Len()
		if l > n {
			l = n
		}
		n -= l
		lcr := limitColReader{
			ColReader: cr,
			n:         l,
		}
		execute.AppendCols(lcr, builder, t.colMap)
		return nil
	})
	return nil
}

type limitColReader struct {
	query.ColReader
	n int
}

func (cr limitColReader) Len() int {
	return cr.n
}

func (cr limitColReader) Bools(j int) []bool {
	return cr.ColReader.Bools(j)[:cr.n]
}

func (cr limitColReader) Ints(j int) []int64 {
	return cr.ColReader.Ints(j)[:cr.n]
}

func (cr limitColReader) UInts(j int) []uint64 {
	return cr.ColReader.UInts(j)[:cr.n]
}

func (cr limitColReader) Floats(j int) []float64 {
	return cr.ColReader.Floats(j)[:cr.n]
}

func (cr limitColReader) Strings(j int) []string {
	return cr.ColReader.Strings(j)[:cr.n]
}

func (cr limitColReader) Times(j int) []execute.Time {
	return cr.ColReader.Times(j)[:cr.n]
}

func (t *limitTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *limitTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *limitTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
