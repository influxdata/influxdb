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

// LimitOpSpec limits the number of rows returned per table.
// Currently offset is not supported.
type LimitOpSpec struct {
	N      int64 `json:"n"`
	Offset int64 `json:"offset"`
}

var limitSignature = query.DefaultFunctionSignature()

func init() {
	limitSignature.Params["n"] = semantic.Int
	limitSignature.Params["offset"] = semantic.Int

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

	if offset, ok, err := args.GetInt("offset"); err != nil {
		return nil, err
	} else if ok {
		spec.Offset = offset
	}

	return spec, nil
}

func newLimitOp() query.OperationSpec {
	return new(LimitOpSpec)
}

func (s *LimitOpSpec) Kind() query.OperationKind {
	return LimitKind
}

type LimitProcedureSpec struct {
	N      int64 `json:"n"`
	Offset int64 `json:"offset"`
}

func newLimitProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*LimitOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	return &LimitProcedureSpec{
		N:      spec.N,
		Offset: spec.Offset,
	}, nil
}

func (s *LimitProcedureSpec) Kind() plan.ProcedureKind {
	return LimitKind
}
func (s *LimitProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(LimitProcedureSpec)
	*ns = *s
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
		//selectSpec.PointsOffset = 0
		selectSpec.SeriesLimit = 0
		selectSpec.SeriesOffset = 0
		return
	}
	selectSpec.LimitSet = true
	selectSpec.PointsLimit = s.N
	//selectSpec.PointsOffset = s.Offset
	selectSpec.SeriesLimit = 0
	selectSpec.SeriesOffset = 0
}

func createLimitTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*LimitProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewLimitTransformation(d, cache, s)
	return t, d, nil
}

type limitTransformation struct {
	d     execute.Dataset
	cache execute.TableBuilderCache

	n, offset int

	colMap []int
}

func NewLimitTransformation(d execute.Dataset, cache execute.TableBuilderCache, spec *LimitProcedureSpec) *limitTransformation {
	return &limitTransformation{
		d:      d,
		cache:  cache,
		n:      int(spec.N),
		offset: int(spec.Offset),
	}
}

func (t *limitTransformation) RetractTable(id execute.DatasetID, key query.GroupKey) error {
	return t.d.RetractTable(key)
}

func (t *limitTransformation) Process(id execute.DatasetID, tbl query.Table) error {
	builder, created := t.cache.TableBuilder(tbl.Key())
	if !created {
		return fmt.Errorf("limit found duplicate table with key: %v", tbl.Key())
	}
	execute.AddTableCols(tbl, builder)

	ncols := builder.NCols()
	if cap(t.colMap) < ncols {
		t.colMap = make([]int, ncols)
		for j := range t.colMap {
			t.colMap[j] = j
		}
	} else {
		t.colMap = t.colMap[:ncols]
	}

	// AppendTable with limit
	n := t.n
	offset := t.offset
	tbl.Do(func(cr query.ColReader) error {
		if n <= 0 {
			// Returning an error terminates iteration
			return errors.New("finished")
		}
		l := cr.Len()
		if l <= offset {
			offset -= l
			// Skip entire batch
			return nil
		}
		start := offset
		stop := l
		count := stop - start
		if count > n {
			count = n
			stop = start + count
		}
		n -= count
		lcr := sliceColReader{
			ColReader: cr,
			start:     start,
			stop:      stop,
		}
		execute.AppendCols(lcr, builder, t.colMap)
		return nil
	})
	return nil
}

type sliceColReader struct {
	query.ColReader
	start, stop int
}

func (cr sliceColReader) Len() int {
	return cr.stop
}

func (cr sliceColReader) Bools(j int) []bool {
	return cr.ColReader.Bools(j)[cr.start:cr.stop]
}

func (cr sliceColReader) Ints(j int) []int64 {
	return cr.ColReader.Ints(j)[cr.start:cr.stop]
}

func (cr sliceColReader) UInts(j int) []uint64 {
	return cr.ColReader.UInts(j)[cr.start:cr.stop]
}

func (cr sliceColReader) Floats(j int) []float64 {
	return cr.ColReader.Floats(j)[cr.start:cr.stop]
}

func (cr sliceColReader) Strings(j int) []string {
	return cr.ColReader.Strings(j)[cr.start:cr.stop]
}

func (cr sliceColReader) Times(j int) []execute.Time {
	return cr.ColReader.Times(j)[cr.start:cr.stop]
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
