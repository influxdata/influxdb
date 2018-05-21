package functions

import (
	"fmt"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
)

const UniqueKind = "unique"

type UniqueOpSpec struct {
	Column string `json:"column"`
}

var uniqueSignature = query.DefaultFunctionSignature()

func init() {
	uniqueSignature.Params["column"] = semantic.String

	query.RegisterFunction(UniqueKind, createUniqueOpSpec, uniqueSignature)
	query.RegisterOpSpec(UniqueKind, newUniqueOp)
	plan.RegisterProcedureSpec(UniqueKind, newUniqueProcedure, UniqueKind)
	execute.RegisterTransformation(UniqueKind, createUniqueTransformation)
}

func createUniqueOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(UniqueOpSpec)

	if col, ok, err := args.GetString("column"); err != nil {
		return nil, err
	} else if ok {
		spec.Column = col
	} else {
		spec.Column = execute.DefaultValueColLabel
	}

	return spec, nil
}

func newUniqueOp() query.OperationSpec {
	return new(UniqueOpSpec)
}

func (s *UniqueOpSpec) Kind() query.OperationKind {
	return UniqueKind
}

type UniqueProcedureSpec struct {
	Column string
}

func newUniqueProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*UniqueOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &UniqueProcedureSpec{
		Column: spec.Column,
	}, nil
}

func (s *UniqueProcedureSpec) Kind() plan.ProcedureKind {
	return UniqueKind
}
func (s *UniqueProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(UniqueProcedureSpec)

	*ns = *s

	return ns
}

func createUniqueTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*UniqueProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewBlockBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewUniqueTransformation(d, cache, s)
	return t, d, nil
}

type uniqueTransformation struct {
	d     execute.Dataset
	cache execute.BlockBuilderCache

	column string
}

func NewUniqueTransformation(d execute.Dataset, cache execute.BlockBuilderCache, spec *UniqueProcedureSpec) *uniqueTransformation {
	return &uniqueTransformation{
		d:      d,
		cache:  cache,
		column: spec.Column,
	}
}

func (t *uniqueTransformation) RetractBlock(id execute.DatasetID, key execute.PartitionKey) error {
	return t.d.RetractBlock(key)
}

func (t *uniqueTransformation) Process(id execute.DatasetID, b execute.Block) error {
	builder, created := t.cache.BlockBuilder(b.Key())
	if !created {
		return fmt.Errorf("unique found duplicate block with key: %v", b.Key())
	}
	execute.AddBlockCols(b, builder)

	colIdx := execute.ColIdx(t.column, builder.Cols())
	if colIdx < 0 {
		return fmt.Errorf("no column %q exists", t.column)
	}
	col := builder.Cols()[colIdx]

	var (
		boolUnique   map[bool]bool
		intUnique    map[int64]bool
		uintUnique   map[uint64]bool
		floatUnique  map[float64]bool
		stringUnique map[string]bool
		timeUnique   map[execute.Time]bool
	)
	switch col.Type {
	case execute.TBool:
		boolUnique = make(map[bool]bool)
	case execute.TInt:
		intUnique = make(map[int64]bool)
	case execute.TUInt:
		uintUnique = make(map[uint64]bool)
	case execute.TFloat:
		floatUnique = make(map[float64]bool)
	case execute.TString:
		stringUnique = make(map[string]bool)
	case execute.TTime:
		timeUnique = make(map[execute.Time]bool)
	}

	return b.Do(func(cr execute.ColReader) error {
		l := cr.Len()
		for i := 0; i < l; i++ {
			// Check unique
			switch col.Type {
			case execute.TBool:
				v := cr.Bools(colIdx)[i]
				if boolUnique[v] {
					continue
				}
				boolUnique[v] = true
			case execute.TInt:
				v := cr.Ints(colIdx)[i]
				if intUnique[v] {
					continue
				}
				intUnique[v] = true
			case execute.TUInt:
				v := cr.UInts(colIdx)[i]
				if uintUnique[v] {
					continue
				}
				uintUnique[v] = true
			case execute.TFloat:
				v := cr.Floats(colIdx)[i]
				if floatUnique[v] {
					continue
				}
				floatUnique[v] = true
			case execute.TString:
				v := cr.Strings(colIdx)[i]
				if stringUnique[v] {
					continue
				}
				stringUnique[v] = true
			case execute.TTime:
				v := cr.Times(colIdx)[i]
				if timeUnique[v] {
					continue
				}
				timeUnique[v] = true
			}

			execute.AppendRecord(i, cr, builder)
		}
		return nil
	})
}

func (t *uniqueTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *uniqueTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *uniqueTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
