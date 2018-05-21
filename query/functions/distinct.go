package functions

import (
	"fmt"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
)

const DistinctKind = "distinct"

type DistinctOpSpec struct {
	Column string `json:"column"`
}

var distinctSignature = query.DefaultFunctionSignature()

func init() {
	distinctSignature.Params["column"] = semantic.String

	query.RegisterFunction(DistinctKind, createDistinctOpSpec, distinctSignature)
	query.RegisterOpSpec(DistinctKind, newDistinctOp)
	plan.RegisterProcedureSpec(DistinctKind, newDistinctProcedure, DistinctKind)
	plan.RegisterRewriteRule(DistinctPointLimitRewriteRule{})
	execute.RegisterTransformation(DistinctKind, createDistinctTransformation)
}

func createDistinctOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(DistinctOpSpec)

	if col, ok, err := args.GetString("column"); err != nil {
		return nil, err
	} else if ok {
		spec.Column = col
	} else {
		spec.Column = execute.DefaultValueColLabel
	}

	return spec, nil
}

func newDistinctOp() query.OperationSpec {
	return new(DistinctOpSpec)
}

func (s *DistinctOpSpec) Kind() query.OperationKind {
	return DistinctKind
}

type DistinctProcedureSpec struct {
	Column string
}

func newDistinctProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*DistinctOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &DistinctProcedureSpec{
		Column: spec.Column,
	}, nil
}

func (s *DistinctProcedureSpec) Kind() plan.ProcedureKind {
	return DistinctKind
}
func (s *DistinctProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(DistinctProcedureSpec)

	*ns = *s

	return ns
}

type DistinctPointLimitRewriteRule struct {
}

func (r DistinctPointLimitRewriteRule) Root() plan.ProcedureKind {
	return FromKind
}

func (r DistinctPointLimitRewriteRule) Rewrite(pr *plan.Procedure, planner plan.PlanRewriter) error {
	fromSpec, ok := pr.Spec.(*FromProcedureSpec)
	if !ok {
		return nil
	}

	var distinct *DistinctProcedureSpec
	pr.DoChildren(func(child *plan.Procedure) {
		if d, ok := child.Spec.(*DistinctProcedureSpec); ok {
			distinct = d
		}
	})
	if distinct == nil {
		return nil
	}

	groupStar := !fromSpec.GroupingSet && distinct.Column != execute.DefaultValueColLabel
	groupByColumn := fromSpec.GroupingSet && ((len(fromSpec.GroupKeys) > 0 && execute.ContainsStr(fromSpec.GroupKeys, distinct.Column)) || (len(fromSpec.GroupExcept) > 0 && !execute.ContainsStr(fromSpec.GroupExcept, distinct.Column)))
	if groupStar || groupByColumn {
		fromSpec.LimitSet = true
		fromSpec.PointsLimit = -1
		return nil
	}
	return nil
}

func createDistinctTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*DistinctProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewBlockBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewDistinctTransformation(d, cache, s)
	return t, d, nil
}

type distinctTransformation struct {
	d     execute.Dataset
	cache execute.BlockBuilderCache

	column string
}

func NewDistinctTransformation(d execute.Dataset, cache execute.BlockBuilderCache, spec *DistinctProcedureSpec) *distinctTransformation {
	return &distinctTransformation{
		d:      d,
		cache:  cache,
		column: spec.Column,
	}
}

func (t *distinctTransformation) RetractBlock(id execute.DatasetID, key query.PartitionKey) error {
	return t.d.RetractBlock(key)
}

func (t *distinctTransformation) Process(id execute.DatasetID, b query.Block) error {
	builder, created := t.cache.BlockBuilder(b.Key())
	if !created {
		return fmt.Errorf("distinct found duplicate block with key: %v", b.Key())
	}

	colIdx := execute.ColIdx(t.column, b.Cols())
	if colIdx < 0 {
		return fmt.Errorf("no column %q exists", t.column)
	}
	col := b.Cols()[colIdx]

	execute.AddBlockKeyCols(b.Key(), builder)
	colIdx = builder.AddCol(query.ColMeta{
		Label: execute.DefaultValueColLabel,
		Type:  col.Type,
	})

	if b.Key().HasCol(t.column) {
		j := execute.ColIdx(t.column, b.Key().Cols())
		switch col.Type {
		case query.TBool:
			builder.AppendBool(colIdx, b.Key().ValueBool(j))
		case query.TInt:
			builder.AppendInt(colIdx, b.Key().ValueInt(j))
		case query.TUInt:
			builder.AppendUInt(colIdx, b.Key().ValueUInt(j))
		case query.TFloat:
			builder.AppendFloat(colIdx, b.Key().ValueFloat(j))
		case query.TString:
			builder.AppendString(colIdx, b.Key().ValueString(j))
		case query.TTime:
			builder.AppendTime(colIdx, b.Key().ValueTime(j))
		}

		execute.AppendKeyValues(b.Key(), builder)
		// TODO: this is a hack
		return b.Do(func(query.ColReader) error {
			return nil
		})
	}

	var (
		boolDistinct   map[bool]bool
		intDistinct    map[int64]bool
		uintDistinct   map[uint64]bool
		floatDistinct  map[float64]bool
		stringDistinct map[string]bool
		timeDistinct   map[execute.Time]bool
	)
	switch col.Type {
	case query.TBool:
		boolDistinct = make(map[bool]bool)
	case query.TInt:
		intDistinct = make(map[int64]bool)
	case query.TUInt:
		uintDistinct = make(map[uint64]bool)
	case query.TFloat:
		floatDistinct = make(map[float64]bool)
	case query.TString:
		stringDistinct = make(map[string]bool)
	case query.TTime:
		timeDistinct = make(map[execute.Time]bool)
	}

	return b.Do(func(cr query.ColReader) error {
		l := cr.Len()
		for i := 0; i < l; i++ {
			// Check distinct
			switch col.Type {
			case query.TBool:
				v := cr.Bools(colIdx)[i]
				if boolDistinct[v] {
					continue
				}
				boolDistinct[v] = true
				builder.AppendBool(colIdx, v)
			case query.TInt:
				v := cr.Ints(colIdx)[i]
				if intDistinct[v] {
					continue
				}
				intDistinct[v] = true
				builder.AppendInt(colIdx, v)
			case query.TUInt:
				v := cr.UInts(colIdx)[i]
				if uintDistinct[v] {
					continue
				}
				uintDistinct[v] = true
				builder.AppendUInt(colIdx, v)
			case query.TFloat:
				v := cr.Floats(colIdx)[i]
				if floatDistinct[v] {
					continue
				}
				floatDistinct[v] = true
				builder.AppendFloat(colIdx, v)
			case query.TString:
				v := cr.Strings(colIdx)[i]
				if stringDistinct[v] {
					continue
				}
				stringDistinct[v] = true
				builder.AppendString(colIdx, v)
			case query.TTime:
				v := cr.Times(colIdx)[i]
				if timeDistinct[v] {
					continue
				}
				timeDistinct[v] = true
				builder.AppendTime(colIdx, v)
			}

			execute.AppendKeyValues(b.Key(), builder)
		}
		return nil
	})
}

func (t *distinctTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *distinctTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *distinctTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
