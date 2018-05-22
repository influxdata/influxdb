package functions

import (
	"fmt"
	"log"
	"sort"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/interpreter"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
)

const MapKind = "map"

type MapOpSpec struct {
	Fn *semantic.FunctionExpression `json:"fn"`
}

var mapSignature = query.DefaultFunctionSignature()

func init() {
	mapSignature.Params["fn"] = semantic.Function

	query.RegisterFunction(MapKind, createMapOpSpec, mapSignature)
	query.RegisterOpSpec(MapKind, newMapOp)
	plan.RegisterProcedureSpec(MapKind, newMapProcedure, MapKind)
	execute.RegisterTransformation(MapKind, createMapTransformation)
}

func createMapOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
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
	return &MapOpSpec{
		Fn: fn,
	}, nil
}

func newMapOp() query.OperationSpec {
	return new(MapOpSpec)
}

func (s *MapOpSpec) Kind() query.OperationKind {
	return MapKind
}

type MapProcedureSpec struct {
	Fn *semantic.FunctionExpression
}

func newMapProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*MapOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &MapProcedureSpec{
		Fn: spec.Fn,
	}, nil
}

func (s *MapProcedureSpec) Kind() plan.ProcedureKind {
	return MapKind
}
func (s *MapProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(MapProcedureSpec)
	ns.Fn = s.Fn.Copy().(*semantic.FunctionExpression)
	return ns
}

func createMapTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*MapProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewBlockBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t, err := NewMapTransformation(d, cache, s)
	if err != nil {
		return nil, nil, err
	}
	return t, d, nil
}

type mapTransformation struct {
	d     execute.Dataset
	cache execute.BlockBuilderCache

	fn *execute.RowMapFn
}

func NewMapTransformation(d execute.Dataset, cache execute.BlockBuilderCache, spec *MapProcedureSpec) (*mapTransformation, error) {
	fn, err := execute.NewRowMapFn(spec.Fn)
	if err != nil {
		return nil, err
	}
	return &mapTransformation{
		d:     d,
		cache: cache,
		fn:    fn,
	}, nil
}

func (t *mapTransformation) RetractBlock(id execute.DatasetID, key query.PartitionKey) error {
	return t.d.RetractBlock(key)
}

func (t *mapTransformation) Process(id execute.DatasetID, b query.Block) error {
	// Prepare the functions for the column types.
	cols := b.Cols()
	err := t.fn.Prepare(cols)
	if err != nil {
		// TODO(nathanielc): Should we not fail the query for failed compilation?
		return err
	}

	return b.Do(func(cr query.ColReader) error {
		l := cr.Len()
		for i := 0; i < l; i++ {
			m, err := t.fn.Eval(i, cr)
			if err != nil {
				log.Printf("failed to evaluate map expression: %v", err)
				continue
			}
			key := execute.PartitionKeyForRow(i, cr)
			builder, created := t.cache.BlockBuilder(key)
			if created {
				// Add columns from function in sorted order
				properties := t.fn.Type().Properties()
				keys := make([]string, 0, len(properties))
				for k := range properties {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				for _, k := range keys {
					builder.AddCol(query.ColMeta{
						Label: k,
						Type:  execute.ConvertFromKind(properties[k].Kind()),
					})
				}
			}
			for j, c := range builder.Cols() {
				v, _ := m.Get(c.Label)
				execute.AppendValue(builder, j, v)
			}
		}
		return nil
	})

}

func (t *mapTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *mapTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *mapTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
