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
	"github.com/influxdata/platform/query/values"
)

const MapKind = "map"

type MapOpSpec struct {
	Fn       *semantic.FunctionExpression `json:"fn"`
	MergeKey bool                         `json:"mergeKey"`
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

	spec := new(MapOpSpec)

	if f, err := args.GetRequiredFunction("fn"); err != nil {
		return nil, err
	} else {
		fn, err := interpreter.ResolveFunction(f)
		if err != nil {
			return nil, err
		}
		spec.Fn = fn
	}

	if m, ok, err := args.GetBool("mergeKey"); err != nil {
		return nil, err
	} else if ok {
		spec.MergeKey = m
	} else {
		spec.MergeKey = true
	}
	return spec, nil
}

func newMapOp() query.OperationSpec {
	return new(MapOpSpec)
}

func (s *MapOpSpec) Kind() query.OperationKind {
	return MapKind
}

type MapProcedureSpec struct {
	Fn       *semantic.FunctionExpression
	MergeKey bool
}

func newMapProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*MapOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &MapProcedureSpec{
		Fn:       spec.Fn,
		MergeKey: spec.MergeKey,
	}, nil
}

func (s *MapProcedureSpec) Kind() plan.ProcedureKind {
	return MapKind
}
func (s *MapProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(MapProcedureSpec)
	*ns = *s
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

	fn       *execute.RowMapFn
	mergeKey bool
}

func NewMapTransformation(d execute.Dataset, cache execute.BlockBuilderCache, spec *MapProcedureSpec) (*mapTransformation, error) {
	fn, err := execute.NewRowMapFn(spec.Fn)
	if err != nil {
		return nil, err
	}
	return &mapTransformation{
		d:        d,
		cache:    cache,
		fn:       fn,
		mergeKey: spec.MergeKey,
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
	// Determine keys return from function
	properties := t.fn.Type().Properties()
	keys := make([]string, 0, len(properties))
	for k := range properties {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Determine on which cols to partition
	on := make(map[string]bool, len(b.Key().Cols()))
	for _, c := range b.Key().Cols() {
		on[c.Label] = t.mergeKey || execute.ContainsStr(keys, c.Label)
	}

	return b.Do(func(cr query.ColReader) error {
		l := cr.Len()
		for i := 0; i < l; i++ {
			m, err := t.fn.Eval(i, cr)
			if err != nil {
				log.Printf("failed to evaluate map expression: %v", err)
				continue
			}
			key := partitionKeyForObject(i, cr, m, on)
			builder, created := t.cache.BlockBuilder(key)
			if created {
				if t.mergeKey {
					execute.AddBlockKeyCols(b.Key(), builder)
				}
				// Add columns from function in sorted order
				for _, k := range keys {
					if t.mergeKey && b.Key().HasCol(k) {
						continue
					}
					builder.AddCol(query.ColMeta{
						Label: k,
						Type:  execute.ConvertFromKind(properties[k].Kind()),
					})
				}
			}
			for j, c := range builder.Cols() {
				v, ok := m.Get(c.Label)
				if !ok {
					if idx := execute.ColIdx(c.Label, b.Key().Cols()); t.mergeKey && idx >= 0 {
						v = b.Key().Value(idx)
					} else {
						// This should be unreachable
						return fmt.Errorf("could not find value for column %q", c.Label)
					}
				}
				execute.AppendValue(builder, j, v)
			}
		}
		return nil
	})
}

func partitionKeyForObject(i int, cr query.ColReader, obj values.Object, on map[string]bool) query.PartitionKey {
	cols := make([]query.ColMeta, 0, len(on))
	vs := make([]values.Value, 0, len(on))
	for j, c := range cr.Cols() {
		if !on[c.Label] {
			continue
		}
		cols = append(cols, c)
		v, ok := obj.Get(c.Label)
		if ok {
			vs = append(vs, v)
		} else {
			switch c.Type {
			case query.TBool:
				vs = append(vs, values.NewBoolValue(cr.Bools(j)[i]))
			case query.TInt:
				vs = append(vs, values.NewIntValue(cr.Ints(j)[i]))
			case query.TUInt:
				vs = append(vs, values.NewUIntValue(cr.UInts(j)[i]))
			case query.TFloat:
				vs = append(vs, values.NewFloatValue(cr.Floats(j)[i]))
			case query.TString:
				vs = append(vs, values.NewStringValue(cr.Strings(j)[i]))
			case query.TTime:
				vs = append(vs, values.NewTimeValue(cr.Times(j)[i]))
			}
		}
	}
	return execute.NewPartitionKey(cols, vs)
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
