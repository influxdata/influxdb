package functions

import (
	"fmt"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
)

const SetKind = "set"

type SetOpSpec struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

var setSignature = query.DefaultFunctionSignature()

func init() {
	setSignature.Params["key"] = semantic.String
	setSignature.Params["value"] = semantic.String

	query.RegisterFunction(SetKind, createSetOpSpec, setSignature)
	query.RegisterOpSpec(SetKind, newSetOp)
	plan.RegisterProcedureSpec(SetKind, newSetProcedure, SetKind)
	execute.RegisterTransformation(SetKind, createSetTransformation)
}

func createSetOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(SetOpSpec)
	key, err := args.GetRequiredString("key")
	if err != nil {
		return nil, err
	}
	spec.Key = key

	value, err := args.GetRequiredString("value")
	if err != nil {
		return nil, err
	}
	spec.Value = value

	return spec, nil
}

func newSetOp() query.OperationSpec {
	return new(SetOpSpec)
}

func (s *SetOpSpec) Kind() query.OperationKind {
	return SetKind
}

type SetProcedureSpec struct {
	Key, Value string
}

func newSetProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	s, ok := qs.(*SetOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	p := &SetProcedureSpec{
		Key:   s.Key,
		Value: s.Value,
	}
	return p, nil
}

func (s *SetProcedureSpec) Kind() plan.ProcedureKind {
	return SetKind
}
func (s *SetProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(SetProcedureSpec)
	ns.Key = s.Key
	ns.Value = s.Value
	return ns
}

func createSetTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*SetProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewBlockBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewSetTransformation(d, cache, s)
	return t, d, nil
}

type setTransformation struct {
	d     execute.Dataset
	cache execute.BlockBuilderCache

	key, value string
}

func NewSetTransformation(
	d execute.Dataset,
	cache execute.BlockBuilderCache,
	spec *SetProcedureSpec,
) execute.Transformation {
	return &setTransformation{
		d:     d,
		cache: cache,
		key:   spec.Key,
		value: spec.Value,
	}
}

func (t *setTransformation) RetractBlock(id execute.DatasetID, key query.GroupKey) error {
	// TODO
	return nil
}

func (t *setTransformation) Process(id execute.DatasetID, b query.Block) error {
	key := b.Key()
	if idx := execute.ColIdx(t.key, key.Cols()); idx >= 0 {
		// Update key
		cols := make([]query.ColMeta, len(key.Cols()))
		vs := make([]values.Value, len(key.Cols()))
		for j, c := range key.Cols() {
			cols[j] = c
			if j == idx {
				vs[j] = values.NewStringValue(t.value)
			} else {
				vs[j] = key.Value(j)
			}
		}
		key = execute.NewGroupKey(cols, vs)
	}
	builder, created := t.cache.BlockBuilder(key)
	if created {
		execute.AddBlockCols(b, builder)
		if !execute.HasCol(t.key, builder.Cols()) {
			builder.AddCol(query.ColMeta{
				Label: t.key,
				Type:  query.TString,
			})
		}
	}
	idx := execute.ColIdx(t.key, builder.Cols())
	return b.Do(func(cr query.ColReader) error {
		for j := range cr.Cols() {
			if j == idx {
				continue
			}
			execute.AppendCol(j, j, cr, builder)
		}
		// Set new value
		l := cr.Len()
		for i := 0; i < l; i++ {
			builder.AppendString(idx, t.value)
		}
		return nil
	})
}

func (t *setTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *setTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *setTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
