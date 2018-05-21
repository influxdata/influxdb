package functions

import (
	"fmt"
	"sort"
	"strings"

	"github.com/influxdata/ifql/interpreter"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/plan"
	"github.com/influxdata/ifql/semantic"
)

const KeysKind = "keys"

var (
	keysExceptDefaultValue = []string{"_time", "_value"}
)

type KeysOpSpec struct {
	Except []string `json:"except"`
}

var keysSignature = query.DefaultFunctionSignature()

func init() {
	keysSignature.Params["except"] = semantic.NewArrayType(semantic.String)

	query.RegisterFunction(KeysKind, createKeysOpSpec, keysSignature)
	query.RegisterOpSpec(KeysKind, newKeysOp)
	plan.RegisterProcedureSpec(KeysKind, newKeysProcedure, KeysKind)
	plan.RegisterRewriteRule(KeysPointLimitRewriteRule{})
	execute.RegisterTransformation(KeysKind, createKeysTransformation)
}

func createKeysOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(KeysOpSpec)
	if array, ok, err := args.GetArray("except", semantic.String); err != nil {
		return nil, err
	} else if ok {
		spec.Except, err = interpreter.ToStringArray(array)
		if err != nil {
			return nil, err
		}
	} else {
		spec.Except = keysExceptDefaultValue
	}

	return spec, nil
}

func newKeysOp() query.OperationSpec {
	return new(KeysOpSpec)
}

func (s *KeysOpSpec) Kind() query.OperationKind {
	return KeysKind
}

type KeysProcedureSpec struct {
	Except []string
}

func newKeysProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*KeysOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &KeysProcedureSpec{
		Except: spec.Except,
	}, nil
}

func (s *KeysProcedureSpec) Kind() plan.ProcedureKind {
	return KeysKind
}

func (s *KeysProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(KeysProcedureSpec)

	*ns = *s

	return ns
}

type KeysPointLimitRewriteRule struct {
}

func (r KeysPointLimitRewriteRule) Root() plan.ProcedureKind {
	return FromKind
}

func (r KeysPointLimitRewriteRule) Rewrite(pr *plan.Procedure, planner plan.PlanRewriter) error {
	fromSpec, ok := pr.Spec.(*FromProcedureSpec)
	if !ok {
		return nil
	}

	var keys *KeysProcedureSpec
	pr.DoChildren(func(child *plan.Procedure) {
		if d, ok := child.Spec.(*KeysProcedureSpec); ok {
			keys = d
		}
	})
	if keys == nil {
		return nil
	}

	if !fromSpec.LimitSet {
		fromSpec.LimitSet = true
		fromSpec.PointsLimit = -1
	}
	return nil
}

func createKeysTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*KeysProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewBlockBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewKeysTransformation(d, cache, s)
	return t, d, nil
}

type keysTransformation struct {
	d     execute.Dataset
	cache execute.BlockBuilderCache

	except []string
}

func NewKeysTransformation(d execute.Dataset, cache execute.BlockBuilderCache, spec *KeysProcedureSpec) *keysTransformation {
	var except []string
	if len(spec.Except) > 0 {
		except = append([]string{}, spec.Except...)
		sort.Strings(except)
	}

	return &keysTransformation{
		d:      d,
		cache:  cache,
		except: except,
	}
}

func (t *keysTransformation) RetractBlock(id execute.DatasetID, key execute.PartitionKey) error {
	return t.d.RetractBlock(key)
}

func (t *keysTransformation) Process(id execute.DatasetID, b execute.Block) error {
	builder, created := t.cache.BlockBuilder(b.Key())
	if !created {
		return fmt.Errorf("keys found duplicate block with key: %v", b.Key())
	}

	execute.AddBlockKeyCols(b.Key(), builder)
	colIdx := builder.AddCol(execute.ColMeta{Label: execute.DefaultValueColLabel, Type: execute.TString})

	cols := b.Cols()
	sort.Slice(cols, func(i, j int) bool {
		return cols[i].Label < cols[j].Label
	})

	var i int
	if len(t.except) > 0 {
		var j int
		for i < len(cols) && j < len(t.except) {
			c := strings.Compare(cols[i].Label, t.except[j])
			if c < 0 {
				execute.AppendKeyValues(b.Key(), builder)
				builder.AppendString(colIdx, cols[i].Label)
				i++
			} else if c > 0 {
				j++
			} else {
				i++
				j++
			}
		}
	}

	// add remaining
	for ; i < len(cols); i++ {
		execute.AppendKeyValues(b.Key(), builder)
		builder.AppendString(colIdx, cols[i].Label)
	}

	// TODO: this is a hack
	return b.Do(func(execute.ColReader) error {
		return nil
	})
}

func (t *keysTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *keysTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *keysTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
