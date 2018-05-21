package functions

import (
	"errors"
	"fmt"
	"sort"

	"github.com/influxdata/ifql/interpreter"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/plan"
	"github.com/influxdata/ifql/semantic"
)

const GroupKind = "group"

type GroupOpSpec struct {
	By     []string `json:"by"`
	Except []string `json:"except"`
}

var groupSignature = query.DefaultFunctionSignature()

func init() {
	groupSignature.Params["by"] = semantic.NewArrayType(semantic.String)
	groupSignature.Params["except"] = semantic.NewArrayType(semantic.String)

	query.RegisterFunction(GroupKind, createGroupOpSpec, groupSignature)
	query.RegisterOpSpec(GroupKind, newGroupOp)
	plan.RegisterProcedureSpec(GroupKind, newGroupProcedure, GroupKind)
	plan.RegisterRewriteRule(AggregateGroupRewriteRule{})
	execute.RegisterTransformation(GroupKind, createGroupTransformation)
}

func createGroupOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(GroupOpSpec)
	if array, ok, err := args.GetArray("by", semantic.String); err != nil {
		return nil, err
	} else if ok {
		spec.By, err = interpreter.ToStringArray(array)
		if err != nil {
			return nil, err
		}
	}
	if array, ok, err := args.GetArray("except", semantic.String); err != nil {
		return nil, err
	} else if ok {
		spec.Except, err = interpreter.ToStringArray(array)
		if err != nil {
			return nil, err
		}
	}

	if len(spec.By) > 0 && len(spec.Except) > 0 {
		return nil, errors.New(`cannot specify both "by" and "except" keyword arguments`)
	}
	return spec, nil
}

func newGroupOp() query.OperationSpec {
	return new(GroupOpSpec)
}

func (s *GroupOpSpec) Kind() query.OperationKind {
	return GroupKind
}

type GroupProcedureSpec struct {
	By     []string
	Except []string
}

func newGroupProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*GroupOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	p := &GroupProcedureSpec{
		By:     spec.By,
		Except: spec.Except,
	}
	return p, nil
}

func (s *GroupProcedureSpec) Kind() plan.ProcedureKind {
	return GroupKind
}
func (s *GroupProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(GroupProcedureSpec)

	ns.By = make([]string, len(s.By))
	copy(ns.By, s.By)

	ns.Except = make([]string, len(s.Except))
	copy(ns.Except, s.Except)

	return ns
}

func (s *GroupProcedureSpec) PushDownRules() []plan.PushDownRule {
	return []plan.PushDownRule{{
		Root:    FromKind,
		Through: []plan.ProcedureKind{LimitKind, RangeKind, FilterKind},
		Match: func(spec plan.ProcedureSpec) bool {
			selectSpec := spec.(*FromProcedureSpec)
			return !selectSpec.AggregateSet
		},
	}}
}

func (s *GroupProcedureSpec) PushDown(root *plan.Procedure, dup func() *plan.Procedure) {
	selectSpec := root.Spec.(*FromProcedureSpec)
	if selectSpec.GroupingSet {
		root = dup()
		selectSpec = root.Spec.(*FromProcedureSpec)
		selectSpec.OrderByTime = false
		selectSpec.GroupingSet = false
		selectSpec.MergeAll = false
		selectSpec.GroupKeys = nil
		selectSpec.GroupExcept = nil
		return
	}
	selectSpec.GroupingSet = true
	// TODO implement OrderByTime
	//selectSpec.OrderByTime = true

	// Merge all series into a single group if we have no specific grouping dimensions.
	selectSpec.MergeAll = len(s.By) == 0 && len(s.Except) == 0
	selectSpec.GroupKeys = s.By
	selectSpec.GroupExcept = s.Except
}

type AggregateGroupRewriteRule struct {
}

func (r AggregateGroupRewriteRule) Root() plan.ProcedureKind {
	return FromKind
}

func (r AggregateGroupRewriteRule) Rewrite(pr *plan.Procedure, planner plan.PlanRewriter) error {
	var agg *plan.Procedure
	pr.DoChildren(func(child *plan.Procedure) {
		if _, ok := child.Spec.(plan.AggregateProcedureSpec); ok {
			agg = child
		}
	})
	if agg == nil {
		return nil
	}
	fromSpec := pr.Spec.(*FromProcedureSpec)
	if fromSpec.AggregateSet {
		return nil
	}

	// Rewrite
	isoFrom, err := planner.IsolatePath(pr, agg)
	if err != nil {
		return err
	}
	return r.rewrite(isoFrom, planner)
}

func (r AggregateGroupRewriteRule) rewrite(fromPr *plan.Procedure, planner plan.PlanRewriter) error {
	fromSpec := fromPr.Spec.(*FromProcedureSpec)
	aggPr := fromPr.Child(0)
	aggSpec := aggPr.Spec.(plan.AggregateProcedureSpec)

	fromSpec.AggregateSet = true
	fromSpec.AggregateMethod = aggSpec.AggregateMethod()

	if err := planner.RemoveBranch(aggPr); err != nil {
		return err
	}

	planner.AddChild(fromPr, aggSpec.ReAggregateSpec())
	return nil
}

func createGroupTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*GroupProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewBlockBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewGroupTransformation(d, cache, s)
	return t, d, nil
}

type groupTransformation struct {
	d     execute.Dataset
	cache execute.BlockBuilderCache

	keys   []string
	except []string

	// Ignoring is true of len(keys) == 0 && len(except) > 0
	ignoring bool
}

func NewGroupTransformation(d execute.Dataset, cache execute.BlockBuilderCache, spec *GroupProcedureSpec) *groupTransformation {
	t := &groupTransformation{
		d:        d,
		cache:    cache,
		keys:     spec.By,
		except:   spec.Except,
		ignoring: len(spec.By) == 0 && len(spec.Except) > 0,
	}
	sort.Strings(t.keys)
	sort.Strings(t.except)
	return t
}

func (t *groupTransformation) RetractBlock(id execute.DatasetID, key execute.PartitionKey) (err error) {
	//TODO(nathanielc): Investigate if this can be smarter and not retract all blocks with the same time bounds.
	panic("not implemented")
	//t.cache.ForEachBuilder(func(bk execute.BlockKey, builder execute.BlockBuilder) {
	//	if err != nil {
	//		return
	//	}
	//	if meta.Bounds().Equal(builder.Bounds()) {
	//		err = t.d.RetractBlock(bk)
	//	}
	//})
	//return
}

func (t *groupTransformation) Process(id execute.DatasetID, b execute.Block) error {
	cols := b.Cols()
	on := make(map[string]bool, len(cols))
	if len(t.keys) > 0 {
		for _, k := range t.keys {
			on[k] = true
		}
	} else if len(t.except) > 0 {
	COLS:
		for _, c := range cols {
			for _, label := range t.except {
				if c.Label == label {
					continue COLS
				}
			}
			on[c.Label] = true
		}
	}
	return b.Do(func(cr execute.ColReader) error {
		l := cr.Len()
		for i := 0; i < l; i++ {
			key := execute.PartitionKeyForRowOn(i, cr, on)
			builder, created := t.cache.BlockBuilder(key)
			if created {
				execute.AddBlockCols(b, builder)
			}
			execute.AppendRecord(i, cr, builder)
		}
		return nil
	})
}

func (t *groupTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *groupTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *groupTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
