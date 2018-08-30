package functions

import (
	"errors"
	"fmt"
	"sort"

	"math/bits"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/interpreter"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/semantic"
)

const GroupKind = "group"

type GroupOpSpec struct {
	By     []string `json:"by"`
	Except []string `json:"except"`
	All    bool     `json:"all"`
	None   bool     `json:"none"`
}

var groupSignature = query.DefaultFunctionSignature()

func init() {
	groupSignature.Params["by"] = semantic.NewArrayType(semantic.String)
	groupSignature.Params["except"] = semantic.NewArrayType(semantic.String)
	groupSignature.Params["none"] = semantic.Bool
	groupSignature.Params["all"] = semantic.Bool

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

	if val, ok, err := args.GetBool("none"); err != nil {
		return nil, err
	} else if ok && val {
		spec.None = true
	}
	if val, ok, err := args.GetBool("all"); err != nil {
		return nil, err
	} else if ok && val {
		spec.All = true
	}

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

	switch bits.OnesCount(uint(groupModeFromSpec(spec))) {
	case 0:
		// empty args
		spec.All = true
	case 1:
		// all good
	default:
		return nil, errors.New(`specify one of "by", "except", "none" or "all" keyword arguments`)
	}

	return spec, nil
}

func newGroupOp() query.OperationSpec {
	return new(GroupOpSpec)
}

func (s *GroupOpSpec) Kind() query.OperationKind {
	return GroupKind
}

type GroupMode int

const (
	// GroupModeDefault will use the default grouping of GroupModeAll.
	GroupModeDefault GroupMode = 0

	// GroupModeNone merges all series into a single group.
	GroupModeNone GroupMode = 1 << iota
	// GroupModeAll produces a separate table for each series.
	GroupModeAll
	// GroupModeBy produces a table for each unique value of the specified GroupKeys.
	GroupModeBy
	// GroupModeExcept produces a table for the unique values of all keys, except those specified by GroupKeys.
	GroupModeExcept
)

func groupModeFromSpec(spec *GroupOpSpec) GroupMode {
	var mode GroupMode
	if spec.All {
		mode |= GroupModeAll
	}
	if spec.None {
		mode |= GroupModeNone
	}
	if len(spec.By) > 0 {
		mode |= GroupModeBy
	}
	if len(spec.Except) > 0 {
		mode |= GroupModeExcept
	}
	if mode == GroupModeDefault {
		mode = GroupModeAll
	}
	return mode
}

type GroupProcedureSpec struct {
	GroupMode GroupMode
	GroupKeys []string
}

func newGroupProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*GroupOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	mode := groupModeFromSpec(spec)
	var keys []string
	switch mode {
	case GroupModeAll:
	case GroupModeNone:
	case GroupModeBy:
		keys = spec.By
	case GroupModeExcept:
		keys = spec.Except
	default:
		return nil, fmt.Errorf("invalid GroupOpSpec; multiple modes detected")
	}

	p := &GroupProcedureSpec{
		GroupMode: mode,
		GroupKeys: keys,
	}
	return p, nil
}

func (s *GroupProcedureSpec) Kind() plan.ProcedureKind {
	return GroupKind
}
func (s *GroupProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(GroupProcedureSpec)

	ns.GroupMode = s.GroupMode

	ns.GroupKeys = make([]string, len(s.GroupKeys))
	copy(ns.GroupKeys, s.GroupKeys)

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
		selectSpec.GroupMode = GroupModeDefault
		selectSpec.GroupKeys = nil
		return
	}
	selectSpec.GroupingSet = true
	// TODO implement OrderByTime
	//selectSpec.OrderByTime = true

	selectSpec.GroupMode = s.GroupMode
	selectSpec.GroupKeys = s.GroupKeys
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
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewGroupTransformation(d, cache, s)
	return t, d, nil
}

type groupTransformation struct {
	d     execute.Dataset
	cache execute.TableBuilderCache

	mode GroupMode
	keys []string
}

func NewGroupTransformation(d execute.Dataset, cache execute.TableBuilderCache, spec *GroupProcedureSpec) *groupTransformation {
	t := &groupTransformation{
		d:     d,
		cache: cache,
		mode:  spec.GroupMode,
		keys:  spec.GroupKeys,
	}
	sort.Strings(t.keys)
	return t
}

func (t *groupTransformation) RetractTable(id execute.DatasetID, key query.GroupKey) (err error) {
	panic("not implemented")
}

func (t *groupTransformation) Process(id execute.DatasetID, tbl query.Table) error {
	cols := tbl.Cols()
	on := make(map[string]bool, len(cols))
	if t.mode == GroupModeBy && len(t.keys) > 0 {
		for _, k := range t.keys {
			on[k] = true
		}
	} else if t.mode == GroupModeExcept && len(t.keys) > 0 {
	COLS:
		for _, c := range cols {
			for _, label := range t.keys {
				if c.Label == label {
					continue COLS
				}
			}
			on[c.Label] = true
		}
	}
	colMap := make([]int, 0, len(tbl.Cols()))
	return tbl.Do(func(cr query.ColReader) error {
		l := cr.Len()
		for i := 0; i < l; i++ {
			key := execute.GroupKeyForRowOn(i, cr, on)
			builder, created := t.cache.TableBuilder(key)
			if created {
				execute.AddTableCols(tbl, builder)
			}
			colMap = execute.ColMap(colMap, builder, cr)
			execute.AppendMappedRecord(i, cr, builder, colMap)
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
