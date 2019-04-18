package influxdb

import (
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/stdlib/universe"
)

// func init() {
// 	plan.RegisterPhysicalRules(
// 		PushDownRangeRule{},
// 		PushDownFilterRule{},
// 	)
// }

// PushDownRangeRule pushes down a range filter to storage
type PushDownRangeRule struct{}

func (rule PushDownRangeRule) Name() string {
	return "PushDownRangeRule"
}

// Pattern matches 'from |> range'
func (rule PushDownRangeRule) Pattern() plan.Pattern {
	return plan.Pat(universe.RangeKind, plan.Pat(FromKind))
}

// Rewrite converts 'from |> range' into 'ReadRange'
func (rule PushDownRangeRule) Rewrite(node plan.Node) (plan.Node, bool, error) {
	fromNode := node.Predecessors()[0]
	fromSpec := fromNode.ProcedureSpec().(*FromProcedureSpec)

	rangeSpec := node.ProcedureSpec().(*universe.RangeProcedureSpec)
	return plan.CreatePhysicalNode("ReadRange", &ReadRangePhysSpec{
		Bucket:   fromSpec.Bucket,
		BucketID: fromSpec.BucketID,
		Bounds:   rangeSpec.Bounds,
	}), true, nil
}

// PushDownFilterRule is a rule that pushes filters into from procedures to be evaluated in the storage layer.
// This rule is likely to be replaced by a more generic rule when we have a better
// framework for pushing filters, etc into sources.
type PushDownFilterRule struct{}

func (PushDownFilterRule) Name() string {
	return "PushDownFilterRule"
}

func (PushDownFilterRule) Pattern() plan.Pattern {
	return plan.Pat(universe.FilterKind, plan.Pat(ReadRangePhysKind))
}

func (PushDownFilterRule) Rewrite(pn plan.Node) (plan.Node, bool, error) {
	filterSpec := pn.ProcedureSpec().(*universe.FilterProcedureSpec)
	fromNode := pn.Predecessors()[0]
	fromSpec := fromNode.ProcedureSpec().(*ReadRangePhysSpec)

	bodyExpr, ok := filterSpec.Fn.Block.Body.(semantic.Expression)
	if !ok {
		return pn, false, nil
	}

	if len(filterSpec.Fn.Block.Parameters.List) != 1 {
		// I would expect that type checking would catch this, but just to be safe...
		return pn, false, nil
	}

	paramName := filterSpec.Fn.Block.Parameters.List[0].Key.Name

	pushable, notPushable, err := semantic.PartitionPredicates(bodyExpr, func(e semantic.Expression) (bool, error) {
		return isPushableExpr(paramName, e)
	})
	if err != nil {
		return nil, false, err
	}

	if pushable == nil {
		// Nothing could be pushed down, no rewrite can happen
		return pn, false, nil
	}

	newFromSpec := fromSpec.Copy().(*ReadRangePhysSpec)
	if newFromSpec.FilterSet {
		newBody := semantic.ExprsToConjunction(newFromSpec.Filter.Block.Body.(semantic.Expression), pushable)
		newFromSpec.Filter.Block.Body = newBody
	} else {
		newFromSpec.FilterSet = true
		newFromSpec.Filter = filterSpec.Fn.Copy().(*semantic.FunctionExpression)
		newFromSpec.Filter.Block.Body = pushable
	}

	if notPushable == nil {
		// All predicates could be pushed down, so eliminate the filter
		mergedNode, err := plan.MergeToPhysicalNode(pn, fromNode, newFromSpec)
		if err != nil {
			return nil, false, err
		}
		return mergedNode, true, nil
	}

	err = fromNode.ReplaceSpec(newFromSpec)
	if err != nil {
		return nil, false, err
	}

	newFilterSpec := filterSpec.Copy().(*universe.FilterProcedureSpec)
	newFilterSpec.Fn.Block.Body = notPushable
	if err := pn.ReplaceSpec(newFilterSpec); err != nil {
		return nil, false, err
	}

	return pn, true, nil
}
