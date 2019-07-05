package influxdb

import (
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/stdlib/universe"
)

func init() {
	plan.RegisterPhysicalRules(
		PushDownRangeRule{},
		PushDownFilterRule{},
		PushDownGroupRule{},
		PushDownReadTagKeysRule{},
		PushDownReadTagValuesRule{},
	)
}

// PushDownGroupRule pushes down a group operation to storage
type PushDownGroupRule struct{}

func (rule PushDownGroupRule) Name() string {
	return "PushDownGroupRule"
}

func (rule PushDownGroupRule) Pattern() plan.Pattern {
	return plan.Pat(universe.GroupKind, plan.Pat(ReadRangePhysKind))
}

func (rule PushDownGroupRule) Rewrite(node plan.Node) (plan.Node, bool, error) {
	src := node.Predecessors()[0].ProcedureSpec().(*ReadRangePhysSpec)
	grp := node.ProcedureSpec().(*universe.GroupProcedureSpec)

	switch grp.GroupMode {
	case
		flux.GroupModeBy:
	default:
		return node, false, nil
	}

	for _, col := range grp.GroupKeys {
		// Storage can only group by tag keys.
		// Note the columns _start and _stop are ok since all tables
		// coming from storage will have the same _start and _values.
		if col == execute.DefaultTimeColLabel || col == execute.DefaultValueColLabel {
			return node, false, nil
		}
	}

	return plan.CreatePhysicalNode("ReadGroup", &ReadGroupPhysSpec{
		ReadRangePhysSpec: *src.Copy().(*ReadRangePhysSpec),
		GroupMode:         grp.GroupMode,
		GroupKeys:         grp.GroupKeys,
	}), true, nil
}

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

// PushDownReadTagKeysRule matches 'ReadRange |> keys() |> keep() |> distinct()'.
// The 'from()' must have already been merged with 'range' and, optionally,
// may have been merged with 'filter'.
// If any other properties have been set on the from procedure,
// this rule will not rewrite anything.
type PushDownReadTagKeysRule struct{}

func (rule PushDownReadTagKeysRule) Name() string {
	return "PushDownReadTagKeysRule"
}

func (rule PushDownReadTagKeysRule) Pattern() plan.Pattern {
	return plan.Pat(universe.DistinctKind,
		plan.Pat(universe.SchemaMutationKind,
			plan.Pat(universe.KeysKind,
				plan.Pat(ReadRangePhysKind))))
}

func (rule PushDownReadTagKeysRule) Rewrite(pn plan.Node) (plan.Node, bool, error) {
	// Retrieve the nodes and specs for all of the predecessors.
	distinctSpec := pn.ProcedureSpec().(*universe.DistinctProcedureSpec)
	keepNode := pn.Predecessors()[0]
	keepSpec := keepNode.ProcedureSpec().(*universe.SchemaMutationProcedureSpec)
	keysNode := keepNode.Predecessors()[0]
	keysSpec := keysNode.ProcedureSpec().(*universe.KeysProcedureSpec)
	fromNode := keysNode.Predecessors()[0]
	fromSpec := fromNode.ProcedureSpec().(*ReadRangePhysSpec)

	// A filter spec would have already been merged into the
	// from spec if it existed so we will take that one when
	// constructing our own replacement. We do not care about it
	// at the moment though which is why it is not in the pattern.

	// The tag keys mechanism doesn't know about fields so we cannot
	// push down _field comparisons in 1.x.
	if hasFieldExpr(fromSpec.Filter) {
		return pn, false, nil
	}

	// The schema mutator needs to correspond to a keep call
	// on the column specified by the keys procedure.
	if len(keepSpec.Mutations) != 1 {
		return pn, false, nil
	} else if m, ok := keepSpec.Mutations[0].(*universe.KeepOpSpec); !ok {
		return pn, false, nil
	} else if m.Predicate != nil || len(m.Columns) != 1 {
		// We have a keep mutator, but it uses a function or
		// it retains more than one column so it does not match
		// what we want.
		return pn, false, nil
	} else if m.Columns[0] != keysSpec.Column {
		// We are not keeping the value column so this optimization
		// will not work.
		return pn, false, nil
	}

	// The distinct spec should keep only the value column.
	if distinctSpec.Column != keysSpec.Column {
		return pn, false, nil
	}

	// We have passed all of the necessary prerequisites
	// so construct the procedure spec.
	return plan.CreatePhysicalNode("ReadTagKeys", &ReadTagKeysPhysSpec{
		ReadRangePhysSpec: *fromSpec.Copy().(*ReadRangePhysSpec),
	}), true, nil
}

func hasFieldExpr(expr semantic.Expression) bool {
	hasField := false
	v := semantic.CreateVisitor(func(node semantic.Node) {
		switch n := node.(type) {
		case *semantic.MemberExpression:
			if n.Property == "_field" {
				hasField = true
			}
		}
	})
	semantic.Walk(v, expr)
	return hasField
}

// PushDownReadTagValuesRule matches 'ReadRange |> keep(columns: [tag]) |> group() |> distinct(column: tag)'.
// The 'from()' must have already been merged with 'range' and, optionally,
// may have been merged with 'filter'.
// If any other properties have been set on the from procedure,
// this rule will not rewrite anything.
type PushDownReadTagValuesRule struct{}

func (rule PushDownReadTagValuesRule) Name() string {
	return "PushDownReadTagValuesRule"
}

func (rule PushDownReadTagValuesRule) Pattern() plan.Pattern {
	return plan.Pat(universe.DistinctKind,
		plan.Pat(universe.GroupKind,
			plan.Pat(universe.SchemaMutationKind,
				plan.Pat(ReadRangePhysKind))))
}

func (rule PushDownReadTagValuesRule) Rewrite(pn plan.Node) (plan.Node, bool, error) {
	// Retrieve the nodes and specs for all of the predecessors.
	distinctNode := pn
	distinctSpec := distinctNode.ProcedureSpec().(*universe.DistinctProcedureSpec)
	groupNode := distinctNode.Predecessors()[0]
	groupSpec := groupNode.ProcedureSpec().(*universe.GroupProcedureSpec)
	keepNode := groupNode.Predecessors()[0]
	keepSpec := keepNode.ProcedureSpec().(*universe.SchemaMutationProcedureSpec)
	fromNode := keepNode.Predecessors()[0]
	fromSpec := fromNode.ProcedureSpec().(*ReadRangePhysSpec)

	// A filter spec would have already been merged into the
	// from spec if it existed so we will take that one when
	// constructing our own replacement. We do not care about it
	// at the moment though which is why it is not in the pattern.

	// All of the values need to be grouped into the same table.
	if groupSpec.GroupMode != flux.GroupModeBy {
		return pn, false, nil
	} else if len(groupSpec.GroupKeys) > 0 {
		return pn, false, nil
	}

	// The column that distinct is for will be the tag key.
	tagKey := distinctSpec.Column
	if !isValidTagKeyForTagValues(tagKey) {
		return pn, false, nil
	}

	// The schema mutator needs to correspond to a keep call
	// on the tag key column.
	if len(keepSpec.Mutations) != 1 {
		return pn, false, nil
	} else if m, ok := keepSpec.Mutations[0].(*universe.KeepOpSpec); !ok {
		return pn, false, nil
	} else if m.Predicate != nil || len(m.Columns) != 1 {
		// We have a keep mutator, but it uses a function or
		// it retains more than one column so it does not match
		// what we want.
		return pn, false, nil
	} else if m.Columns[0] != tagKey {
		// We are not keeping the value column so this optimization
		// will not work.
		return pn, false, nil
	}

	// We have passed all of the necessary prerequisites
	// so construct the procedure spec.
	return plan.CreatePhysicalNode("ReadTagValues", &ReadTagValuesPhysSpec{
		ReadRangePhysSpec: *fromSpec.Copy().(*ReadRangePhysSpec),
		TagKey:            tagKey,
	}), true, nil
}

var invalidTagKeysForTagValues = []string{
	execute.DefaultTimeColLabel,
	execute.DefaultValueColLabel,
	execute.DefaultStartColLabel,
	execute.DefaultStopColLabel,
	// TODO(jsternberg): There just doesn't seem to be a good way to do this
	// in the 1.x line of the release.
	"_field",
}

// isValidTagKeyForTagValues returns true if the given key can
// be used in a tag values call.
func isValidTagKeyForTagValues(key string) bool {
	for _, k := range invalidTagKeysForTagValues {
		if k == key {
			return false
		}
	}
	return true
}

// isPushableExpr determines if a predicate expression can be pushed down into the storage layer.
func isPushableExpr(paramName string, expr semantic.Expression) (bool, error) {
	switch e := expr.(type) {
	case *semantic.LogicalExpression:
		b, err := isPushableExpr(paramName, e.Left)
		if err != nil {
			return false, err
		}

		if !b {
			return false, nil
		}

		return isPushableExpr(paramName, e.Right)

	case *semantic.BinaryExpression:
		if isPushablePredicate(paramName, e) {
			return true, nil
		}
	}

	return false, nil
}
func isPushablePredicate(paramName string, be *semantic.BinaryExpression) bool {
	// Manual testing seems to indicate that (at least right now) we can
	// only handle predicates of the form <fn param>.<property> <op> <literal>
	// and the literal must be on the RHS.

	if !isLiteral(be.Right) {
		return false
	}

	if isField(paramName, be.Left) && isPushableFieldOperator(be.Operator) {
		return true
	}

	if isTag(paramName, be.Left) && isPushableTagOperator(be.Operator) {
		return true
	}

	return false
}

func isLiteral(e semantic.Expression) bool {
	switch e.(type) {
	case *semantic.StringLiteral:
		return true
	case *semantic.IntegerLiteral:
		return true
	case *semantic.BooleanLiteral:
		return true
	case *semantic.FloatLiteral:
		return true
	case *semantic.RegexpLiteral:
		return true
	}

	return false
}

const fieldValueProperty = "_value"

func isTag(paramName string, e semantic.Expression) bool {
	memberExpr := validateMemberExpr(paramName, e)
	return memberExpr != nil && memberExpr.Property != fieldValueProperty
}

func isField(paramName string, e semantic.Expression) bool {
	memberExpr := validateMemberExpr(paramName, e)
	return memberExpr != nil && memberExpr.Property == fieldValueProperty
}

func validateMemberExpr(paramName string, e semantic.Expression) *semantic.MemberExpression {
	memberExpr, ok := e.(*semantic.MemberExpression)
	if !ok {
		return nil
	}

	idExpr, ok := memberExpr.Object.(*semantic.IdentifierExpression)
	if !ok {
		return nil
	}

	if idExpr.Name != paramName {
		return nil
	}

	return memberExpr
}

func isPushableTagOperator(kind ast.OperatorKind) bool {
	pushableOperators := []ast.OperatorKind{
		ast.EqualOperator,
		ast.NotEqualOperator,
		ast.RegexpMatchOperator,
		ast.NotRegexpMatchOperator,
	}

	for _, op := range pushableOperators {
		if op == kind {
			return true
		}
	}

	return false
}

func isPushableFieldOperator(kind ast.OperatorKind) bool {
	if isPushableTagOperator(kind) {
		return true
	}

	// Fields can be filtered by anything that tags can be filtered by,
	// plus range operators.

	moreOperators := []ast.OperatorKind{
		ast.LessThanEqualOperator,
		ast.LessThanOperator,
		ast.GreaterThanEqualOperator,
		ast.GreaterThanOperator,
	}

	for _, op := range moreOperators {
		if op == kind {
			return true
		}
	}

	return false
}
