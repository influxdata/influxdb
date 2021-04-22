package influxdb

import (
	"context"
	"math"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/flux/stdlib/universe"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2/kit/feature"
)

func init() {
	plan.RegisterPhysicalRules(
		FromStorageRule{},
		PushDownRangeRule{},
		PushDownFilterRule{},
		PushDownGroupRule{},
		PushDownReadTagKeysRule{},
		PushDownReadTagValuesRule{},
		SortedPivotRule{},
		PushDownWindowAggregateRule{},
		PushDownWindowAggregateByTimeRule{},
		PushDownBareAggregateRule{},
		GroupWindowAggregateTransposeRule{},
		PushDownGroupAggregateRule{},
	)
	plan.RegisterLogicalRules(
		MergeFiltersRule{},
	)
}

type FromStorageRule struct{}

func (rule FromStorageRule) Name() string {
	return "influxdata/influxdb.FromStorageRule"
}

func (rule FromStorageRule) Pattern() plan.Pattern {
	return plan.Pat(influxdb.FromKind)
}

func (rule FromStorageRule) Rewrite(ctx context.Context, node plan.Node) (plan.Node, bool, error) {
	fromSpec := node.ProcedureSpec().(*influxdb.FromProcedureSpec)
	if fromSpec.Host != nil {
		return node, false, nil
	} else if fromSpec.Org != nil {
		return node, false, &flux.Error{
			Code: codes.Unimplemented,
			Msg:  "reads from the storage engine cannot read from a separate organization; please specify a host or remove the organization",
		}
	}

	return plan.CreateLogicalNode("fromStorage", &FromStorageProcedureSpec{
		Bucket: fromSpec.Bucket,
	}), true, nil
}

// PushDownGroupRule pushes down a group operation to storage
type PushDownGroupRule struct{}

func (rule PushDownGroupRule) Name() string {
	return "PushDownGroupRule"
}

func (rule PushDownGroupRule) Pattern() plan.Pattern {
	return plan.Pat(universe.GroupKind, plan.Pat(ReadRangePhysKind))
}

func (rule PushDownGroupRule) Rewrite(ctx context.Context, node plan.Node) (plan.Node, bool, error) {
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

	return plan.CreateUniquePhysicalNode(ctx, "ReadGroup", &ReadGroupPhysSpec{
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
func (rule PushDownRangeRule) Rewrite(ctx context.Context, node plan.Node) (plan.Node, bool, error) {
	fromNode := node.Predecessors()[0]
	fromSpec := fromNode.ProcedureSpec().(*FromStorageProcedureSpec)
	rangeSpec := node.ProcedureSpec().(*universe.RangeProcedureSpec)
	return plan.CreateUniquePhysicalNode(ctx, "ReadRange", &ReadRangePhysSpec{
		Bucket:   fromSpec.Bucket.Name,
		BucketID: fromSpec.Bucket.ID,
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

func (PushDownFilterRule) Rewrite(ctx context.Context, pn plan.Node) (plan.Node, bool, error) {
	filterSpec := pn.ProcedureSpec().(*universe.FilterProcedureSpec)
	fromNode := pn.Predecessors()[0]
	fromSpec := fromNode.ProcedureSpec().(*ReadRangePhysSpec)

	// Cannot push down when keeping empty tables.
	if filterSpec.KeepEmptyTables {
		return pn, false, nil
	}

	bodyExpr, ok := filterSpec.Fn.Fn.GetFunctionBodyExpression()
	if !ok {
		return pn, false, nil
	}

	if len(filterSpec.Fn.Fn.Parameters.List) != 1 {
		// I would expect that type checking would catch this, but just to be safe...
		return pn, false, nil
	}

	paramName := filterSpec.Fn.Fn.Parameters.List[0].Key.Name

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
	pushable, _ = rewritePushableExpr(pushable)

	// Convert the pushable expression to a storage predicate.
	predicate, err := ToStoragePredicate(pushable, paramName)
	if err != nil {
		return nil, false, err
	}

	// If the filter has already been set, then combine the existing predicate
	// with the new one.
	if fromSpec.Filter != nil {
		mergedPredicate, err := mergePredicates(ast.AndOperator, fromSpec.Filter, predicate)
		if err != nil {
			return nil, false, err
		}
		predicate = mergedPredicate
	}

	// Copy the specification and set the predicate.
	newFromSpec := fromSpec.Copy().(*ReadRangePhysSpec)
	newFromSpec.Filter = predicate

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
	newFilterSpec.Fn.Fn.Block = &semantic.Block{
		Body: []semantic.Statement{
			&semantic.ReturnStatement{Argument: notPushable},
		},
	}
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

func (rule PushDownReadTagKeysRule) Rewrite(ctx context.Context, pn plan.Node) (plan.Node, bool, error) {
	// Retrieve the nodes and specs for all of the predecessors.
	distinctSpec := pn.ProcedureSpec().(*universe.DistinctProcedureSpec)
	keepNode := pn.Predecessors()[0]
	keepSpec := asSchemaMutationProcedureSpec(keepNode.ProcedureSpec())
	keysNode := keepNode.Predecessors()[0]
	keysSpec := keysNode.ProcedureSpec().(*universe.KeysProcedureSpec)
	fromNode := keysNode.Predecessors()[0]
	fromSpec := fromNode.ProcedureSpec().(*ReadRangePhysSpec)

	// A filter spec would have already been merged into the
	// from spec if it existed so we will take that one when
	// constructing our own replacement. We do not care about it
	// at the moment though which is why it is not in the pattern.

	// The schema mutator needs to correspond to a keep call
	// on the column specified by the keys procedure.
	if len(keepSpec.Mutations) != 1 {
		return pn, false, nil
	} else if m, ok := keepSpec.Mutations[0].(*universe.KeepOpSpec); !ok {
		return pn, false, nil
	} else if m.Predicate.Fn != nil || len(m.Columns) != 1 {
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
	return plan.CreateUniquePhysicalNode(ctx, "ReadTagKeys", &ReadTagKeysPhysSpec{
		ReadRangePhysSpec: *fromSpec.Copy().(*ReadRangePhysSpec),
	}), true, nil
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

func (rule PushDownReadTagValuesRule) Rewrite(ctx context.Context, pn plan.Node) (plan.Node, bool, error) {
	// Retrieve the nodes and specs for all of the predecessors.
	distinctNode := pn
	distinctSpec := distinctNode.ProcedureSpec().(*universe.DistinctProcedureSpec)
	groupNode := distinctNode.Predecessors()[0]
	groupSpec := groupNode.ProcedureSpec().(*universe.GroupProcedureSpec)
	keepNode := groupNode.Predecessors()[0]
	keepSpec := asSchemaMutationProcedureSpec(keepNode.ProcedureSpec())
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
	} else if m.Predicate.Fn != nil || len(m.Columns) != 1 {
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
	return plan.CreateUniquePhysicalNode(ctx, "ReadTagValues", &ReadTagValuesPhysSpec{
		ReadRangePhysSpec: *fromSpec.Copy().(*ReadRangePhysSpec),
		TagKey:            tagKey,
	}), true, nil
}

var invalidTagKeysForTagValues = []string{
	execute.DefaultTimeColLabel,
	execute.DefaultValueColLabel,
	execute.DefaultStartColLabel,
	execute.DefaultStopColLabel,
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

	case *semantic.UnaryExpression:
		if isPushableUnaryPredicate(paramName, e) {
			return true, nil
		}

	case *semantic.BinaryExpression:
		if isPushableBinaryPredicate(paramName, e) {
			return true, nil
		}
	}

	return false, nil
}

func isPushableUnaryPredicate(paramName string, ue *semantic.UnaryExpression) bool {
	switch ue.Operator {
	case ast.NotOperator:
		// TODO(jsternberg): We should be able to rewrite `not r.host == "tag"` to `r.host != "tag"`
		// but that is beyond what we do right now.
		arg, ok := ue.Argument.(*semantic.UnaryExpression)
		if !ok {
			return false
		}
		return isPushableUnaryPredicate(paramName, arg)
	case ast.ExistsOperator:
		return isTag(paramName, ue.Argument)
	default:
		return false
	}
}

func isPushableBinaryPredicate(paramName string, be *semantic.BinaryExpression) bool {
	// Manual testing seems to indicate that (at least right now) we can
	// only handle predicates of the form <fn param>.<property> <op> <literal>
	// and the literal must be on the RHS.

	if !isLiteral(be.Right) {
		return false
	}

	// If the predicate is a string literal, we are comparing for equality,
	// it is a tag, and it is empty, then it is not pushable.
	//
	// This is because the storage engine does not consider there a difference
	// between a tag with an empty value and a non-existant tag. We have made
	// the decision that a missing tag is null and not an empty string, so empty
	// string isn't something that can be returned from the storage layer.
	if lit, ok := be.Right.(*semantic.StringLiteral); ok {
		if be.Operator == ast.EqualOperator && isTag(paramName, be.Left) && lit.Value == "" {
			// The string literal is pushable if the operator is != because
			// != "" will evaluate to true with everything that has a tag value
			// and false when the tag value is null.
			return false
		}
	}

	if isField(paramName, be.Left) && isPushableFieldOperator(be.Operator) {
		return true
	}

	if isTag(paramName, be.Left) && isPushableTagOperator(be.Operator) {
		return true
	}

	return false
}

// rewritePushableExpr will rewrite the expression for the storage layer.
func rewritePushableExpr(e semantic.Expression) (semantic.Expression, bool) {
	switch e := e.(type) {
	case *semantic.UnaryExpression:
		var changed bool
		if arg, ok := rewritePushableExpr(e.Argument); ok {
			e = e.Copy().(*semantic.UnaryExpression)
			e.Argument = arg
			changed = true
		}

		switch e.Operator {
		case ast.NotOperator:
			if be, ok := e.Argument.(*semantic.BinaryExpression); ok {
				switch be.Operator {
				case ast.EqualOperator:
					be = be.Copy().(*semantic.BinaryExpression)
					be.Operator = ast.NotEqualOperator
					return be, true
				case ast.NotEqualOperator:
					be = be.Copy().(*semantic.BinaryExpression)
					be.Operator = ast.EqualOperator
					return be, true
				}
			}
		case ast.ExistsOperator:
			return &semantic.BinaryExpression{
				Operator: ast.NotEqualOperator,
				Left:     e.Argument,
				Right: &semantic.StringLiteral{
					Value: "",
				},
			}, true
		}
		return e, changed

	case *semantic.BinaryExpression:
		left, lok := rewritePushableExpr(e.Left)
		right, rok := rewritePushableExpr(e.Right)
		if lok || rok {
			e = e.Copy().(*semantic.BinaryExpression)
			e.Left, e.Right = left, right
			return e, true
		}

	case *semantic.LogicalExpression:
		left, lok := rewritePushableExpr(e.Left)
		right, rok := rewritePushableExpr(e.Right)
		if lok || rok {
			e = e.Copy().(*semantic.LogicalExpression)
			e.Left, e.Right = left, right
			return e, true
		}
	}
	return e, false
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

// SortedPivotRule is a rule that optimizes a pivot when it is directly
// after an influxdb from.
type SortedPivotRule struct{}

func (SortedPivotRule) Name() string {
	return "SortedPivotRule"
}

func (SortedPivotRule) Pattern() plan.Pattern {
	return plan.Pat(universe.PivotKind, plan.Pat(ReadRangePhysKind))
}

func (SortedPivotRule) Rewrite(ctx context.Context, pn plan.Node) (plan.Node, bool, error) {
	pivotSpec := pn.ProcedureSpec().Copy().(*universe.PivotProcedureSpec)
	pivotSpec.IsSortedByFunc = func(cols []string, desc bool) bool {
		if desc {
			return false
		}

		// The only thing that disqualifies this from being
		// sorted is if the _value column is mentioned or if
		// the tag does not exist.
		for _, label := range cols {
			if label == execute.DefaultTimeColLabel {
				continue
			} else if label == execute.DefaultValueColLabel {
				return false
			}

			// Everything else is a tag. Even if the tag does not exist,
			// this is still considered sorted since sorting doesn't depend
			// on a tag existing.
		}

		// We are already sorted.
		return true
	}
	pivotSpec.IsKeyColumnFunc = func(label string) bool {
		if label == execute.DefaultTimeColLabel || label == execute.DefaultValueColLabel {
			return false
		}
		// Everything else would be a tag if it existed.
		// The transformation itself will catch if the column does not exist.
		return true
	}

	if err := pn.ReplaceSpec(pivotSpec); err != nil {
		return nil, false, err
	}
	return pn, false, nil
}

//
// Push Down of window aggregates.
// ReadRangePhys |> window |> { min, max, mean, count, sum }
//
type PushDownWindowAggregateRule struct{}

func (PushDownWindowAggregateRule) Name() string {
	return "PushDownWindowAggregateRule"
}

var windowPushableAggs = []plan.ProcedureKind{
	universe.CountKind,
	universe.SumKind,
	universe.MinKind,
	universe.MaxKind,
	universe.MeanKind,
	universe.FirstKind,
	universe.LastKind,
}

func (rule PushDownWindowAggregateRule) Pattern() plan.Pattern {
	return plan.OneOf(windowPushableAggs,
		plan.Pat(universe.WindowKind, plan.Pat(ReadRangePhysKind)))
}

func canPushWindowedAggregate(ctx context.Context, fnNode plan.Node) bool {
	// Check the aggregate function spec. Require the operation on _value
	// and check the feature flag associated with the aggregate function.
	switch fnNode.Kind() {
	case universe.MinKind:
		minSpec := fnNode.ProcedureSpec().(*universe.MinProcedureSpec)
		return minSpec.Column == execute.DefaultValueColLabel
	case universe.MaxKind:
		maxSpec := fnNode.ProcedureSpec().(*universe.MaxProcedureSpec)
		return maxSpec.Column == execute.DefaultValueColLabel
	case universe.MeanKind:
		meanSpec := fnNode.ProcedureSpec().(*universe.MeanProcedureSpec)
		return len(meanSpec.Columns) == 1 &&
			meanSpec.Columns[0] == execute.DefaultValueColLabel
	case universe.CountKind:
		countSpec := fnNode.ProcedureSpec().(*universe.CountProcedureSpec)
		return len(countSpec.Columns) == 1 &&
			countSpec.Columns[0] == execute.DefaultValueColLabel
	case universe.SumKind:
		sumSpec := fnNode.ProcedureSpec().(*universe.SumProcedureSpec)
		return len(sumSpec.Columns) == 1 &&
			sumSpec.Columns[0] == execute.DefaultValueColLabel
	case universe.FirstKind:
		firstSpec := fnNode.ProcedureSpec().(*universe.FirstProcedureSpec)
		return firstSpec.Column == execute.DefaultValueColLabel
	case universe.LastKind:
		lastSpec := fnNode.ProcedureSpec().(*universe.LastProcedureSpec)
		return lastSpec.Column == execute.DefaultValueColLabel
	}
	return true
}

func isPushableWindow(windowSpec *universe.WindowProcedureSpec) bool {
	// every and period must be equal
	// every.isNegative must be false
	// offset.isNegative must be false
	// timeColumn: must be "_time"
	// startColumn: must be "_start"
	// stopColumn: must be "_stop"
	// createEmpty: must be false
	window := windowSpec.Window
	return window.Every.Equal(window.Period) &&
		!window.Every.IsNegative() &&
		!window.Offset.IsNegative() &&
		windowSpec.TimeColumn == "_time" &&
		windowSpec.StartColumn == "_start" &&
		windowSpec.StopColumn == "_stop"
}

func (PushDownWindowAggregateRule) Rewrite(ctx context.Context, pn plan.Node) (plan.Node, bool, error) {
	fnNode := pn
	if !canPushWindowedAggregate(ctx, fnNode) {
		return pn, false, nil
	}

	windowNode := fnNode.Predecessors()[0]
	windowSpec := windowNode.ProcedureSpec().(*universe.WindowProcedureSpec)
	fromNode := windowNode.Predecessors()[0]
	fromSpec := fromNode.ProcedureSpec().(*ReadRangePhysSpec)

	if !isPushableWindow(windowSpec) {
		return pn, false, nil
	}

	// Rule passes.
	return plan.CreateUniquePhysicalNode(ctx, "ReadWindowAggregate", &ReadWindowAggregatePhysSpec{
		ReadRangePhysSpec: *fromSpec.Copy().(*ReadRangePhysSpec),
		Aggregates:        []plan.ProcedureKind{fnNode.Kind()},
		WindowEvery:       windowSpec.Window.Every,
		Offset:            windowSpec.Window.Offset,
		CreateEmpty:       windowSpec.CreateEmpty,
	}), true, nil
}

// PushDownWindowAggregateWithTimeRule will match the given pattern.
// ReadWindowAggregatePhys |> duplicate |> window(every: inf)
//
// If this pattern matches and the arguments to duplicate are
// matching time column names, it will set the time column on
// the spec.
type PushDownWindowAggregateByTimeRule struct{}

func (PushDownWindowAggregateByTimeRule) Name() string {
	return "PushDownWindowAggregateByTimeRule"
}

func (rule PushDownWindowAggregateByTimeRule) Pattern() plan.Pattern {
	return plan.Pat(universe.WindowKind,
		plan.Pat(universe.SchemaMutationKind,
			plan.Pat(ReadWindowAggregatePhysKind)))
}

func (PushDownWindowAggregateByTimeRule) Rewrite(ctx context.Context, pn plan.Node) (plan.Node, bool, error) {
	windowNode := pn
	windowSpec := windowNode.ProcedureSpec().(*universe.WindowProcedureSpec)

	duplicateNode := windowNode.Predecessors()[0]
	duplicateSpec, duplicateSpecOk := func() (*universe.DuplicateOpSpec, bool) {
		s := asSchemaMutationProcedureSpec(duplicateNode.ProcedureSpec())
		if len(s.Mutations) != 1 {
			return nil, false
		}
		mutator, ok := s.Mutations[0].(*universe.DuplicateOpSpec)
		return mutator, ok
	}()
	if !duplicateSpecOk {
		return pn, false, nil
	}

	// The As field must be the default time value
	// and the column must be start or stop.
	if duplicateSpec.As != execute.DefaultTimeColLabel ||
		(duplicateSpec.Column != execute.DefaultStartColLabel && duplicateSpec.Column != execute.DefaultStopColLabel) {
		return pn, false, nil
	}

	// window(every: inf)
	if windowSpec.Window.Every != values.ConvertDurationNsecs(math.MaxInt64) ||
		windowSpec.Window.Every != windowSpec.Window.Period ||
		windowSpec.TimeColumn != execute.DefaultTimeColLabel ||
		windowSpec.StartColumn != execute.DefaultStartColLabel ||
		windowSpec.StopColumn != execute.DefaultStopColLabel ||
		windowSpec.CreateEmpty {
		return pn, false, nil
	}

	// Cannot rewrite if already was rewritten.
	windowAggregateNode := duplicateNode.Predecessors()[0]
	windowAggregateSpec := windowAggregateNode.ProcedureSpec().(*ReadWindowAggregatePhysSpec)
	if windowAggregateSpec.TimeColumn != "" {
		return pn, false, nil
	}

	// Rule passes.
	windowAggregateSpec.TimeColumn = duplicateSpec.Column
	return plan.CreateUniquePhysicalNode(ctx, "ReadWindowAggregateByTime", windowAggregateSpec), true, nil
}

// PushDownBareAggregateRule is a rule that allows pushing down of aggregates
// that are directly over a ReadRange source.
type PushDownBareAggregateRule struct{}

func (p PushDownBareAggregateRule) Name() string {
	return "PushDownBareAggregateRule"
}

func (p PushDownBareAggregateRule) Pattern() plan.Pattern {
	return plan.OneOf(windowPushableAggs,
		plan.Pat(ReadRangePhysKind))
}

func (p PushDownBareAggregateRule) Rewrite(ctx context.Context, pn plan.Node) (plan.Node, bool, error) {
	fnNode := pn
	if !canPushWindowedAggregate(ctx, fnNode) {
		return pn, false, nil
	}

	fromNode := fnNode.Predecessors()[0]
	fromSpec := fromNode.ProcedureSpec().(*ReadRangePhysSpec)

	return plan.CreateUniquePhysicalNode(ctx, "ReadWindowAggregate", &ReadWindowAggregatePhysSpec{
		ReadRangePhysSpec: *fromSpec.Copy().(*ReadRangePhysSpec),
		Aggregates:        []plan.ProcedureKind{fnNode.Kind()},
		WindowEvery:       flux.ConvertDuration(math.MaxInt64 * time.Nanosecond),
	}), true, nil
}

// GroupWindowAggregateTransposeRule will match the given pattern.
// ReadGroupPhys |> window |> { min, max, count, sum }
//
// This pattern will use the PushDownWindowAggregateRule to determine
// if the ReadWindowAggregatePhys operation is available before it will
// rewrite the above. This rewrites the above to:
//
// ReadWindowAggregatePhys |> group(columns: ["_start", "_stop", ...]) |> { min, max, sum }
//
// The count aggregate uses sum to merge the results.
type GroupWindowAggregateTransposeRule struct{}

func (p GroupWindowAggregateTransposeRule) Name() string {
	return "GroupWindowAggregateTransposeRule"
}

var windowMergeablePushAggs = []plan.ProcedureKind{
	universe.MinKind,
	universe.MaxKind,
	universe.CountKind,
	universe.SumKind,
}

func (p GroupWindowAggregateTransposeRule) Pattern() plan.Pattern {
	return plan.OneOf(windowMergeablePushAggs,
		plan.Pat(universe.WindowKind, plan.Pat(ReadGroupPhysKind)))
}

func (p GroupWindowAggregateTransposeRule) Rewrite(ctx context.Context, pn plan.Node) (plan.Node, bool, error) {
	if !feature.GroupWindowAggregateTranspose().Enabled(ctx) {
		return pn, false, nil
	}

	fnNode := pn
	if !canPushWindowedAggregate(ctx, fnNode) {
		return pn, false, nil
	}

	windowNode := fnNode.Predecessors()[0]
	windowSpec := windowNode.ProcedureSpec().(*universe.WindowProcedureSpec)

	if !isPushableWindow(windowSpec) {
		return pn, false, nil
	}

	fromNode := windowNode.Predecessors()[0]
	fromSpec := fromNode.ProcedureSpec().(*ReadGroupPhysSpec)

	// This only works with GroupModeBy. It is the case
	// that ReadGroup, which we depend on as a predecessor,
	// only works with GroupModeBy so it should be impossible
	// to fail this condition, but we add this here for extra
	// protection.
	if fromSpec.GroupMode != flux.GroupModeBy {
		return pn, false, nil
	}

	// Perform the rewrite by replacing each of the nodes.
	newFromNode := plan.CreateUniquePhysicalNode(ctx, "ReadWindowAggregate", &ReadWindowAggregatePhysSpec{
		ReadRangePhysSpec: *fromSpec.ReadRangePhysSpec.Copy().(*ReadRangePhysSpec),
		Aggregates:        []plan.ProcedureKind{fnNode.Kind()},
		WindowEvery:       windowSpec.Window.Every,
		Offset:            windowSpec.Window.Offset,
		CreateEmpty:       windowSpec.CreateEmpty,
	})

	// Replace the window node with a group node first.
	groupKeys := make([]string, len(fromSpec.GroupKeys), len(fromSpec.GroupKeys)+2)
	copy(groupKeys, fromSpec.GroupKeys)
	if !execute.ContainsStr(groupKeys, execute.DefaultStartColLabel) {
		groupKeys = append(groupKeys, execute.DefaultStartColLabel)
	}
	if !execute.ContainsStr(groupKeys, execute.DefaultStopColLabel) {
		groupKeys = append(groupKeys, execute.DefaultStopColLabel)
	}
	newGroupNode := plan.CreateUniquePhysicalNode(ctx, "group", &universe.GroupProcedureSpec{
		GroupMode: flux.GroupModeBy,
		GroupKeys: groupKeys,
	})
	newFromNode.AddSuccessors(newGroupNode)
	newGroupNode.AddPredecessors(newFromNode)

	// Attach the existing function node to the new group node.
	fnNode.ClearPredecessors()
	newGroupNode.AddSuccessors(fnNode)
	fnNode.AddPredecessors(newGroupNode)

	// Replace the spec for the function if needed.
	switch spec := fnNode.ProcedureSpec().(type) {
	case *universe.CountProcedureSpec:
		newFnNode := plan.CreateUniquePhysicalNode(ctx, "sum", &universe.SumProcedureSpec{
			AggregateConfig: spec.AggregateConfig,
		})
		plan.ReplaceNode(fnNode, newFnNode)
		fnNode = newFnNode
	default:
		// No replacement required. The procedure is idempotent so
		// we can use it over and over again and get the same result.
	}
	return fnNode, true, nil
}

//
// Push Down of group aggregates.
// ReadGroupPhys |> { count }
//
type PushDownGroupAggregateRule struct{}

func (PushDownGroupAggregateRule) Name() string {
	return "PushDownGroupAggregateRule"
}

func (rule PushDownGroupAggregateRule) Pattern() plan.Pattern {
	return plan.OneOf(
		[]plan.ProcedureKind{
			universe.CountKind,
			universe.SumKind,
			universe.FirstKind,
			universe.LastKind,
			universe.MinKind,
			universe.MaxKind,
		},
		plan.Pat(ReadGroupPhysKind))
}

func (PushDownGroupAggregateRule) Rewrite(ctx context.Context, pn plan.Node) (plan.Node, bool, error) {
	group := pn.Predecessors()[0].ProcedureSpec().(*ReadGroupPhysSpec)
	// Cannot push down multiple aggregates
	if len(group.AggregateMethod) > 0 {
		return pn, false, nil
	}

	if !canPushGroupedAggregate(ctx, pn) {
		return pn, false, nil
	}

	switch pn.Kind() {
	case universe.CountKind:
		// ReadGroup() -> count => ReadGroup(count)
		node := plan.CreateUniquePhysicalNode(ctx, "ReadGroupAggregate", &ReadGroupPhysSpec{
			ReadRangePhysSpec: group.ReadRangePhysSpec,
			GroupMode:         group.GroupMode,
			GroupKeys:         group.GroupKeys,
			AggregateMethod:   universe.CountKind,
		})
		return node, true, nil
	case universe.SumKind:
		// ReadGroup() -> sum => ReadGroup(sum)
		node := plan.CreateUniquePhysicalNode(ctx, "ReadGroupAggregate", &ReadGroupPhysSpec{
			ReadRangePhysSpec: group.ReadRangePhysSpec,
			GroupMode:         group.GroupMode,
			GroupKeys:         group.GroupKeys,
			AggregateMethod:   universe.SumKind,
		})
		return node, true, nil
	case universe.FirstKind:
		// ReadGroup() -> first => ReadGroup(first)
		node := plan.CreateUniquePhysicalNode(ctx, "ReadGroupAggregate", &ReadGroupPhysSpec{
			ReadRangePhysSpec: group.ReadRangePhysSpec,
			GroupMode:         group.GroupMode,
			GroupKeys:         group.GroupKeys,
			AggregateMethod:   universe.FirstKind,
		})
		return node, true, nil
	case universe.LastKind:
		// ReadGroup() -> last => ReadGroup(last)
		node := plan.CreateUniquePhysicalNode(ctx, "ReadGroupAggregate", &ReadGroupPhysSpec{
			ReadRangePhysSpec: group.ReadRangePhysSpec,
			GroupMode:         group.GroupMode,
			GroupKeys:         group.GroupKeys,
			AggregateMethod:   universe.LastKind,
		})
		return node, true, nil
	case universe.MinKind:
		// ReadGroup() -> min => ReadGroup(min)
		node := plan.CreateUniquePhysicalNode(ctx, "ReadGroupAggregate", &ReadGroupPhysSpec{
			ReadRangePhysSpec: group.ReadRangePhysSpec,
			GroupMode:         group.GroupMode,
			GroupKeys:         group.GroupKeys,
			AggregateMethod:   universe.MinKind,
		})
		return node, true, nil
	case universe.MaxKind:
		// ReadGroup() -> max => ReadGroup(max)
		node := plan.CreateUniquePhysicalNode(ctx, "ReadGroupAggregate", &ReadGroupPhysSpec{
			ReadRangePhysSpec: group.ReadRangePhysSpec,
			GroupMode:         group.GroupMode,
			GroupKeys:         group.GroupKeys,
			AggregateMethod:   universe.MaxKind,
		})
		return node, true, nil
	}
	return pn, false, nil
}

func canPushGroupedAggregate(ctx context.Context, pn plan.Node) bool {
	switch pn.Kind() {
	case universe.CountKind:
		agg := pn.ProcedureSpec().(*universe.CountProcedureSpec)
		return len(agg.Columns) == 1 && agg.Columns[0] == execute.DefaultValueColLabel
	case universe.SumKind:
		agg := pn.ProcedureSpec().(*universe.SumProcedureSpec)
		return len(agg.Columns) == 1 && agg.Columns[0] == execute.DefaultValueColLabel
	case universe.FirstKind:
		agg := pn.ProcedureSpec().(*universe.FirstProcedureSpec)
		return agg.Column == execute.DefaultValueColLabel
	case universe.LastKind:
		agg := pn.ProcedureSpec().(*universe.LastProcedureSpec)
		return agg.Column == execute.DefaultValueColLabel
	case universe.MaxKind:
		agg := pn.ProcedureSpec().(*universe.MaxProcedureSpec)
		return agg.Column == execute.DefaultValueColLabel
	case universe.MinKind:
		agg := pn.ProcedureSpec().(*universe.MinProcedureSpec)
		return agg.Column == execute.DefaultValueColLabel
	}
	return false
}

func asSchemaMutationProcedureSpec(spec plan.ProcedureSpec) *universe.SchemaMutationProcedureSpec {
	if s, ok := spec.(*universe.DualImplProcedureSpec); ok {
		spec = s.ProcedureSpec
	}
	return spec.(*universe.SchemaMutationProcedureSpec)
}

type MergeFiltersRule struct{}

func (MergeFiltersRule) Name() string {
	return universe.MergeFiltersRule{}.Name()
}

func (MergeFiltersRule) Pattern() plan.Pattern {
	return universe.MergeFiltersRule{}.Pattern()
}

func (r MergeFiltersRule) Rewrite(ctx context.Context, pn plan.Node) (plan.Node, bool, error) {
	return universe.MergeFiltersRule{}.Rewrite(ctx, pn)
}
