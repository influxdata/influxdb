package reads

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxql"
	"github.com/pkg/errors"
)

const (
	fieldKey       = "_field"
	measurementKey = "_measurement"
	valueKey       = "_value"
	fieldRef       = "$"
)

// NodeVisitor can be called by Walk to traverse the Node hierarchy.
// The Visit() function is called once per node.
type NodeVisitor interface {
	Visit(*datatypes.Node) NodeVisitor
}

func WalkChildren(v NodeVisitor, node *datatypes.Node) {
	for _, n := range node.Children {
		WalkNode(v, n)
	}
}

func WalkNode(v NodeVisitor, node *datatypes.Node) {
	if v = v.Visit(node); v == nil {
		return
	}

	WalkChildren(v, node)
}

func PredicateToExprString(p *datatypes.Predicate) string {
	if p == nil {
		return "[none]"
	}

	var v predicateExpressionPrinter
	WalkNode(&v, p.Root)
	return v.Buffer.String()
}

type predicateExpressionPrinter struct {
	bytes.Buffer
}

func (v *predicateExpressionPrinter) Visit(n *datatypes.Node) NodeVisitor {
	switch n.NodeType {
	case datatypes.NodeTypeLogicalExpression:
		if len(n.Children) > 0 {
			var op string
			if n.GetLogical() == datatypes.LogicalAnd {
				op = " AND "
			} else {
				op = " OR "
			}
			WalkNode(v, n.Children[0])
			for _, e := range n.Children[1:] {
				v.Buffer.WriteString(op)
				WalkNode(v, e)
			}
		}

		return nil

	case datatypes.NodeTypeParenExpression:
		if len(n.Children) == 1 {
			v.Buffer.WriteString("( ")
			WalkNode(v, n.Children[0])
			v.Buffer.WriteString(" )")
		}

		return nil

	case datatypes.NodeTypeComparisonExpression:
		WalkNode(v, n.Children[0])
		v.Buffer.WriteByte(' ')
		switch n.GetComparison() {
		case datatypes.ComparisonEqual:
			v.Buffer.WriteByte('=')
		case datatypes.ComparisonNotEqual:
			v.Buffer.WriteString("!=")
		case datatypes.ComparisonStartsWith:
			v.Buffer.WriteString("startsWith")
		case datatypes.ComparisonRegex:
			v.Buffer.WriteString("=~")
		case datatypes.ComparisonNotRegex:
			v.Buffer.WriteString("!~")
		case datatypes.ComparisonLess:
			v.Buffer.WriteByte('<')
		case datatypes.ComparisonLessEqual:
			v.Buffer.WriteString("<=")
		case datatypes.ComparisonGreater:
			v.Buffer.WriteByte('>')
		case datatypes.ComparisonGreaterEqual:
			v.Buffer.WriteString(">=")
		}

		v.Buffer.WriteByte(' ')
		WalkNode(v, n.Children[1])
		return nil

	case datatypes.NodeTypeTagRef:
		v.Buffer.WriteByte('\'')
		v.Buffer.WriteString(n.GetTagRefValue())
		v.Buffer.WriteByte('\'')
		return nil

	case datatypes.NodeTypeFieldRef:
		v.Buffer.WriteByte('$')
		return nil

	case datatypes.NodeTypeLiteral:
		switch val := n.Value.(type) {
		case *datatypes.Node_StringValue:
			v.Buffer.WriteString(strconv.Quote(val.StringValue))

		case *datatypes.Node_RegexValue:
			v.Buffer.WriteByte('/')
			v.Buffer.WriteString(val.RegexValue)
			v.Buffer.WriteByte('/')

		case *datatypes.Node_IntegerValue:
			v.Buffer.WriteString(strconv.FormatInt(val.IntegerValue, 10))

		case *datatypes.Node_UnsignedValue:
			v.Buffer.WriteString(strconv.FormatUint(val.UnsignedValue, 10))

		case *datatypes.Node_FloatValue:
			v.Buffer.WriteString(strconv.FormatFloat(val.FloatValue, 'f', 10, 64))

		case *datatypes.Node_BooleanValue:
			if val.BooleanValue {
				v.Buffer.WriteString("true")
			} else {
				v.Buffer.WriteString("false")
			}
		}

		return nil

	default:
		return v
	}
}

func toStoragePredicate(f *semantic.FunctionExpression) (*datatypes.Predicate, error) {
	if f.Block.Parameters == nil || len(f.Block.Parameters.List) != 1 {
		return nil, errors.New("storage predicate functions must have exactly one parameter")
	}

	root, err := toStoragePredicateHelper(f.Block.Body.(semantic.Expression), f.Block.Parameters.List[0].Key.Name)
	if err != nil {
		return nil, err
	}

	return &datatypes.Predicate{
		Root: root,
	}, nil
}

func toStoragePredicateHelper(n semantic.Expression, objectName string) (*datatypes.Node, error) {
	switch n := n.(type) {
	case *semantic.LogicalExpression:
		left, err := toStoragePredicateHelper(n.Left, objectName)
		if err != nil {
			return nil, errors.Wrap(err, "left hand side")
		}
		right, err := toStoragePredicateHelper(n.Right, objectName)
		if err != nil {
			return nil, errors.Wrap(err, "right hand side")
		}
		children := []*datatypes.Node{left, right}
		switch n.Operator {
		case ast.AndOperator:
			return &datatypes.Node{
				NodeType: datatypes.NodeTypeLogicalExpression,
				Value:    &datatypes.Node_Logical_{Logical: datatypes.LogicalAnd},
				Children: children,
			}, nil
		case ast.OrOperator:
			return &datatypes.Node{
				NodeType: datatypes.NodeTypeLogicalExpression,
				Value:    &datatypes.Node_Logical_{Logical: datatypes.LogicalOr},
				Children: children,
			}, nil
		default:
			return nil, fmt.Errorf("unknown logical operator %v", n.Operator)
		}
	case *semantic.BinaryExpression:
		left, err := toStoragePredicateHelper(n.Left, objectName)
		if err != nil {
			return nil, errors.Wrap(err, "left hand side")
		}
		right, err := toStoragePredicateHelper(n.Right, objectName)
		if err != nil {
			return nil, errors.Wrap(err, "right hand side")
		}
		children := []*datatypes.Node{left, right}
		op, err := toComparisonOperator(n.Operator)
		if err != nil {
			return nil, err
		}
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeComparisonExpression,
			Value:    &datatypes.Node_Comparison_{Comparison: op},
			Children: children,
		}, nil
	case *semantic.StringLiteral:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_StringValue{
				StringValue: n.Value,
			},
		}, nil
	case *semantic.IntegerLiteral:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_IntegerValue{
				IntegerValue: n.Value,
			},
		}, nil
	case *semantic.BooleanLiteral:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_BooleanValue{
				BooleanValue: n.Value,
			},
		}, nil
	case *semantic.FloatLiteral:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_FloatValue{
				FloatValue: n.Value,
			},
		}, nil
	case *semantic.RegexpLiteral:
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeLiteral,
			Value: &datatypes.Node_RegexValue{
				RegexValue: n.Value.String(),
			},
		}, nil
	case *semantic.MemberExpression:
		// Sanity check that the object is the objectName identifier
		if ident, ok := n.Object.(*semantic.IdentifierExpression); !ok || ident.Name != objectName {
			return nil, fmt.Errorf("unknown object %q", n.Object)
		}
		switch n.Property {
		case fieldKey:
			return &datatypes.Node{
				NodeType: datatypes.NodeTypeTagRef,
				Value: &datatypes.Node_TagRefValue{
					TagRefValue: models.FieldKeyTagKey,
				},
			}, nil
		case measurementKey:
			return &datatypes.Node{
				NodeType: datatypes.NodeTypeTagRef,
				Value: &datatypes.Node_TagRefValue{
					TagRefValue: models.MeasurementTagKey,
				},
			}, nil
		case valueKey:
			return &datatypes.Node{
				NodeType: datatypes.NodeTypeFieldRef,
				Value: &datatypes.Node_FieldRefValue{
					FieldRefValue: valueKey,
				},
			}, nil

		}
		return &datatypes.Node{
			NodeType: datatypes.NodeTypeTagRef,
			Value: &datatypes.Node_TagRefValue{
				TagRefValue: n.Property,
			},
		}, nil
	case *semantic.DurationLiteral:
		return nil, errors.New("duration literals not supported in storage predicates")
	case *semantic.DateTimeLiteral:
		return nil, errors.New("time literals not supported in storage predicates")
	default:
		return nil, fmt.Errorf("unsupported semantic expression type %T", n)
	}
}

func toComparisonOperator(o ast.OperatorKind) (datatypes.Node_Comparison, error) {
	switch o {
	case ast.EqualOperator:
		return datatypes.ComparisonEqual, nil
	case ast.NotEqualOperator:
		return datatypes.ComparisonNotEqual, nil
	case ast.RegexpMatchOperator:
		return datatypes.ComparisonRegex, nil
	case ast.NotRegexpMatchOperator:
		return datatypes.ComparisonNotRegex, nil
	case ast.StartsWithOperator:
		return datatypes.ComparisonStartsWith, nil
	case ast.LessThanOperator:
		return datatypes.ComparisonLess, nil
	case ast.LessThanEqualOperator:
		return datatypes.ComparisonLessEqual, nil
	case ast.GreaterThanOperator:
		return datatypes.ComparisonGreater, nil
	case ast.GreaterThanEqualOperator:
		return datatypes.ComparisonGreaterEqual, nil
	default:
		return 0, fmt.Errorf("unknown operator %v", o)
	}
}

// NodeToExpr transforms a predicate node to an influxql.Expr.
func NodeToExpr(node *datatypes.Node, remap map[string]string) (influxql.Expr, error) {
	v := &nodeToExprVisitor{remap: remap}
	WalkNode(v, node)
	if err := v.Err(); err != nil {
		return nil, err
	}

	if len(v.exprs) > 1 {
		return nil, errors.New("invalid expression")
	}

	if len(v.exprs) == 0 {
		return nil, nil
	}

	// TODO(edd): It would be preferable if RewriteRegexConditions was a
	// package level function in influxql.
	stmt := &influxql.SelectStatement{
		Condition: v.exprs[0],
	}
	stmt.RewriteRegexConditions()
	return stmt.Condition, nil
}

type nodeToExprVisitor struct {
	remap map[string]string
	exprs []influxql.Expr
	err   error
}

func (v *nodeToExprVisitor) Visit(n *datatypes.Node) NodeVisitor {
	if v.err != nil {
		return nil
	}

	switch n.NodeType {
	case datatypes.NodeTypeLogicalExpression:
		if len(n.Children) > 1 {
			op := influxql.AND
			if n.GetLogical() == datatypes.LogicalOr {
				op = influxql.OR
			}

			WalkNode(v, n.Children[0])
			if v.err != nil {
				return nil
			}

			for i := 1; i < len(n.Children); i++ {
				WalkNode(v, n.Children[i])
				if v.err != nil {
					return nil
				}

				if len(v.exprs) >= 2 {
					lhs, rhs := v.pop2()
					v.exprs = append(v.exprs, &influxql.BinaryExpr{LHS: lhs, Op: op, RHS: rhs})
				}
			}

			return nil
		}

	case datatypes.NodeTypeParenExpression:
		if len(n.Children) != 1 {
			v.err = errors.New("parenExpression expects one child")
			return nil
		}

		WalkNode(v, n.Children[0])
		if v.err != nil {
			return nil
		}

		if len(v.exprs) > 0 {
			v.exprs = append(v.exprs, &influxql.ParenExpr{Expr: v.pop()})
		}

		return nil

	case datatypes.NodeTypeComparisonExpression:
		WalkChildren(v, n)

		if len(v.exprs) < 2 {
			v.err = errors.New("comparisonExpression expects two children")
			return nil
		}

		lhs, rhs := v.pop2()

		be := &influxql.BinaryExpr{LHS: lhs, RHS: rhs}
		switch n.GetComparison() {
		case datatypes.ComparisonEqual:
			be.Op = influxql.EQ
		case datatypes.ComparisonNotEqual:
			be.Op = influxql.NEQ
		case datatypes.ComparisonStartsWith:
			// TODO(sgc): rewrite to anchored RE, as index does not support startsWith yet
			v.err = errors.New("startsWith not implemented")
			return nil
		case datatypes.ComparisonRegex:
			be.Op = influxql.EQREGEX
		case datatypes.ComparisonNotRegex:
			be.Op = influxql.NEQREGEX
		case datatypes.ComparisonLess:
			be.Op = influxql.LT
		case datatypes.ComparisonLessEqual:
			be.Op = influxql.LTE
		case datatypes.ComparisonGreater:
			be.Op = influxql.GT
		case datatypes.ComparisonGreaterEqual:
			be.Op = influxql.GTE
		default:
			v.err = errors.New("invalid comparison operator")
			return nil
		}

		v.exprs = append(v.exprs, be)

		return nil

	case datatypes.NodeTypeTagRef:
		ref := n.GetTagRefValue()
		if v.remap != nil {
			if nk, ok := v.remap[ref]; ok {
				ref = nk
			}
		}

		v.exprs = append(v.exprs, &influxql.VarRef{Val: ref, Type: influxql.Tag})
		return nil

	case datatypes.NodeTypeFieldRef:
		v.exprs = append(v.exprs, &influxql.VarRef{Val: fieldRef})
		return nil

	case datatypes.NodeTypeLiteral:
		switch val := n.Value.(type) {
		case *datatypes.Node_StringValue:
			v.exprs = append(v.exprs, &influxql.StringLiteral{Val: val.StringValue})

		case *datatypes.Node_RegexValue:
			// TODO(sgc): consider hashing the RegexValue and cache compiled version
			re, err := regexp.Compile(val.RegexValue)
			if err != nil {
				v.err = err
			}
			v.exprs = append(v.exprs, &influxql.RegexLiteral{Val: re})
			return nil

		case *datatypes.Node_IntegerValue:
			v.exprs = append(v.exprs, &influxql.IntegerLiteral{Val: val.IntegerValue})

		case *datatypes.Node_UnsignedValue:
			v.exprs = append(v.exprs, &influxql.UnsignedLiteral{Val: val.UnsignedValue})

		case *datatypes.Node_FloatValue:
			v.exprs = append(v.exprs, &influxql.NumberLiteral{Val: val.FloatValue})

		case *datatypes.Node_BooleanValue:
			v.exprs = append(v.exprs, &influxql.BooleanLiteral{Val: val.BooleanValue})

		default:
			v.err = errors.New("unexpected literal type")
			return nil
		}

		return nil

	default:
		return v
	}
	return nil
}

func (v *nodeToExprVisitor) Err() error {
	return v.err
}

func (v *nodeToExprVisitor) pop() influxql.Expr {
	if len(v.exprs) == 0 {
		panic("stack empty")
	}

	var top influxql.Expr
	top, v.exprs = v.exprs[len(v.exprs)-1], v.exprs[:len(v.exprs)-1]
	return top
}

func (v *nodeToExprVisitor) pop2() (influxql.Expr, influxql.Expr) {
	if len(v.exprs) < 2 {
		panic("stack empty")
	}

	rhs := v.exprs[len(v.exprs)-1]
	lhs := v.exprs[len(v.exprs)-2]
	v.exprs = v.exprs[:len(v.exprs)-2]
	return lhs, rhs
}

func IsTrueBooleanLiteral(expr influxql.Expr) bool {
	b, ok := expr.(*influxql.BooleanLiteral)
	if ok {
		return b.Val
	}
	return false
}

func RewriteExprRemoveFieldValue(expr influxql.Expr) influxql.Expr {
	return influxql.RewriteExpr(expr, func(expr influxql.Expr) influxql.Expr {
		if be, ok := expr.(*influxql.BinaryExpr); ok {
			if ref, ok := be.LHS.(*influxql.VarRef); ok {
				if ref.Val == fieldRef {
					return &influxql.BooleanLiteral{Val: true}
				}
			}
		}

		return expr
	})
}

type hasRefs struct {
	refs  []string
	found []bool
}

func (v *hasRefs) allFound() bool {
	for _, val := range v.found {
		if !val {
			return false
		}
	}
	return true
}

func (v *hasRefs) Visit(node influxql.Node) influxql.Visitor {
	if v.allFound() {
		return nil
	}

	if n, ok := node.(*influxql.VarRef); ok {
		for i, r := range v.refs {
			if !v.found[i] && r == n.Val {
				v.found[i] = true
				if v.allFound() {
					return nil
				}
			}
		}
	}
	return v
}

func HasFieldValueKey(expr influxql.Expr) bool {
	refs := hasRefs{refs: []string{fieldRef}, found: make([]bool, 1)}
	influxql.Walk(&refs, expr)
	return refs.found[0]
}
