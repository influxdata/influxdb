package parser

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/platform/query/ast"
)

func toIfaceSlice(v interface{}) []interface{} {
	if v == nil {
		return nil
	}
	return v.([]interface{})
}

func toDurationSlice(durations []*singleDurationLiteral) []ast.Duration {
	durs := make([]ast.Duration, len(durations))
	for i, d := range durations {
		durs[i] = ast.Duration{
			Magnitude: d.magnitude.Value,
			Unit:      d.unit,
		}
	}
	return durs
}

func program(body interface{}, text []byte, pos position) (*ast.Program, error) {
	return &ast.Program{
		Body:     body.([]ast.Statement),
		BaseNode: base(text, pos),
	}, nil
}

func srcElems(head, tails interface{}) ([]ast.Statement, error) {
	elems := []ast.Statement{head.(ast.Statement)}
	for _, tail := range toIfaceSlice(tails) {
		elem := toIfaceSlice(tail)[1] // Skip whitespace
		elems = append(elems, elem.(ast.Statement))
	}
	return elems, nil
}

func blockstmt(body interface{}, text []byte, pos position) (*ast.BlockStatement, error) {
	bodySlice := toIfaceSlice(body)
	statements := make([]ast.Statement, len(bodySlice))
	for i, s := range bodySlice {
		stmt := toIfaceSlice(s)[1] // Skip whitespace
		statements[i] = stmt.(ast.Statement)
	}
	return &ast.BlockStatement{
		BaseNode: base(text, pos),
		Body:     statements,
	}, nil
}

func optionstmt(id, expr interface{}, text []byte, pos position) (*ast.OptionStatement, error) {
	return &ast.OptionStatement{
		BaseNode: base(text, pos),
		Declaration: &ast.VariableDeclarator{
			ID:   id.(*ast.Identifier),
			Init: expr.(ast.Expression),
		},
	}, nil
}

func varstmt(declaration interface{}, text []byte, pos position) (*ast.VariableDeclaration, error) {
	return &ast.VariableDeclaration{
		Declarations: []*ast.VariableDeclarator{declaration.(*ast.VariableDeclarator)},
		BaseNode:     base(text, pos),
	}, nil
}

func vardecl(id, initializer interface{}, text []byte, pos position) (*ast.VariableDeclarator, error) {
	return &ast.VariableDeclarator{
		ID:   id.(*ast.Identifier),
		Init: initializer.(ast.Expression),
	}, nil
}

func exprstmt(expr interface{}, text []byte, pos position) (*ast.ExpressionStatement, error) {
	return &ast.ExpressionStatement{
		Expression: expr.(ast.Expression),
		BaseNode:   base(text, pos),
	}, nil
}

func returnstmt(argument interface{}, text []byte, pos position) (*ast.ReturnStatement, error) {
	return &ast.ReturnStatement{
		BaseNode: base(text, pos),
		Argument: argument.(ast.Expression),
	}, nil
}

func pipeExprs(head, tail interface{}, text []byte, pos position) (*ast.PipeExpression, error) {
	var arg ast.Expression
	arg = head.(ast.Expression)

	var pe *ast.PipeExpression
	for _, t := range toIfaceSlice(tail) {
		pe = toIfaceSlice(t)[1].(*ast.PipeExpression)
		pe.Argument = arg
		arg = pe
	}
	return pe, nil
}

func incompletePipeExpr(call interface{}, text []byte, pos position) (*ast.PipeExpression, error) {
	return &ast.PipeExpression{
		Call:     call.(*ast.CallExpression),
		BaseNode: base(text, pos),
	}, nil
}

func memberexprs(head, tail interface{}, text []byte, pos position) (ast.Expression, error) {
	res := head.(ast.Expression)
	for _, prop := range toIfaceSlice(tail) {
		res = &ast.MemberExpression{
			Object:   res,
			Property: prop.(ast.Expression),
			BaseNode: base(text, pos),
		}
	}
	return res, nil
}

func memberexpr(object, property interface{}, text []byte, pos position) (*ast.MemberExpression, error) {
	m := &ast.MemberExpression{
		BaseNode: base(text, pos),
	}

	if object != nil {
		m.Object = object.(ast.Expression)
	}

	if property != nil {
		m.Property = property.(*ast.Identifier)
	}

	return m, nil
}

func callexpr(callee, args interface{}, text []byte, pos position) (*ast.CallExpression, error) {
	c := &ast.CallExpression{
		BaseNode: base(text, pos),
	}

	if callee != nil {
		c.Callee = callee.(ast.Expression)
	}

	if args != nil {
		c.Arguments = []ast.Expression{args.(*ast.ObjectExpression)}
	}
	return c, nil
}

func callexprs(head, tail interface{}, text []byte, pos position) (ast.Expression, error) {
	expr := head.(ast.Expression)
	for _, i := range toIfaceSlice(tail) {
		switch elem := i.(type) {
		case *ast.CallExpression:
			elem.Callee = expr
			expr = elem
		case *ast.MemberExpression:
			elem.Object = expr
			expr = elem
		}
	}
	return expr, nil
}

func arrowfunc(params interface{}, body interface{}, text []byte, pos position) *ast.ArrowFunctionExpression {
	paramsSlice := toIfaceSlice(params)
	paramsList := make([]*ast.Property, len(paramsSlice))
	for i, p := range paramsSlice {
		paramsList[i] = p.(*ast.Property)
	}
	return &ast.ArrowFunctionExpression{
		BaseNode: base(text, pos),
		Params:   paramsList,
		Body:     body.(ast.Node),
	}
}

func objectexpr(first, rest interface{}, text []byte, pos position) (*ast.ObjectExpression, error) {
	props := []*ast.Property{first.(*ast.Property)}
	if rest != nil {
		for _, prop := range toIfaceSlice(rest) {
			props = append(props, prop.(*ast.Property))
		}
	}

	return &ast.ObjectExpression{
		Properties: props,
		BaseNode:   base(text, pos),
	}, nil
}

func property(key, value interface{}, text []byte, pos position) (*ast.Property, error) {
	var v ast.Expression
	if value != nil {
		v = value.(ast.Expression)
	}
	return &ast.Property{
		Key:      key.(*ast.Identifier),
		Value:    v,
		BaseNode: base(text, pos),
	}, nil
}

func identifier(text []byte, pos position) (*ast.Identifier, error) {
	return &ast.Identifier{
		Name:     string(text),
		BaseNode: base(text, pos),
	}, nil
}

func array(first, rest interface{}, text []byte, pos position) *ast.ArrayExpression {
	var elements []ast.Expression
	if first != nil {
		elements = append(elements, first.(ast.Expression))
	}
	if rest != nil {
		for _, el := range rest.([]interface{}) {
			elements = append(elements, el.(ast.Expression))
		}
	}
	return &ast.ArrayExpression{
		Elements: elements,
		BaseNode: base(text, pos),
	}
}

func logicalExpression(head, tails interface{}, text []byte, pos position) (ast.Expression, error) {
	res := head.(ast.Expression)
	for _, tail := range toIfaceSlice(tails) {
		right := toIfaceSlice(tail)
		res = &ast.LogicalExpression{
			Left:     res,
			Right:    right[3].(ast.Expression),
			Operator: right[1].(ast.LogicalOperatorKind),
			BaseNode: base(text, pos),
		}
	}
	return res, nil
}

func logicalOp(text []byte) (ast.LogicalOperatorKind, error) {
	return ast.LogicalOperatorLookup(strings.ToLower(string(text))), nil
}

func binaryExpression(head, tails interface{}, text []byte, pos position) (ast.Expression, error) {
	res := head.(ast.Expression)
	for _, tail := range toIfaceSlice(tails) {
		right := toIfaceSlice(tail)
		res = &ast.BinaryExpression{
			Left:     res,
			Right:    right[3].(ast.Expression),
			Operator: right[1].(ast.OperatorKind),
			BaseNode: base(text, pos),
		}
	}
	return res, nil
}

func unaryExpression(op, argument interface{}, text []byte, pos position) (*ast.UnaryExpression, error) {
	return &ast.UnaryExpression{
		Operator: op.(ast.OperatorKind),
		Argument: argument.(ast.Expression),
		BaseNode: base(text, pos),
	}, nil
}

func operator(text []byte) (ast.OperatorKind, error) {
	return ast.OperatorLookup(strings.ToLower(string(text))), nil
}

func stringLiteral(text []byte, pos position) (*ast.StringLiteral, error) {
	s, err := strconv.Unquote(string(text))
	if err != nil {
		return nil, err
	}
	return &ast.StringLiteral{
		BaseNode: base(text, pos),
		Value:    s,
	}, nil
}

func pipeLiteral(text []byte, pos position) *ast.PipeLiteral {
	return &ast.PipeLiteral{
		BaseNode: base(text, pos),
	}
}

func booleanLiteral(b bool, text []byte, pos position) (*ast.BooleanLiteral, error) {
	return &ast.BooleanLiteral{
		BaseNode: base(text, pos),
		Value:    b,
	}, nil
}

func integerLiteral(text []byte, pos position) (*ast.IntegerLiteral, error) {
	n, err := strconv.ParseInt(string(text), 10, 64)
	if err != nil {
		return nil, err
	}
	return &ast.IntegerLiteral{
		BaseNode: base(text, pos),
		Value:    n,
	}, nil
}

func numberLiteral(text []byte, pos position) (*ast.FloatLiteral, error) {
	n, err := strconv.ParseFloat(string(text), 64)
	if err != nil {
		return nil, err
	}
	return &ast.FloatLiteral{
		BaseNode: base(text, pos),
		Value:    n,
	}, nil
}

func regexLiteral(chars interface{}, text []byte, pos position) (*ast.RegexpLiteral, error) {
	b := new(strings.Builder)
	for _, char := range toIfaceSlice(chars) {
		b.Write(char.([]byte))
	}

	r, err := regexp.Compile(b.String())
	if err != nil {
		return nil, err
	}
	return &ast.RegexpLiteral{
		BaseNode: base(text, pos),
		Value:    r,
	}, nil
}

func durationLiteral(durations interface{}, text []byte, pos position) (*ast.DurationLiteral, error) {
	durs := toIfaceSlice(durations)
	literals := make([]*singleDurationLiteral, len(durs))
	for i, d := range durs {
		literals[i] = d.(*singleDurationLiteral)
	}
	return &ast.DurationLiteral{
		BaseNode: base(text, pos),
		Values:   toDurationSlice(literals),
	}, nil
}

func singleDuration(mag, unit interface{}, text []byte, pos position) (*singleDurationLiteral, error) {
	return &singleDurationLiteral{
		// Not an AST node
		magnitude: mag.(*ast.IntegerLiteral),
		unit:      string(unit.([]byte)),
	}, nil
}

type singleDurationLiteral struct {
	magnitude *ast.IntegerLiteral
	unit      string
}

func datetime(text []byte, pos position) (*ast.DateTimeLiteral, error) {
	t, err := time.Parse(time.RFC3339Nano, string(text))
	if err != nil {
		return nil, err
	}
	return &ast.DateTimeLiteral{
		BaseNode: base(text, pos),
		Value:    t,
	}, nil
}

func base(text []byte, pos position) *ast.BaseNode {
	return &ast.BaseNode{
		Loc: &ast.SourceLocation{
			Start: ast.Position{
				Line:   pos.line,
				Column: pos.col,
			},
			End: ast.Position{
				Line:   pos.line,
				Column: pos.col + len(text),
			},
			Source: source(text),
		},
	}
}

func source(text []byte) *string {
	str := string(text)
	return &str
}
