package flux

import "github.com/influxdata/flux/ast"

// File creates a new *ast.File.
func File(name string, imports []*ast.ImportDeclaration, body []ast.Statement) *ast.File {
	return &ast.File{
		Name:    name,
		Imports: imports,
		Body:    body,
	}
}

// GreaterThan returns a greater than *ast.BinaryExpression.
func GreaterThan(lhs, rhs ast.Expression) *ast.BinaryExpression {
	return &ast.BinaryExpression{
		Operator: ast.GreaterThanOperator,
		Left:     lhs,
		Right:    rhs,
	}
}

// LessThan returns a less than *ast.BinaryExpression.
func LessThan(lhs, rhs ast.Expression) *ast.BinaryExpression {
	return &ast.BinaryExpression{
		Operator: ast.LessThanOperator,
		Left:     lhs,
		Right:    rhs,
	}
}

// Member returns an *ast.MemberExpression where the key is p and the values is c.
func Member(p, c string) *ast.MemberExpression {
	return &ast.MemberExpression{
		Object:   &ast.Identifier{Name: p},
		Property: &ast.Identifier{Name: c},
	}
}

// And returns an and *ast.LogicalExpression.
func And(lhs, rhs ast.Expression) *ast.LogicalExpression {
	return &ast.LogicalExpression{
		Operator: ast.AndOperator,
		Left:     lhs,
		Right:    rhs,
	}
}

// Pipe returns a *ast.PipeExpression that is a piped sequence of call expressions starting at base.
// It requires at least one call expression and will panic otherwise.
func Pipe(base ast.Expression, calls ...*ast.CallExpression) *ast.PipeExpression {
	if len(calls) < 1 {
		panic("must pipe forward to at least one *ast.CallExpression")
	}
	pe := appendPipe(base, calls[0])
	for _, call := range calls[1:] {
		pe = appendPipe(pe, call)
	}

	return pe
}

func appendPipe(base ast.Expression, next *ast.CallExpression) *ast.PipeExpression {
	return &ast.PipeExpression{
		Argument: base,
		Call:     next,
	}
}

// Call returns a *ast.CallExpression that is a function call of fn with args.
func Call(fn ast.Expression, args *ast.ObjectExpression) *ast.CallExpression {
	return &ast.CallExpression{
		Callee: fn,
		Arguments: []ast.Expression{
			args,
		},
	}
}

// ExpressionStatement returns an *ast.ExpressionStagement of e.
func ExpressionStatement(e ast.Expression) *ast.ExpressionStatement {
	return &ast.ExpressionStatement{Expression: e}
}

// Function returns an *ast.FunctionExpression with params with body b.
func Function(params []*ast.Property, b ast.Expression) *ast.FunctionExpression {
	return &ast.FunctionExpression{
		Params: params,
		Body:   b,
	}
}

// String returns an *ast.StringLiteral of s.
func String(s string) *ast.StringLiteral {
	return &ast.StringLiteral{
		Value: s,
	}
}

// Identifier returns an *ast.Identifier of i.
func Identifier(i string) *ast.Identifier {
	return &ast.Identifier{Name: i}
}

// Float returns an *ast.FloatLiteral of f.
func Float(f float64) *ast.FloatLiteral {
	return &ast.FloatLiteral{
		Value: f,
	}
}

// DefineVariable returns an *ast.VariableAssignment of id to the e. (e.g. id = <expression>)
func DefineVariable(id string, e ast.Expression) *ast.VariableAssignment {
	return &ast.VariableAssignment{
		ID: &ast.Identifier{
			Name: id,
		},
		Init: e,
	}
}

// DefineTaskOption returns an *ast.OptionStatement with the object provided. (e.g. option task = {...})
func DefineTaskOption(o *ast.ObjectExpression) *ast.OptionStatement {
	return &ast.OptionStatement{
		Assignment: DefineVariable("task", o),
	}
}

// Property returns an *ast.Property of key to e. (e.g. key: <expression>)
func Property(key string, e ast.Expression) *ast.Property {
	return &ast.Property{
		Key: &ast.Identifier{
			Name: key,
		},
		Value: e,
	}
}

// Object returns an *ast.ObjectExpression with properties ps.
func Object(ps ...*ast.Property) *ast.ObjectExpression {
	return &ast.ObjectExpression{
		Properties: ps,
	}
}

// FunctionParams returns a slice of *ast.Property for the parameters of a function.
func FunctionParams(args ...string) []*ast.Property {
	var params []*ast.Property
	for _, arg := range args {
		params = append(params, &ast.Property{Key: &ast.Identifier{Name: arg}})
	}
	return params
}

// Imports returns a []*ast.ImportDeclaration for each package in pkgs.
func Imports(pkgs ...string) []*ast.ImportDeclaration {
	var is []*ast.ImportDeclaration
	for _, pkg := range pkgs {
		is = append(is, ImportDeclaration(pkg))
	}
	return is
}

// ImportDeclaration returns an *ast.ImportDeclaration for pkg.
func ImportDeclaration(pkg string) *ast.ImportDeclaration {
	return &ast.ImportDeclaration{
		Path: &ast.StringLiteral{
			Value: pkg,
		},
	}
}
