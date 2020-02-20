package flub

import (
	"fmt"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
)

type File struct {
	pkgname  string
	nodes    []*Node
	vars     map[string]ast.Expression
	varnames []string
	id       int
}

// NewFile will construct a new File with the given package name.
func NewFile(pkgname string) *File {
	return &File{
		pkgname: pkgname,
		vars:    make(map[string]ast.Expression),
	}
}

// Call will invoke a function with the given arguments.
// It will construct a new pipeline that is chained to this
// start Node.
func (f *File) Call(name string, args ...*KeyValuePair) Pipeline {
	expr := callExpr(name, args...)
	return f.newPipeline(expr)
}

// From calls the from() function with the bucket parameter.
func (f *File) From(bucket string) Pipeline {
	return f.Call("from",
		KV("bucket", String(bucket)),
	)
}

func (f *File) newNode(create func() ast.Expression) *Node {
	n := &Node{f: f, create: create}
	f.nodes = append(f.nodes, n)
	return n
}

func (f *File) newPipeline(start ast.Expression) Pipeline {
	return Pipeline{n: f.newNode(func() ast.Expression {
		return start
	})}
}

func (f *File) genVarName() string {
	for {
		id := fmt.Sprintf("var%d", f.id)
		f.id++
		if _, ok := f.vars[id]; !ok {
			return id
		}
	}
}

// Build will construct the AST file.
// After the file is constructed, the original file generator
// should not be reused.
func (f *File) Build() *ast.File {
	file := &ast.File{
		Package: &ast.PackageClause{
			Name: &ast.Identifier{Name: f.pkgname},
		},
	}

	// Instantiate the body backwards.
	for i := len(f.nodes) - 1; i >= 0; i-- {
		n := f.nodes[i]
		if n.children == 0 {
			file.Body = append(file.Body, &ast.ExpressionStatement{
				Expression: n.get(),
			})
		} else if n.children > 1 || n.varname != "" {
			file.Body = append(file.Body, &ast.VariableAssignment{
				ID:   &ast.Identifier{Name: n.varname},
				Init: f.vars[n.varname],
			})
		}
	}

	for i, j := 0, len(file.Body)-1; i < j; i, j = i+1, j-1 {
		file.Body[i], file.Body[j] = file.Body[j], file.Body[i]
	}
	return file
}

// Format will generate the AST and then format it as a string.
func (f *File) Format() string {
	return ast.Format(f.Build())
}

// Node is a Node within the AST pipeline. Node is used to lazily
// construct the AST expression and to redirect those using the Node
// to an identifier if the Node is assigned to a variable.
type Node struct {
	f        *File
	expr     ast.Expression
	create   func() ast.Expression
	varname  string
	children int
}

func (n *Node) get() ast.Expression {
	if n.expr != nil {
		return n.expr
	}

	// We have never instantiated this Node.
	// Perform that instantiation.
	n.expr = n.create()

	// If we have multiple children, we have
	// to wrap ourselves in a variable.
	// If a variable name has been specified,
	// try to use that.
	if n.children > 1 || n.varname != "" {
		if n.varname == "" {
			n.varname = n.f.genVarName()
		}
		n.f.vars[n.varname] = n.expr
		n.f.varnames = append(n.f.varnames, n.varname)
		n.expr = &ast.Identifier{Name: n.varname}
	}
	return n.expr
}

// Pipeline is a pipeline of calls that are chained to each other.
type Pipeline struct {
	n *Node
}

// As will assign the last Node in the AST to a variable with the given name.
func (p Pipeline) As(name string) Pipeline {
	if p.n.varname != "" {
		panic("variable name redeclared")
	}
	p.n.varname = name
	return Pipeline{n: p.n}
}

// Call will invoke a function and pipe the last Node into the new function call.
func (p Pipeline) Call(name string, args ...*KeyValuePair) Pipeline {
	n := p.n.f.newNode(func() ast.Expression {
		return &ast.PipeExpression{
			Argument: p.n.get(),
			Call:     callExpr(name, args...),
		}
	})
	p.n.children++
	return Pipeline{n: n}
}

// Range will pipe in the last Node and invoke the range() function.
func (p Pipeline) Range(start time.Duration) Pipeline {
	return p.Call("range",
		KV("start", Duration(start)),
	)
}

// Yield will pipe in the last Node and invoke the yield() function.
func (p Pipeline) Yield(name string) Pipeline {
	return p.Call("yield",
		KV("name", String(name)),
	)
}

// KeyValuePair is a pair of keys and values.
type KeyValuePair struct {
	key   string
	value *Node
}

func KV(key string, value *Node) *KeyValuePair {
	value.children++
	return &KeyValuePair{
		key:   key,
		value: value,
	}
}

func asProperties(pairs []*KeyValuePair) []*ast.Property {
	properties := make([]*ast.Property, 0, len(pairs))
	for _, kv := range pairs {
		properties = append(properties, &ast.Property{
			Key:   &ast.Identifier{Name: kv.key},
			Value: kv.value.get(),
		})
	}
	return properties
}

// callExpr will construct an *ast.CallExpression with the given name and arguments.
func callExpr(name string, args ...*KeyValuePair) *ast.CallExpression {
	var arguments []ast.Expression
	if len(args) > 0 {
		arguments = []ast.Expression{
			&ast.ObjectExpression{Properties: asProperties(args)},
		}
	}
	return &ast.CallExpression{
		Callee:    &ast.Identifier{Name: name},
		Arguments: arguments,
	}
}

// String will construct a string literal.
func String(v string) *Node {
	return &Node{
		expr: &ast.StringLiteral{
			Value: v,
		},
	}
}

// Duration will construct a duration literal.
func Duration(v time.Duration) *Node {
	d := flux.ConvertDuration(v)
	return &Node{
		expr: &ast.DurationLiteral{
			Values: d.AsValues(),
		},
	}
}
