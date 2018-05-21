package compiler

import (
	"errors"
	"fmt"

	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
)

func Compile(f *semantic.FunctionExpression, inTypes map[string]semantic.Type, builtinScope Scope, builtinDeclarations semantic.DeclarationScope) (Func, error) {
	if builtinDeclarations == nil {
		builtinDeclarations = make(semantic.DeclarationScope)
	}
	for k, t := range inTypes {
		builtinDeclarations[k] = semantic.NewExternalVariableDeclaration(k, t)
	}
	semantic.SolveTypes(f, builtinDeclarations)
	declarations := make(map[string]semantic.VariableDeclaration, len(inTypes))
	for k, t := range inTypes {
		declarations[k] = semantic.NewExternalVariableDeclaration(k, t)
	}
	f = f.Copy().(*semantic.FunctionExpression)
	semantic.ApplyNewDeclarations(f, declarations)

	root, err := compile(f.Body, builtinScope)
	if err != nil {
		return nil, err
	}
	cpy := make(map[string]semantic.Type)
	for k, v := range inTypes {
		cpy[k] = v
	}
	return compiledFn{
		root:    root,
		inTypes: cpy,
	}, nil
}

func compile(n semantic.Node, builtIns Scope) (Evaluator, error) {
	switch n := n.(type) {
	case *semantic.BlockStatement:
		body := make([]Evaluator, len(n.Body))
		for i, s := range n.Body {
			node, err := compile(s, builtIns)
			if err != nil {
				return nil, err
			}
			body[i] = node
		}
		return &blockEvaluator{
			t:    n.ReturnStatement().Argument.Type(),
			body: body,
		}, nil
	case *semantic.ExpressionStatement:
		return nil, errors.New("statement does nothing, sideffects are not supported by the compiler")
	case *semantic.ReturnStatement:
		node, err := compile(n.Argument, builtIns)
		if err != nil {
			return nil, err
		}
		return returnEvaluator{
			Evaluator: node,
		}, nil
	case *semantic.NativeVariableDeclaration:
		node, err := compile(n.Init, builtIns)
		if err != nil {
			return nil, err
		}
		return &declarationEvaluator{
			t:    n.Init.Type(),
			id:   n.Identifier.Name,
			init: node,
		}, nil
	case *semantic.ObjectExpression:
		properties := make(map[string]Evaluator, len(n.Properties))
		propertyTypes := make(map[string]semantic.Type, len(n.Properties))
		for _, p := range n.Properties {
			node, err := compile(p.Value, builtIns)
			if err != nil {
				return nil, err
			}
			properties[p.Key.Name] = node
			propertyTypes[p.Key.Name] = node.Type()
		}
		return &objEvaluator{
			t:          semantic.NewObjectType(propertyTypes),
			properties: properties,
		}, nil
	case *semantic.IdentifierExpression:
		if v, ok := builtIns[n.Name]; ok {
			//Resolve any built in identifiers now
			return &valueEvaluator{
				value: v,
			}, nil
		}
		return &identifierEvaluator{
			t:    n.Type(),
			name: n.Name,
		}, nil
	case *semantic.MemberExpression:
		object, err := compile(n.Object, builtIns)
		if err != nil {
			return nil, err
		}
		return &memberEvaluator{
			t:        n.Type(),
			object:   object,
			property: n.Property,
		}, nil
	case *semantic.BooleanLiteral:
		return &booleanEvaluator{
			t: n.Type(),
			b: n.Value,
		}, nil
	case *semantic.IntegerLiteral:
		return &integerEvaluator{
			t: n.Type(),
			i: n.Value,
		}, nil
	case *semantic.FloatLiteral:
		return &floatEvaluator{
			t: n.Type(),
			f: n.Value,
		}, nil
	case *semantic.StringLiteral:
		return &stringEvaluator{
			t: n.Type(),
			s: n.Value,
		}, nil
	case *semantic.RegexpLiteral:
		return &regexpEvaluator{
			t: n.Type(),
			r: n.Value,
		}, nil
	case *semantic.DateTimeLiteral:
		return &timeEvaluator{
			t:    n.Type(),
			time: values.ConvertTime(n.Value),
		}, nil
	case *semantic.UnaryExpression:
		node, err := compile(n.Argument, builtIns)
		if err != nil {
			return nil, err
		}
		return &unaryEvaluator{
			t:    n.Type(),
			node: node,
		}, nil
	case *semantic.LogicalExpression:
		l, err := compile(n.Left, builtIns)
		if err != nil {
			return nil, err
		}
		r, err := compile(n.Right, builtIns)
		if err != nil {
			return nil, err
		}
		return &logicalEvaluator{
			t:        n.Type(),
			operator: n.Operator,
			left:     l,
			right:    r,
		}, nil
	case *semantic.BinaryExpression:
		l, err := compile(n.Left, builtIns)
		if err != nil {
			return nil, err
		}
		lt := l.Type()
		r, err := compile(n.Right, builtIns)
		if err != nil {
			return nil, err
		}
		rt := r.Type()
		f, err := values.LookupBinaryFunction(values.BinaryFuncSignature{
			Operator: n.Operator,
			Left:     lt,
			Right:    rt,
		})
		if err != nil {
			return nil, err
		}
		return &binaryEvaluator{
			t:     n.Type(),
			left:  l,
			right: r,
			f:     f,
		}, nil
	case *semantic.CallExpression:
		callee, err := compile(n.Callee, builtIns)
		if err != nil {
			return nil, err
		}
		args, err := compile(n.Arguments, builtIns)
		if err != nil {
			return nil, err
		}
		return &callEvaluator{
			t:      n.Type(),
			callee: callee,
			args:   args,
		}, nil
	case *semantic.FunctionExpression:
		body, err := compile(n.Body, builtIns)
		if err != nil {
			return nil, err
		}
		params := make([]functionParam, len(n.Params))
		for i, param := range n.Params {
			params[i] = functionParam{
				Key:  param.Key.Name,
				Type: param.Type(),
			}
			if param.Default != nil {
				d, err := compile(param.Default, builtIns)
				if err != nil {
					return nil, err
				}
				params[i].Default = d
			}
		}
		return &functionEvaluator{
			t:      n.Type(),
			params: params,
			body:   body,
		}, nil
	default:
		return nil, fmt.Errorf("unknown semantic node of type %T", n)
	}
}

// CompilationCache caches compilation results based on the types of the input parameters.
type CompilationCache struct {
	fn   *semantic.FunctionExpression
	root *compilationCacheNode
}

func NewCompilationCache(fn *semantic.FunctionExpression, scope Scope, decls semantic.DeclarationScope) *CompilationCache {
	return &CompilationCache{
		fn: fn,
		root: &compilationCacheNode{
			scope: scope,
			decls: decls,
		},
	}
}

// Compile returnes a compiled function bsaed on the provided types.
// The result will be cached for subsequent calls.
func (c *CompilationCache) Compile(types map[string]semantic.Type) (Func, error) {
	return c.root.compile(c.fn, 0, types)
}

type compilationCacheNode struct {
	scope Scope
	decls semantic.DeclarationScope

	children map[semantic.Type]*compilationCacheNode

	fn  Func
	err error
}

// compile recursively searches for a matching child node that has compiled the function.
// If the compilation has not been performed previously its result is cached and returned.
func (c *compilationCacheNode) compile(fn *semantic.FunctionExpression, idx int, types map[string]semantic.Type) (Func, error) {
	if idx == len(fn.Params) {
		// We are the matching child, return the cached result or do the compilation.
		if c.fn == nil && c.err == nil {
			c.fn, c.err = Compile(fn, types, c.scope, c.decls)
		}
		return c.fn, c.err
	}
	// Find the matching child based on the order.
	next := fn.Params[idx].Key.Name
	t := types[next]
	child := c.children[t]
	if child == nil {
		child = &compilationCacheNode{
			scope: c.scope,
			decls: c.decls,
		}
		if c.children == nil {
			c.children = make(map[semantic.Type]*compilationCacheNode)
		}
		c.children[t] = child
	}
	return child.compile(fn, idx+1, types)
}
