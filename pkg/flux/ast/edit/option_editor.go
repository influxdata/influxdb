package edit

import (
	"fmt"

	"github.com/influxdata/flux/ast"
)

// `OptionFn` is a function that, provided with an `OptionStatement`, returns
// an `Expression` or an error. It is used by `Option` functions to edit
// AST's options statements.
type OptionFn func(opt *ast.OptionStatement) (ast.Expression, error)

// `Option` passes the `OptionStatement` in the AST rooted at `node` that has the
// specified identifier to `fn`.
// The function can have side effects on the option statement
// and/or return a non-nil `Expression` that is set as value for the option.
// If the value returned by the edit function is `nil` (or an error is returned) no new value is set
// for the option statement (but any, maybe partial, side effect is applied).
// `Option` returns whether it could find and edit the option (possibly with errors) or not.
func Option(node ast.Node, optionIdentifier string, fn OptionFn) (bool, error) {
	oe := &optionEditor{identifier: optionIdentifier, optionFn: fn, err: nil}
	ast.Walk(oe, node)

	if oe.err != nil {
		return oe.found, oe.err
	}

	return oe.found, nil
}

// Creates an `OptionFn` for setting the value of an `OptionStatement`.
func OptionValueFn(expr ast.Expression) OptionFn {
	return func(opt *ast.OptionStatement) (ast.Expression, error) {
		return expr, nil
	}
}

// Creates an `OptionFn` for updating the values of an `OptionStatement` that has an
// `ObjectExpression` as value. Returns error if the child of the option statement is not
// an object expression. If some key is not a property of the object it is added.
func OptionObjectFn(keyMap map[string]ast.Expression) OptionFn {
	return func(opt *ast.OptionStatement) (ast.Expression, error) {
		a, ok := opt.Assignment.(*ast.VariableAssignment)
		if !ok {
			return nil, fmt.Errorf("option assignment must be variable assignment")
		}
		obj, ok := a.Init.(*ast.ObjectExpression)
		if !ok {
			return nil, fmt.Errorf("value is %s, not an object expression", a.Init.Type())
		}

		// check that every specified property exists in the object
		found := make(map[string]bool, len(obj.Properties))
		for _, p := range obj.Properties {
			found[p.Key.Key()] = true
		}

		for k := range keyMap {
			if !found[k] {
				obj.Properties = append(obj.Properties, &ast.Property{
					Key:   &ast.Identifier{Name: k},
					Value: keyMap[k],
				})
			}
		}

		for _, p := range obj.Properties {
			exp, found := keyMap[p.Key.Key()]
			if found {
				p.Value = exp
			}
		}

		return nil, nil
	}
}

//Finds the `OptionStatement` with the specified `identifier` and updates its value.
//There shouldn't be more then one option statement with the same identifier
//in a valid query.
type optionEditor struct {
	identifier string
	optionFn   OptionFn
	err        error
	found      bool
}

func (v *optionEditor) Visit(node ast.Node) ast.Visitor {
	if os, ok := node.(*ast.OptionStatement); ok {
		switch a := os.Assignment.(type) {
		case *ast.VariableAssignment:
			if a.ID.Name == v.identifier {
				v.found = true

				newInit, err := v.optionFn(os)

				if err != nil {
					v.err = err
				} else if newInit != nil {
					a.Init = newInit
				}

				return nil
			}
		case *ast.MemberAssignment:
			id, ok := a.Member.Object.(*ast.Identifier)
			if ok {
				name := id.Name + "." + a.Member.Property.Key()
				if name == v.identifier {
					v.found = true

					newInit, err := v.optionFn(os)

					if err != nil {
						v.err = err
					} else if newInit != nil {
						a.Init = newInit
					}

					return nil
				}
			}
		}
	}

	return v
}

func (v *optionEditor) Done(node ast.Node) {}
