package edit

import (
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/codes"
)

// GetOption finds and returns the init for the option's variable assignment
func GetOption(file *ast.File, name string) (ast.Expression, error) {
	for _, st := range file.Body {
		if val, ok := st.(*ast.OptionStatement); ok {
			assign := val.Assignment
			if va, ok := assign.(*ast.VariableAssignment); ok {
				if va.ID.Name == name {
					if ok {
						return va.Init, nil
					}
				}
			}
		}
	}

	return nil, &flux.Error{
		Code: codes.Internal,
		Msg:  "Option not found",
	}
}

// SetOption replaces an existing option's init with the provided init or adds
// the option if it doesn't exist. The file AST is mutated in place.
func SetOption(file *ast.File, name string, expr ast.Expression) {
	// check for the correct file
	for _, st := range file.Body {
		if val, ok := st.(*ast.OptionStatement); ok {
			assign := val.Assignment
			if va, ok := assign.(*ast.VariableAssignment); ok {
				if va.ID.Name == name {
					// replace the variable assignment's init
					va.Init = expr
					return
				}
			}
		}
	}
	// option was not found. prepend new option to body
	file.Body = append([]ast.Statement{&ast.OptionStatement{
		Assignment: &ast.VariableAssignment{
			ID:   &ast.Identifier{Name: name},
			Init: expr,
		},
	}}, file.Body...)
}

// DeleteOption removes an option if it exists. The file AST is mutated in place.
func DeleteOption(file *ast.File, name string) {
	for i, st := range file.Body {
		if val, ok := st.(*ast.OptionStatement); ok {
			assign := val.Assignment
			if va, ok := assign.(*ast.VariableAssignment); ok {
				if va.ID.Name == name {
					file.Body = append(file.Body[:i], file.Body[i+1:]...)
					return
				}
			}
		}
	}
}

// GetProperty finds and returns the AST node for the property value.
func GetProperty(obj *ast.ObjectExpression, key string) (ast.Expression, error) {
	for _, prop := range obj.Properties {
		if key == prop.Key.Key() {
			return prop.Value, nil
		}
	}
	return nil, &flux.Error{
		Code: codes.Internal,
		Msg:  "Property not found",
	}
}

// SetProperty replaces an existing property definition with the provided object expression or adds
// the property if it doesn't exist. The object expression AST is mutated in place.
func SetProperty(obj *ast.ObjectExpression, key string, value ast.Expression) {
	for _, prop := range obj.Properties {
		if key == prop.Key.Key() {
			prop.Value = value
			return
		}
	}

	obj.Properties = append(obj.Properties, &ast.Property{
		BaseNode: obj.BaseNode,
		Key:      &ast.Identifier{Name: key},
		Value:    value,
	})
}

// DeleteProperty removes a property from the object expression if it exists.
// The object expression AST is mutated in place.
func DeleteProperty(obj *ast.ObjectExpression, key string) {
	for i, prop := range obj.Properties {
		if key == prop.Key.Key() {
			obj.Properties = append(obj.Properties[:i], obj.Properties[i+1:]...)
			return
		}
	}
}
