package query

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/runtime"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/influxdb/v2"
)

// Visitor for searching usages of a selected option's properties in a Flux script
type optionPropertyReferenceCollector struct {
	props map[string]semantic.Expression
	// The references of the selected option's properties.
	refs map[string]*OptionPropertyReference
	// Error during the scan, if any.
	err *influxdb.Error
	// The selected option's identifier and its derived identifiers.
	identifiers map[string]bool
}

type OptionPropertyReference struct {
	propertyName  string
	propertyValue semantic.Expression
	refs          []*semantic.MemberExpression
}

// We do not attempt to track any references to the selected option's identifiers
// before we encounter an option statement to assign its value.
func (o *optionPropertyReferenceCollector) tracking(ident string) bool {
	return o.props != nil && o.identifiers[ident]
}
func (o *optionPropertyReferenceCollector) track(ident string) {
	if o.props != nil {
		o.identifiers[ident] = true
	}
}
func (o *optionPropertyReferenceCollector) untrack(ident string) {
	if o.props != nil {
		delete(o.identifiers, ident)
	}
}

// Create a map of option properties.
func (o *optionPropertyReferenceCollector) mapProperties(obj *semantic.ObjectExpression) {
	if o.props != nil {
		return
	}
	o.props = make(map[string]semantic.Expression)
	for _, prop := range obj.Properties {
		o.props[prop.Key.Key()] = prop.Value
	}
}

// Find an option property
func (o *optionPropertyReferenceCollector) findProperty(key string) semantic.Expression {
	if o.props == nil {
		return nil
	}
	return o.props[key]
}

// Clone the current visitor, usually used when the scope changes to handle shadowing.
func (o *optionPropertyReferenceCollector) clone() *optionPropertyReferenceCollector {
	// Only the identifiers map is deep-copied for handling shadows.
	clone := &optionPropertyReferenceCollector{
		refs:        o.refs,
		props:       o.props,
		err:         o.err,
		identifiers: make(map[string]bool),
	}
	for k, v := range o.identifiers {
		clone.identifiers[k] = v
	}
	return clone
}

// Scan the semantic graph for usages of the selected option's properties.
func (o *optionPropertyReferenceCollector) Visit(node semantic.Node) semantic.Visitor {
	if o.identifiers == nil || len(o.identifiers) == 0 || o.err != nil {
		// If we already have no identifiers to look for, it means the selected option is
		// completely shadowed in the current scope.
		return nil
	}
	if o.refs == nil {
		o.refs = make(map[string]*OptionPropertyReference)
	}
	switch typed := node.(type) {
	case *semantic.OptionStatement:
		if o.props != nil {
			return nil
		}
		if a, ok := typed.Assignment.(*semantic.NativeVariableAssignment); ok && o.identifiers[a.Identifier.Name] {
			if obj, ok := a.Init.(*semantic.ObjectExpression); ok {
				o.mapProperties(obj)
			} else {
				o.err = &influxdb.Error{
					Code: influxdb.EInvalid,
					Msg:  fmt.Sprintf("Invalid option %s, expected ObjectExpression, got %s.", a.Identifier.Name, a.Init.NodeType()),
				}
			}
		}
		// Return nil here to prevent the visitor from visiting typed.Assignment
		return nil
	case *semantic.Block:
		// Shadowing can take place inside the block, give it a copy of the current visitor
		// so it has its own set of tracked identifiers.
		return o.clone()
	case *semantic.FunctionExpression:
		w := o.clone()
		for _, param := range typed.Parameters.List {
			if o.tracking(param.Key.Name) {
				// shadow found
				w.untrack(param.Key.Name)
			}
			if len(w.identifiers) == 0 {
				// All tracked identifiers are shadowed in this function, no need to visit further.
				return nil
			}
		}
		return w
	case *semantic.NativeVariableAssignment:
		switch init := typed.Init.(type) {
		case *semantic.IdentifierExpression:
			if o.tracking(init.Name) {
				// x = v or v = v
				o.track(typed.Identifier.Name)
				return o
			}
		case *semantic.ObjectExpression:
			// x = {v with something}
			if init.With != nil && o.tracking(init.With.Name) {
				o.track(typed.Identifier.Name)
				return o
			}
		}
		// v = something
		if o.tracking(typed.Identifier.Name) {
			// shadow found
			o.untrack(typed.Identifier.Name)
		}
	case *semantic.MemberExpression:
		if ident, ok := typed.Object.(*semantic.IdentifierExpression); ok {
			if o.tracking(ident.Name) && o.findProperty(typed.Property) != nil {
				ref := o.refs[typed.Property]
				if ref == nil {
					ref = &OptionPropertyReference{
						propertyName:  typed.Property,
						propertyValue: o.findProperty(typed.Property),
						refs:          make([]*semantic.MemberExpression, 0, 1),
					}
					o.refs[typed.Property] = ref
				}
				ref.refs = append(ref.refs, typed)
			}
		}
	}
	return o
}

func (o *optionPropertyReferenceCollector) Done(node semantic.Node) {}

func (o *optionPropertyReferenceCollector) GetResult() map[string]*OptionPropertyReference {
	return o.refs
}

func FindFluxOptionReferences(ctx context.Context, source string, extern json.RawMessage) (map[string]*OptionPropertyReference, error) {
	c := lang.FluxCompiler{
		Query:  source,
		Extern: extern,
	}
	return FindOptionReferences(ctx, c)
}

func FindOptionReferences(ctx context.Context, c flux.Compiler) (map[string]*OptionPropertyReference, error) {
	prog, err := c.Compile(ctx, runtime.Default)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  "compilation failed",
			Err:  err,
		}
	}
	astProg, ok := prog.(*lang.AstProgram)
	if !ok {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  "FindOptionReferences() only accepts AstProgram",
		}
	}
	if err := astProg.Normalize(); err != nil {
		return nil, err
	}
	semPkg, err := runtime.AnalyzePackage(astProg.Ast)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Err:  err,
		}
	}

	collector := &optionPropertyReferenceCollector{
		identifiers: map[string]bool{"v": true},
	}
	semantic.Walk(collector, semPkg)
	if collector.err != nil {
		return nil, err
	}
	return collector.GetResult(), nil
}
