package complete

import (
	"errors"
	"fmt"
	"sort"

	"github.com/influxdata/platform/query/interpreter"
	"github.com/influxdata/platform/query/semantic"
)

type functionType interface {
	Params() map[string]semantic.Type
}

// FunctionSuggestion provides information about a function
type FunctionSuggestion struct {
	Params map[string]string
}

// Completer provides methods for suggestions in IFQL queries
type Completer struct {
	scope        *interpreter.Scope
	declarations semantic.DeclarationScope
}

// NewCompleter creates a new completer from scope and declarations
func NewCompleter(scope *interpreter.Scope, declarations semantic.DeclarationScope) Completer {
	return Completer{scope: scope, declarations: declarations}
}

// Names returns the slice of names of declared expressions
func (c Completer) Names() []string {
	names := c.scope.Names()
	sort.Strings(names)
	return names
}

// Declaration returns a declaration based on the expression name, if one exists
func (c Completer) Declaration(name string) (semantic.VariableDeclaration, error) {
	d, ok := c.declarations[name]
	if !ok {
		return d, errors.New("could not find declaration")
	}

	return d, nil
}

// FunctionNames returns all declaration names of the Function Kind
func (c Completer) FunctionNames() []string {
	funcs := []string{}

	for name, d := range c.declarations {
		if isFunction(d) {
			funcs = append(funcs, name)
		}
	}

	sort.Strings(funcs)

	return funcs
}

// FunctionSuggestion returns information needed for autocomplete suggestions for a function
func (c Completer) FunctionSuggestion(name string) (FunctionSuggestion, error) {
	var s FunctionSuggestion

	d, err := c.Declaration(name)
	if err != nil {
		return s, err
	}

	if !isFunction(d) {
		return s, fmt.Errorf("name ( %s ) is not a function", name)
	}

	funcType, ok := d.InitType().(functionType)
	if !ok {
		return s, errors.New("could not cast function type")
	}

	params := map[string]string{}

	for k, v := range funcType.Params() {
		params[k] = v.Kind().String()
	}

	s = FunctionSuggestion{
		Params: params,
	}

	return s, nil
}

func isFunction(d semantic.VariableDeclaration) bool {
	return d.InitType().Kind() == semantic.Function
}
