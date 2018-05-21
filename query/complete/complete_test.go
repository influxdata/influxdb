package complete

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	_ "github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/interpreter"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
)

var scope *interpreter.Scope
var declarations semantic.DeclarationScope

func init() {
	query.FinalizeRegistration()
	s, d := query.BuiltIns()
	scope = interpreter.NewScopeWithValues(s)
	declarations = d
}

func TestNames(t *testing.T) {
	s := interpreter.NewScope()
	var v values.Value
	s.Set("boom", v)
	s.Set("tick", v)

	c := NewCompleter(s, semantic.DeclarationScope{})

	results := c.Names()
	expected := []string{
		"boom",
		"tick",
	}

	if !cmp.Equal(results, expected) {
		t.Error(cmp.Diff(results, expected), "unexpected names from declarations")
	}
}

func TestDeclaration(t *testing.T) {
	name := "range"
	expected := declarations[name].ID()

	declaration, _ := NewCompleter(scope, declarations).Declaration(name)
	result := declaration.ID()

	if !cmp.Equal(result, expected) {
		t.Error(cmp.Diff(result, expected), "unexpected declaration for name")
	}
}

func TestFunctionNames(t *testing.T) {
	d := make(semantic.DeclarationScope)
	d["boom"] = semantic.NewExternalVariableDeclaration(
		"boom", semantic.NewFunctionType(semantic.FunctionSignature{}))

	d["noBoom"] = semantic.NewExternalVariableDeclaration("noBoom", semantic.String)

	s := interpreter.NewScope()
	c := NewCompleter(s, d)
	results := c.FunctionNames()

	expected := []string{
		"boom",
	}

	if !cmp.Equal(results, expected) {
		t.Error(cmp.Diff(results, expected), "unexpected function names")
	}
}

func TestFunctionSuggestion(t *testing.T) {
	name := "range"
	result, _ := NewCompleter(scope, declarations).FunctionSuggestion(name)

	expected := FunctionSuggestion{
		Params: map[string]string{
			"start": semantic.Time.String(),
			"stop":  semantic.Time.String(),
			"table": query.TableObjectType.Kind().String(),
		},
	}

	if !cmp.Equal(result, expected) {
		t.Error(cmp.Diff(result, expected), "does not match expected suggestion")
	}
}
