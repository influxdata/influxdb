package complete_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform/query/complete"
	"github.com/influxdata/platform/query/interpreter"
	"github.com/influxdata/platform/query/semantic"
	"github.com/influxdata/platform/query/values"
)

func TestNames(t *testing.T) {
	s := interpreter.NewScope()
	var v values.Value
	s.Set("boom", v)
	s.Set("tick", v)

	c := complete.NewCompleter(s, semantic.DeclarationScope{})

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
	name := "foo"
	scope := interpreter.NewScope()
	scope.Set(name, values.NewIntValue(5))
	declarations := make(semantic.DeclarationScope)
	declarations[name] = semantic.NewExternalVariableDeclaration(name, semantic.Int)

	expected := declarations[name].ID()

	declaration, _ := complete.NewCompleter(scope, declarations).Declaration(name)
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
	c := complete.NewCompleter(s, d)
	results := c.FunctionNames()

	expected := []string{
		"boom",
	}

	if !cmp.Equal(results, expected) {
		t.Error(cmp.Diff(results, expected), "unexpected function names")
	}
}

func TestFunctionSuggestion(t *testing.T) {
	name := "bar"
	scope := interpreter.NewScope()
	declarations := make(semantic.DeclarationScope)
	declarations[name] = semantic.NewExternalVariableDeclaration(
		name,
		semantic.NewFunctionType(
			semantic.FunctionSignature{
				Params: map[string]semantic.Type{
					"start": semantic.Time,
					"stop":  semantic.Time,
				},
			},
		),
	)
	result, _ := complete.NewCompleter(scope, declarations).FunctionSuggestion(name)

	expected := complete.FunctionSuggestion{
		Params: map[string]string{
			"start": semantic.Time.String(),
			"stop":  semantic.Time.String(),
		},
	}

	if !cmp.Equal(result, expected) {
		t.Error(cmp.Diff(result, expected), "does not match expected suggestion")
	}
}
