// Package language exposes the flux parser as an interface.
package fluxlang

import (
	"context"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/ast/astutil"
	"github.com/influxdata/flux/complete"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/flux/runtime"
	"github.com/influxdata/flux/values"
)

// SourceQuery is a query for a source.
type SourceQuery struct {
	Query string `json:"query"`
	Type  string `json:"type"`
}

// FluxLanguageService is a service for interacting with flux code.
type FluxLanguageService interface {
	// Parse will take flux source code and produce a package.
	// If there are errors when parsing, the first error is returned.
	// An ast.Package may be returned when a parsing error occurs,
	// but it may be null if parsing didn't even occur.
	Parse(source string) (*ast.Package, error)

	// Format will produce a string for the given *ast.File.
	Format(f *ast.File) (string, error)

	// EvalAST will evaluate and run an AST.
	EvalAST(ctx context.Context, astPkg *ast.Package) ([]interpreter.SideEffect, values.Scope, error)

	// Completer will return a flux completer.
	Completer() complete.Completer
}

// DefaultService is the default language service.
var DefaultService FluxLanguageService = defaultService{}

type defaultService struct{}

func (d defaultService) Parse(source string) (pkg *ast.Package, err error) {
	pkg = parser.ParseSource(source)
	if ast.Check(pkg) > 0 {
		err = ast.GetError(pkg)
	}
	return pkg, err
}

func (d defaultService) Format(f *ast.File) (string, error) {
	return astutil.Format(f)
}

func (d defaultService) EvalAST(ctx context.Context, astPkg *ast.Package) ([]interpreter.SideEffect, values.Scope, error) {
	return runtime.EvalAST(ctx, astPkg)
}

func (d defaultService) Completer() complete.Completer {
	return complete.NewCompleter(runtime.Prelude())
}
