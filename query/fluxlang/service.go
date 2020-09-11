// Package language exposes the flux parser as an interface.
package fluxlang

import (
	"context"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/complete"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2"
)

// DefaultService is the default language service.
var DefaultService influxdb.FluxLanguageService = defaultService{}

type defaultService struct{}

func (d defaultService) Parse(source string) (pkg *ast.Package, err error) {
	pkg = parser.ParseSource(source)
	if ast.Check(pkg) > 0 {
		err = ast.GetError(pkg)
	}
	return pkg, err
}

func (d defaultService) EvalAST(ctx context.Context, astPkg *ast.Package) ([]interpreter.SideEffect, values.Scope, error) {
	return flux.EvalAST(ctx, astPkg)
}

func (d defaultService) Completer() complete.Completer {
	return complete.NewCompleter(flux.Prelude())
}
