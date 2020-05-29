package influxdb

import (
	"context"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/complete"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/values"
)

// TODO(desa): These files are possibly a temporary. This is needed
// as a part of the source work that is being done.
// See https://github.com/influxdata/platform/issues/594 for more info.

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

	// EvalAST will evaluate and run an AST.
	EvalAST(ctx context.Context, astPkg *ast.Package) ([]interpreter.SideEffect, values.Scope, error)

	// Completer will return a flux completer.
	Completer() complete.Completer
}
