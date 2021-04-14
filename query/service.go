package query

import (
	"context"
	"io"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
)

// QueryService represents a type capable of performing queries.
type QueryService interface {
	check.Checker

	// Query submits a query for execution returning a results iterator.
	// Cancel must be called on any returned results to free resources.
	Query(ctx context.Context, req *Request) (flux.ResultIterator, error)
}

// AsyncQueryService represents a service for performing queries where the results are delivered asynchronously.
type AsyncQueryService interface {
	// Query submits a query for execution returning immediately.
	// Done must be called on any returned Query objects.
	Query(ctx context.Context, req *Request) (flux.Query, error)
}

// ProxyQueryService performs queries and encodes the result into a writer.
// The results are opaque to a ProxyQueryService.
type ProxyQueryService interface {
	check.Checker

	// Query performs the requested query and encodes the results into w.
	// The number of bytes written to w is returned __independent__ of any error.
	Query(ctx context.Context, w io.Writer, req *ProxyRequest) (flux.Statistics, error)
}

// Parse will take flux source code and produce a package.
// If there are errors when parsing, the first error is returned.
// An ast.Package may be returned when a parsing error occurs,
// but it may be null if parsing didn't even occur.
//
// This will return an error if the FluxLanguageService is nil.
func Parse(lang fluxlang.FluxLanguageService, source string) (*ast.Package, error) {
	if lang == nil {
		return nil, &errors.Error{
			Code: errors.EInternal,
			Msg:  "flux is not configured; cannot parse",
		}
	}
	return lang.Parse(source)
}

// EvalAST will evaluate and run an AST.
//
// This will return an error if the FluxLanguageService is nil.
func EvalAST(ctx context.Context, lang fluxlang.FluxLanguageService, astPkg *ast.Package) ([]interpreter.SideEffect, values.Scope, error) {
	if lang == nil {
		return nil, nil, &errors.Error{
			Code: errors.EInternal,
			Msg:  "flux is not configured; cannot evaluate",
		}
	}
	return lang.EvalAST(ctx, astPkg)
}
