package query

import (
	"context"

	"github.com/influxdata/platform"
)

// Transpiler can convert a query from a source lanague into a query spec.
type Transpiler interface {
	// Transpile will perform the transpilation.
	Transpile(ctx context.Context, txt string) (*Spec, error)
}

// QueryWithTranspile executes a query by first transpiling the query.
func QueryWithTranspile(ctx context.Context, orgID platform.ID, q string, qs QueryService, transpiler Transpiler) (ResultIterator, error) {
	spec, err := transpiler.Transpile(ctx, q)
	if err != nil {
		return nil, err
	}

	return qs.Query(ctx, orgID, spec)
}
