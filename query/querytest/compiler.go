package querytest

import (
	"context"

	"github.com/influxdata/platform/query"
)

// FromCSVCompiler wraps a compiler and replaces all From operations with FromCSV
type FromCSVCompiler struct {
	query.Compiler
	InputFile string
}

func (c FromCSVCompiler) Compile(ctx context.Context) (*query.Spec, error) {
	spec, err := c.Compiler.Compile(ctx)
	if err != nil {
		return nil, err
	}
	ReplaceFromSpec(spec, c.InputFile)
	return spec, nil
}
