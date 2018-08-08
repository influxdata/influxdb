package mock

import (
	"context"

	"github.com/influxdata/platform/query"
)

type Compiler struct {
	CompileFn func(ctx context.Context) (*query.Spec, error)
	Type      query.CompilerType
}

func (c Compiler) Compile(ctx context.Context) (*query.Spec, error) {
	return c.CompileFn(ctx)
}
func (c Compiler) CompilerType() query.CompilerType {
	if c.Type == "" {
		return "mockCompiler"
	}
	return c.Type
}
