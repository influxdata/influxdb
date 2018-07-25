package mock

import (
	"context"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
)

var _ execute.Executor = (*Executor)(nil)

// Executor is a mock implementation of an execute.Executor.
type Executor struct {
	ExecuteFn func(ctx context.Context, orgID platform.ID, p *plan.PlanSpec, a *execute.Allocator) (map[string]query.Result, error)
}

// NewExecutor returns a mock Executor where its methods will return zero values.
func NewExecutor() *Executor {
	return &Executor{
		ExecuteFn: func(context.Context, platform.ID, *plan.PlanSpec, *execute.Allocator) (map[string]query.Result, error) {
			return nil, nil
		},
	}
}

func (e *Executor) Execute(ctx context.Context, orgID platform.ID, p *plan.PlanSpec, a *execute.Allocator) (map[string]query.Result, error) {
	return e.ExecuteFn(ctx, orgID, p, a)
}
