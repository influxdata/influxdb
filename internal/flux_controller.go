package internal

import (
	"context"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/memory"
	"github.com/prometheus/client_golang/prometheus"
)

type FluxControllerMock struct {
	QueryFn func(ctx context.Context, compiler flux.Compiler) (flux.Query, error)
}

func NewFluxControllerMock() *FluxControllerMock {
	return &FluxControllerMock{
		QueryFn: func(ctx context.Context, compiler flux.Compiler) (query flux.Query, e error) {
			p, err := compiler.Compile(ctx)
			if err != nil {
				return nil, err
			}
			alloc := &memory.Allocator{}
			return p.Start(ctx, alloc)
		},
	}
}

func (m *FluxControllerMock) Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error) {
	return m.QueryFn(ctx, compiler)
}

func (m *FluxControllerMock) PrometheusCollectors() []prometheus.Collector { return nil }
