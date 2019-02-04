package internal

import (
	"context"

	"github.com/influxdata/flux"
	"github.com/prometheus/client_golang/prometheus"
)

type FluxControllerMock struct {
	QueryFn func(ctx context.Context, compiler flux.Compiler) (flux.Query, error)
}

func NewFluxControllerMock() *FluxControllerMock {
	return &FluxControllerMock{
		QueryFn: func(ctx context.Context, compiler flux.Compiler) (query flux.Query, e error) {
			return NewFluxQueryMock(), nil
		},
	}
}

func (m *FluxControllerMock) Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error) {
	return m.QueryFn(ctx, compiler)
}

func (m *FluxControllerMock) PrometheusCollectors() []prometheus.Collector { return nil }

type FluxQueryMock struct {
	SpecFn   func(*FluxQueryMock) *flux.Spec
	ReadyFn  func(*FluxQueryMock) <-chan map[string]flux.Result
	DoneFn   func(*FluxQueryMock)
	CancelFn func(*FluxQueryMock)
	ErrFn    func(*FluxQueryMock) error
}

func NewFluxQueryMock() *FluxQueryMock {
	return &FluxQueryMock{
		SpecFn: func(*FluxQueryMock) *flux.Spec { return &flux.Spec{} },
		ReadyFn: func(*FluxQueryMock) <-chan map[string]flux.Result {
			ch := make(chan map[string]flux.Result)
			close(ch)
			return ch
		},
		DoneFn:   func(*FluxQueryMock) {},
		CancelFn: func(*FluxQueryMock) {},
		ErrFn:    func(*FluxQueryMock) error { return nil },
	}
}

func (m *FluxQueryMock) Spec() *flux.Spec                     { return m.SpecFn(m) }
func (m *FluxQueryMock) Ready() <-chan map[string]flux.Result { return m.ReadyFn(m) }
func (m *FluxQueryMock) Done()                                { m.DoneFn(m) }
func (m *FluxQueryMock) Cancel()                              { m.CancelFn(m) }
func (m *FluxQueryMock) Err() error                           { return m.ErrFn(m) }
func (m *FluxQueryMock) Statistics() flux.Statistics          { return flux.Statistics{} }

type FluxResultMock struct {
}

func (*FluxResultMock) Name() string {
	panic("implement me")
}

func (*FluxResultMock) Tables() flux.TableIterator {
	panic("implement me")
}
