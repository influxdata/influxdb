package pkger

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/metrics"
	"github.com/influxdata/influxdb/kit/prom"
)

type metricsMW struct {
	mc interface {
		RED(method string) func(error) error
	}

	next SVC
}

// MWMetrics adds metrics functionality for the service.
func MWMetrics(reg *prom.Registry) SVCMiddleware {
	mc := metrics.MustNew(reg, "service", "pkger")
	return func(svc SVC) SVC {
		return &metricsMW{
			mc:   mc,
			next: svc,
		}
	}
}

var _ SVC = (*metricsMW)(nil)

func (s *metricsMW) CreatePkg(ctx context.Context, setters ...CreatePkgSetFn) (*Pkg, error) {
	m := s.mc.RED("create")
	pkg, err := s.next.CreatePkg(ctx, setters...)
	return pkg, m(err)
}

func (s *metricsMW) DryRun(ctx context.Context, orgID, userID influxdb.ID, pkg *Pkg) (Summary, Diff, error) {
	m := s.mc.RED("dry_run")
	sum, diff, err := s.next.DryRun(ctx, orgID, userID, pkg)
	return sum, diff, m(err)
}

func (s *metricsMW) Apply(ctx context.Context, orgID, userID influxdb.ID, pkg *Pkg, opts ...ApplyOptFn) (Summary, error) {
	m := s.mc.RED("apply")
	sum, err := s.next.Apply(ctx, orgID, userID, pkg, opts...)
	return sum, m(err)
}
