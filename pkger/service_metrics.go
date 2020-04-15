package pkger

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/influxdata/influxdb/v2/kit/prom"
)

type mwMetrics struct {
	// RED metrics
	rec *metric.REDClient

	next SVC
}

var _ SVC = (*mwMetrics)(nil)

// MWMetrics is a metrics service middleware for the notification endpoint service.
func MWMetrics(reg *prom.Registry) SVCMiddleware {
	return func(svc SVC) SVC {
		return &mwMetrics{
			rec:  metric.New(reg, "pkger"),
			next: svc,
		}
	}
}

func (s *mwMetrics) InitStack(ctx context.Context, userID influxdb.ID, newStack Stack) (Stack, error) {
	rec := s.rec.Record("init_stack")
	stack, err := s.next.InitStack(ctx, userID, newStack)
	return stack, rec(err)
}

func (s *mwMetrics) CreatePkg(ctx context.Context, setters ...CreatePkgSetFn) (*Pkg, error) {
	rec := s.rec.Record("create_pkg")
	pkg, err := s.next.CreatePkg(ctx, setters...)
	return pkg, rec(err)
}

func (s *mwMetrics) DryRun(ctx context.Context, orgID, userID influxdb.ID, pkg *Pkg, opts ...ApplyOptFn) (Summary, Diff, error) {
	rec := s.rec.Record("dry_run")
	sum, diff, err := s.next.DryRun(ctx, orgID, userID, pkg, opts...)
	return sum, diff, rec(err)
}

func (s *mwMetrics) Apply(ctx context.Context, orgID, userID influxdb.ID, pkg *Pkg, opts ...ApplyOptFn) (Summary, Diff, error) {
	rec := s.rec.Record("apply")
	sum, diff, err := s.next.Apply(ctx, orgID, userID, pkg, opts...)
	return sum, diff, rec(err)
}
