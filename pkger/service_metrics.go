package pkger

import (
	"context"
	"net/url"
	"strings"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/prometheus/client_golang/prometheus"
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
			rec:  metric.New(reg, "pkger", metric.WithVec(templateVec())),
			next: svc,
		}
	}
}

func (s *mwMetrics) InitStack(ctx context.Context, userID influxdb.ID, newStack Stack) (Stack, error) {
	rec := s.rec.Record("init_stack")
	stack, err := s.next.InitStack(ctx, userID, newStack)
	return stack, rec(err)
}

func (s *mwMetrics) DeleteStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID influxdb.ID }) error {
	rec := s.rec.Record("delete_stack")
	return rec(s.next.DeleteStack(ctx, identifiers))
}

func (s *mwMetrics) ExportStack(ctx context.Context, orgID, stackID influxdb.ID) (*Pkg, error) {
	rec := s.rec.Record("export_stack")
	pkg, err := s.next.ExportStack(ctx, orgID, stackID)
	return pkg, rec(err)
}

func (s *mwMetrics) ListStacks(ctx context.Context, orgID influxdb.ID, f ListFilter) ([]Stack, error) {
	rec := s.rec.Record("list_stacks")
	stacks, err := s.next.ListStacks(ctx, orgID, f)
	return stacks, rec(err)
}

func (s *mwMetrics) CreatePkg(ctx context.Context, setters ...CreatePkgSetFn) (*Pkg, error) {
	rec := s.rec.Record("create_pkg")
	pkg, err := s.next.CreatePkg(ctx, setters...)
	return pkg, rec(err)
}

func (s *mwMetrics) DryRun(ctx context.Context, orgID, userID influxdb.ID, opts ...ApplyOptFn) (PkgImpactSummary, error) {
	rec := s.rec.Record("dry_run")
	impact, err := s.next.DryRun(ctx, orgID, userID, opts...)
	return impact, rec(err, metric.RecordAdditional(map[string]interface{}{
		"sources": impact.Sources,
	}))
}

func (s *mwMetrics) Apply(ctx context.Context, orgID, userID influxdb.ID, opts ...ApplyOptFn) (PkgImpactSummary, error) {
	rec := s.rec.Record("apply")
	impact, err := s.next.Apply(ctx, orgID, userID, opts...)
	return impact, rec(err, metric.RecordAdditional(map[string]interface{}{
		"sources": impact.Sources,
	}))
}

func templateVec() metric.VecOpts {
	return metric.VecOpts{
		Name:       "template_count",
		Help:       "Number of installations per template",
		LabelNames: []string{"method", "source"},
		CounterFn: func(vec *prometheus.CounterVec, o metric.CollectFnOpts) {
			if o.Err != nil {
				return
			}

			// safe to ignore the failed type assertion, a zero value
			// provides a nil slice, so no worries.
			sources, _ := o.AdditionalProps["sources"].([]string)
			for _, source := range normalizeRemoteSources(sources) {
				vec.
					With(prometheus.Labels{
						"method": o.Method,
						"source": source.String(),
					}).
					Inc()
			}
		},
	}
}

func normalizeRemoteSources(sources []string) []url.URL {
	var out []url.URL
	for _, source := range sources {
		u, err := url.Parse(source)
		if err != nil {
			continue
		}
		if !strings.HasPrefix(u.Scheme, "http") {
			continue
		}
		if u.Host == "raw.githubusercontent.com" {
			u.Host = "github.com"
			u.Path = normalizeRawGithubPath(u.Path)
		}
		out = append(out, *u)
	}
	return out
}

func normalizeRawGithubPath(rawPath string) string {
	parts := strings.Split(rawPath, "/")
	if len(parts) < 4 {
		return rawPath
	}
	// keep /account/repo as base, then append the blob to it
	tail := append([]string{"blob"}, parts[3:]...)
	parts = append(parts[:3], tail...)
	return strings.Join(parts, "/")
}
