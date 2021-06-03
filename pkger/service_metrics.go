package pkger

import (
	"context"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/prometheus/client_golang/prometheus"
)

type mwMetrics struct {
	// RED metrics
	rec *metric.REDClient
	// Installed template count metrics
	templateCounts *prometheus.CounterVec

	next SVC
}

var _ SVC = (*mwMetrics)(nil)

// MWMetrics is a metrics service middleware for the notification endpoint service.
func MWMetrics(reg *prom.Registry) SVCMiddleware {
	return func(svc SVC) SVC {
		m := &mwMetrics{
			rec: metric.New(reg, "pkger", metric.WithVec(templateVec()), metric.WithVec(exportVec())),
			templateCounts: prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: "templates",
				Subsystem: "installed",
				Name:      "count",
				Help:      "Total number of templates installed by name.",
			}, []string{"template"}),
			next: svc,
		}
		reg.MustRegister(m.templateCounts)
		return m
	}
}

func (s *mwMetrics) InitStack(ctx context.Context, userID platform.ID, newStack StackCreate) (Stack, error) {
	rec := s.rec.Record("init_stack")
	stack, err := s.next.InitStack(ctx, userID, newStack)
	return stack, rec(err)
}

func (s *mwMetrics) UninstallStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) (Stack, error) {
	rec := s.rec.Record("uninstall_stack")
	stack, err := s.next.UninstallStack(ctx, identifiers)
	return stack, rec(err)
}

func (s *mwMetrics) DeleteStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) error {
	rec := s.rec.Record("delete_stack")
	return rec(s.next.DeleteStack(ctx, identifiers))
}

func (s *mwMetrics) ListStacks(ctx context.Context, orgID platform.ID, f ListFilter) ([]Stack, error) {
	rec := s.rec.Record("list_stacks")
	stacks, err := s.next.ListStacks(ctx, orgID, f)
	return stacks, rec(err)
}

func (s *mwMetrics) ReadStack(ctx context.Context, id platform.ID) (Stack, error) {
	rec := s.rec.Record("read_stack")
	stack, err := s.next.ReadStack(ctx, id)
	return stack, rec(err)
}

func (s *mwMetrics) UpdateStack(ctx context.Context, upd StackUpdate) (Stack, error) {
	rec := s.rec.Record("update_stack")
	stack, err := s.next.UpdateStack(ctx, upd)
	return stack, rec(err)
}

func (s *mwMetrics) Export(ctx context.Context, opts ...ExportOptFn) (*Template, error) {
	rec := s.rec.Record("export")
	opt, err := exportOptFromOptFns(opts)
	if err != nil {
		return nil, rec(err)
	}

	template, err := s.next.Export(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return template, rec(err, metric.RecordAdditional(map[string]interface{}{
		"num_org_ids": len(opt.OrgIDs),
		"summary":     template.Summary(),
		"by_stack":    opt.StackID != 0,
	}))
}

func (s *mwMetrics) DryRun(ctx context.Context, orgID, userID platform.ID, opts ...ApplyOptFn) (ImpactSummary, error) {
	rec := s.rec.Record("dry_run")
	impact, err := s.next.DryRun(ctx, orgID, userID, opts...)
	return impact, rec(err, applyMetricAdditions(orgID, userID, impact.Sources))
}

func (s *mwMetrics) Apply(ctx context.Context, orgID, userID platform.ID, opts ...ApplyOptFn) (ImpactSummary, error) {
	rec := s.rec.Record("apply")
	impact, err := s.next.Apply(ctx, orgID, userID, opts...)
	if err == nil {
		s.templateCounts.WithLabelValues(impact.communityName()).Inc()
	}
	return impact, rec(err, applyMetricAdditions(orgID, userID, impact.Sources))
}

func applyMetricAdditions(orgID, userID platform.ID, sources []string) func(*metric.CollectFnOpts) {
	return metric.RecordAdditional(map[string]interface{}{
		"org_id":  orgID.String(),
		"sources": sources,
		"user_id": userID.String(),
	})
}

func exportVec() metric.VecOpts {
	const (
		byStack         = "by_stack"
		numOrgIDs       = "num_org_ids"
		bkts            = "buckets"
		checks          = "checks"
		dashes          = "dashboards"
		endpoints       = "endpoints"
		labels          = "labels"
		labelMappings   = "label_mappings"
		rules           = "rules"
		tasks           = "tasks"
		telegrafConfigs = "telegraf_configs"
		variables       = "variables"
	)
	return metric.VecOpts{
		Name: "template_export",
		Help: "Metrics for resources being exported",
		LabelNames: []string{
			"method",
			byStack,
			numOrgIDs,
			bkts,
			checks,
			dashes,
			endpoints,
			labels,
			labelMappings,
			rules,
			tasks,
			telegrafConfigs,
			variables,
		},
		CounterFn: func(vec *prometheus.CounterVec, o metric.CollectFnOpts) {
			if o.Err != nil {
				return
			}

			orgID, _ := o.AdditionalProps[numOrgIDs].(int)
			sum, _ := o.AdditionalProps["sum"].(Summary)
			st, _ := o.AdditionalProps[byStack].(bool)

			vec.
				With(prometheus.Labels{
					"method":        o.Method,
					byStack:         strconv.FormatBool(st),
					numOrgIDs:       strconv.Itoa(orgID),
					bkts:            strconv.Itoa(len(sum.Buckets)),
					checks:          strconv.Itoa(len(sum.Checks)),
					dashes:          strconv.Itoa(len(sum.Dashboards)),
					endpoints:       strconv.Itoa(len(sum.NotificationEndpoints)),
					labels:          strconv.Itoa(len(sum.Labels)),
					labelMappings:   strconv.Itoa(len(sum.LabelMappings)),
					rules:           strconv.Itoa(len(sum.NotificationRules)),
					tasks:           strconv.Itoa(len(sum.Tasks)),
					telegrafConfigs: strconv.Itoa(len(sum.TelegrafConfigs)),
					variables:       strconv.Itoa(len(sum.TelegrafConfigs)),
				}).
				Inc()
		},
		HistogramFn: nil,
	}
}

func templateVec() metric.VecOpts {
	return metric.VecOpts{
		Name:       "template_count",
		Help:       "Number of installations per template",
		LabelNames: []string{"method", "source", "user_id", "org_id"},
		CounterFn: func(vec *prometheus.CounterVec, o metric.CollectFnOpts) {
			if o.Err != nil {
				return
			}

			orgID, _ := o.AdditionalProps["org_id"].(string)
			userID, _ := o.AdditionalProps["user_id"].(string)

			// safe to ignore the failed type assertion, a zero value
			// provides a nil slice, so no worries.
			sources, _ := o.AdditionalProps["sources"].([]string)
			for _, source := range normalizeRemoteSources(sources) {
				vec.
					With(prometheus.Labels{
						"method":  o.Method,
						"source":  source.String(),
						"org_id":  orgID,
						"user_id": userID,
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
		if u.Host == githubRawContentHost {
			u.Host = githubHost
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
	return path.Join(parts...)
}
