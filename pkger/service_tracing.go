package pkger

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/opentracing/opentracing-go/log"
)

type traceMW struct {
	next SVC
}

// MWTracing adds tracing functionality for the service.
func MWTracing() SVCMiddleware {
	return func(svc SVC) SVC {
		return &traceMW{next: svc}
	}
}

var _ SVC = (*traceMW)(nil)

func (s *traceMW) InitStack(ctx context.Context, userID platform.ID, newStack StackCreate) (Stack, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	return s.next.InitStack(ctx, userID, newStack)
}

func (s *traceMW) UninstallStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) (Stack, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	return s.next.UninstallStack(ctx, identifiers)
}

func (s *traceMW) DeleteStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	return s.next.DeleteStack(ctx, identifiers)
}

func (s *traceMW) ListStacks(ctx context.Context, orgID platform.ID, f ListFilter) ([]Stack, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	stacks, err := s.next.ListStacks(ctx, orgID, f)
	span.LogFields(
		log.String("org_id", orgID.String()),
		log.Int("num_stacks", len(stacks)),
	)
	return stacks, err
}

func (s *traceMW) ReadStack(ctx context.Context, id platform.ID) (Stack, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	return s.next.ReadStack(ctx, id)
}

func (s *traceMW) UpdateStack(ctx context.Context, upd StackUpdate) (Stack, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	return s.next.UpdateStack(ctx, upd)
}

func (s *traceMW) Export(ctx context.Context, opts ...ExportOptFn) (template *Template, err error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	return s.next.Export(ctx, opts...)
}

func (s *traceMW) DryRun(ctx context.Context, orgID, userID platform.ID, opts ...ApplyOptFn) (ImpactSummary, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	span.LogKV("orgID", orgID.String(), "userID", userID.String())
	defer span.Finish()
	return s.next.DryRun(ctx, orgID, userID, opts...)
}

func (s *traceMW) Apply(ctx context.Context, orgID, userID platform.ID, opts ...ApplyOptFn) (ImpactSummary, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	span.LogKV("orgID", orgID.String(), "userID", userID.String())
	defer span.Finish()
	return s.next.Apply(ctx, orgID, userID, opts...)
}
