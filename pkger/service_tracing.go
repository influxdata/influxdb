package pkger

import (
	"context"

	"github.com/influxdata/influxdb/v2"
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

func (s *traceMW) InitStack(ctx context.Context, userID influxdb.ID, newStack Stack) (Stack, error) {
	span, ctx := tracing.StartSpanFromContextWithOperationName(ctx, "InitStack")
	defer span.Finish()
	return s.next.InitStack(ctx, userID, newStack)
}

func (s *traceMW) ListStacks(ctx context.Context, orgID influxdb.ID, f ListFilter) ([]Stack, error) {
	span, ctx := tracing.StartSpanFromContextWithOperationName(ctx, "ListStacks")
	defer span.Finish()

	stacks, err := s.next.ListStacks(ctx, orgID, f)
	span.LogFields(
		log.String("org_id", orgID.String()),
		log.Int("num_stacks", len(stacks)),
	)
	return stacks, err
}

func (s *traceMW) CreatePkg(ctx context.Context, setters ...CreatePkgSetFn) (pkg *Pkg, err error) {
	span, ctx := tracing.StartSpanFromContextWithOperationName(ctx, "CreatePkg")
	defer span.Finish()
	return s.next.CreatePkg(ctx, setters...)
}

func (s *traceMW) DryRun(ctx context.Context, orgID, userID influxdb.ID, pkg *Pkg, opts ...ApplyOptFn) (sum Summary, diff Diff, err error) {
	span, ctx := tracing.StartSpanFromContextWithOperationName(ctx, "DryRun")
	span.LogKV("orgID", orgID.String(), "userID", userID.String())
	defer span.Finish()
	return s.next.DryRun(ctx, orgID, userID, pkg, opts...)
}

func (s *traceMW) Apply(ctx context.Context, orgID, userID influxdb.ID, pkg *Pkg, opts ...ApplyOptFn) (sum Summary, diff Diff, err error) {
	span, ctx := tracing.StartSpanFromContextWithOperationName(ctx, "Apply")
	span.LogKV("orgID", orgID.String(), "userID", userID.String())
	defer span.Finish()
	return s.next.Apply(ctx, orgID, userID, pkg, opts...)
}
