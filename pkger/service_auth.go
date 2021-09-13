package pkger

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

type AuthAgent interface {
	IsWritable(ctx context.Context, orgID platform.ID, resType influxdb.ResourceType) error
	OrgPermissions(ctx context.Context, orgID platform.ID, action influxdb.Action, rest ...influxdb.Action) error
}

type authMW struct {
	authAgent AuthAgent
	next      SVC
}

var _ SVC = (*authMW)(nil)

// MWAuth is an auth service middleware for the packager domain.
func MWAuth(authAgent AuthAgent) SVCMiddleware {
	return func(svc SVC) SVC {
		return &authMW{
			authAgent: authAgent,
			next:      svc,
		}
	}
}

func (s *authMW) InitStack(ctx context.Context, userID platform.ID, newStack StackCreate) (Stack, error) {
	err := s.authAgent.IsWritable(ctx, newStack.OrgID, ResourceTypeStack)
	if err != nil {
		return Stack{}, err
	}
	return s.next.InitStack(ctx, userID, newStack)
}

func (s *authMW) UninstallStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) (Stack, error) {
	err := s.authAgent.IsWritable(ctx, identifiers.OrgID, ResourceTypeStack)
	if err != nil {
		return Stack{}, err
	}
	return s.next.UninstallStack(ctx, identifiers)
}

func (s *authMW) DeleteStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) error {
	err := s.authAgent.IsWritable(ctx, identifiers.OrgID, ResourceTypeStack)
	if err != nil {
		return err
	}
	return s.next.DeleteStack(ctx, identifiers)
}

func (s *authMW) ListStacks(ctx context.Context, orgID platform.ID, f ListFilter) ([]Stack, error) {
	err := s.authAgent.OrgPermissions(ctx, orgID, influxdb.ReadAction)
	if err != nil {
		return nil, err
	}
	return s.next.ListStacks(ctx, orgID, f)
}

func (s *authMW) ReadStack(ctx context.Context, id platform.ID) (Stack, error) {
	st, err := s.next.ReadStack(ctx, id)
	if err != nil {
		return Stack{}, err
	}

	err = s.authAgent.OrgPermissions(ctx, st.OrgID, influxdb.ReadAction)
	if err != nil {
		return Stack{}, err
	}
	return st, nil
}

func (s *authMW) UpdateStack(ctx context.Context, upd StackUpdate) (Stack, error) {
	stack, err := s.next.ReadStack(ctx, upd.ID)
	if err != nil {
		return Stack{}, err
	}

	err = s.authAgent.IsWritable(ctx, stack.OrgID, ResourceTypeStack)
	if err != nil {
		return Stack{}, err
	}
	return s.next.UpdateStack(ctx, upd)
}

func (s *authMW) Export(ctx context.Context, opts ...ExportOptFn) (*Template, error) {
	opt, err := exportOptFromOptFns(opts)
	if err != nil {
		return nil, err
	}
	if opt.StackID != 0 {
		if _, err := s.ReadStack(ctx, opt.StackID); err != nil {
			return nil, err
		}
	}
	return s.next.Export(ctx, opts...)
}

func (s *authMW) DryRun(ctx context.Context, orgID, userID platform.ID, opts ...ApplyOptFn) (ImpactSummary, error) {
	return s.next.DryRun(ctx, orgID, userID, opts...)
}

func (s *authMW) Apply(ctx context.Context, orgID, userID platform.ID, opts ...ApplyOptFn) (ImpactSummary, error) {
	return s.next.Apply(ctx, orgID, userID, opts...)
}
