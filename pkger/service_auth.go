package pkger

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

type AuthAgent interface {
	IsWritable(ctx context.Context, orgID influxdb.ID, resType influxdb.ResourceType) error
	OrgPermissions(ctx context.Context, orgID influxdb.ID, action influxdb.Action, rest ...influxdb.Action) error
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

func (s *authMW) InitStack(ctx context.Context, userID influxdb.ID, newStack Stack) (Stack, error) {
	err := s.authAgent.IsWritable(ctx, newStack.OrgID, ResourceTypeStack)
	if err != nil {
		return Stack{}, err
	}
	return s.next.InitStack(ctx, userID, newStack)
}

func (s *authMW) DeleteStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID influxdb.ID }) error {
	err := s.authAgent.IsWritable(ctx, identifiers.OrgID, ResourceTypeStack)
	if err != nil {
		return err
	}
	return s.next.DeleteStack(ctx, identifiers)
}

func (s *authMW) ListStacks(ctx context.Context, orgID influxdb.ID, f ListFilter) ([]Stack, error) {
	err := s.authAgent.OrgPermissions(ctx, orgID, influxdb.ReadAction)
	if err != nil {
		return nil, err
	}
	return s.next.ListStacks(ctx, orgID, f)
}

func (s *authMW) CreatePkg(ctx context.Context, setters ...CreatePkgSetFn) (*Pkg, error) {
	return s.next.CreatePkg(ctx, setters...)
}

func (s *authMW) DryRun(ctx context.Context, orgID, userID influxdb.ID, pkg *Pkg, opts ...ApplyOptFn) (PkgImpactSummary, error) {
	return s.next.DryRun(ctx, orgID, userID, pkg, opts...)
}

func (s *authMW) Apply(ctx context.Context, orgID, userID influxdb.ID, pkg *Pkg, opts ...ApplyOptFn) (PkgImpactSummary, error) {
	return s.next.Apply(ctx, orgID, userID, pkg, opts...)
}
