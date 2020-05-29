package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
)

var _ influxdb.OnboardingService = (*AuthedOnboardSvc)(nil)

// TODO (al): remove authorizer/org when the org service moves to tenant

// AuthedOnboardSvc wraps a influxdb.OnboardingService and authorizes actions
// against it appropriately.
type AuthedOnboardSvc struct {
	s influxdb.OnboardingService
}

// NewAuthedOnboardSvc constructs an instance of an authorizing org serivce.
func NewAuthedOnboardSvc(s influxdb.OnboardingService) *AuthedOnboardSvc {
	return &AuthedOnboardSvc{
		s: s,
	}
}

// IsOnboarding pass through. this is handled by the underlying service layer
func (s *AuthedOnboardSvc) IsOnboarding(ctx context.Context) (bool, error) {
	return s.s.IsOnboarding(ctx)
}

// OnboardInitialUser pass through. this is handled by the underlying service layer
func (s *AuthedOnboardSvc) OnboardInitialUser(ctx context.Context, req *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	return s.s.OnboardInitialUser(ctx, req)
}

// OnboardUser needs to confirm this user has access to do global create for multiple resources
func (s *AuthedOnboardSvc) OnboardUser(ctx context.Context, req *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	if _, _, err := authorizer.AuthorizeWriteGlobal(ctx, influxdb.OrgsResourceType); err != nil {
		return nil, err
	}
	if _, _, err := authorizer.AuthorizeWriteGlobal(ctx, influxdb.UsersResourceType); err != nil {
		return nil, err
	}
	if _, _, err := authorizer.AuthorizeWriteGlobal(ctx, influxdb.BucketsResourceType); err != nil {
		return nil, err
	}
	return s.s.OnboardUser(ctx, req)
}
