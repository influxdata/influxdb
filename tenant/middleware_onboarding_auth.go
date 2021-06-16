package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

var _ influxdb.OnboardingService = (*AuthedOnboardSvc)(nil)

// TODO (al): remove authorizer/org when the org service moves to tenant

// AuthedOnboardSvc wraps a influxdb.OnboardingService and authorizes actions
// against it appropriately.
type AuthedOnboardSvc struct {
	s influxdb.OnboardingService
}

// NewAuthedOnboardSvc constructs an instance of an authorizing org service.
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
