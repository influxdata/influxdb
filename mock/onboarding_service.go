package mock

import (
	"context"

	platform "github.com/influxdata/influxdb"
)

var _ platform.OnboardingService = (*OnboardingService)(nil)

// OnboardingService is a mock implementation of platform.OnboardingService.
type OnboardingService struct {
	BasicAuthService
	BucketService
	OrganizationService
	UserService
	AuthorizationService

	IsOnboardingFn func(context.Context) (bool, error)
	GenerateFn     func(context.Context, *platform.OnboardingRequest) (*platform.OnboardingResults, error)
}

// NewOnboardingService returns a mock of OnboardingService where its methods will return zero values.
func NewOnboardingService() *OnboardingService {
	return &OnboardingService{
		IsOnboardingFn: func(context.Context) (bool, error) { return false, nil },
		GenerateFn: func(context.Context, *platform.OnboardingRequest) (*platform.OnboardingResults, error) {
			return nil, nil
		},
	}
}

// IsOnboarding determine if onboarding request is allowed.
func (s *OnboardingService) IsOnboarding(ctx context.Context) (bool, error) {
	return s.IsOnboardingFn(ctx)
}

// Generate OnboardingResults.
func (s *OnboardingService) Generate(ctx context.Context, req *platform.OnboardingRequest) (*platform.OnboardingResults, error) {
	return s.GenerateFn(ctx, req)
}
