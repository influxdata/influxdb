package mock

import (
	"context"

	platform "github.com/influxdata/influxdb/v2"
)

var _ platform.OnboardingService = (*OnboardingService)(nil)

// OnboardingService is a mock implementation of platform.OnboardingService.
type OnboardingService struct {
	PasswordsService
	BucketService
	OrganizationService
	UserService
	AuthorizationService

	IsOnboardingFn       func(context.Context) (bool, error)
	OnboardInitialUserFn func(context.Context, *platform.OnboardingRequest) (*platform.OnboardingResults, error)
	OnboardUserFn        func(context.Context, *platform.OnboardingRequest) (*platform.OnboardingResults, error)
}

// NewOnboardingService returns a mock of OnboardingService where its methods will return zero values.
func NewOnboardingService() *OnboardingService {
	return &OnboardingService{
		IsOnboardingFn: func(context.Context) (bool, error) { return false, nil },
		OnboardInitialUserFn: func(context.Context, *platform.OnboardingRequest) (*platform.OnboardingResults, error) {
			return nil, nil
		},
		OnboardUserFn: func(context.Context, *platform.OnboardingRequest) (*platform.OnboardingResults, error) {
			return nil, nil
		},
	}
}

// IsOnboarding determine if onboarding request is allowed.
func (s *OnboardingService) IsOnboarding(ctx context.Context) (bool, error) {
	return s.IsOnboardingFn(ctx)
}

// OnboardInitialUser OnboardingResults.
func (s *OnboardingService) OnboardInitialUser(ctx context.Context, req *platform.OnboardingRequest) (*platform.OnboardingResults, error) {
	return s.OnboardInitialUserFn(ctx, req)
}
