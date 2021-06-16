package tenant

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kv"
	"go.uber.org/zap"
)

type OnboardService struct {
	service     *Service
	authSvc     influxdb.AuthorizationService
	alwaysAllow bool
	log         *zap.Logger
}

type OnboardServiceOptionFn func(*OnboardService)

// WithAlwaysAllowInitialUser configures the OnboardService to
// always return true for IsOnboarding to allow multiple
// initial onboard requests.
func WithAlwaysAllowInitialUser() OnboardServiceOptionFn {
	return func(s *OnboardService) {
		s.alwaysAllow = true
	}
}

func WithOnboardingLogger(logger *zap.Logger) OnboardServiceOptionFn {
	return func(s *OnboardService) {
		s.log = logger
	}
}

func NewOnboardService(svc *Service, as influxdb.AuthorizationService, opts ...OnboardServiceOptionFn) influxdb.OnboardingService {
	s := &OnboardService{
		service: svc,
		authSvc: as,
		log:     zap.NewNop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// IsOnboarding determine if onboarding request is allowed.
func (s *OnboardService) IsOnboarding(ctx context.Context) (bool, error) {
	if s.alwaysAllow {
		return true, nil
	}

	allowed := false
	err := s.service.store.View(ctx, func(tx kv.Tx) error {
		// we are allowed to onboard a user if we have no users or orgs
		users, _ := s.service.store.ListUsers(ctx, tx, influxdb.FindOptions{Limit: 1})
		orgs, _ := s.service.store.ListOrgs(ctx, tx, influxdb.FindOptions{Limit: 1})
		if len(users) == 0 && len(orgs) == 0 {
			allowed = true
		}
		return nil
	})
	return allowed, err
}

// OnboardInitialUser allows us to onboard a new user if is onboarding is allowed
func (s *OnboardService) OnboardInitialUser(ctx context.Context, req *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	allowed, err := s.IsOnboarding(ctx)
	if err != nil {
		return nil, err
	}

	if !allowed {
		return nil, ErrOnboardingNotAllowed
	}

	return s.onboardUser(ctx, req, func(platform.ID, platform.ID) []influxdb.Permission { return influxdb.OperPermissions() })
}

// onboardUser allows us to onboard new users.
func (s *OnboardService) onboardUser(ctx context.Context, req *influxdb.OnboardingRequest, permFn func(orgID, userID platform.ID) []influxdb.Permission) (*influxdb.OnboardingResults, error) {
	if req == nil || req.User == "" || req.Org == "" || req.Bucket == "" {
		return nil, ErrOnboardInvalid
	}

	result := &influxdb.OnboardingResults{}

	// create a user
	user := &influxdb.User{
		Name:   req.User,
		Status: influxdb.Active,
	}

	if err := s.service.CreateUser(ctx, user); err != nil {
		return nil, err
	}

	// create users password
	if req.Password != "" {
		if err := s.service.SetPassword(ctx, user.ID, req.Password); err != nil {
			// Try to clean up.
			if cleanupErr := s.service.DeleteUser(ctx, user.ID); cleanupErr != nil {
				s.log.Error(
					"couldn't clean up user after failing to set password",
					zap.String("user", user.Name),
					zap.String("user_id", user.ID.String()),
					zap.Error(cleanupErr),
				)
			}
			return nil, err
		}
	}

	// set the new user in the context
	ctx = icontext.SetAuthorizer(ctx, &influxdb.Authorization{
		UserID: user.ID,
	})

	// create users org
	org := &influxdb.Organization{
		Name: req.Org,
	}

	if err := s.service.CreateOrganization(ctx, org); err != nil {
		return nil, err
	}

	// create orgs buckets
	ub := &influxdb.Bucket{
		OrgID:           org.ID,
		Name:            req.Bucket,
		Type:            influxdb.BucketTypeUser,
		RetentionPeriod: req.RetentionPeriod(),
	}

	if err := s.service.CreateBucket(ctx, ub); err != nil {
		return nil, err
	}

	result.User = user
	result.Org = org
	result.Bucket = ub

	// bolt doesn't lock per collection or record so we have to close our transaction
	// before we can reach out to the auth service.
	result.Auth = &influxdb.Authorization{
		Description: fmt.Sprintf("%s's Token", req.User),
		Permissions: permFn(result.Org.ID, result.User.ID),
		Token:       req.Token,
		UserID:      result.User.ID,
		OrgID:       result.Org.ID,
	}

	return result, s.authSvc.CreateAuthorization(ctx, result.Auth)
}
