package tenant

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

type OnboardService struct {
	service     *Service
	authSvc     influxdb.AuthorizationService
	alwaysAllow bool
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

func NewOnboardService(svc *Service, as influxdb.AuthorizationService, opts ...OnboardServiceOptionFn) influxdb.OnboardingService {
	s := &OnboardService{
		service: svc,
		authSvc: as,
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

// OnboardInitialUser allows us to onboard a new user if is onboarding is allowd
func (s *OnboardService) OnboardInitialUser(ctx context.Context, req *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	allowed, err := s.IsOnboarding(ctx)
	if err != nil {
		return nil, err
	}

	if !allowed {
		return nil, ErrOnboardingNotAllowed
	}

	return s.onboardUser(ctx, req, func(influxdb.ID) []influxdb.Permission { return influxdb.OperPermissions() })
}

// OnboardUser allows us to onboard a new user if is onboarding is allowed
func (s *OnboardService) OnboardUser(ctx context.Context, req *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	return s.onboardUser(ctx, req, influxdb.OwnerPermissions)
}

// onboardUser allows us to onboard new users.
func (s *OnboardService) onboardUser(ctx context.Context, req *influxdb.OnboardingRequest, permFn func(orgID influxdb.ID) []influxdb.Permission) (*influxdb.OnboardingResults, error) {
	if req == nil || req.User == "" || req.Password == "" || req.Org == "" || req.Bucket == "" {
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
		s.service.SetPassword(ctx, user.ID, req.Password)
	}

	// create users org
	org := &influxdb.Organization{
		Name: req.Org,
	}

	if err := s.service.CreateOrganization(ctx, org); err != nil {
		return nil, err
	}

	// create urm
	err := s.service.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
		UserID:       user.ID,
		UserType:     influxdb.Owner,
		MappingType:  influxdb.UserMappingType,
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   org.ID,
	})

	if err != nil {
		return nil, err
	}

	// create orgs buckets
	ub := &influxdb.Bucket{
		OrgID:           org.ID,
		Name:            req.Bucket,
		Type:            influxdb.BucketTypeUser,
		RetentionPeriod: time.Duration(req.RetentionPeriod) * time.Hour,
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
		Permissions: permFn(result.Org.ID),
		Token:       req.Token,
		UserID:      result.User.ID,
		OrgID:       result.Org.ID,
	}

	return result, s.authSvc.CreateAuthorization(ctx, result.Auth)
}
