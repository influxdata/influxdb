package onboarding

import (
	"context"
	"fmt"
	"strings"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"go.uber.org/zap"
)

var (
	// ErrOnboardingNotAllowed occurs when request to onboard comes in and we are not allowing this request
	ErrOnboardingNotAllowed = &errors.Error{
		Code: errors.EConflict,
		Msg:  "onboarding has already been completed",
	}
)

type TenantService interface {
	RLock()
	RUnlock()

	FindUsers(context.Context, influxdb.UserFilter, ...influxdb.FindOptions) ([]*influxdb.User, int, error)
	FindOrganizations(context.Context, influxdb.OrganizationFilter, ...influxdb.FindOptions) ([]*influxdb.Organization, int, error)

	CreateUser(context.Context, *influxdb.User) error
	SetPassword(context.Context, platform.ID, string) error
	DeleteUser(context.Context, platform.ID) error

	CreateOrganization(context.Context, *influxdb.Organization) error
	CreateBucket(context.Context, *influxdb.Bucket) error
}

type service struct {
	tenantService TenantService
	authSvc       influxdb.AuthorizationService
	alwaysAllow   bool
	log           *zap.Logger
}

var _ influxdb.OnboardingService = (*service)(nil)

type ServiceOptionFn func(*service)

// WithAlwaysAllowInitialUser configures the OnboardService to
// always return true for IsOnboarding to allow multiple
// initial onboard requests.
func WithAlwaysAllowInitialUser() ServiceOptionFn {
	return func(s *service) {
		s.alwaysAllow = true
	}
}

func WithLogger(logger *zap.Logger) ServiceOptionFn {
	return func(s *service) {
		s.log = logger
	}
}

func NewService(ts TenantService, as influxdb.AuthorizationService, opts ...ServiceOptionFn) influxdb.OnboardingService {
	s := &service{
		tenantService: ts,
		authSvc:       as,
		log:           zap.NewNop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// IsOnboarding determine if onboarding request is allowed.
func (s *service) IsOnboarding(ctx context.Context) (bool, error) {
	if s.alwaysAllow {
		return true, nil
	}

	s.tenantService.RLock()
	defer s.tenantService.RUnlock()

	// We are allowed to onboard a user if we have no users or orgs.
	users, _, err := s.tenantService.FindUsers(ctx, influxdb.UserFilter{}, influxdb.FindOptions{Limit: 1})
	if err != nil {
		return false, err
	}
	orgs, _, err := s.tenantService.FindOrganizations(ctx, influxdb.OrganizationFilter{}, influxdb.FindOptions{Limit: 1})
	if err != nil {
		return false, err
	}

	return len(users) == 0 && len(orgs) == 0, nil
}

// OnboardInitialUser allows us to onboard a new user if is onboarding is allowed
func (s *service) OnboardInitialUser(ctx context.Context, req *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
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
func (s *service) onboardUser(ctx context.Context, req *influxdb.OnboardingRequest, permFn func(orgID, userID platform.ID) []influxdb.Permission) (*influxdb.OnboardingResults, error) {
	if req == nil {
		return nil, &errors.Error{
			Code: errors.EEmptyValue,
			Msg:  "onboarding failed: no request body provided",
		}
	}

	var missingFields []string
	if req.User == "" {
		missingFields = append(missingFields, "username")
	}
	if req.Org == "" {
		missingFields = append(missingFields, "org")
	}
	if req.Bucket == "" {
		missingFields = append(missingFields, "bucket")
	}
	if len(missingFields) > 0 {
		return nil, &errors.Error{
			Code: errors.EUnprocessableEntity,
			Msg:  fmt.Sprintf("onboarding failed: missing required fields [%s]", strings.Join(missingFields, ",")),
		}
	}

	result := &influxdb.OnboardingResults{}

	// create a user
	user := &influxdb.User{
		Name:   req.User,
		Status: influxdb.Active,
	}

	if err := s.tenantService.CreateUser(ctx, user); err != nil {
		return nil, err
	}

	// create users password
	if req.Password != "" {
		if err := s.tenantService.SetPassword(ctx, user.ID, req.Password); err != nil {
			// Try to clean up.
			if cleanupErr := s.tenantService.DeleteUser(ctx, user.ID); cleanupErr != nil {
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

	if err := s.tenantService.CreateOrganization(ctx, org); err != nil {
		return nil, err
	}

	// create orgs buckets
	ub := &influxdb.Bucket{
		OrgID:           org.ID,
		Name:            req.Bucket,
		Type:            influxdb.BucketTypeUser,
		RetentionPeriod: req.RetentionPeriod(),
	}

	if err := s.tenantService.CreateBucket(ctx, ub); err != nil {
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
