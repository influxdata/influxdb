package inmem

import (
	"context"
	"time"

	"github.com/influxdata/platform"
)

const onboardingKey = "onboarding_key"

var _ platform.OnboardingService = (*Service)(nil)

// IsOnboarding checks onboardingBucket
// to see if the onboarding key is true.
func (s *Service) IsOnboarding(ctx context.Context) (isOnboarding bool, err error) {
	result, ok := s.onboardingKV.Load(onboardingKey)
	isOnboarding = !ok || !result.(bool)
	return isOnboarding, nil
}

// PutOnboardingStatus will put the isOnboarding to storage
func (s *Service) PutOnboardingStatus(ctx context.Context, v bool) error {
	s.onboardingKV.Store(onboardingKey, v)
	return nil
}

// Generate OnboardingResults from onboarding request,
// update storage so this request will be disabled for the second run.
func (s *Service) Generate(ctx context.Context, req *platform.OnboardingRequest) (*platform.OnboardingResults, error) {
	isOnboarding, err := s.IsOnboarding(ctx)
	if err != nil {
		return nil, err
	}
	if !isOnboarding {
		return nil, &platform.Error{
			Code: platform.EConflict,
			Msg:  "onboarding has already been completed",
		}
	}

	if req.Password == "" {
		return nil, &platform.Error{
			Code: platform.EEmptyValue,
			Msg:  "password is empty",
		}
	}

	if req.User == "" {
		return nil, &platform.Error{
			Code: platform.EEmptyValue,
			Msg:  "username is empty",
		}
	}

	if req.Org == "" {
		return nil, &platform.Error{
			Code: platform.EEmptyValue,
			Msg:  "org name is empty",
		}
	}

	if req.Bucket == "" {
		return nil, &platform.Error{
			Code: platform.EEmptyValue,
			Msg:  "bucket name is empty",
		}
	}

	u := &platform.User{Name: req.User}
	if err := s.CreateUser(ctx, u); err != nil {
		return nil, err
	}

	if err = s.SetPassword(ctx, u.Name, req.Password); err != nil {
		return nil, err
	}

	o := &platform.Organization{
		Name: req.Org,
	}
	if err = s.CreateOrganization(ctx, o); err != nil {
		return nil, err
	}
	bucket := &platform.Bucket{
		Name:            req.Bucket,
		Organization:    o.Name,
		OrganizationID:  o.ID,
		RetentionPeriod: time.Duration(req.RetentionPeriod) * time.Hour,
	}
	if err = s.CreateBucket(ctx, bucket); err != nil {
		return nil, err
	}
	auth := &platform.Authorization{
		User:   u.Name,
		UserID: u.ID,
		Permissions: []platform.Permission{
			platform.CreateUserPermission,
			platform.DeleteUserPermission,
			platform.Permission{
				Resource: platform.OrganizationResource,
				Action:   platform.WriteAction,
			},
			platform.WriteBucketPermission(bucket.ID),
		},
	}
	if err = s.CreateAuthorization(ctx, auth); err != nil {
		return nil, err
	}

	if err = s.PutOnboardingStatus(ctx, true); err != nil {
		return nil, err
	}

	return &platform.OnboardingResults{
		User:   u,
		Org:    o,
		Bucket: bucket,
		Auth:   auth,
	}, nil
}
