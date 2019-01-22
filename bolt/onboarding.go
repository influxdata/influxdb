package bolt

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/bbolt"
	platform "github.com/influxdata/influxdb"
)

var onboardingBucket = []byte("onboardingv1")
var onboardingKey = []byte("onboarding_key")

var _ platform.OnboardingService = (*Client)(nil)

func (c *Client) initializeOnboarding(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(onboardingBucket)); err != nil {
		return err
	}
	return nil
}

// IsOnboarding checks onboardingBucket
// to see if the onboarding key is true.
func (c *Client) IsOnboarding(ctx context.Context) (isOnboarding bool, err error) {
	err = c.db.View(func(tx *bolt.Tx) error {
		result := tx.Bucket(onboardingBucket).Get(onboardingKey)
		isOnboarding = len(result) == 0
		return nil
	})
	return isOnboarding, err
}

// PutOnboardingStatus will update the flag,
// so future onboarding request will be denied.
func (c *Client) PutOnboardingStatus(ctx context.Context, v bool) error {
	if v {
		return c.db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket(onboardingBucket).Put(onboardingKey, []byte{0x1})
		})
	}
	return nil
}

// Generate OnboardingResults from onboarding request,
// update db so this request will be disabled for the second run.
func (c *Client) Generate(ctx context.Context, req *platform.OnboardingRequest) (*platform.OnboardingResults, error) {
	isOnboarding, err := c.IsOnboarding(ctx)
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
	if err := c.CreateUser(ctx, u); err != nil {
		return nil, err
	}

	if err = c.SetPassword(ctx, u.Name, req.Password); err != nil {
		return nil, err
	}

	o := &platform.Organization{
		Name: req.Org,
	}
	if err = c.CreateOrganization(ctx, o); err != nil {
		return nil, err
	}
	bucket := &platform.Bucket{
		Name:            req.Bucket,
		Organization:    o.Name,
		OrganizationID:  o.ID,
		RetentionPeriod: time.Duration(req.RetentionPeriod) * time.Hour,
	}
	if err = c.CreateBucket(ctx, bucket); err != nil {
		return nil, err
	}

	if err := c.CreateUserResourceMapping(ctx, &platform.UserResourceMapping{
		ResourceType: platform.OrgsResourceType,
		ResourceID:   o.ID,
		UserID:       u.ID,
		UserType:     platform.Owner,
	}); err != nil {
		return nil, err
	}

	auth := &platform.Authorization{
		UserID:      u.ID,
		Description: fmt.Sprintf("%s's Token", u.Name),
		OrgID:       o.ID,
		Permissions: platform.OperPermissions(),
		Token:       req.Token,
	}
	if err = c.CreateAuthorization(ctx, auth); err != nil {
		return nil, err
	}

	if err = c.PutOnboardingStatus(ctx, true); err != nil {
		return nil, err
	}

	return &platform.OnboardingResults{
		User:   u,
		Org:    o,
		Bucket: bucket,
		Auth:   auth,
	}, nil
}
