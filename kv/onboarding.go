package kv

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
)

var (
	onboardingBucket = []byte("onboardingv1")
	onboardingKey    = []byte("onboarding_key")
)

var _ influxdb.OnboardingService = (*Service)(nil)

// IsOnboarding means if the initial setup of influxdb has happened.
// true means that the onboarding setup has not yet happened.
// false means that the onboarding has been completed.
func (s *Service) IsOnboarding(ctx context.Context) (bool, error) {
	notSetup := true
	err := s.kv.View(ctx, func(tx Tx) error {
		bucket, err := tx.Bucket(onboardingBucket)
		if err != nil {
			return err
		}
		v, err := bucket.Get(onboardingKey)
		// If the sentinel onboarding key is not found, then, setup
		// has not been performed.
		if IsNotFound(err) {
			notSetup = true
			return nil
		}
		if err != nil {
			return err
		}
		// If the sentinel key has any bytes whatsoever, then,
		if len(v) > 0 {
			notSetup = false // this means that it is setup.  I hate bools.
		}
		return nil
	})
	return notSetup, err
}

// PutOnboardingStatus will update the flag,
// so future onboarding request will be denied.
// true means that onboarding is NOT needed.
// false means that onboarding is needed.
func (s *Service) PutOnboardingStatus(ctx context.Context, hasBeenOnboarded bool) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.putOnboardingStatus(ctx, tx, hasBeenOnboarded)
	})
}

func (s *Service) putOnboardingStatus(ctx context.Context, tx Tx, hasBeenOnboarded bool) error {
	if hasBeenOnboarded {
		return s.setOnboarded(ctx, tx)
	}
	return s.setOffboarded(ctx, tx)
}

func (s *Service) setOffboarded(ctx context.Context, tx Tx) error {
	bucket, err := tx.Bucket(onboardingBucket)
	if err != nil {
		// TODO(goller): check err
		return err
	}
	err = bucket.Delete(onboardingKey)
	if err != nil {
		// TODO(goller): check err
		return err
	}
	return nil
}

func (s *Service) setOnboarded(ctx context.Context, tx Tx) error {
	bucket, err := tx.Bucket(onboardingBucket)
	if err != nil {
		// TODO(goller): check err
		return err
	}
	err = bucket.Put(onboardingKey, []byte{0x1})
	if err != nil {
		// TODO(goller): check err
		return err
	}
	return nil
}

// OnboardInitialUser OnboardingResults from onboarding request,
// update db so this request will be disabled for the second run.
func (s *Service) OnboardInitialUser(ctx context.Context, req *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	isOnboarding, err := s.IsOnboarding(ctx)
	if err != nil {
		return nil, err
	}
	if !isOnboarding {
		return nil, &influxdb.Error{
			Code: influxdb.EConflict,
			Msg:  "onboarding has already been completed",
		}
	}

	if err := req.Valid(); err != nil {
		return nil, err
	}

	u := &influxdb.User{Name: req.User}
	o := &influxdb.Organization{Name: req.Org}
	bucket := &influxdb.Bucket{
		Name:            req.Bucket,
		RetentionPeriod: time.Duration(req.RetentionPeriod) * time.Hour,
	}
	mapping := &influxdb.UserResourceMapping{
		ResourceType: influxdb.OrgsResourceType,
		UserType:     influxdb.Owner,
	}
	auth := &influxdb.Authorization{
		Description: fmt.Sprintf("%s's Token", u.Name),
		Permissions: influxdb.OperPermissions(),
		Token:       req.Token,
	}

	err = s.kv.Update(ctx, func(tx Tx) error {
		if err := s.createUser(ctx, tx, u); err != nil {
			return err
		}

		if err := s.setPassword(ctx, tx, u.ID, req.Password); err != nil {
			return err
		}

		if err := s.createOrganization(ctx, tx, o); err != nil {
			return err
		}

		bucket.OrgID = o.ID
		if err := s.createBucket(ctx, tx, bucket); err != nil {
			return err
		}

		mapping.ResourceID = o.ID
		mapping.UserID = u.ID
		if err := s.createUserResourceMapping(ctx, tx, mapping); err != nil {
			return err
		}

		auth.UserID = u.ID
		auth.OrgID = o.ID
		if err := s.createAuthorization(ctx, tx, auth); err != nil {
			return err
		}

		return s.putOnboardingStatus(ctx, tx, true)
	})
	if err != nil {
		return nil, err
	}

	return &influxdb.OnboardingResults{
		User:   u,
		Org:    o,
		Bucket: bucket,
		Auth:   auth,
	}, nil
}

func (s *Service) OnboardUser(ctx context.Context, req *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	return nil, errors.New("not yet implemented")
}
