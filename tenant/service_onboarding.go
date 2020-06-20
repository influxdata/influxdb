package tenant

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

type OnboardService struct {
	store   *Store
	authSvc influxdb.AuthorizationService

	eventPublisher chan influxdb.Event
	eventConsumers []chan<- influxdb.Event
}

func NewOnboardService(st *Store, as influxdb.AuthorizationService, eventStreams ...chan<- influxdb.Event) *OnboardService {
	svc := &OnboardService{
		store:   st,
		authSvc: as,
	}
	if len(eventStreams) > 0 {
		svc.eventPublisher = make(chan influxdb.Event, 1)
		svc.eventConsumers = eventStreams
	}
	return svc
}

func (s *OnboardService) Start(ctx context.Context) {
	if s.eventPublisher != nil {
		defer close(s.eventPublisher)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-s.eventPublisher:
			for _, consumer := range s.eventConsumers {
				select {
				case <-ctx.Done():
					return
				case consumer <- ev:
				}
			}
		}
	}
}

func (s *OnboardService) sendEvent(ctx context.Context, eventType influxdb.EventType, a influxdb.Authorization) error {
	if s.eventPublisher == nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.eventPublisher <- influxdb.Event{
		Type: eventType,
		Auth: a,
	}:
		return nil
	}
}

// IsOnboarding determine if onboarding request is allowed.
func (s *OnboardService) IsOnboarding(ctx context.Context) (bool, error) {
	allowed := false
	err := s.store.View(ctx, func(tx kv.Tx) error {
		// we are allowed to onboard a user if we have no users or orgs
		users, _ := s.store.ListUsers(ctx, tx, influxdb.FindOptions{Limit: 1})
		orgs, _ := s.store.ListOrgs(ctx, tx, influxdb.FindOptions{Limit: 1})
		if len(users) == 0 && len(orgs) == 0 {
			allowed = true
		}
		return nil
	})
	return allowed, err
}

// OnboardInitialUser allows us to onboard a new user when onboarding is allowed.
func (s *OnboardService) OnboardInitialUser(ctx context.Context, req *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	allowed, err := s.IsOnboarding(ctx)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, ErrOnboardingNotAllowed
	}

	res, err := s.onboardUser(ctx, req, func(influxdb.ID) []influxdb.Permission { return influxdb.OperPermissions() })
	if err != nil {
		return nil, err
	}
	s.sendEvent(ctx, influxdb.EventSetupComplete, *res.Auth)

	return res, nil
}

// OnboardUser allows us to onboard a new user if is onboarding is allowed
func (s *OnboardService) OnboardUser(ctx context.Context, req *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	return s.onboardUser(ctx, req, influxdb.OwnerPermissions)
}

// onboardUser allows us to onboard new users.
func (s *OnboardService) onboardUser(ctx context.Context, req *influxdb.OnboardingRequest, permFn func(orgID influxdb.ID) []influxdb.Permission) (*influxdb.OnboardingResults, error) {
	if req == nil || req.User == "" || req.Password == "" || req.Org == "" || req.Bucket == "" {
		// which values are missing for user? could be a more informative error message here
		return nil, ErrOnboardInvalid
	}

	result := new(influxdb.OnboardingResults)

	err := s.store.Update(ctx, func(tx kv.Tx) error {
		// create a user
		user := &influxdb.User{
			Name:   req.User,
			Status: influxdb.Active,
		}

		if err := s.store.CreateUser(ctx, tx, user); err != nil {
			return err
		}

		// create users password
		if req.Password != "" {
			passHash, err := encryptPassword(req.Password)
			if err != nil {
				return err
			}

			s.store.SetPassword(ctx, tx, user.ID, passHash)
		}

		// create users org
		org := &influxdb.Organization{
			Name: req.Org,
		}

		if err := s.store.CreateOrg(ctx, tx, org); err != nil {
			return err
		}

		// create urm
		err := s.store.CreateURM(ctx, tx, &influxdb.UserResourceMapping{
			UserID:       user.ID,
			UserType:     influxdb.Owner,
			MappingType:  influxdb.UserMappingType,
			ResourceType: influxdb.OrgsResourceType,
			ResourceID:   org.ID,
		})
		if err != nil {
			return err
		}

		// create orgs buckets
		ub := &influxdb.Bucket{
			OrgID:           org.ID,
			Name:            req.Bucket,
			Type:            influxdb.BucketTypeUser,
			RetentionPeriod: time.Duration(req.RetentionPeriod) * time.Hour,
		}

		if err := s.store.CreateBucket(ctx, tx, ub); err != nil {
			return err
		}

		tb := &influxdb.Bucket{
			OrgID:           org.ID,
			Type:            influxdb.BucketTypeSystem,
			Name:            influxdb.TasksSystemBucketName,
			RetentionPeriod: influxdb.TasksSystemBucketRetention,
			Description:     "System bucket for task logs",
		}

		if err := s.store.CreateBucket(ctx, tx, tb); err != nil {
			return err
		}

		mb := &influxdb.Bucket{
			OrgID:           org.ID,
			Type:            influxdb.BucketTypeSystem,
			Name:            influxdb.MonitoringSystemBucketName,
			RetentionPeriod: influxdb.MonitoringSystemBucketRetention,
			Description:     "System bucket for monitoring logs",
		}

		if err := s.store.CreateBucket(ctx, tx, mb); err != nil {
			return err
		}

		result.User = user
		result.Org = org
		result.Bucket = ub
		return nil
	})

	if err != nil {
		return result, err
	}

	// bolt doesn't lock per collection or record so we have to close our transaction
	// before we can reach out to the auth service.
	result.Auth = &influxdb.Authorization{
		Description: fmt.Sprintf("%s's Token", req.User),
		Permissions: permFn(result.Org.ID),
		Token:       req.Token,
		UserID:      result.User.ID,
		OrgID:       result.Org.ID,
	}

	if err := s.authSvc.CreateAuthorization(ctx, result.Auth); err != nil {
		return nil, err
	}

	return result, nil
}
