package session

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/rand"
	"github.com/influxdata/influxdb/v2/snowflake"
)

// Service implements the influxdb.SessionService interface and
// handles communication between session and the necessary user and urm services
type Service struct {
	store         *Storage
	userService   influxdb.UserService
	urmService    influxdb.UserResourceMappingService
	authService   influxdb.AuthorizationService
	sessionLength time.Duration

	idGen    influxdb.IDGenerator
	tokenGen influxdb.TokenGenerator

	disableAuthorizationsForMaxPermissions func(context.Context) bool
}

// NewService creates a new session service
func NewService(store *Storage, userService influxdb.UserService, urmService influxdb.UserResourceMappingService, authSvc influxdb.AuthorizationService, sessionLength time.Duration) *Service {
	if sessionLength <= 0 {
		sessionLength = time.Hour
	}
	return &Service{
		store:         store,
		userService:   userService,
		urmService:    urmService,
		authService:   authSvc,
		sessionLength: sessionLength,
		idGen:         snowflake.NewIDGenerator(),
		tokenGen:      rand.NewTokenGenerator(64),
		disableAuthorizationsForMaxPermissions: func(context.Context) bool {
			return false
		},
	}
}

// WithMaxPermissionFunc sets the useAuthorizationsForMaxPermissions function
// which can trigger whether or not max permissions uses the users authorizations
// to derive maximum permissions.
func (s *Service) WithMaxPermissionFunc(fn func(context.Context) bool) {
	s.disableAuthorizationsForMaxPermissions = fn
}

// FindSession finds a session based on the session key
func (s *Service) FindSession(ctx context.Context, key string) (*influxdb.Session, error) {
	session, err := s.store.FindSessionByKey(ctx, key)
	if err != nil {
		return nil, err
	}

	// TODO: We want to be able to store permissions in the session
	// but the contract provided by urm's doesn't give us enough information to quickly repopulate our
	// session permissions on updates so we are required to pull the permissions every time we find the session.
	permissions, err := s.getPermissionSet(ctx, session.UserID)
	if err != nil {
		return nil, err
	}

	session.Permissions = permissions
	return session, nil
}

// ExpireSession removes a session from the system
func (s *Service) ExpireSession(ctx context.Context, key string) error {
	session, err := s.store.FindSessionByKey(ctx, key)
	if err != nil {
		return err
	}
	return s.store.DeleteSession(ctx, session.ID)
}

// CreateSession
func (s *Service) CreateSession(ctx context.Context, user string) (*influxdb.Session, error) {
	u, err := s.userService.FindUser(ctx, influxdb.UserFilter{
		Name: &user,
	})
	if err != nil {
		return nil, err
	}

	token, err := s.tokenGen.Token()
	if err != nil {
		return nil, err
	}

	// for now we are not storing the permissions because we need to pull them every time we find
	// so we might as well keep the session stored small
	now := time.Now()
	session := &influxdb.Session{
		ID:        s.idGen.ID(),
		Key:       token,
		CreatedAt: now,
		ExpiresAt: now.Add(s.sessionLength),
		UserID:    u.ID,
	}

	return session, s.store.CreateSession(ctx, session)
}

// RenewSession update the sessions expiration time
func (s *Service) RenewSession(ctx context.Context, session *influxdb.Session, newExpiration time.Time) error {
	if session == nil {
		return &influxdb.Error{
			Msg: "session is nil",
		}
	}
	return s.store.RefreshSession(ctx, session.ID, newExpiration)
}

func (s *Service) getPermissionSet(ctx context.Context, uid influxdb.ID) ([]influxdb.Permission, error) {

	mappings, _, err := s.urmService.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{UserID: uid}, influxdb.FindOptions{Limit: 100})
	if err != nil {
		return nil, err
	}

	permissions, err := permissionFromMapping(mappings)
	if err != nil {
		return nil, err
	}

	if len(mappings) == 100 {
		// if we got 100 mappings we probably need to pull more pages
		// account for paginated results
		for i := len(mappings); len(mappings) > 0; i += len(mappings) {
			mappings, _, err = s.urmService.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{UserID: uid}, influxdb.FindOptions{Offset: i, Limit: 100})
			if err != nil {
				return nil, err
			}
			pms, err := permissionFromMapping(mappings)
			if err != nil {
				return nil, err
			}
			permissions = append(permissions, pms...)
		}
	}

	if !s.disableAuthorizationsForMaxPermissions(ctx) {
		as, _, err := s.authService.FindAuthorizations(ctx, influxdb.AuthorizationFilter{UserID: &uid})
		if err != nil {
			return nil, err
		}
		for _, a := range as {
			permissions = append(permissions, a.Permissions...)
		}
	}

	permissions = append(permissions, influxdb.MePermissions(uid)...)
	return permissions, nil
}

func permissionFromMapping(mappings []*influxdb.UserResourceMapping) ([]influxdb.Permission, error) {
	ps := make([]influxdb.Permission, 0, len(mappings))
	for _, m := range mappings {
		p, err := m.ToPermissions()
		if err != nil {
			return nil, &influxdb.Error{
				Err: err,
			}
		}

		ps = append(ps, p...)
	}

	return ps, nil
}
