package mock

import (
	"context"

	platform "github.com/influxdata/influxdb"
	"go.uber.org/zap"
)

// AuthorizationService is a mock implementation of a retention.AuthorizationService, which
// also makes it a suitable mock to use wherever an platform.AuthorizationService is required.
type AuthorizationService struct {
	// Methods for a retention.AuthorizationService
	OpenFn       func() error
	CloseFn      func() error
	WithLoggerFn func(l *zap.Logger)

	// Methods for an platform.AuthorizationService
	FindAuthorizationByIDFn    func(context.Context, platform.ID) (*platform.Authorization, error)
	FindAuthorizationByTokenFn func(context.Context, string) (*platform.Authorization, error)
	FindAuthorizationsFn       func(context.Context, platform.AuthorizationFilter, ...platform.FindOptions) ([]*platform.Authorization, int, error)
	CreateAuthorizationFn      func(context.Context, *platform.Authorization) error
	DeleteAuthorizationFn      func(context.Context, platform.ID) error
	SetAuthorizationStatusFn   func(context.Context, platform.ID, platform.Status) error
}

// NewAuthorizationService returns a mock AuthorizationService where its methods will return
// zero values.
func NewAuthorizationService() *AuthorizationService {
	return &AuthorizationService{
		FindAuthorizationByIDFn:    func(context.Context, platform.ID) (*platform.Authorization, error) { return nil, nil },
		FindAuthorizationByTokenFn: func(context.Context, string) (*platform.Authorization, error) { return nil, nil },
		FindAuthorizationsFn: func(context.Context, platform.AuthorizationFilter, ...platform.FindOptions) ([]*platform.Authorization, int, error) {
			return nil, 0, nil
		},
		CreateAuthorizationFn:    func(context.Context, *platform.Authorization) error { return nil },
		DeleteAuthorizationFn:    func(context.Context, platform.ID) error { return nil },
		SetAuthorizationStatusFn: func(context.Context, platform.ID, platform.Status) error { return nil },
	}
}

// FindAuthorizationByID returns a single authorization by ID.
func (s *AuthorizationService) FindAuthorizationByID(ctx context.Context, id platform.ID) (*platform.Authorization, error) {
	return s.FindAuthorizationByIDFn(ctx, id)
}

func (s *AuthorizationService) FindAuthorizationByToken(ctx context.Context, t string) (*platform.Authorization, error) {
	return s.FindAuthorizationByTokenFn(ctx, t)
}

// FindAuthorizations returns a list of authorizations that match filter and the total count of matching authorizations.
func (s *AuthorizationService) FindAuthorizations(ctx context.Context, filter platform.AuthorizationFilter, opts ...platform.FindOptions) ([]*platform.Authorization, int, error) {
	return s.FindAuthorizationsFn(ctx, filter, opts...)
}

// CreateAuthorization creates a new authorization and sets b.ID with the new identifier.
func (s *AuthorizationService) CreateAuthorization(ctx context.Context, authorization *platform.Authorization) error {
	return s.CreateAuthorizationFn(ctx, authorization)
}

// DeleteAuthorization removes a authorization by ID.
func (s *AuthorizationService) DeleteAuthorization(ctx context.Context, id platform.ID) error {
	return s.DeleteAuthorizationFn(ctx, id)
}

func (s *AuthorizationService) SetAuthorizationStatus(ctx context.Context, id platform.ID, status platform.Status) error {
	return s.SetAuthorizationStatusFn(ctx, id, status)
}
