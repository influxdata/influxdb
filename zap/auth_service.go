package logger

import (
	"context"

	"go.uber.org/zap"

	"github.com/influxdata/platform"
)

// AuthorizationService manages authorizations.
type AuthorizationService struct {
	Logger               *zap.Logger
	AuthorizationService platform.AuthorizationService
}

// FindAuthorizationByToken returns an authorization given a token, and logs any errors.
func (s *AuthorizationService) FindAuthorizationByToken(ctx context.Context, t string) (a *platform.Authorization, err error) {
	defer func() {
		if err != nil {
			s.Logger.Info("error finding authorization by token", zap.Error(err))
		}
	}()

	return s.AuthorizationService.FindAuthorizationByToken(ctx, t)
}

// FindAuthorizations returns authorizations given a filter, and logs any errors.
func (s *AuthorizationService) FindAuthorizations(ctx context.Context, filter platform.AuthorizationFilter, opt ...platform.FindOptions) (as []*platform.Authorization, i int, err error) {
	defer func() {
		if err != nil {
			s.Logger.Info("error finding authorizations", zap.Error(err))
		}
	}()

	return s.AuthorizationService.FindAuthorizations(ctx, filter, opt...)
}

// CreateAuthorization creates an authorization, and logs any errors.
func (s *AuthorizationService) CreateAuthorization(ctx context.Context, a *platform.Authorization) (err error) {
	defer func() {
		if err != nil {
			s.Logger.Info("error creating authorization", zap.Error(err))
		}
	}()

	return s.AuthorizationService.CreateAuthorization(ctx, a)
}

// DeleteAuthorization deletes an authorization, and logs any errors.
func (s *AuthorizationService) DeleteAuthorization(ctx context.Context, t string) (err error) {
	defer func() {
		if err != nil {
			s.Logger.Info("error deleting authorization", zap.Error(err))
		}
	}()

	return s.AuthorizationService.DeleteAuthorization(ctx, t)
}
