package zap

import (
	"context"

	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"go.uber.org/zap"
)

var _ platform.AuthorizationService = (*AuthorizationService)(nil)

// AuthorizationService manages authorizations.
type AuthorizationService struct {
	log                  *zap.Logger
	AuthorizationService platform.AuthorizationService
}

// FindAuthorizationByID returns an authorization given an id, and logs any errors.
func (s *AuthorizationService) FindAuthorizationByID(ctx context.Context, id platform2.ID) (a *platform.Authorization, err error) {
	defer func() {
		if err != nil {
			s.log.Info("Error finding authorization by id", zap.Error(err))
		}
	}()

	return s.AuthorizationService.FindAuthorizationByID(ctx, id)
}

// FindAuthorizationByToken returns an authorization given a token, and logs any errors.
func (s *AuthorizationService) FindAuthorizationByToken(ctx context.Context, t string) (a *platform.Authorization, err error) {
	defer func() {
		if err != nil {
			s.log.Info("Error finding authorization by token", zap.Error(err))
		}
	}()

	return s.AuthorizationService.FindAuthorizationByToken(ctx, t)
}

// FindAuthorizations returns authorizations given a filter, and logs any errors.
func (s *AuthorizationService) FindAuthorizations(ctx context.Context, filter platform.AuthorizationFilter, opt ...platform.FindOptions) (as []*platform.Authorization, i int, err error) {
	defer func() {
		if err != nil {
			s.log.Info("Error finding authorizations", zap.Error(err))
		}
	}()

	return s.AuthorizationService.FindAuthorizations(ctx, filter, opt...)
}

// CreateAuthorization creates an authorization, and logs any errors.
func (s *AuthorizationService) CreateAuthorization(ctx context.Context, a *platform.Authorization) (err error) {
	defer func() {
		if err != nil {
			s.log.Info("Error creating authorization", zap.Error(err))
		}
	}()

	return s.AuthorizationService.CreateAuthorization(ctx, a)
}

// DeleteAuthorization deletes an authorization, and logs any errors.
func (s *AuthorizationService) DeleteAuthorization(ctx context.Context, id platform2.ID) (err error) {
	defer func() {
		if err != nil {
			s.log.Info("Error deleting authorization", zap.Error(err))
		}
	}()

	return s.AuthorizationService.DeleteAuthorization(ctx, id)
}

// UpdateAuthorization updates an authorization's status, description and logs any errors.
func (s *AuthorizationService) UpdateAuthorization(ctx context.Context, id platform2.ID, upd *platform.AuthorizationUpdate) (a *platform.Authorization, err error) {
	defer func() {
		if err != nil {
			s.log.Info("Error updating authorization", zap.Error(err))
		}
	}()

	return s.AuthorizationService.UpdateAuthorization(ctx, id, upd)
}
