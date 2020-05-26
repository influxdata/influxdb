package authorization

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
)

type AuthedAuthorizationService struct {
	s  influxdb.AuthorizationService
	ts TenantService
}

var _ influxdb.AuthorizationService = (*AuthedAuthorizationService)(nil)

func NewAuthedAuthorizationService(s influxdb.AuthorizationService, ts TenantService) *AuthedAuthorizationService {
	return &AuthedAuthorizationService{
		s:  s,
		ts: ts,
	}
}

func (s *AuthedAuthorizationService) CreateAuthorization(ctx context.Context, a *influxdb.Authorization) error {
	if _, _, err := authorizer.AuthorizeCreate(ctx, influxdb.AuthorizationsResourceType, a.OrgID); err != nil {
		return err
	}
	if _, _, err := authorizer.AuthorizeWriteResource(ctx, influxdb.UsersResourceType, a.UserID); err != nil {
		return err
	}
	if err := authorizer.VerifyPermissions(ctx, a.Permissions); err != nil {
		return err
	}

	return s.s.CreateAuthorization(ctx, a)
}

func (s *AuthedAuthorizationService) FindAuthorizationByToken(ctx context.Context, t string) (*influxdb.Authorization, error) {
	a, err := s.s.FindAuthorizationByToken(ctx, t)
	if err != nil {
		return nil, err
	}
	if _, _, err := authorizer.AuthorizeRead(ctx, influxdb.AuthorizationsResourceType, a.ID, a.OrgID); err != nil {
		return nil, err
	}
	if _, _, err := authorizer.AuthorizeReadResource(ctx, influxdb.UsersResourceType, a.UserID); err != nil {
		return nil, err
	}
	return a, nil
}

func (s *AuthedAuthorizationService) FindAuthorizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Authorization, error) {
	a, err := s.s.FindAuthorizationByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := authorizer.AuthorizeRead(ctx, influxdb.AuthorizationsResourceType, a.ID, a.OrgID); err != nil {
		return nil, err
	}
	if _, _, err := authorizer.AuthorizeReadResource(ctx, influxdb.UsersResourceType, a.UserID); err != nil {
		return nil, err
	}
	return a, nil
}

func (s *AuthedAuthorizationService) FindAuthorizations(ctx context.Context, filter influxdb.AuthorizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
	// TODO: we'll likely want to push this operation into the database eventually since fetching the whole list of data
	// will likely be expensive.
	as, _, err := s.s.FindAuthorizations(ctx, filter, opt...)
	if err != nil {
		return nil, 0, err
	}
	return authorizer.AuthorizeFindAuthorizations(ctx, as)
}

func (s *AuthedAuthorizationService) UpdateAuthorization(ctx context.Context, id influxdb.ID, upd *influxdb.AuthorizationUpdate) (*influxdb.Authorization, error) {
	a, err := s.s.FindAuthorizationByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := authorizer.AuthorizeWrite(ctx, influxdb.AuthorizationsResourceType, a.ID, a.OrgID); err != nil {
		return nil, err
	}
	if _, _, err := authorizer.AuthorizeWriteResource(ctx, influxdb.UsersResourceType, a.UserID); err != nil {
		return nil, err
	}
	return s.s.UpdateAuthorization(ctx, id, upd)
}

func (s *AuthedAuthorizationService) DeleteAuthorization(ctx context.Context, id influxdb.ID) error {
	a, err := s.s.FindAuthorizationByID(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := authorizer.AuthorizeWrite(ctx, influxdb.AuthorizationsResourceType, a.ID, a.OrgID); err != nil {
		return err
	}
	if _, _, err := authorizer.AuthorizeWriteResource(ctx, influxdb.UsersResourceType, a.UserID); err != nil {
		return err
	}
	return s.s.DeleteAuthorization(ctx, id)
}

// VerifyPermissions ensures that an authorization is allowed all of the appropriate permissions.
func VerifyPermissions(ctx context.Context, ps []influxdb.Permission) error {
	for _, p := range ps {
		if err := authorizer.IsAllowed(ctx, p); err != nil {
			return &influxdb.Error{
				Err:  err,
				Msg:  fmt.Sprintf("permission %s is not allowed", p),
				Code: influxdb.EForbidden,
			}
		}
	}
	return nil
}
