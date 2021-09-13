package authorizer

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var _ influxdb.AuthorizationService = (*AuthorizationService)(nil)

// AuthorizationService wraps a influxdb.AuthorizationService and authorizes actions
// against it appropriately.
type AuthorizationService struct {
	s influxdb.AuthorizationService
}

// NewAuthorizationService constructs an instance of an authorizing authorization service.
func NewAuthorizationService(s influxdb.AuthorizationService) *AuthorizationService {
	return &AuthorizationService{
		s: s,
	}
}

// FindAuthorizationByID checks to see if the authorizer on context has read access to the id provided.
func (s *AuthorizationService) FindAuthorizationByID(ctx context.Context, id platform.ID) (*influxdb.Authorization, error) {
	a, err := s.s.FindAuthorizationByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.AuthorizationsResourceType, a.ID, a.OrgID); err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeReadResource(ctx, influxdb.UsersResourceType, a.UserID); err != nil {
		return nil, err
	}
	return a, nil
}

// FindAuthorizationByToken retrieves the authorization and checks to see if the authorizer on context has read access to the authorization.
func (s *AuthorizationService) FindAuthorizationByToken(ctx context.Context, t string) (*influxdb.Authorization, error) {
	a, err := s.s.FindAuthorizationByToken(ctx, t)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.AuthorizationsResourceType, a.ID, a.OrgID); err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeReadResource(ctx, influxdb.UsersResourceType, a.UserID); err != nil {
		return nil, err
	}
	return a, nil
}

// FindAuthorizations retrieves all authorizations that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *AuthorizationService) FindAuthorizations(ctx context.Context, filter influxdb.AuthorizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
	// TODO: we'll likely want to push this operation into the database eventually since fetching the whole list of data
	// will likely be expensive.
	as, _, err := s.s.FindAuthorizations(ctx, filter, opt...)
	if err != nil {
		return nil, 0, err
	}
	return AuthorizeFindAuthorizations(ctx, as)
}

// CreateAuthorization checks to see if the authorizer on context has write access to the global authorizations resource.
func (s *AuthorizationService) CreateAuthorization(ctx context.Context, a *influxdb.Authorization) error {
	if _, _, err := AuthorizeCreate(ctx, influxdb.AuthorizationsResourceType, a.OrgID); err != nil {
		return err
	}
	if _, _, err := AuthorizeWriteResource(ctx, influxdb.UsersResourceType, a.UserID); err != nil {
		return err
	}
	if err := VerifyPermissions(ctx, a.Permissions); err != nil {
		return err
	}
	return s.s.CreateAuthorization(ctx, a)
}

// UpdateAuthorization checks to see if the authorizer on context has write access to the authorization provided.
func (s *AuthorizationService) UpdateAuthorization(ctx context.Context, id platform.ID, upd *influxdb.AuthorizationUpdate) (*influxdb.Authorization, error) {
	a, err := s.s.FindAuthorizationByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.AuthorizationsResourceType, a.ID, a.OrgID); err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWriteResource(ctx, influxdb.UsersResourceType, a.UserID); err != nil {
		return nil, err
	}
	return s.s.UpdateAuthorization(ctx, id, upd)
}

// DeleteAuthorization checks to see if the authorizer on context has write access to the authorization provided.
func (s *AuthorizationService) DeleteAuthorization(ctx context.Context, id platform.ID) error {
	a, err := s.s.FindAuthorizationByID(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.AuthorizationsResourceType, a.ID, a.OrgID); err != nil {
		return err
	}
	if _, _, err := AuthorizeWriteResource(ctx, influxdb.UsersResourceType, a.UserID); err != nil {
		return err
	}
	return s.s.DeleteAuthorization(ctx, id)
}

// VerifyPermissions ensures that an authorization is allowed all of the appropriate permissions.
func VerifyPermissions(ctx context.Context, ps []influxdb.Permission) error {
	for _, p := range ps {
		if err := IsAllowed(ctx, p); err != nil {
			return &errors.Error{
				Err:  err,
				Msg:  fmt.Sprintf("permission %s is not allowed", p),
				Code: errors.EForbidden,
			}
		}
	}
	return nil
}
