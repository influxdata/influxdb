package authorizer

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb"
)

var _ influxdb.AuthorizationService = (*AuthorizationService)(nil)

// AuthorizationService wraps a influxdb.AuthorizationService and authorizes actions
// against it appropriately.
type AuthorizationService struct {
	s influxdb.AuthorizationService
}

// NewAuthorizationService constructs an instance of an authorizing authorization serivce.
func NewAuthorizationService(s influxdb.AuthorizationService) *AuthorizationService {
	return &AuthorizationService{
		s: s,
	}
}

func newAuthorizationPermission(a influxdb.Action, id influxdb.ID) (*influxdb.Permission, error) {
	p := &influxdb.Permission{
		Action: a,
		Resource: influxdb.Resource{
			Type: influxdb.UsersResourceType,
			ID:   &id,
		},
	}
	return p, p.Valid()
}

func authorizeReadAuthorization(ctx context.Context, id influxdb.ID) error {
	p, err := newAuthorizationPermission(influxdb.ReadAction, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeWriteAuthorization(ctx context.Context, id influxdb.ID) error {
	p, err := newAuthorizationPermission(influxdb.WriteAction, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

// FindAuthorizationByID checks to see if the authorizer on context has read access to the id provided.
func (s *AuthorizationService) FindAuthorizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Authorization, error) {
	a, err := s.s.FindAuthorizationByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeReadAuthorization(ctx, a.UserID); err != nil {
		return nil, err
	}

	return a, nil
}

// FindAuthorization retrieves the authorization and checks to see if the authorizer on context has read access to the authorization.
func (s *AuthorizationService) FindAuthorizationByToken(ctx context.Context, t string) (*influxdb.Authorization, error) {
	a, err := s.s.FindAuthorizationByToken(ctx, t)
	if err != nil {
		return nil, err
	}

	if err := authorizeReadAuthorization(ctx, a.UserID); err != nil {
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

	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	authorizations := as[:0]
	for _, a := range as {
		err := authorizeReadAuthorization(ctx, a.UserID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}

		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}

		authorizations = append(authorizations, a)
	}

	return authorizations, len(authorizations), nil
}

// CreateAuthorization checks to see if the authorizer on context has write access to the global authorizations resource.
func (s *AuthorizationService) CreateAuthorization(ctx context.Context, a *influxdb.Authorization) error {
	if err := authorizeWriteAuthorization(ctx, a.UserID); err != nil {
		return err
	}

	for _, p := range a.Permissions {
		if err := IsAllowed(ctx, p); err != nil {
			return &influxdb.Error{
				Err:  err,
				Msg:  fmt.Sprintf("cannot create authorization with permission %s", p),
				Code: influxdb.EForbidden,
			}
		}
	}

	return s.s.CreateAuthorization(ctx, a)
}

// SetAuthorizationStatus checks to see if the authorizer on context has write access to the authorization provided.
func (s *AuthorizationService) SetAuthorizationStatus(ctx context.Context, id influxdb.ID, st influxdb.Status) error {
	a, err := s.s.FindAuthorizationByID(ctx, id)
	if err != nil {
		return err
	}

	if err := authorizeWriteAuthorization(ctx, a.UserID); err != nil {
		return err
	}

	return s.s.SetAuthorizationStatus(ctx, id, st)
}

// DeleteAuthorization checks to see if the authorizer on context has write access to the authorization provided.
func (s *AuthorizationService) DeleteAuthorization(ctx context.Context, id influxdb.ID) error {
	a, err := s.s.FindAuthorizationByID(ctx, id)
	if err != nil {
		return err
	}

	if err := authorizeWriteAuthorization(ctx, a.UserID); err != nil {
		return err
	}

	return s.s.DeleteAuthorization(ctx, id)
}
