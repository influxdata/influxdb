package authorization

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
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
	if err := verifyUserAndOrgPermissions(a.Permissions); err != nil {
		return err
	}
	if err := s.verifyBucketPermissions(ctx, a.Permissions); err != nil {
		return err
	}
	if err := verifyPermissionsWithoutOrg(ctx, a.Permissions); err != nil {
		return err
	}
	for _, v := range a.Permissions {
		if v.Resource.Type == influxdb.InstanceResourceType {
			return fmt.Errorf("authorizations cannot be created with the instance type, it is only used during setup")
		}
	}

	return s.s.CreateAuthorization(ctx, a)
}

// verifyUserAndOrgPermissions checks that no permission names a user or an
// organization by ID and also has an organization. Users and organizations don't
// belong to an organization, so a permission that names one by ID can't be
// scoped to one.
func verifyUserAndOrgPermissions(ps []influxdb.Permission) error {
	for _, p := range ps {
		r := p.Resource
		if r.ID == nil || r.OrgID == nil {
			continue
		}
		if r.Type == influxdb.UsersResourceType || r.Type == influxdb.OrgsResourceType {
			return errPermissionNotAllowed(p)
		}
	}
	return nil
}

// verifyBucketPermissions checks that each bucket permission that names both a
// bucket ID and an organization refers to an existing bucket in that
// organization. A permission that does not is rejected with the same error
// authorizer.VerifyPermissions returns for a permission the caller does not
// hold.
func (s *AuthedAuthorizationService) verifyBucketPermissions(ctx context.Context, ps []influxdb.Permission) error {
	for _, p := range ps {
		r := p.Resource
		if r.Type != influxdb.BucketsResourceType || r.ID == nil || r.OrgID == nil {
			continue
		}
		if _, err := influxdb.FindBucketInOrg(ctx, s.ts, *r.OrgID, *r.ID); err != nil {
			if errors.ErrorCode(err) != errors.ENotFound {
				return err
			}
			return errPermissionNotAllowed(p)
		}
	}
	return nil
}

// verifyPermissionsWithoutOrg checks that the caller holds each permission that
// names a resource by ID with no organization through a permission that also has
// no organization. A permission with no organization is wider than one with an
// organization, so the narrower permission can't be used to grant it.
func verifyPermissionsWithoutOrg(ctx context.Context, ps []influxdb.Permission) error {
	auth, err := icontext.GetAuthorizer(ctx)
	if err != nil {
		return err
	}
	held, err := auth.PermissionSet()
	if err != nil {
		return err
	}
	for _, p := range ps {
		if p.Resource.ID == nil || p.Resource.OrgID != nil {
			continue
		}
		if !heldWithoutOrg(held, p) {
			return errPermissionNotAllowed(p)
		}
	}
	return nil
}

// heldWithoutOrg reports whether a permission with no organization in held
// matches p.
func heldWithoutOrg(held influxdb.PermissionSet, p influxdb.Permission) bool {
	for _, h := range held {
		if h.Resource.OrgID == nil && h.Matches(p) {
			return true
		}
	}
	return false
}

// errPermissionNotAllowed returns the error authorizer.VerifyPermissions returns
// for a permission the caller does not hold.
func errPermissionNotAllowed(p influxdb.Permission) error {
	return &errors.Error{
		Code: errors.EForbidden,
		Msg:  fmt.Sprintf("permission %s is not allowed", p),
		Err: &errors.Error{
			Code: errors.EUnauthorized,
			Msg:  fmt.Sprintf("%s is unauthorized", p),
		},
	}
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

func (s *AuthedAuthorizationService) FindAuthorizationByID(ctx context.Context, id platform.ID) (*influxdb.Authorization, error) {
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

func (s *AuthedAuthorizationService) UpdateAuthorization(ctx context.Context, id platform.ID, upd *influxdb.AuthorizationUpdate) (*influxdb.Authorization, error) {
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

func (s *AuthedAuthorizationService) DeleteAuthorization(ctx context.Context, id platform.ID) error {
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
			return &errors.Error{
				Err:  err,
				Msg:  fmt.Sprintf("permission %s is not allowed", p),
				Code: errors.EForbidden,
			}
		}
	}
	return nil
}
