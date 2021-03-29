package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
)

var _ influxdb.OrganizationService = (*OrgService)(nil)

// OrgService wraps a influxdb.OrganizationService and authorizes actions
// against it appropriately.
type OrgService struct {
	s influxdb.OrganizationService
}

// NewOrgService constructs an instance of an authorizing org service.
func NewOrgService(s influxdb.OrganizationService) *OrgService {
	return &OrgService{
		s: s,
	}
}

// FindOrganizationByID checks to see if the authorizer on context has read access to the id provided.
func (s *OrgService) FindOrganizationByID(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
	if _, _, err := AuthorizeReadOrg(ctx, id); err != nil {
		return nil, err
	}
	return s.s.FindOrganizationByID(ctx, id)
}

// FindOrganization retrieves the organization and checks to see if the authorizer on context has read access to the org.
func (s *OrgService) FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
	o, err := s.s.FindOrganization(ctx, filter)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeReadOrg(ctx, o.ID); err != nil {
		return nil, err
	}
	return o, nil
}

// FindOrganizations retrieves all organizations that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *OrgService) FindOrganizations(ctx context.Context, filter influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
	if filter.Name == nil && filter.ID == nil && filter.UserID == nil {
		// if the user doesnt have permission to look up all orgs we need to add this users id to the filter to save lookup time
		auth, err := icontext.GetAuthorizer(ctx)
		if err != nil {
			return nil, 0, err
		}
		if _, _, err := AuthorizeReadGlobal(ctx, influxdb.OrgsResourceType); err != nil {
			userid := auth.GetUserID()
			filter.UserID = &userid
		}
	}

	// TODO: we'll likely want to push this operation into the database eventually since fetching the whole list of data
	// will likely be expensive.
	os, _, err := s.s.FindOrganizations(ctx, filter, opt...)
	if err != nil {
		return nil, 0, err
	}
	return AuthorizeFindOrganizations(ctx, os)
}

// CreateOrganization checks to see if the authorizer on context has write access to the global orgs resource.
func (s *OrgService) CreateOrganization(ctx context.Context, o *influxdb.Organization) error {
	if _, _, err := AuthorizeWriteGlobal(ctx, influxdb.OrgsResourceType); err != nil {
		return err
	}
	return s.s.CreateOrganization(ctx, o)
}

// UpdateOrganization checks to see if the authorizer on context has write access to the organization provided.
func (s *OrgService) UpdateOrganization(ctx context.Context, id platform.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
	if _, _, err := AuthorizeWriteOrg(ctx, id); err != nil {
		return nil, err
	}
	return s.s.UpdateOrganization(ctx, id, upd)
}

// DeleteOrganization checks to see if the authorizer on context has write access to the organization provided.
func (s *OrgService) DeleteOrganization(ctx context.Context, id platform.ID) error {
	if _, _, err := AuthorizeWriteOrg(ctx, id); err != nil {
		return err
	}
	return s.s.DeleteOrganization(ctx, id)
}
