package authorizer

import (
	"context"

	"github.com/influxdata/influxdb"
)

var _ influxdb.OrganizationService = (*OrgService)(nil)

// OrgService wraps a influxdb.OrganizationService and authorizes actions
// against it appropriately.
type OrgService struct {
	s influxdb.OrganizationService
}

// NewOrgService constructs an instance of an authorizing org serivce.
func NewOrgService(s influxdb.OrganizationService) *OrgService {
	return &OrgService{
		s: s,
	}
}

func newOrgPermission(a influxdb.Action, id influxdb.ID) (*influxdb.Permission, error) {
	p := &influxdb.Permission{
		Action: a,
		Resource: influxdb.Resource{
			Type: influxdb.OrgsResourceType,
			ID:   &id,
		},
	}

	return p, p.Valid()
}

func authorizeReadOrg(ctx context.Context, id influxdb.ID) error {
	p, err := newOrgPermission(influxdb.ReadAction, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeWriteOrg(ctx context.Context, id influxdb.ID) error {
	p, err := newOrgPermission(influxdb.WriteAction, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

// FindOrganizationByID checks to see if the authorizer on context has read access to the id provided.
func (s *OrgService) FindOrganizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
	if err := authorizeReadOrg(ctx, id); err != nil {
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

	if err := authorizeReadOrg(ctx, o.ID); err != nil {
		return nil, err
	}

	return o, nil
}

// FindOrganizations retrieves all organizations that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *OrgService) FindOrganizations(ctx context.Context, filter influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
	// TODO: we'll likely want to push this operation into the database eventually since fetching the whole list of data
	// will likely be expensive.
	os, _, err := s.s.FindOrganizations(ctx, filter, opt...)
	if err != nil {
		return nil, 0, err
	}

	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	orgs := os[:0]
	for _, o := range os {
		err := authorizeReadOrg(ctx, o.ID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}

		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}

		orgs = append(orgs, o)
	}

	return orgs, len(orgs), nil
}

// CreateOrganization checks to see if the authorizer on context has write access to the global orgs resource.
func (s *OrgService) CreateOrganization(ctx context.Context, o *influxdb.Organization) error {
	p, err := influxdb.NewGlobalPermission(influxdb.WriteAction, influxdb.OrgsResourceType)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return s.s.CreateOrganization(ctx, o)
}

// UpdateOrganization checks to see if the authorizer on context has write access to the organization provided.
func (s *OrgService) UpdateOrganization(ctx context.Context, id influxdb.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
	if err := authorizeWriteOrg(ctx, id); err != nil {
		return nil, err
	}

	return s.s.UpdateOrganization(ctx, id, upd)
}

// DeleteOrganization checks to see if the authorizer on context has write access to the organization provided.
func (s *OrgService) DeleteOrganization(ctx context.Context, id influxdb.ID) error {
	if err := authorizeWriteOrg(ctx, id); err != nil {
		return err
	}

	return s.s.DeleteOrganization(ctx, id)
}
