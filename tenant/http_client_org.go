package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
)

// OrgClientService connects to Influx via HTTP using tokens to manage organizations
type OrgClientService struct {
	Client *httpc.Client
	// OpPrefix is for not found errors.
	OpPrefix string
}

func (o orgsResponse) toInfluxdb() []*influxdb.Organization {
	orgs := make([]*influxdb.Organization, len(o.Organizations))
	for i := range o.Organizations {
		orgs[i] = &o.Organizations[i].Organization
	}
	return orgs
}

// FindOrganizationByID gets a single organization with a given id using HTTP.
func (s *OrgClientService) FindOrganizationByID(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
	filter := influxdb.OrganizationFilter{ID: &id}
	o, err := s.FindOrganization(ctx, filter)
	if err != nil {
		return nil, &errors.Error{
			Err: err,
			Op:  s.OpPrefix + influxdb.OpFindOrganizationByID,
		}
	}
	return o, nil
}

// FindOrganization gets a single organization matching the filter using HTTP.
func (s *OrgClientService) FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
	if filter.ID == nil && filter.Name == nil {
		return nil, influxdb.ErrInvalidOrgFilter
	}
	os, n, err := s.FindOrganizations(ctx, filter)
	if err != nil {
		return nil, &errors.Error{
			Err: err,
			Op:  s.OpPrefix + influxdb.OpFindOrganization,
		}
	}

	if n == 0 {
		return nil, &errors.Error{
			Code: errors.ENotFound,
			Op:   s.OpPrefix + influxdb.OpFindOrganization,
			Msg:  "organization not found",
		}
	}

	return os[0], nil
}

// FindOrganizations returns all organizations that match the filter via HTTP.
func (s *OrgClientService) FindOrganizations(ctx context.Context, filter influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	params := influxdb.FindOptionParams(opt...)
	if filter.Name != nil {
		span.LogKV("org", *filter.Name)
		params = append(params, [2]string{"org", *filter.Name})
	}
	if filter.ID != nil {
		span.LogKV("org-id", *filter.ID)
		params = append(params, [2]string{"orgID", filter.ID.String()})
	}
	for _, o := range opt {
		if o.Offset != 0 {
			span.LogKV("offset", o.Offset)
		}
		span.LogKV("descending", o.Descending)
		if o.Limit > 0 {
			span.LogKV("limit", o.Limit)
		}
		if o.SortBy != "" {
			span.LogKV("sortBy", o.SortBy)
		}
	}

	var os orgsResponse
	err := s.Client.
		Get(prefixOrganizations).
		QueryParams(params...).
		DecodeJSON(&os).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	orgs := os.toInfluxdb()
	return orgs, len(orgs), nil
}

// CreateOrganization creates an organization.
func (s *OrgClientService) CreateOrganization(ctx context.Context, o *influxdb.Organization) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if o.Name != "" {
		span.LogKV("org", o.Name)
	}
	if o.ID != 0 {
		span.LogKV("org-id", o.ID)
	}

	return s.Client.
		PostJSON(o, prefixOrganizations).
		DecodeJSON(o).
		Do(ctx)
}

// UpdateOrganization updates the organization over HTTP.
func (s *OrgClientService) UpdateOrganization(ctx context.Context, id platform.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	span.LogKV("org-id", id)
	span.LogKV("name", upd.Name)

	var o influxdb.Organization
	err := s.Client.
		PatchJSON(upd, prefixOrganizations, id.String()).
		DecodeJSON(&o).
		Do(ctx)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	return &o, nil
}

// DeleteOrganization removes organization id over HTTP.
func (s *OrgClientService) DeleteOrganization(ctx context.Context, id platform.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return s.Client.
		Delete(prefixOrganizations, id.String()).
		Do(ctx)
}
