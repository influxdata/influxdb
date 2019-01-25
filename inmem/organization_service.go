package inmem

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb"
)

const (
	errOrganizationNotFound = "organization not found"
)

func (s *Service) loadOrganization(id influxdb.ID) (*influxdb.Organization, *influxdb.Error) {
	i, ok := s.organizationKV.Load(id.String())
	if !ok {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  errOrganizationNotFound,
		}
	}

	b, ok := i.(*influxdb.Organization)
	if !ok {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("type %T is not a organization", i),
		}
	}
	return b, nil
}

func (s *Service) forEachOrganization(ctx context.Context, opts influxdb.FindOptions, fn func(b *influxdb.Organization) bool) error {
	var err error
	orgs := make([]*influxdb.Organization, 0)
	s.organizationKV.Range(func(k, v interface{}) bool {
		o, ok := v.(*influxdb.Organization)
		if !ok {
			err = fmt.Errorf("type %T is not a organization", v)
			return false
		}

		orgs = append(orgs, o)
		return true
	})

	influxdb.SortOrganizations(opts, orgs)

	for _, o := range orgs {
		if !fn(o) {
			return nil
		}
	}

	return err
}

func (s *Service) filterOrganizations(ctx context.Context, fn func(b *influxdb.Organization) bool, opts influxdb.FindOptions) ([]*influxdb.Organization, *influxdb.Error) {
	var count int
	orgs := []*influxdb.Organization{}
	err := s.forEachOrganization(ctx, opts, func(o *influxdb.Organization) bool {
		if fn(o) {
			if count >= opts.Offset {
				orgs = append(orgs, o)
			}
			count++
		}
		if opts.Limit > 0 && len(orgs) >= opts.Limit {
			return false
		}
		return true
	})

	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return orgs, nil
}

// FindOrganizationByID returns a single organization by ID.
func (s *Service) FindOrganizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
	o, pe := s.loadOrganization(id)
	if pe != nil {
		return nil, &influxdb.Error{
			Op:  OpPrefix + influxdb.OpFindOrganizationByID,
			Err: pe,
		}
	}
	return o, nil
}

// FindOrganization returns the first organization that matches a filter.
func (s *Service) FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
	op := OpPrefix + influxdb.OpFindOrganization
	if filter.ID == nil && filter.Name == nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Op:   op,
			Msg:  "no filter parameters provided",
		}
	}

	if filter.ID != nil {
		o, err := s.FindOrganizationByID(ctx, *filter.ID)
		if err != nil {
			return nil, &influxdb.Error{
				Op:  op,
				Err: err,
			}
		}
		return o, nil
	}

	orgs, n, err := s.FindOrganizations(ctx, filter, influxdb.FindOptions{Limit: 1})
	if err != nil {
		return nil, &influxdb.Error{
			Op:  op,
			Err: err,
		}
	}
	if n < 1 {
		msg := errOrganizationNotFound
		if filter.Name != nil {
			msg = fmt.Sprintf("organization name \"%s\" not found", *filter.Name)
		}

		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Op:   op,
			Msg:  msg,
		}
	}

	return orgs[0], nil
}

// FindOrganizations returns a list of organizations that match filter and the total count of matching organizations.
func (s *Service) FindOrganizations(ctx context.Context, filter influxdb.OrganizationFilter, opts influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
	op := OpPrefix + influxdb.OpFindOrganizations
	if filter.ID != nil {
		o, err := s.FindOrganizationByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, &influxdb.Error{
				Op:  op,
				Err: err,
			}
		}

		return []*influxdb.Organization{o}, 1, nil
	}

	filterFunc := func(o *influxdb.Organization) bool { return true }
	if filter.Name != nil {
		filterFunc = func(o *influxdb.Organization) bool {
			return o.Name == *filter.Name
		}
	}

	orgs, pe := s.filterOrganizations(ctx, filterFunc, opts)
	if pe != nil {
		return nil, 0, &influxdb.Error{
			Err: pe,
			Op:  op,
		}
	}

	if len(orgs) == 0 {
		msg := errOrganizationNotFound
		if filter.Name != nil {
			msg = fmt.Sprintf("organization name \"%s\" not found", *filter.Name)
		}

		return nil, 0, &influxdb.Error{
			Code: influxdb.ENotFound,
			Op:   op,
			Msg:  msg,
		}
	}

	return orgs, len(orgs), nil
}

func (s *Service) findOrganizationByName(ctx context.Context, n string) (*influxdb.Organization, *influxdb.Error) {
	o, err := s.FindOrganization(ctx, influxdb.OrganizationFilter{Name: &n})
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}
	return o, nil
}

// CreateOrganization creates a new organization and sets b.ID with the new identifier.
func (s *Service) CreateOrganization(ctx context.Context, o *influxdb.Organization) error {
	op := OpPrefix + influxdb.OpCreateOrganization
	if _, err := s.FindOrganization(ctx, influxdb.OrganizationFilter{Name: &o.Name}); err == nil {
		return &influxdb.Error{
			Code: influxdb.EConflict,
			Op:   op,
			Msg:  fmt.Sprintf("organization with name %s already exists", o.Name),
		}

	}
	o.ID = s.IDGenerator.ID()
	err := s.PutOrganization(ctx, o)
	if err != nil {
		return &influxdb.Error{
			Op:  op,
			Err: err,
		}
	}
	return nil
}

// PutOrganization will put a organization without setting an ID.
func (s *Service) PutOrganization(ctx context.Context, o *influxdb.Organization) error {
	s.organizationKV.Store(o.ID.String(), o)
	return nil
}

// UpdateOrganization updates a organization according the parameters set on upd.
func (s *Service) UpdateOrganization(ctx context.Context, id influxdb.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
	o, err := s.FindOrganizationByID(ctx, id)
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
			Op:  OpPrefix + influxdb.OpUpdateOrganization,
		}
	}

	if upd.Name != nil {
		o.Name = *upd.Name
	}

	s.organizationKV.Store(o.ID.String(), o)

	return o, nil
}

// DeleteOrganization deletes a organization and prunes it from the index.
func (s *Service) DeleteOrganization(ctx context.Context, id influxdb.ID) error {
	if _, err := s.FindOrganizationByID(ctx, id); err != nil {
		return &influxdb.Error{
			Err: err,
			Op:  OpPrefix + influxdb.OpDeleteOrganization,
		}
	}
	s.organizationKV.Delete(id.String())
	return nil
}
