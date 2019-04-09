package inmem

import (
	"context"
	"fmt"

	platform "github.com/influxdata/influxdb"
)

const (
	errOrganizationNotFound = "organization not found"
)

func (s *Service) loadOrganization(id platform.ID) (*platform.Organization, *platform.Error) {
	i, ok := s.organizationKV.Load(id.String())
	if !ok {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  errOrganizationNotFound,
		}
	}

	b, ok := i.(*platform.Organization)
	if !ok {
		return nil, &platform.Error{
			Code: platform.EInternal,
			Msg:  fmt.Sprintf("type %T is not a organization", i),
		}
	}
	return b, nil
}

func (s *Service) forEachOrganization(ctx context.Context, fn func(b *platform.Organization) bool) error {
	var err error
	s.organizationKV.Range(func(k, v interface{}) bool {
		o, ok := v.(*platform.Organization)
		if !ok {
			err = fmt.Errorf("type %T is not a organization", v)
			return false
		}

		return fn(o)
	})

	return err
}

func (s *Service) filterOrganizations(ctx context.Context, fn func(b *platform.Organization) bool) ([]*platform.Organization, *platform.Error) {
	orgs := []*platform.Organization{}
	err := s.forEachOrganization(ctx, func(o *platform.Organization) bool {
		if fn(o) {
			orgs = append(orgs, o)
		}
		return true
	})

	if err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}

	return orgs, nil
}

// FindOrganizationByID returns a single organization by ID.
func (s *Service) FindOrganizationByID(ctx context.Context, id platform.ID) (*platform.Organization, error) {
	o, pe := s.loadOrganization(id)
	if pe != nil {
		return nil, &platform.Error{
			Op:  OpPrefix + platform.OpFindOrganizationByID,
			Err: pe,
		}
	}
	return o, nil
}

// FindOrganization returns the first organization that matches a filter.
func (s *Service) FindOrganization(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
	op := OpPrefix + platform.OpFindOrganization
	if filter.ID == nil && filter.Name == nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Op:   op,
			Msg:  "no filter parameters provided",
		}
	}

	if filter.ID != nil {
		o, err := s.FindOrganizationByID(ctx, *filter.ID)
		if err != nil {
			return nil, &platform.Error{
				Op:  op,
				Err: err,
			}
		}
		return o, nil
	}

	orgs, n, err := s.FindOrganizations(ctx, filter)
	if err != nil {
		return nil, &platform.Error{
			Op:  op,
			Err: err,
		}
	}
	if n < 1 {
		msg := errOrganizationNotFound
		if filter.Name != nil {
			msg = fmt.Sprintf("organization name \"%s\" not found", *filter.Name)
		}

		return nil, &platform.Error{
			Code: platform.ENotFound,
			Op:   op,
			Msg:  msg,
		}
	}

	return orgs[0], nil
}

// FindOrganizations returns a list of organizations that match filter and the total count of matching organizations.
func (s *Service) FindOrganizations(ctx context.Context, filter platform.OrganizationFilter, opt ...platform.FindOptions) ([]*platform.Organization, int, error) {
	op := OpPrefix + platform.OpFindOrganizations
	if filter.ID != nil {
		o, err := s.FindOrganizationByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, &platform.Error{
				Op:  op,
				Err: err,
			}
		}

		return []*platform.Organization{o}, 1, nil
	}

	filterFunc := func(o *platform.Organization) bool { return true }
	if filter.Name != nil {
		filterFunc = func(o *platform.Organization) bool {
			return o.Name == *filter.Name
		}
	}

	orgs, pe := s.filterOrganizations(ctx, filterFunc)
	if pe != nil {
		return nil, 0, &platform.Error{
			Err: pe,
			Op:  op,
		}
	}

	if len(orgs) == 0 {
		msg := errOrganizationNotFound
		if filter.Name != nil {
			msg = fmt.Sprintf("organization name \"%s\" not found", *filter.Name)
		}

		return orgs, 0, &platform.Error{
			Code: platform.ENotFound,
			Op:   op,
			Msg:  msg,
		}
	}

	return orgs, len(orgs), nil
}

func (s *Service) findOrganizationByName(ctx context.Context, n string) (*platform.Organization, *platform.Error) {
	o, err := s.FindOrganization(ctx, platform.OrganizationFilter{Name: &n})
	if err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}
	return o, nil
}

// CreateOrganization creates a new organization and sets b.ID with the new identifier.
func (s *Service) CreateOrganization(ctx context.Context, o *platform.Organization) error {
	op := OpPrefix + platform.OpCreateOrganization
	if _, err := s.FindOrganization(ctx, platform.OrganizationFilter{Name: &o.Name}); err == nil {
		return &platform.Error{
			Code: platform.EConflict,
			Op:   op,
			Msg:  fmt.Sprintf("organization with name %s already exists", o.Name),
		}

	}
	o.ID = s.IDGenerator.ID()
	err := s.PutOrganization(ctx, o)
	if err != nil {
		return &platform.Error{
			Op:  op,
			Err: err,
		}
	}
	return nil
}

// PutOrganization will put a organization without setting an ID.
func (s *Service) PutOrganization(ctx context.Context, o *platform.Organization) error {
	s.organizationKV.Store(o.ID.String(), o)
	return nil
}

// UpdateOrganization updates a organization according the parameters set on upd.
func (s *Service) UpdateOrganization(ctx context.Context, id platform.ID, upd platform.OrganizationUpdate) (*platform.Organization, error) {
	o, err := s.FindOrganizationByID(ctx, id)
	if err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  OpPrefix + platform.OpUpdateOrganization,
		}
	}

	if upd.Name != nil {
		o.Name = *upd.Name
	}

	s.organizationKV.Store(o.ID.String(), o)

	return o, nil
}

// DeleteOrganization deletes a organization and prunes it from the index.
func (s *Service) DeleteOrganization(ctx context.Context, id platform.ID) error {
	if _, err := s.FindOrganizationByID(ctx, id); err != nil {
		return &platform.Error{
			Err: err,
			Op:  OpPrefix + platform.OpDeleteOrganization,
		}
	}
	s.organizationKV.Delete(id.String())
	return nil
}
