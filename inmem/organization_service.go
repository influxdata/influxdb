package inmem

import (
	"context"
	"fmt"

	"github.com/influxdata/platform"
)

var (
	errOrganizationNotFound = fmt.Errorf("organization not found")
)

func (s *Service) loadOrganization(id platform.ID) (*platform.Organization, error) {
	i, ok := s.organizationKV.Load(id.String())
	if !ok {
		return nil, errOrganizationNotFound
	}

	b, ok := i.(*platform.Organization)
	if !ok {
		return nil, fmt.Errorf("type %T is not a organization", i)
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

func (s *Service) filterOrganizations(ctx context.Context, fn func(b *platform.Organization) bool) ([]*platform.Organization, error) {
	orgs := []*platform.Organization{}
	err := s.forEachOrganization(ctx, func(o *platform.Organization) bool {
		if fn(o) {
			orgs = append(orgs, o)
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	return orgs, nil
}

// FindOrganizationByID returns a single organization by ID.
func (s *Service) FindOrganizationByID(ctx context.Context, id platform.ID) (*platform.Organization, error) {
	return s.loadOrganization(id)
}

// FindOrganization returns the first organization that matches a filter.
func (s *Service) FindOrganization(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
	if filter.ID == nil && filter.Name == nil {
		return nil, fmt.Errorf("no filter parameters provided")
	}

	if filter.ID != nil {
		return s.FindOrganizationByID(ctx, *filter.ID)
	}

	orgs, n, err := s.FindOrganizations(ctx, filter)
	if err != nil {
		return nil, err
	}
	if n < 1 {
		return nil, fmt.Errorf("organization not found")
	}

	return orgs[0], nil
}

func (s *Service) FindOrganizations(ctx context.Context, filter platform.OrganizationFilter, opt ...platform.FindOptions) ([]*platform.Organization, int, error) {
	if filter.ID != nil {
		o, err := s.FindOrganizationByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.Organization{o}, 1, nil
	}

	filterFunc := func(o *platform.Organization) bool { return true }
	if filter.Name != nil {
		filterFunc = func(o *platform.Organization) bool {
			return o.Name == *filter.Name
		}
	}

	orgs, err := s.filterOrganizations(ctx, filterFunc)
	if err != nil {
		return nil, 0, err
	}

	return orgs, len(orgs), nil
}

func (c *Service) findOrganizationByName(ctx context.Context, n string) (*platform.Organization, error) {
	return c.FindOrganization(ctx, platform.OrganizationFilter{Name: &n})
}

func (s *Service) CreateOrganization(ctx context.Context, o *platform.Organization) error {
	if _, err := s.FindOrganization(ctx, platform.OrganizationFilter{Name: &o.Name}); err == nil {
		return fmt.Errorf("organization with name %s already exists", o.Name)
	}
	o.ID = s.IDGenerator.ID()
	return s.PutOrganization(ctx, o)
}

func (s *Service) PutOrganization(ctx context.Context, o *platform.Organization) error {
	s.organizationKV.Store(o.ID.String(), o)
	return nil
}

func (s *Service) UpdateOrganization(ctx context.Context, id platform.ID, upd platform.OrganizationUpdate) (*platform.Organization, error) {
	o, err := s.FindOrganizationByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		o.Name = *upd.Name
	}

	s.organizationKV.Store(o.ID.String(), o)

	return o, nil
}

func (s *Service) DeleteOrganization(ctx context.Context, id platform.ID) error {
	if _, err := s.FindOrganizationByID(ctx, id); err != nil {
		return err
	}
	s.organizationKV.Delete(id.String())
	return nil
}
