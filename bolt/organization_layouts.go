package bolt

import (
	"context"

	"github.com/influxdata/chronograf"
)

type OrganizationLayoutsStore struct {
	client *Client
}

func (s *OrganizationLayoutsStore) All(ctx context.Context) ([]chronograf.Layout, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return nil, err
	}
	ds, err := s.client.LayoutsStore.All(ctx)
	if err != nil {
		return nil, err
	}

	dashboards := ds[:0]
	for _, d := range ds {
		if d.Organization == org {
			dashboards = append(dashboards, d)
		}
	}

	return dashboards, nil
}

func (s *OrganizationLayoutsStore) Add(ctx context.Context, d chronograf.Layout) (chronograf.Layout, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return chronograf.Layout{}, err
	}

	d.Organization = org
	return s.client.LayoutsStore.Add(ctx, d)
}

func (s *OrganizationLayoutsStore) Delete(ctx context.Context, d chronograf.Layout) error {
	d, err := s.client.LayoutsStore.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.client.LayoutsStore.Delete(ctx, d)
}

func (s *OrganizationLayoutsStore) Get(ctx context.Context, id string) (chronograf.Layout, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return chronograf.Layout{}, err
	}

	d, err := s.client.LayoutsStore.Get(ctx, id)
	if err != nil {
		return chronograf.Layout{}, err
	}

	if d.Organization != org {
		return chronograf.Layout{}, chronograf.ErrLayoutNotFound
	}

	return d, nil
}

func (s *OrganizationLayoutsStore) Update(ctx context.Context, d chronograf.Layout) error {
	_, err := s.client.LayoutsStore.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.client.LayoutsStore.Update(ctx, d)
}
