package bolt

import (
	"context"

	"github.com/influxdata/chronograf"
)

type OrganizationDashboardsStore struct {
	client *Client
}

func (s *OrganizationDashboardsStore) All(ctx context.Context) ([]chronograf.Dashboard, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return nil, err
	}
	ds, err := s.client.DashboardsStore.All(ctx)
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

func (s *OrganizationDashboardsStore) Add(ctx context.Context, d chronograf.Dashboard) (chronograf.Dashboard, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return chronograf.Dashboard{}, err
	}

	d.Organization = org
	return s.client.DashboardsStore.Add(ctx, d)
}

func (s *OrganizationDashboardsStore) Delete(ctx context.Context, d chronograf.Dashboard) error {
	d, err := s.client.DashboardsStore.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.client.DashboardsStore.Delete(ctx, d)
}

func (s *OrganizationDashboardsStore) Get(ctx context.Context, id chronograf.DashboardID) (chronograf.Dashboard, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return chronograf.Dashboard{}, err
	}

	d, err := s.client.DashboardsStore.Get(ctx, id)
	if err != nil {
		return chronograf.Dashboard{}, err
	}

	if d.Organization != org {
		return chronograf.Dashboard{}, chronograf.ErrDashboardNotFound
	}

	return d, nil
}

func (s *OrganizationDashboardsStore) Update(ctx context.Context, d chronograf.Dashboard) error {
	_, err := s.client.DashboardsStore.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.client.DashboardsStore.Update(ctx, d)
}
