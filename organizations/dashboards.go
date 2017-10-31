package organizations

import (
	"context"

	"github.com/influxdata/chronograf"
)

var _ chronograf.DashboardsStore = &DashboardsStore{}

type DashboardsStore struct {
	store        chronograf.DashboardsStore
	organization string
}

func NewDashboardsStore(s chronograf.DashboardsStore, org string) *DashboardsStore {
	return &DashboardsStore{
		store:        s,
		organization: org,
	}
}

func (s *DashboardsStore) All(ctx context.Context) ([]chronograf.Dashboard, error) {
	err := validOrganization(ctx)
	if err != nil {
		return nil, err
	}

	ds, err := s.store.All(ctx)
	if err != nil {
		return nil, err
	}

	dashboards := ds[:0]
	for _, d := range ds {
		if d.Organization == s.organization {
			dashboards = append(dashboards, d)
		}
	}

	return dashboards, nil
}

func (s *DashboardsStore) Add(ctx context.Context, d chronograf.Dashboard) (chronograf.Dashboard, error) {
	err := validOrganization(ctx)
	if err != nil {
		return chronograf.Dashboard{}, err
	}

	d.Organization = s.organization
	return s.store.Add(ctx, d)
}

func (s *DashboardsStore) Delete(ctx context.Context, d chronograf.Dashboard) error {
	err := validOrganization(ctx)
	if err != nil {
		return err
	}

	d, err = s.store.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.store.Delete(ctx, d)
}

func (s *DashboardsStore) Get(ctx context.Context, id chronograf.DashboardID) (chronograf.Dashboard, error) {
	err := validOrganization(ctx)
	if err != nil {
		return chronograf.Dashboard{}, err
	}

	d, err := s.store.Get(ctx, id)
	if err != nil {
		return chronograf.Dashboard{}, err
	}

	if d.Organization != s.organization {
		return chronograf.Dashboard{}, chronograf.ErrDashboardNotFound
	}

	return d, nil
}

func (s *DashboardsStore) Update(ctx context.Context, d chronograf.Dashboard) error {
	err := validOrganization(ctx)
	if err != nil {
		return err
	}

	_, err = s.store.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.store.Update(ctx, d)
}
