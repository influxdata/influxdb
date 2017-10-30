package organizations

import (
	"context"

	"github.com/influxdata/chronograf"
)

var _ chronograf.DashboardsStore = &DashboardsStore{}

type DashboardsStore struct {
	store chronograf.DashboardsStore
}

func NewDashboardsStore(s chronograf.DashboardsStore) *DashboardsStore {
	return &DashboardsStore{
		store: s,
	}
}

func (s *DashboardsStore) All(ctx context.Context) ([]chronograf.Dashboard, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return nil, err
	}
	ds, err := s.store.All(ctx)
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

func (s *DashboardsStore) Add(ctx context.Context, d chronograf.Dashboard) (chronograf.Dashboard, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return chronograf.Dashboard{}, err
	}

	d.Organization = org
	return s.store.Add(ctx, d)
}

func (s *DashboardsStore) Delete(ctx context.Context, d chronograf.Dashboard) error {
	d, err := s.store.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.store.Delete(ctx, d)
}

func (s *DashboardsStore) Get(ctx context.Context, id chronograf.DashboardID) (chronograf.Dashboard, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return chronograf.Dashboard{}, err
	}

	d, err := s.store.Get(ctx, id)
	if err != nil {
		return chronograf.Dashboard{}, err
	}

	if d.Organization != org {
		return chronograf.Dashboard{}, chronograf.ErrDashboardNotFound
	}

	return d, nil
}

func (s *DashboardsStore) Update(ctx context.Context, d chronograf.Dashboard) error {
	_, err := s.store.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.store.Update(ctx, d)
}
