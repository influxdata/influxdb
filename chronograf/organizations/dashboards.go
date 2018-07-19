package organizations

import (
	"context"

	"github.com/influxdata/chronograf"
)

// ensure that DashboardsStore implements chronograf.DashboardStore
var _ chronograf.DashboardsStore = &DashboardsStore{}

// DashboardsStore facade on a DashboardStore that filters dashboards
// by organization.
type DashboardsStore struct {
	store        chronograf.DashboardsStore
	organization string
}

// NewDashboardsStore creates a new DashboardsStore from an existing
// chronograf.DashboardStore and an organization string
func NewDashboardsStore(s chronograf.DashboardsStore, org string) *DashboardsStore {
	return &DashboardsStore{
		store:        s,
		organization: org,
	}
}

// All retrieves all dashboards from the underlying DashboardStore and filters them
// by organization.
func (s *DashboardsStore) All(ctx context.Context) ([]chronograf.Dashboard, error) {
	err := validOrganization(ctx)
	if err != nil {
		return nil, err
	}

	ds, err := s.store.All(ctx)
	if err != nil {
		return nil, err
	}

	// This filters dashboards without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	dashboards := ds[:0]
	for _, d := range ds {
		if d.Organization == s.organization {
			dashboards = append(dashboards, d)
		}
	}

	return dashboards, nil
}

// Add creates a new Dashboard in the DashboardsStore with dashboard.Organization set to be the
// organization from the dashboard store.
func (s *DashboardsStore) Add(ctx context.Context, d chronograf.Dashboard) (chronograf.Dashboard, error) {
	err := validOrganization(ctx)
	if err != nil {
		return chronograf.Dashboard{}, err
	}

	d.Organization = s.organization
	return s.store.Add(ctx, d)
}

// Delete the dashboard from DashboardsStore
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

// Get returns a Dashboard if the id exists and belongs to the organization that is set.
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

// Update the dashboard in DashboardsStore.
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
