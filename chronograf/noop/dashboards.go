package noop

import (
	"context"
	"fmt"

	"github.com/influxdata/platform/chronograf"
)

// ensure DashboardsStore implements chronograf.DashboardsStore
var _ chronograf.DashboardsStore = &DashboardsStore{}

type DashboardsStore struct{}

func (s *DashboardsStore) All(context.Context) ([]chronograf.Dashboard, error) {
	return nil, fmt.Errorf("no dashboards found")
}

func (s *DashboardsStore) Add(context.Context, chronograf.Dashboard) (chronograf.Dashboard, error) {
	return chronograf.Dashboard{}, fmt.Errorf("failed to add dashboard")
}

func (s *DashboardsStore) Delete(context.Context, chronograf.Dashboard) error {
	return fmt.Errorf("failed to delete dashboard")
}

func (s *DashboardsStore) Get(ctx context.Context, ID chronograf.DashboardID) (chronograf.Dashboard, error) {
	return chronograf.Dashboard{}, chronograf.ErrDashboardNotFound
}

func (s *DashboardsStore) Update(context.Context, chronograf.Dashboard) error {
	return fmt.Errorf("failed to update dashboard")
}
