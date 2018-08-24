package mocks

import (
	"context"

	"github.com/influxdata/platform/chronograf"
)

var _ chronograf.DashboardsStore = &DashboardsStore{}

type DashboardsStore struct {
	AddF    func(ctx context.Context, newDashboard chronograf.Dashboard) (chronograf.Dashboard, error)
	AllF    func(ctx context.Context) ([]chronograf.Dashboard, error)
	DeleteF func(ctx context.Context, target chronograf.Dashboard) error
	GetF    func(ctx context.Context, id chronograf.DashboardID) (chronograf.Dashboard, error)
	UpdateF func(ctx context.Context, target chronograf.Dashboard) error
}

func (d *DashboardsStore) Add(ctx context.Context, newDashboard chronograf.Dashboard) (chronograf.Dashboard, error) {
	return d.AddF(ctx, newDashboard)
}

func (d *DashboardsStore) All(ctx context.Context) ([]chronograf.Dashboard, error) {
	return d.AllF(ctx)
}

func (d *DashboardsStore) Delete(ctx context.Context, target chronograf.Dashboard) error {
	return d.DeleteF(ctx, target)
}

func (d *DashboardsStore) Get(ctx context.Context, id chronograf.DashboardID) (chronograf.Dashboard, error) {
	return d.GetF(ctx, id)
}

func (d *DashboardsStore) Update(ctx context.Context, target chronograf.Dashboard) error {
	return d.UpdateF(ctx, target)
}
