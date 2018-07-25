package mocks

import (
	"context"

	"github.com/influxdata/platform/chronograf"
	platform "github.com/influxdata/platform/chronograf/v2"
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

var _ platform.DashboardService = &DashboardService{}

type DashboardService struct {
	CreateDashboardF   func(context.Context, *platform.Dashboard) error
	FindDashboardByIDF func(context.Context, platform.ID) (*platform.Dashboard, error)
	FindDashboardsF    func(context.Context, platform.DashboardFilter) ([]*platform.Dashboard, int, error)
	UpdateDashboardF   func(context.Context, platform.ID, platform.DashboardUpdate) (*platform.Dashboard, error)
	DeleteDashboardF   func(context.Context, platform.ID) error
}

func (s *DashboardService) FindDashboardByID(ctx context.Context, id platform.ID) (*platform.Dashboard, error) {
	return s.FindDashboardByIDF(ctx, id)
}

func (s *DashboardService) FindDashboards(ctx context.Context, filter platform.DashboardFilter) ([]*platform.Dashboard, int, error) {
	return s.FindDashboardsF(ctx, filter)
}

func (s *DashboardService) CreateDashboard(ctx context.Context, b *platform.Dashboard) error {
	return s.CreateDashboardF(ctx, b)
}

func (s *DashboardService) UpdateDashboard(ctx context.Context, id platform.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
	return s.UpdateDashboardF(ctx, id, upd)
}

func (s *DashboardService) DeleteDashboard(ctx context.Context, id platform.ID) error {
	return s.DeleteDashboardF(ctx, id)
}
