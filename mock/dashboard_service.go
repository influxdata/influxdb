package mock

import (
	"context"

	"github.com/influxdata/platform"
)

var _ platform.DashboardService = &DashboardService{}

type DashboardService struct {
	CreateDashboardF   func(context.Context, *platform.Dashboard) error
	FindDashboardByIDF func(context.Context, platform.ID) (*platform.Dashboard, error)
	FindDashboardsF    func(context.Context, platform.DashboardFilter) ([]*platform.Dashboard, int, error)
	UpdateDashboardF   func(context.Context, platform.ID, platform.DashboardUpdate) (*platform.Dashboard, error)
	DeleteDashboardF   func(context.Context, platform.ID) error

	AddDashboardCellF      func(ctx context.Context, id platform.ID, c *platform.Cell, opts platform.AddDashboardCellOptions) error
	RemoveDashboardCellF   func(ctx context.Context, dashboardID platform.ID, cellID platform.ID) error
	UpdateDashboardCellF   func(ctx context.Context, dashbaordID platform.ID, cellID platform.ID, upd platform.CellUpdate) (*platform.Cell, error)
	CopyDashboardCellF     func(ctx context.Context, dashbaordID platform.ID, cellID platform.ID) (*platform.Cell, error)
	ReplaceDashboardCellsF func(ctx context.Context, id platform.ID, cs []*platform.Cell) error
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

func (s *DashboardService) AddDashboardCell(ctx context.Context, id platform.ID, c *platform.Cell, opts platform.AddDashboardCellOptions) error {
	return s.AddDashboardCellF(ctx, id, c, opts)
}

func (s *DashboardService) ReplaceDashboardCells(ctx context.Context, id platform.ID, cs []*platform.Cell) error {
	return s.ReplaceDashboardCellsF(ctx, id, cs)
}

func (s *DashboardService) RemoveDashboardCell(ctx context.Context, dashboardID platform.ID, cellID platform.ID) error {
	return s.RemoveDashboardCellF(ctx, dashboardID, cellID)
}

func (s *DashboardService) UpdateDashboardCell(ctx context.Context, dashboardID platform.ID, cellID platform.ID, upd platform.CellUpdate) (*platform.Cell, error) {
	return s.UpdateDashboardCellF(ctx, dashboardID, cellID, upd)
}

func (s *DashboardService) CopyDashboardCell(ctx context.Context, dashboardID platform.ID, cellID platform.ID) (*platform.Cell, error) {
	return s.CopyDashboardCell(ctx, dashboardID, cellID)
}
