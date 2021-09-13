package mock

import (
	"context"

	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
)

var _ platform.DashboardService = &DashboardService{}

type DashboardService struct {
	CreateDashboardF       func(context.Context, *platform.Dashboard) error
	CreateDashboardCalls   SafeCount
	FindDashboardByIDF     func(context.Context, platform2.ID) (*platform.Dashboard, error)
	FindDashboardByIDCalls SafeCount
	FindDashboardsF        func(context.Context, platform.DashboardFilter, platform.FindOptions) ([]*platform.Dashboard, int, error)
	FindDashboardsCalls    SafeCount
	UpdateDashboardF       func(context.Context, platform2.ID, platform.DashboardUpdate) (*platform.Dashboard, error)
	UpdateDashboardCalls   SafeCount
	DeleteDashboardF       func(context.Context, platform2.ID) error
	DeleteDashboardCalls   SafeCount

	AddDashboardCellF            func(ctx context.Context, id platform2.ID, c *platform.Cell, opts platform.AddDashboardCellOptions) error
	AddDashboardCellCalls        SafeCount
	RemoveDashboardCellF         func(ctx context.Context, dashboardID platform2.ID, cellID platform2.ID) error
	RemoveDashboardCellCalls     SafeCount
	GetDashboardCellViewF        func(ctx context.Context, dashboardID platform2.ID, cellID platform2.ID) (*platform.View, error)
	GetDashboardCellViewCalls    SafeCount
	UpdateDashboardCellViewF     func(ctx context.Context, dashboardID platform2.ID, cellID platform2.ID, upd platform.ViewUpdate) (*platform.View, error)
	UpdateDashboardCellViewCalls SafeCount
	UpdateDashboardCellF         func(ctx context.Context, dashboardID platform2.ID, cellID platform2.ID, upd platform.CellUpdate) (*platform.Cell, error)
	UpdateDashboardCellCalls     SafeCount
	CopyDashboardCellF           func(ctx context.Context, dashboardID platform2.ID, cellID platform2.ID) (*platform.Cell, error)
	CopyDashboardCellCalls       SafeCount
	ReplaceDashboardCellsF       func(ctx context.Context, id platform2.ID, cs []*platform.Cell) error
	ReplaceDashboardCellsCalls   SafeCount
}

// NewDashboardService returns a mock of DashboardService where its methods will return zero values.
func NewDashboardService() *DashboardService {
	return &DashboardService{
		CreateDashboardF:   func(context.Context, *platform.Dashboard) error { return nil },
		FindDashboardByIDF: func(context.Context, platform2.ID) (*platform.Dashboard, error) { return nil, nil },
		FindDashboardsF: func(context.Context, platform.DashboardFilter, platform.FindOptions) ([]*platform.Dashboard, int, error) {
			return nil, 0, nil
		},
		UpdateDashboardF: func(context.Context, platform2.ID, platform.DashboardUpdate) (*platform.Dashboard, error) {
			return nil, nil
		},
		DeleteDashboardF: func(context.Context, platform2.ID) error { return nil },

		AddDashboardCellF: func(ctx context.Context, id platform2.ID, c *platform.Cell, opts platform.AddDashboardCellOptions) error {
			return nil
		},
		RemoveDashboardCellF: func(ctx context.Context, dashboardID platform2.ID, cellID platform2.ID) error { return nil },
		GetDashboardCellViewF: func(ctx context.Context, dashboardID platform2.ID, cellID platform2.ID) (*platform.View, error) {
			return nil, nil
		},
		UpdateDashboardCellViewF: func(ctx context.Context, dashboardID platform2.ID, cellID platform2.ID, upd platform.ViewUpdate) (*platform.View, error) {
			return nil, nil
		},
		UpdateDashboardCellF: func(ctx context.Context, dashboardID platform2.ID, cellID platform2.ID, upd platform.CellUpdate) (*platform.Cell, error) {
			return nil, nil
		},
		CopyDashboardCellF: func(ctx context.Context, dashboardID platform2.ID, cellID platform2.ID) (*platform.Cell, error) {
			return nil, nil
		},
		ReplaceDashboardCellsF: func(ctx context.Context, id platform2.ID, cs []*platform.Cell) error { return nil },
	}
}

func (s *DashboardService) FindDashboardByID(ctx context.Context, id platform2.ID) (*platform.Dashboard, error) {
	defer s.FindDashboardByIDCalls.IncrFn()()
	return s.FindDashboardByIDF(ctx, id)
}

func (s *DashboardService) FindDashboards(ctx context.Context, filter platform.DashboardFilter, opts platform.FindOptions) ([]*platform.Dashboard, int, error) {
	defer s.FindDashboardsCalls.IncrFn()()
	return s.FindDashboardsF(ctx, filter, opts)
}

func (s *DashboardService) CreateDashboard(ctx context.Context, b *platform.Dashboard) error {
	defer s.CreateDashboardCalls.IncrFn()()
	return s.CreateDashboardF(ctx, b)
}

func (s *DashboardService) UpdateDashboard(ctx context.Context, id platform2.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
	defer s.UpdateDashboardCalls.IncrFn()()
	return s.UpdateDashboardF(ctx, id, upd)
}

func (s *DashboardService) DeleteDashboard(ctx context.Context, id platform2.ID) error {
	defer s.DeleteDashboardCalls.IncrFn()()
	return s.DeleteDashboardF(ctx, id)
}

func (s *DashboardService) GetDashboardCellView(ctx context.Context, dashboardID, cellID platform2.ID) (*platform.View, error) {
	defer s.GetDashboardCellViewCalls.IncrFn()()
	return s.GetDashboardCellViewF(ctx, dashboardID, cellID)
}

func (s *DashboardService) UpdateDashboardCellView(ctx context.Context, dashboardID, cellID platform2.ID, upd platform.ViewUpdate) (*platform.View, error) {
	defer s.UpdateDashboardCellViewCalls.IncrFn()()
	return s.UpdateDashboardCellViewF(ctx, dashboardID, cellID, upd)
}

func (s *DashboardService) AddDashboardCell(ctx context.Context, id platform2.ID, c *platform.Cell, opts platform.AddDashboardCellOptions) error {
	defer s.AddDashboardCellCalls.IncrFn()()
	return s.AddDashboardCellF(ctx, id, c, opts)
}

func (s *DashboardService) ReplaceDashboardCells(ctx context.Context, id platform2.ID, cs []*platform.Cell) error {
	defer s.ReplaceDashboardCellsCalls.IncrFn()()
	return s.ReplaceDashboardCellsF(ctx, id, cs)
}

func (s *DashboardService) RemoveDashboardCell(ctx context.Context, dashboardID platform2.ID, cellID platform2.ID) error {
	defer s.RemoveDashboardCellCalls.IncrFn()()
	return s.RemoveDashboardCellF(ctx, dashboardID, cellID)
}

func (s *DashboardService) UpdateDashboardCell(ctx context.Context, dashboardID platform2.ID, cellID platform2.ID, upd platform.CellUpdate) (*platform.Cell, error) {
	defer s.UpdateDashboardCellCalls.IncrFn()()
	return s.UpdateDashboardCellF(ctx, dashboardID, cellID, upd)
}

func (s *DashboardService) CopyDashboardCell(ctx context.Context, dashboardID platform2.ID, cellID platform2.ID) (*platform.Cell, error) {
	defer s.CopyDashboardCellCalls.IncrFn()()
	return s.CopyDashboardCellF(ctx, dashboardID, cellID)
}
