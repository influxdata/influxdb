package inmem

import (
	"context"
	"fmt"
	"sync"

	"github.com/influxdata/platform"
)

func (s *Service) loadDashboard(ctx context.Context, id platform.ID) (*platform.Dashboard, error) {
	i, ok := s.dashboardKV.Load(id.String())
	if !ok {
		return nil, fmt.Errorf("dashboard not found")
	}

	d, ok := i.(*platform.Dashboard)
	if !ok {
		return nil, fmt.Errorf("type %T is not a dashboard", i)
	}
	return d, nil
}

// FindDashboardByID returns a single dashboard by ID.
func (s *Service) FindDashboardByID(ctx context.Context, id platform.ID) (*platform.Dashboard, error) {
	return s.loadDashboard(ctx, id)
}

func filterDashboardFn(filter platform.DashboardFilter) func(d *platform.Dashboard) bool {
	if len(filter.IDs) > 0 {
		var sm sync.Map
		for _, id := range filter.IDs {
			sm.Store(id.String(), true)
		}
		return func(d *platform.Dashboard) bool {
			_, ok := sm.Load(d.ID.String())
			return ok
		}
	}

	return func(d *platform.Dashboard) bool { return true }
}

// FindDashboards implements platform.DashboardService interface.
func (s *Service) FindDashboards(ctx context.Context, filter platform.DashboardFilter, opts platform.FindOptions) ([]*platform.Dashboard, int, error) {
	if len(filter.IDs) == 1 {
		d, err := s.FindDashboardByID(ctx, *filter.IDs[0])
		if err != nil {
			return nil, 0, err
		}

		return []*platform.Dashboard{d}, 1, nil
	}

	var ds []*platform.Dashboard
	var err error
	filterF := filterDashboardFn(filter)
	s.dashboardKV.Range(func(k, v interface{}) bool {
		d, ok := v.(*platform.Dashboard)
		if !ok {
			return false
		}

		if filterF(d) {
			ds = append(ds, d)
		}
		return true
	})

	platform.SortDashboards(opts.SortBy, ds)

	return ds, len(ds), err
}

// CreateDashboard implements platform.DashboardService interface.
func (s *Service) CreateDashboard(ctx context.Context, d *platform.Dashboard) error {
	d.ID = s.IDGenerator.ID()
	d.Meta.CreatedAt = s.time()
	return s.PutDashboardWithMeta(ctx, d)
}

// PutDashboard implements platform.DashboardService interface.
func (s *Service) PutDashboard(ctx context.Context, o *platform.Dashboard) error {
	s.dashboardKV.Store(o.ID.String(), o)
	return nil
}

// PutDashboardWithMeta sets a dashboard while updating the meta field of a dashboard.
func (s *Service) PutDashboardWithMeta(ctx context.Context, d *platform.Dashboard) error {
	d.Meta.UpdatedAt = s.time()
	return s.PutDashboard(ctx, d)
}

// UpdateDashboard implements platform.DashboardService interface.
func (s *Service) UpdateDashboard(ctx context.Context, id platform.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
	if err := upd.Valid(); err != nil {
		return nil, err
	}

	d, err := s.FindDashboardByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := upd.Apply(d); err != nil {
		return nil, err
	}

	if err := s.PutDashboardWithMeta(ctx, d); err != nil {
		return nil, err
	}

	return d, nil
}

// DeleteDashboard implements platform.DashboardService interface.
func (s *Service) DeleteDashboard(ctx context.Context, id platform.ID) error {
	if _, err := s.FindDashboardByID(ctx, id); err != nil {
		return err
	}
	s.dashboardKV.Delete(id.String())
	return nil
}

// AddDashboardCell adds a new cell to the dashboard.
func (s *Service) AddDashboardCell(ctx context.Context, id platform.ID, cell *platform.Cell, opts platform.AddDashboardCellOptions) error {
	d, err := s.FindDashboardByID(ctx, id)
	if err != nil {
		return err
	}
	cell.ID = s.IDGenerator.ID()
	if err := s.createViewIfNotExists(ctx, cell, opts); err != nil {
		return err
	}

	d.Cells = append(d.Cells, cell)
	return s.PutDashboardWithMeta(ctx, d)
}

// PutDashboardCell replaces a dashboad cell with the cell contents.
func (s *Service) PutDashboardCell(ctx context.Context, id platform.ID, cell *platform.Cell) error {
	d, err := s.FindDashboardByID(ctx, id)
	if err != nil {
		return err
	}
	view := &platform.View{}
	view.ID = cell.ViewID
	if err := s.PutView(ctx, view); err != nil {
		return err
	}

	d.Cells = append(d.Cells, cell)
	return s.PutDashboard(ctx, d)
}

// RemoveDashboardCell removes a dashboard cell from the dashboard.
func (s *Service) RemoveDashboardCell(ctx context.Context, dashboardID platform.ID, cellID platform.ID) error {
	d, err := s.FindDashboardByID(ctx, dashboardID)
	if err != nil {
		return err
	}

	idx := -1
	for i, cell := range d.Cells {
		if cell.ID == cellID {
			idx = i
			break
		}
	}
	if idx == -1 {
		return platform.ErrCellNotFound
	}

	if err := s.DeleteView(ctx, d.Cells[idx].ViewID); err != nil {
		return err
	}

	d.Cells = append(d.Cells[:idx], d.Cells[idx+1:]...)
	return s.PutDashboardWithMeta(ctx, d)

}

// UpdateDashboardCell will remove a cell from a dashboard.
func (s *Service) UpdateDashboardCell(ctx context.Context, dashboardID platform.ID, cellID platform.ID, upd platform.CellUpdate) (*platform.Cell, error) {
	if err := upd.Valid(); err != nil {
		return nil, err
	}

	d, err := s.FindDashboardByID(ctx, dashboardID)
	if err != nil {
		return nil, err
	}

	idx := -1
	for i, cell := range d.Cells {
		if cell.ID == cellID {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil, platform.ErrCellNotFound
	}

	if err := upd.Apply(d.Cells[idx]); err != nil {
		return nil, err
	}

	cell := d.Cells[idx]

	if err := s.PutDashboardWithMeta(ctx, d); err != nil {
		return nil, err
	}

	return cell, nil
}

// ReplaceDashboardCells replaces many dashboard cells.
func (s *Service) ReplaceDashboardCells(ctx context.Context, id platform.ID, cs []*platform.Cell) error {
	d, err := s.FindDashboardByID(ctx, id)
	if err != nil {
		return err
	}

	ids := map[string]*platform.Cell{}
	for _, cell := range d.Cells {
		ids[cell.ID.String()] = cell
	}

	for _, cell := range cs {
		if !cell.ID.Valid() {
			return fmt.Errorf("cannot provide empty cell id")
		}

		cl, ok := ids[cell.ID.String()]
		if !ok {
			return fmt.Errorf("cannot replace cells that were not already present")
		}

		if cl.ViewID != cell.ViewID {
			return fmt.Errorf("cannot update view id in replace")
		}
	}

	d.Cells = cs

	return s.PutDashboardWithMeta(ctx, d)
}
