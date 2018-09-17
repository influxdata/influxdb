package inmem

import (
	"bytes"
	"context"
	"fmt"

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
	if filter.ID != nil {
		return func(d *platform.Dashboard) bool {
			return bytes.Equal(d.ID, *filter.ID)
		}
	}

	return func(d *platform.Dashboard) bool { return true }
}

// FindDashboards implements platform.DashboardService interface.
func (s *Service) FindDashboards(ctx context.Context, filter platform.DashboardFilter) ([]*platform.Dashboard, int, error) {
	if filter.ID != nil {
		d, err := s.FindDashboardByID(ctx, *filter.ID)
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
	return ds, len(ds), err
}

// CreateDashboard implements platform.DashboardService interface.
func (s *Service) CreateDashboard(ctx context.Context, d *platform.Dashboard) error {
	d.ID = s.IDGenerator.ID()
	return s.PutDashboard(ctx, d)
}

// PutDashboard implements platform.DashboardService interface.
func (s *Service) PutDashboard(ctx context.Context, o *platform.Dashboard) error {
	s.dashboardKV.Store(o.ID.String(), o)
	return nil
}

// UpdateDashboard implements platform.DashboardService interface.
func (s *Service) UpdateDashboard(ctx context.Context, id platform.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
	o, err := s.FindDashboardByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		o.Name = *upd.Name
	}

	s.dashboardKV.Store(o.ID.String(), o)
	return o, nil
}

// DeleteDashboard implements platform.DashboardService interface.
func (s *Service) DeleteDashboard(ctx context.Context, id platform.ID) error {
	if _, err := s.FindDashboardByID(ctx, id); err != nil {
		return err
	}
	s.dashboardKV.Delete(id.String())
	return nil
}

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
	return s.PutDashboard(ctx, d)
}

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

func (s *Service) RemoveDashboardCell(ctx context.Context, dashboardID platform.ID, cellID platform.ID) error {
	d, err := s.FindDashboardByID(ctx, dashboardID)
	if err != nil {
		return err
	}

	idx := -1
	for i, cell := range d.Cells {
		if bytes.Equal(cell.ID, cellID) {
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
	return s.PutDashboard(ctx, d)

}

func (s *Service) UpdateDashboardCell(ctx context.Context, dashboardID platform.ID, cellID platform.ID, upd platform.CellUpdate) (*platform.Cell, error) {
	d, err := s.FindDashboardByID(ctx, dashboardID)
	if err != nil {
		return nil, err
	}

	idx := -1
	for i, cell := range d.Cells {
		if bytes.Equal(cell.ID, cellID) {
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

	if err := s.PutDashboard(ctx, d); err != nil {
		return nil, err
	}

	return cell, nil
}

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
		if len(cell.ID) == 0 {
			return fmt.Errorf("cannot provide empty cell id")
		}

		cl, ok := ids[cell.ID.String()]
		if !ok {
			return fmt.Errorf("cannot replace cells that were not already present")
		}

		if !bytes.Equal(cl.ViewID, cell.ViewID) {
			return fmt.Errorf("cannot update view id in replace")
		}
	}

	d.Cells = cs

	return s.PutDashboard(ctx, d)
}
