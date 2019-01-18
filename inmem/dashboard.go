package inmem

import (
	"context"
	"fmt"

	platform "github.com/influxdata/influxdb"
)

func (s *Service) loadDashboard(ctx context.Context, id platform.ID) (*platform.Dashboard, *platform.Error) {
	i, ok := s.dashboardKV.Load(id.String())
	if !ok {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  platform.ErrDashboardNotFound,
		}
	}

	d, ok := i.(*platform.Dashboard)
	if !ok {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  fmt.Sprintf("type %T is not a dashboard", i),
		}
	}
	return d, nil
}

// FindDashboardByID returns a single dashboard by ID.
func (s *Service) FindDashboardByID(ctx context.Context, id platform.ID) (*platform.Dashboard, error) {
	d, pe := s.loadDashboard(ctx, id)
	if pe != nil {
		return nil, &platform.Error{
			Op:  OpPrefix + platform.OpFindDashboardByID,
			Err: pe,
		}
	}
	return d, nil
}

func filterDashboardFn(filter platform.DashboardFilter) func(d *platform.Dashboard) bool {
	if filter.OrganizationID != nil {
		return func(d *platform.Dashboard) bool {
			return d.OrganizationID == *filter.OrganizationID
		}
	}

	if len(filter.IDs) > 0 {
		m := map[platform.ID]struct{}{}
		for _, id := range filter.IDs {
			if id != nil {
				m[*id] = struct{}{}
			}
		}
		return func(d *platform.Dashboard) bool {
			_, ok := m[d.ID]
			return ok
		}
	}

	return func(d *platform.Dashboard) bool { return true }
}

func (s *Service) forEachDashboard(ctx context.Context, opts platform.FindOptions, fn func(d *platform.Dashboard) bool) error {
	var err error
	ds := make([]*platform.Dashboard, 0)
	s.dashboardKV.Range(func(k, v interface{}) bool {
		d, ok := v.(*platform.Dashboard)
		if !ok {
			err = fmt.Errorf("type %T is not a dashboard", v)
			return false
		}
		ds = append(ds, d)

		return true
	})

	platform.SortDashboards(opts, ds)

	for _, d := range ds {
		if !fn(d) {
			return nil
		}
	}

	return err
}

// FindDashboards implements platform.DashboardService interface.
func (s *Service) FindDashboards(ctx context.Context, filter platform.DashboardFilter, opts platform.FindOptions) ([]*platform.Dashboard, int, error) {
	var ds []*platform.Dashboard
	op := OpPrefix + platform.OpFindDashboards
	if len(filter.IDs) == 1 {
		d, err := s.FindDashboardByID(ctx, *filter.IDs[0])
		if err != nil && platform.ErrorCode(err) != platform.ENotFound {
			return ds, 0, &platform.Error{
				Err: err,
				Op:  op,
			}
		}
		if d == nil {
			return ds, 0, nil
		}
		return []*platform.Dashboard{d}, 1, nil
	}

	var count int
	filterFn := filterDashboardFn(filter)
	err := s.forEachDashboard(ctx, opts, func(d *platform.Dashboard) bool {
		if filterFn(d) {
			if count >= opts.Offset {
				ds = append(ds, d)
			}
			count++
		}
		if opts.Limit > 0 && len(ds) >= opts.Limit {
			return false
		}
		return true
	})

	if err != nil {
		return nil, 0, err
	}

	return ds, len(ds), err
}

// CreateDashboard implements platform.DashboardService interface.
func (s *Service) CreateDashboard(ctx context.Context, d *platform.Dashboard) error {
	d.ID = s.IDGenerator.ID()
	d.Meta.CreatedAt = s.time()
	err := s.PutDashboardWithMeta(ctx, d)
	if err != nil {
		return &platform.Error{
			Err: err,
			Op:  platform.OpCreateDashboard,
		}
	}
	return nil
}

// PutDashboard implements platform.DashboardService interface.
func (s *Service) PutDashboard(ctx context.Context, d *platform.Dashboard) error {
	for _, cell := range d.Cells {
		if err := s.PutCellView(ctx, cell); err != nil {
			return err
		}
	}
	s.dashboardKV.Store(d.ID.String(), d)
	return nil
}

// PutCellView puts the view for a cell.
func (s *Service) PutCellView(ctx context.Context, cell *platform.Cell) error {
	v := &platform.View{}
	v.ID = cell.ID
	return s.PutView(ctx, v)
}

// PutDashboardWithMeta sets a dashboard while updating the meta field of a dashboard.
func (s *Service) PutDashboardWithMeta(ctx context.Context, d *platform.Dashboard) error {
	d.Meta.UpdatedAt = s.time()
	return s.PutDashboard(ctx, d)
}

// UpdateDashboard implements platform.DashboardService interface.
func (s *Service) UpdateDashboard(ctx context.Context, id platform.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
	op := OpPrefix + platform.OpUpdateDashboard
	if err := upd.Valid(); err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  op,
		}
	}

	d, err := s.FindDashboardByID(ctx, id)
	if err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  op,
		}
	}

	if err := upd.Apply(d); err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  op,
		}
	}

	if err := s.PutDashboardWithMeta(ctx, d); err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  op,
		}
	}

	return d, nil
}

// DeleteDashboard implements platform.DashboardService interface.
func (s *Service) DeleteDashboard(ctx context.Context, id platform.ID) error {
	op := OpPrefix + platform.OpDeleteDashboard
	if _, err := s.FindDashboardByID(ctx, id); err != nil {
		return &platform.Error{
			Err: err,
			Op:  op,
		}
	}
	s.dashboardKV.Delete(id.String())
	// err := s.deleteLabel(ctx, platform.LabelFilter{ResourceID: id})
	// if err != nil {
	// 	return &platform.Error{
	// 		Err: err,
	// 		Op:  op,
	// 	}
	// }
	return nil
}

// AddDashboardCell adds a new cell to the dashboard.
func (s *Service) AddDashboardCell(ctx context.Context, id platform.ID, cell *platform.Cell, opts platform.AddDashboardCellOptions) error {
	op := OpPrefix + platform.OpAddDashboardCell
	d, err := s.FindDashboardByID(ctx, id)
	if err != nil {
		return &platform.Error{
			Err: err,
			Op:  op,
		}
	}
	cell.ID = s.IDGenerator.ID()
	if err := s.createCellView(ctx, cell); err != nil {
		return &platform.Error{
			Err: err,
			Op:  op,
		}
	}

	d.Cells = append(d.Cells, cell)
	if err = s.PutDashboardWithMeta(ctx, d); err != nil {
		return &platform.Error{
			Err: err,
			Op:  op,
		}
	}
	return nil
}

func (s *Service) createCellView(ctx context.Context, cell *platform.Cell) *platform.Error {
	// If not view exists create the view
	view := &platform.View{}
	view.ID = cell.ID
	if err := s.PutView(ctx, view); err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	return nil
}

// PutDashboardCell replaces a dashboad cell with the cell contents.
func (s *Service) PutDashboardCell(ctx context.Context, id platform.ID, cell *platform.Cell) error {
	d, err := s.FindDashboardByID(ctx, id)
	if err != nil {
		return err
	}
	view := &platform.View{}
	view.ID = cell.ID
	if err := s.PutView(ctx, view); err != nil {
		return err
	}

	d.Cells = append(d.Cells, cell)
	return s.PutDashboard(ctx, d)
}

// RemoveDashboardCell removes a dashboard cell from the dashboard.
func (s *Service) RemoveDashboardCell(ctx context.Context, dashboardID platform.ID, cellID platform.ID) error {
	op := OpPrefix + platform.OpRemoveDashboardCell
	d, err := s.FindDashboardByID(ctx, dashboardID)
	if err != nil {
		return &platform.Error{
			Err: err,
			Op:  op,
		}
	}

	idx := -1
	for i, cell := range d.Cells {
		if cell.ID == cellID {
			idx = i
			break
		}
	}
	if idx == -1 {
		return &platform.Error{
			Code: platform.ENotFound,
			Op:   op,
			Msg:  platform.ErrCellNotFound,
		}
	}

	if err := s.DeleteView(ctx, d.Cells[idx].ID); err != nil {
		return err
	}

	d.Cells = append(d.Cells[:idx], d.Cells[idx+1:]...)
	return s.PutDashboardWithMeta(ctx, d)

}

// UpdateDashboardCell will remove a cell from a dashboard.
func (s *Service) UpdateDashboardCell(ctx context.Context, dashboardID platform.ID, cellID platform.ID, upd platform.CellUpdate) (*platform.Cell, error) {
	op := OpPrefix + platform.OpUpdateDashboardCell
	if err := upd.Valid(); err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  op,
		}
	}
	d, err := s.FindDashboardByID(ctx, dashboardID)
	if err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  op,
		}
	}

	idx := -1
	for i, cell := range d.Cells {
		if cell.ID == cellID {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil, &platform.Error{
			Msg:  platform.ErrCellNotFound,
			Op:   op,
			Code: platform.ENotFound,
		}
	}

	if err := upd.Apply(d.Cells[idx]); err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  op,
		}
	}

	cell := d.Cells[idx]

	if err := s.PutDashboardWithMeta(ctx, d); err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  op,
		}
	}

	return cell, nil
}

// ReplaceDashboardCells replaces many dashboard cells.
func (s *Service) ReplaceDashboardCells(ctx context.Context, id platform.ID, cs []*platform.Cell) error {
	op := OpPrefix + platform.OpReplaceDashboardCells
	d, err := s.FindDashboardByID(ctx, id)
	if err != nil {
		return &platform.Error{
			Err: err,
			Op:  op,
		}
	}

	ids := map[string]*platform.Cell{}
	for _, cell := range d.Cells {
		ids[cell.ID.String()] = cell
	}

	for _, cell := range cs {
		if !cell.ID.Valid() {
			return &platform.Error{
				Code: platform.EInvalid,
				Op:   op,
				Msg:  "cannot provide empty cell id",
			}
		}

		if _, ok := ids[cell.ID.String()]; !ok {
			return &platform.Error{
				Code: platform.EConflict,
				Op:   op,
				Msg:  "cannot replace cells that were not already present",
			}
		}
	}

	d.Cells = cs

	return s.PutDashboardWithMeta(ctx, d)
}

// GetDashboardCellView retrieves the view for a dashboard cell.
func (s *Service) GetDashboardCellView(ctx context.Context, dashboardID, cellID platform.ID) (*platform.View, error) {
	v, err := s.FindViewByID(ctx, cellID)
	if err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  OpPrefix + platform.OpGetDashboardCellView,
		}
	}

	return v, nil
}

// UpdateDashboardCellView updates the view for a dashboard cell.
func (s *Service) UpdateDashboardCellView(ctx context.Context, dashboardID, cellID platform.ID, upd platform.ViewUpdate) (*platform.View, error) {
	op := OpPrefix + platform.OpUpdateDashboardCellView

	v, err := s.FindViewByID(ctx, cellID)
	if err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  op,
		}
	}

	if err := upd.Apply(v); err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  op,
		}
	}

	if err := s.PutView(ctx, v); err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  op,
		}
	}

	return v, nil
}
