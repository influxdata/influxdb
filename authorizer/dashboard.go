package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
)

var _ influxdb.DashboardService = (*DashboardService)(nil)

// DashboardService wraps a influxdb.DashboardService and authorizes actions
// against it appropriately.
type DashboardService struct {
	s influxdb.DashboardService
}

// NewDashboardService constructs an instance of an authorizing dashboard service.
func NewDashboardService(s influxdb.DashboardService) *DashboardService {
	return &DashboardService{
		s: s,
	}
}

// FindDashboardByID checks to see if the authorizer on context has read access to the id provided.
func (s *DashboardService) FindDashboardByID(ctx context.Context, id platform.ID) (*influxdb.Dashboard, error) {
	b, err := s.s.FindDashboardByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.DashboardsResourceType, id, b.OrganizationID); err != nil {
		return nil, err
	}
	return b, nil
}

// FindDashboards retrieves all dashboards that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *DashboardService) FindDashboards(ctx context.Context, filter influxdb.DashboardFilter, opt influxdb.FindOptions) ([]*influxdb.Dashboard, int, error) {
	// TODO: we'll likely want to push this operation into the database eventually since fetching the whole list of data
	// will likely be expensive.
	ds, _, err := s.s.FindDashboards(ctx, filter, opt)
	if err != nil {
		return nil, 0, err
	}
	return AuthorizeFindDashboards(ctx, ds)
}

// CreateDashboard checks to see if the authorizer on context has write access to the global dashboards resource.
func (s *DashboardService) CreateDashboard(ctx context.Context, b *influxdb.Dashboard) error {
	if _, _, err := AuthorizeCreate(ctx, influxdb.DashboardsResourceType, b.OrganizationID); err != nil {
		return err
	}
	return s.s.CreateDashboard(ctx, b)
}

// UpdateDashboard checks to see if the authorizer on context has write access to the dashboard provided.
func (s *DashboardService) UpdateDashboard(ctx context.Context, id platform.ID, upd influxdb.DashboardUpdate) (*influxdb.Dashboard, error) {
	b, err := s.s.FindDashboardByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.DashboardsResourceType, id, b.OrganizationID); err != nil {
		return nil, err
	}
	return s.s.UpdateDashboard(ctx, id, upd)
}

// DeleteDashboard checks to see if the authorizer on context has write access to the dashboard provided.
func (s *DashboardService) DeleteDashboard(ctx context.Context, id platform.ID) error {
	b, err := s.s.FindDashboardByID(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.DashboardsResourceType, id, b.OrganizationID); err != nil {
		return err
	}
	return s.s.DeleteDashboard(ctx, id)
}

func (s *DashboardService) AddDashboardCell(ctx context.Context, id platform.ID, c *influxdb.Cell, opts influxdb.AddDashboardCellOptions) error {
	b, err := s.s.FindDashboardByID(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.DashboardsResourceType, id, b.OrganizationID); err != nil {
		return err
	}
	return s.s.AddDashboardCell(ctx, id, c, opts)
}

func (s *DashboardService) RemoveDashboardCell(ctx context.Context, dashboardID platform.ID, cellID platform.ID) error {
	b, err := s.s.FindDashboardByID(ctx, dashboardID)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.DashboardsResourceType, dashboardID, b.OrganizationID); err != nil {
		return err
	}
	return s.s.RemoveDashboardCell(ctx, dashboardID, cellID)
}

func (s *DashboardService) UpdateDashboardCell(ctx context.Context, dashboardID platform.ID, cellID platform.ID, upd influxdb.CellUpdate) (*influxdb.Cell, error) {
	b, err := s.s.FindDashboardByID(ctx, dashboardID)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.DashboardsResourceType, dashboardID, b.OrganizationID); err != nil {
		return nil, err
	}
	return s.s.UpdateDashboardCell(ctx, dashboardID, cellID, upd)
}

func (s *DashboardService) GetDashboardCellView(ctx context.Context, dashboardID platform.ID, cellID platform.ID) (*influxdb.View, error) {
	b, err := s.s.FindDashboardByID(ctx, dashboardID)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.DashboardsResourceType, dashboardID, b.OrganizationID); err != nil {
		return nil, err
	}
	return s.s.GetDashboardCellView(ctx, dashboardID, cellID)
}

func (s *DashboardService) UpdateDashboardCellView(ctx context.Context, dashboardID platform.ID, cellID platform.ID, upd influxdb.ViewUpdate) (*influxdb.View, error) {
	b, err := s.s.FindDashboardByID(ctx, dashboardID)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.DashboardsResourceType, dashboardID, b.OrganizationID); err != nil {
		return nil, err
	}
	return s.s.UpdateDashboardCellView(ctx, dashboardID, cellID, upd)
}

func (s *DashboardService) ReplaceDashboardCells(ctx context.Context, id platform.ID, c []*influxdb.Cell) error {
	b, err := s.s.FindDashboardByID(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.DashboardsResourceType, id, b.OrganizationID); err != nil {
		return err
	}
	return s.s.ReplaceDashboardCells(ctx, id, c)
}
