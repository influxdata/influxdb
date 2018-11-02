package platform

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// ErrDashboardNotFound is the error for a missing dashboard.
const ErrDashboardNotFound = ChronografError("dashboard not found")

// ErrCellNotFound is the error for a missing cell.
const ErrCellNotFound = ChronografError("cell not found")

// DashboardService represents a service for managing dashboard data.
type DashboardService interface {
	// FindDashboardByID returns a single dashboard by ID.
	FindDashboardByID(ctx context.Context, id ID) (*Dashboard, error)

	// FindDashboards returns a list of dashboards that match filter and the total count of matching dashboards.
	// Additional options provide pagination & sorting.
	FindDashboards(ctx context.Context, filter DashboardFilter, opts FindOptions) ([]*Dashboard, int, error)

	// CreateDashboard creates a new dashboard and sets b.ID with the new identifier.
	CreateDashboard(ctx context.Context, b *Dashboard) error

	// UpdateDashboard updates a single dashboard with changeset.
	// Returns the new dashboard state after update.
	UpdateDashboard(ctx context.Context, id ID, upd DashboardUpdate) (*Dashboard, error)

	// AddDashboardCell adds a cell to a dashboard.
	AddDashboardCell(ctx context.Context, id ID, c *Cell, opts AddDashboardCellOptions) error

	// RemoveDashboardCell removes a dashbaord.
	RemoveDashboardCell(ctx context.Context, dashboardID, cellID ID) error

	// UpdateDashboardCell replaces the dashboard cell with the provided ID.
	UpdateDashboardCell(ctx context.Context, dashboardID, cellID ID, upd CellUpdate) (*Cell, error)

	// DeleteDashboard removes a dashboard by ID.
	DeleteDashboard(ctx context.Context, id ID) error

	// ReplaceDashboardCells replaces all cells in a dashboard
	ReplaceDashboardCells(ctx context.Context, id ID, c []*Cell) error
}

// Dashboard represents all visual and query data for a dashboard.
type Dashboard struct {
	ID          ID            `json:"id,omitempty"`
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Cells       []*Cell       `json:"cells"`
	Meta        DashboardMeta `json:"meta"`
}

// Dashboard meta contains meta information about dashboards
type DashboardMeta struct {
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// DefaultDashboardFindOptions are the default find options for dashboards
var DefaultDashboardFindOptions = FindOptions{
	SortBy: "ID",
}

// SortDashboards sorts a slice of dashboards by a field.
func SortDashboards(by string, ds []*Dashboard) {
	var sorter func(i, j int) bool
	switch by {
	case "CreatedAt":
		sorter = func(i, j int) bool {
			return ds[j].Meta.CreatedAt.After(ds[i].Meta.CreatedAt)
		}
	case "UpdatedAt":
		sorter = func(i, j int) bool {
			return ds[j].Meta.UpdatedAt.After(ds[i].Meta.UpdatedAt)
		}
	case "Name":
		sorter = func(i, j int) bool {
			return ds[i].Name < ds[j].Name
		}
	default:
		sorter = func(i, j int) bool {
			return ds[i].ID < ds[j].ID
		}
	}

	sort.Slice(ds, sorter)
}

// Cell holds positional information about a cell on dashboard and a reference to a cell.
type Cell struct {
	ID     ID    `json:"id,omitempty"`
	X      int32 `json:"x"`
	Y      int32 `json:"y"`
	W      int32 `json:"w"`
	H      int32 `json:"h"`
	ViewID ID    `json:"viewID"`
}

// DashboardFilter is a filter for dashboards.
type DashboardFilter struct {
	IDs []*ID
}

// DashboardUpdate is the patch structure for a dashboard.
type DashboardUpdate struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
}

// Apply applies an update to a dashboard.
func (u DashboardUpdate) Apply(d *Dashboard) error {
	if u.Name != nil {
		d.Name = *u.Name
	}

	if u.Description != nil {
		d.Description = *u.Description
	}

	return nil
}

// Valid returns an error if the dashboard update is invalid.
func (u DashboardUpdate) Valid() error {
	if u.Name == nil && u.Description == nil {
		return fmt.Errorf("must update at least one attribute")
	}

	return nil
}

// AddDashboardCellOptions are options for adding a dashboard.
type AddDashboardCellOptions struct {
	// UsingView specifies the view that should be used as a template
	// for the new cells view.
	UsingView ID
}

// CellUpdate is the patch structure for a cell.
type CellUpdate struct {
	X      *int32 `json:"x"`
	Y      *int32 `json:"y"`
	W      *int32 `json:"w"`
	H      *int32 `json:"h"`
	ViewID ID     `json:"viewID"`
}

// Apply applies an update to a Cell.
func (u CellUpdate) Apply(c *Cell) error {
	if u.X != nil {
		c.X = *u.X
	}

	if u.Y != nil {
		c.Y = *u.Y
	}

	if u.W != nil {
		c.W = *u.W
	}

	if u.H != nil {
		c.H = *u.H
	}

	if u.ViewID.Valid() {
		c.ViewID = u.ViewID
	}

	return nil
}

// Valid returns an error if the cell update is invalid.
func (u CellUpdate) Valid() error {
	if u.H == nil && u.W == nil && u.Y == nil && u.X == nil && !u.ViewID.Valid() {
		return fmt.Errorf("must update at least one attribute")
	}

	return nil
}
