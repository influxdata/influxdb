package platform

import (
	"context"
	"fmt"
)

// ErrDashboardNotFound is the error for a missing dashboard.
const ErrDashboardNotFound = Error("dashboard not found")

// DashboardService represents a service for managing dashboard data.
type DashboardService interface {
	// FindDashboardByID returns a single dashboard by ID.
	FindDashboardByID(ctx context.Context, id ID) (*Dashboard, error)

	// FindDashboards returns a list of dashboards that match filter and the total count of matching dashboards.
	// Additional options provide pagination & sorting.
	FindDashboards(ctx context.Context, filter DashboardFilter) ([]*Dashboard, int, error)

	// CreateDashboard creates a new dashboard and sets b.ID with the new identifier.
	CreateDashboard(ctx context.Context, b *Dashboard) error

	// UpdateDashboard updates a single dashboard with changeset.
	// Returns the new dashboard state after update.
	UpdateDashboard(ctx context.Context, id ID, upd DashboardUpdate) (*Dashboard, error)

	// DeleteDashboard removes a dashboard by ID.
	DeleteDashboard(ctx context.Context, id ID) error
}

// Dashboard represents all visual and query data for a dashboard
type Dashboard struct {
	ID    ID              `json:"id"`
	Name  string          `json:"name"`
	Cells []DashboardCell `json:"cells"`
}

// DashboardCell holds positional information about a cell on dashboard and a reference to a cell.
type DashboardCell struct {
	X   int32  `json:"x"`
	Y   int32  `json:"y"`
	W   int32  `json:"w"`
	H   int32  `json:"h"`
	Ref string `json:"ref"`
}

// DashboardFilter is a filter for dashboards.
type DashboardFilter struct {
	// TODO(desa): change to be a slice of IDs
	ID *ID
}

// DashboardUpdate is the patch structure for a dashboard.
type DashboardUpdate struct {
	Name  *string         `json:"name"`
	Cells []DashboardCell `json:"cells"`
}

// Valid returns an error if the dashboard update is invalid.
func (u DashboardUpdate) Valid() error {
	if u.Name == nil && u.Cells == nil {
		return fmt.Errorf("must update at least one attribute")
	}

	return nil
}
