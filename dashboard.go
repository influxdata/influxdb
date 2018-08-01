package platform

import (
	"context"
	"encoding/json"
	"fmt"
)

// DashboardService represents a service for managing dashboard data.
type DashboardService interface {
	// FindDashboardByID returns a single dashboard by ID.
	FindDashboardByID(ctx context.Context, id ID) (*Dashboard, error)

	// FindDashboardsByOrganizationID returns a list of dashboards by organization.
	FindDashboardsByOrganizationID(ctx context.Context, orgID ID) ([]*Dashboard, int, error)

	// FindDashboardsByOrganizationName returns a list of dashboards by organization.
	FindDashboardsByOrganizationName(ctx context.Context, org string) ([]*Dashboard, int, error)

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

	// AddDashboardCell adds a new cell to a dashboard.
	AddDashboardCell(ctx context.Context, dashboardID ID, cell *DashboardCell) error

	// ReplaceDashboardCell replaces a single dashboard cell. It expects ID to be set on the provided cell.
	ReplaceDashboardCell(ctx context.Context, dashboardID ID, cell *DashboardCell) error

	// RemoveDashboardCell removes a cell from a dashboard.
	RemoveDashboardCell(ctx context.Context, dashboardID, cellID ID) error
}

// DashboardFilter represents a set of filter that restrict the returned results.
type DashboardFilter struct {
	ID             *ID
	OrganizationID *ID
	Organization   *string
}

// DashboardUpdate represents updates to a dashboard.
// Only fields which are set are updated.
type DashboardUpdate struct {
	Name *string `json:"name,omitempty"`
}

// Dashboard represents all visual and query data for a dashboard
type Dashboard struct {
	// TODO: add meta information fields like created_at, updated_at, created_by, etc
	ID             ID              `json:"id"`
	OrganizationID ID              `json:"organizationID"`
	Organization   string          `json:"organization"`
	Name           string          `json:"name"`
	Cells          []DashboardCell `json:"cells"`
}

// DashboardCell holds positional and visual information for a cell.
type DashboardCell struct {
	DashboardCellContents
	Visualization Visualization
}

type DashboardCellContents struct {
	ID   ID     `json:"id"`
	Name string `json:"name"`

	X int32 `json:"x"`
	Y int32 `json:"y"`
	W int32 `json:"w"`
	H int32 `json:"h"`
}

type Visualization interface {
	Visualization()
}

func (c DashboardCell) MarshalJSON() ([]byte, error) {
	vis, err := MarshalVisualizationJSON(c.Visualization)
	if err != nil {
		return nil, err
	}

	return json.Marshal(struct {
		DashboardCellContents
		Visualization json.RawMessage `json:"visualization"`
	}{
		DashboardCellContents: c.DashboardCellContents,
		Visualization:         vis,
	})
}

func (c *DashboardCell) UnmarshalJSON(b []byte) error {
	if err := json.Unmarshal(b, &c.DashboardCellContents); err != nil {
		return err
	}

	v, err := UnmarshalVisualizationJSON(b)
	if err != nil {
		return err
	}
	c.Visualization = v
	return nil
}

// TODO: fill in with real visualization requirements
type CommonVisualization struct {
	Query string `json:"query"`
}

func (CommonVisualization) Visualization() {}

func UnmarshalVisualizationJSON(b []byte) (Visualization, error) {
	var v struct {
		B json.RawMessage `json:"visualization"`
	}

	if err := json.Unmarshal(b, &v); err != nil {
		return nil, err
	}

	var t struct {
		Type string `json:"type"`
	}

	if err := json.Unmarshal(v.B, &t); err != nil {
		return nil, err
	}

	var vis Visualization
	switch t.Type {
	case "common":
		var qv CommonVisualization
		if err := json.Unmarshal(v.B, &qv); err != nil {
			return nil, err
		}
		vis = qv
	default:
		return nil, fmt.Errorf("unknown type %v", t.Type)
	}

	return vis, nil
}

func MarshalVisualizationJSON(v Visualization) ([]byte, error) {
	var s interface{}
	switch vis := v.(type) {
	case CommonVisualization:
		s = struct {
			Type string `json:"type"`
			CommonVisualization
		}{
			Type:                "common",
			CommonVisualization: vis,
		}
	default:
		return nil, fmt.Errorf("unsupported type")
	}
	return json.Marshal(s)
}
