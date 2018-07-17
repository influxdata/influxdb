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
	// TODO(desa): put these in a separate API
	Templates []Template `json:"templates"`
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
	Name      *string         `json:"name"`
	Cells     []DashboardCell `json:"cells"`
	Templates []Template      `json:"templates"`
}

// Valid returns an error if the dashboard update is invalid.
func (u DashboardUpdate) Valid() error {
	if u.Name == nil && u.Cells == nil && u.Templates == nil {
		return fmt.Errorf("must update at least one attribute")
	}

	return nil
}

// TemplateValue is a value use to replace a template in an InfluxQL query.
type TemplateValue struct {
	Value    string `json:"value"`         // Value is the specific value used to replace a template in an InfluxQL query
	Type     string `json:"type"`          // Type can be tagKey, tagValue, fieldKey, csv, map, measurement, database, constant, influxql
	Selected bool   `json:"selected"`      // Selected states that this variable has been picked to use for replacement
	Key      string `json:"key,omitempty"` // Key is the key for the Value if the Template Type is 'map'
}

// TemplateVar is a named variable within an InfluxQL query to be replaced with Values.
type TemplateVar struct {
	Var    string          `json:"tempVar"` // Var is the string to replace within InfluxQL
	Values []TemplateValue `json:"values"`  // Values are the replacement values within InfluxQL
}

// TemplateQuery is used to retrieve choices for template replacement.
type TemplateQuery struct {
	Command     string `json:"influxql"`     // Command is the query itself
	DB          string `json:"db,omitempty"` // DB is optional and if empty will not be used.
	RP          string `json:"rp,omitempty"` // RP is a retention policy and optional; if empty will not be used.
	Measurement string `json:"measurement"`  // Measurement is the optionally selected measurement for the query
	TagKey      string `json:"tagKey"`       // TagKey is the optionally selected tag key for the query
	FieldKey    string `json:"fieldKey"`     // FieldKey is the optionally selected field key for the query
}

// Template represents a series of choices to replace TemplateVars within InfluxQL.
type Template struct {
	TemplateVar
	ID    ID             `json:"id"`              // ID is the unique ID associated with this template
	Type  string         `json:"type"`            // Type can be fieldKeys, tagKeys, tagValues, csv, constant, measurements, databases, map, influxql, text
	Label string         `json:"label"`           // Label is a user-facing description of the Template
	Query *TemplateQuery `json:"query,omitempty"` // Query is used to generate the choices for a template
}
