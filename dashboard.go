package influxdb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"time"
)

// ErrDashboardNotFound is the error msg for a missing dashboard.
const ErrDashboardNotFound = "dashboard not found"

// ErrCellNotFound is the error msg for a missing cell.
const ErrCellNotFound = "cell not found"

// ErrViewNotFound is the error msg for a missing View.
const ErrViewNotFound = "view not found"

// ops for dashboard service.
const (
	OpFindDashboardByID       = "FindDashboardByID"
	OpFindDashboards          = "FindDashboards"
	OpCreateDashboard         = "CreateDashboard"
	OpUpdateDashboard         = "UpdateDashboard"
	OpAddDashboardCell        = "AddDashboardCell"
	OpRemoveDashboardCell     = "RemoveDashboardCell"
	OpUpdateDashboardCell     = "UpdateDashboardCell"
	OpGetDashboardCellView    = "GetDashboardCellView"
	OpUpdateDashboardCellView = "UpdateDashboardCellView"
	OpDeleteDashboard         = "DeleteDashboard"
	OpReplaceDashboardCells   = "ReplaceDashboardCells"
)

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

	// RemoveDashboardCell removes a dashboard.
	RemoveDashboardCell(ctx context.Context, dashboardID, cellID ID) error

	// UpdateDashboardCell replaces the dashboard cell with the provided ID.
	UpdateDashboardCell(ctx context.Context, dashboardID, cellID ID, upd CellUpdate) (*Cell, error)

	// GetDashboardCellView retrieves a dashboard cells view.
	GetDashboardCellView(ctx context.Context, dashboardID, cellID ID) (*View, error)

	// UpdateDashboardCellView retrieves a dashboard cells view.
	UpdateDashboardCellView(ctx context.Context, dashboardID, cellID ID, upd ViewUpdate) (*View, error)

	// DeleteDashboard removes a dashboard by ID.
	DeleteDashboard(ctx context.Context, id ID) error

	// ReplaceDashboardCells replaces all cells in a dashboard
	ReplaceDashboardCells(ctx context.Context, id ID, c []*Cell) error
}

// Dashboard represents all visual and query data for a dashboard.
type Dashboard struct {
	ID             ID            `json:"id,omitempty"`
	OrganizationID ID            `json:"orgID,omitempty"`
	Name           string        `json:"name"`
	Description    string        `json:"description"`
	Cells          []*Cell       `json:"cells"`
	Meta           DashboardMeta `json:"meta"`
}

// DashboardMeta contains meta information about dashboards
type DashboardMeta struct {
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// DefaultDashboardFindOptions are the default find options for dashboards
var DefaultDashboardFindOptions = FindOptions{
	SortBy: "ID",
}

// SortDashboards sorts a slice of dashboards by a field.
func SortDashboards(opts FindOptions, ds []*Dashboard) {
	var sorter func(i, j int) bool
	switch opts.SortBy {
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
			if opts.Descending {
				return ds[i].ID > ds[j].ID
			}
			return ds[i].ID < ds[j].ID
		}
	}

	sort.Slice(ds, sorter)
}

// Cell holds positional information about a cell on dashboard and a reference to a cell.
type Cell struct {
	ID ID `json:"id,omitempty"`
	CellProperty
	View *View `json:"-"`
}

// Marshals the cell
func (c *Cell) MarshalJSON() ([]byte, error) {
	type resp struct {
		ID             *ID             `json:"id,omitempty"`
		Name           string          `json:"name,omitempty"`
		ViewProperties json.RawMessage `json:"properties,omitempty"`
		CellProperty
	}
	response := resp{
		CellProperty: c.CellProperty,
	}
	if c.ID != 0 {
		response.ID = &c.ID
	}
	if c.View != nil {
		response.Name = c.View.Name
		rawJSON, err := MarshalViewPropertiesJSON(c.View.Properties)
		if err != nil {
			return nil, err
		}
		response.ViewProperties = rawJSON
	}
	return json.Marshal(response)
}

func (c *Cell) UnmarshalJSON(b []byte) error {
	var newCell struct {
		ID             ID              `json:"id,omitempty"`
		Name           string          `json:"name,omitempty"`
		ViewProperties json.RawMessage `json:"properties,omitempty"`
		CellProperty
	}
	if err := json.Unmarshal(b, &newCell); err != nil {
		return err
	}

	c.ID = newCell.ID
	c.CellProperty = newCell.CellProperty

	if newCell.Name != "" {
		if c.View == nil {
			c.View = new(View)
		}
		c.View.Name = newCell.Name
	}

	props, err := UnmarshalViewPropertiesJSON(newCell.ViewProperties)
	if err == nil {
		if c.View == nil {
			c.View = new(View)
		}
		c.View.Properties = props
	}

	return nil
}

// CellProperty contains the properties of a cell.
type CellProperty struct {
	X int32 `json:"x"`
	Y int32 `json:"y"`
	W int32 `json:"w"`
	H int32 `json:"h"`
}

// DashboardFilter is a filter for dashboards.
type DashboardFilter struct {
	IDs            []*ID
	OrganizationID *ID
	Organization   *string
}

// QueryParams turns a dashboard filter into query params
//
// It implements PagingFilter.
func (f DashboardFilter) QueryParams() map[string][]string {
	qp := url.Values{}
	for _, id := range f.IDs {
		if id != nil {
			qp.Add("id", id.String())
		}
	}

	if f.OrganizationID != nil {
		qp.Add("orgID", f.OrganizationID.String())
	}

	if f.Organization != nil {
		qp.Add("org", *f.Organization)
	}

	return qp
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
func (u DashboardUpdate) Valid() *Error {
	if u.Name == nil && u.Description == nil {
		return &Error{
			Code: EInvalid,
			Msg:  "must update at least one attribute",
		}
	}

	return nil
}

// AddDashboardCellOptions are options for adding a dashboard.
type AddDashboardCellOptions struct {
	View *View
}

// CellUpdate is the patch structure for a cell.
type CellUpdate struct {
	X *int32 `json:"x"`
	Y *int32 `json:"y"`
	W *int32 `json:"w"`
	H *int32 `json:"h"`
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

	return nil
}

// Valid returns an error if the cell update is invalid.
func (u CellUpdate) Valid() *Error {
	if u.H == nil && u.W == nil && u.Y == nil && u.X == nil {
		return &Error{
			Code: EInvalid,
			Msg:  "must update at least one attribute",
		}
	}

	return nil
}

// ViewUpdate is a struct for updating Views.
type ViewUpdate struct {
	ViewContentsUpdate
	Properties ViewProperties
}

// Valid validates the update struct. It expects minimal values to be set.
func (u ViewUpdate) Valid() *Error {
	_, ok := u.Properties.(EmptyViewProperties)
	if u.Name == nil && ok {
		return &Error{
			Code: EInvalid,
			Msg:  "expected at least one attribute to be updated",
		}
	}

	return nil
}

// Apply updates a view with the view updates properties.
func (u ViewUpdate) Apply(v *View) error {
	if err := u.Valid(); err != nil {
		return err
	}

	if u.Name != nil {
		v.Name = *u.Name
	}

	if u.Properties != nil {
		v.Properties = u.Properties
	}

	return nil
}

// ViewContentsUpdate is a struct for updating the non properties content of a View.
type ViewContentsUpdate struct {
	Name *string `json:"name"`
}

// ViewFilter represents a set of filter that restrict the returned results.
type ViewFilter struct {
	ID    *ID
	Types []string
}

// View holds positional and visual information for a View.
type View struct {
	ViewContents
	Properties ViewProperties
}

// ViewContents is the id and name of a specific view.
type ViewContents struct {
	ID   ID     `json:"id,omitempty"`
	Name string `json:"name"`
}

// Values for all supported view property types.
const (
	ViewPropertyTypeCheck              = "check"
	ViewPropertyTypeGauge              = "gauge"
	ViewPropertyTypeHeatMap            = "heatmap"
	ViewPropertyTypeHistogram          = "histogram"
	ViewPropertyTypeLogViewer          = "log-viewer"
	ViewPropertyTypeMarkdown           = "markdown"
	ViewPropertyTypeScatter            = "scatter"
	ViewPropertyTypeSingleStat         = "single-stat"
	ViewPropertyTypeSingleStatPlusLine = "line-plus-single-stat"
	ViewPropertyTypeTable              = "table"
	ViewPropertyTypeXY                 = "xy"
)

// ViewProperties is used to mark other structures as conforming to a View.
type ViewProperties interface {
	viewProperties()
	GetType() string
}

// EmptyViewProperties is visualization that has no values
type EmptyViewProperties struct{}

func (v EmptyViewProperties) viewProperties() {}

func (v EmptyViewProperties) GetType() string { return "" }

// UnmarshalViewPropertiesJSON unmarshals JSON bytes into a ViewProperties.
func UnmarshalViewPropertiesJSON(b []byte) (ViewProperties, error) {
	var v struct {
		B json.RawMessage `json:"properties"`
	}

	if err := json.Unmarshal(b, &v); err != nil {
		return nil, err
	}

	if len(v.B) == 0 {
		// Then there wasn't any visualization field, so there's no need unmarshal it
		return EmptyViewProperties{}, nil
	}

	var t struct {
		Shape string `json:"shape"`
		Type  string `json:"type"`
	}

	if err := json.Unmarshal(v.B, &t); err != nil {
		return nil, err
	}

	var vis ViewProperties
	switch t.Shape {
	case "chronograf-v2":
		switch t.Type {
		case ViewPropertyTypeCheck:
			var cv CheckViewProperties
			if err := json.Unmarshal(v.B, &cv); err != nil {
				return nil, err
			}
			vis = cv
		case ViewPropertyTypeXY:
			var xyv XYViewProperties
			if err := json.Unmarshal(v.B, &xyv); err != nil {
				return nil, err
			}
			vis = xyv
		case ViewPropertyTypeSingleStat:
			var ssv SingleStatViewProperties
			if err := json.Unmarshal(v.B, &ssv); err != nil {
				return nil, err
			}
			vis = ssv
		case ViewPropertyTypeGauge:
			var gv GaugeViewProperties
			if err := json.Unmarshal(v.B, &gv); err != nil {
				return nil, err
			}
			vis = gv
		case ViewPropertyTypeTable:
			var tv TableViewProperties
			if err := json.Unmarshal(v.B, &tv); err != nil {
				return nil, err
			}
			vis = tv
		case ViewPropertyTypeMarkdown:
			var mv MarkdownViewProperties
			if err := json.Unmarshal(v.B, &mv); err != nil {
				return nil, err
			}
			vis = mv
		case ViewPropertyTypeLogViewer: // happens in log viewer stays in log viewer.
			var lv LogViewProperties
			if err := json.Unmarshal(v.B, &lv); err != nil {
				return nil, err
			}
			vis = lv
		case ViewPropertyTypeSingleStatPlusLine:
			var lv LinePlusSingleStatProperties
			if err := json.Unmarshal(v.B, &lv); err != nil {
				return nil, err
			}
			vis = lv
		case ViewPropertyTypeHistogram:
			var hv HistogramViewProperties
			if err := json.Unmarshal(v.B, &hv); err != nil {
				return nil, err
			}
			vis = hv
		case ViewPropertyTypeHeatMap:
			var hv HeatmapViewProperties
			if err := json.Unmarshal(v.B, &hv); err != nil {
				return nil, err
			}
			vis = hv
		case ViewPropertyTypeScatter:
			var sv ScatterViewProperties
			if err := json.Unmarshal(v.B, &sv); err != nil {
				return nil, err
			}
			vis = sv
		}
	case "empty":
		var ev EmptyViewProperties
		if err := json.Unmarshal(v.B, &ev); err != nil {
			return nil, err
		}
		vis = ev
	default:
		return nil, fmt.Errorf("unknown shape %v", t.Shape)
	}

	return vis, nil
}

// MarshalViewPropertiesJSON encodes a view into JSON bytes.
func MarshalViewPropertiesJSON(v ViewProperties) ([]byte, error) {
	var s interface{}
	switch vis := v.(type) {
	case SingleStatViewProperties:
		s = struct {
			Shape string `json:"shape"`
			SingleStatViewProperties
		}{
			Shape: "chronograf-v2",

			SingleStatViewProperties: vis,
		}
	case TableViewProperties:
		s = struct {
			Shape string `json:"shape"`
			TableViewProperties
		}{
			Shape: "chronograf-v2",

			TableViewProperties: vis,
		}
	case GaugeViewProperties:
		s = struct {
			Shape string `json:"shape"`
			GaugeViewProperties
		}{
			Shape: "chronograf-v2",

			GaugeViewProperties: vis,
		}
	case XYViewProperties:
		s = struct {
			Shape string `json:"shape"`
			XYViewProperties
		}{
			Shape: "chronograf-v2",

			XYViewProperties: vis,
		}
	case LinePlusSingleStatProperties:
		s = struct {
			Shape string `json:"shape"`
			LinePlusSingleStatProperties
		}{
			Shape: "chronograf-v2",

			LinePlusSingleStatProperties: vis,
		}
	case HistogramViewProperties:
		s = struct {
			Shape string `json:"shape"`
			HistogramViewProperties
		}{
			Shape: "chronograf-v2",

			HistogramViewProperties: vis,
		}
	case HeatmapViewProperties:
		s = struct {
			Shape string `json:"shape"`
			HeatmapViewProperties
		}{
			Shape: "chronograf-v2",

			HeatmapViewProperties: vis,
		}
	case ScatterViewProperties:
		s = struct {
			Shape string `json:"shape"`
			ScatterViewProperties
		}{
			Shape: "chronograf-v2",

			ScatterViewProperties: vis,
		}
	case MarkdownViewProperties:
		s = struct {
			Shape string `json:"shape"`
			MarkdownViewProperties
		}{
			Shape: "chronograf-v2",

			MarkdownViewProperties: vis,
		}
	case LogViewProperties:
		s = struct {
			Shape string `json:"shape"`
			LogViewProperties
		}{
			Shape:             "chronograf-v2",
			LogViewProperties: vis,
		}
	case CheckViewProperties:
		s = struct {
			Shape string `json:"shape"`
			CheckViewProperties
		}{
			Shape: "chronograf-v2",

			CheckViewProperties: vis,
		}
	default:
		s = struct {
			Shape string `json:"shape"`
			EmptyViewProperties
		}{
			Shape:               "empty",
			EmptyViewProperties: EmptyViewProperties{},
		}
	}
	return json.Marshal(s)
}

// MarshalJSON encodes a view to JSON bytes.
func (v View) MarshalJSON() ([]byte, error) {
	viewProperties, err := MarshalViewPropertiesJSON(v.Properties)
	if err != nil {
		return nil, err
	}

	return json.Marshal(struct {
		ViewContents
		ViewProperties json.RawMessage `json:"properties"`
	}{
		ViewContents:   v.ViewContents,
		ViewProperties: viewProperties,
	})
}

// UnmarshalJSON decodes JSON bytes into the corresponding view type (those that implement ViewProperties).
func (c *View) UnmarshalJSON(b []byte) error {
	if err := json.Unmarshal(b, &c.ViewContents); err != nil {
		return err
	}

	v, err := UnmarshalViewPropertiesJSON(b)
	if err != nil {
		return err
	}
	c.Properties = v
	return nil
}

// UnmarshalJSON decodes JSON bytes into the corresponding view update type (those that implement ViewProperties).
func (u *ViewUpdate) UnmarshalJSON(b []byte) error {
	if err := json.Unmarshal(b, &u.ViewContentsUpdate); err != nil {
		return err
	}

	v, err := UnmarshalViewPropertiesJSON(b)
	if err != nil {
		return err
	}
	u.Properties = v
	return nil
}

// MarshalJSON encodes a view to JSON bytes.
func (u ViewUpdate) MarshalJSON() ([]byte, error) {
	vis, err := MarshalViewPropertiesJSON(u.Properties)
	if err != nil {
		return nil, err
	}

	return json.Marshal(struct {
		ViewContentsUpdate
		ViewProperties json.RawMessage `json:"properties,omitempty"`
	}{
		ViewContentsUpdate: u.ViewContentsUpdate,
		ViewProperties:     vis,
	})
}

// LinePlusSingleStatProperties represents options for line plus single stat view in Chronograf
type LinePlusSingleStatProperties struct {
	Queries           []DashboardQuery `json:"queries"`
	Axes              map[string]Axis  `json:"axes"`
	Type              string           `json:"type"`
	Legend            Legend           `json:"legend"`
	ViewColors        []ViewColor      `json:"colors"`
	Prefix            string           `json:"prefix"`
	Suffix            string           `json:"suffix"`
	DecimalPlaces     DecimalPlaces    `json:"decimalPlaces"`
	Note              string           `json:"note"`
	ShowNoteWhenEmpty bool             `json:"showNoteWhenEmpty"`
	XColumn           string           `json:"xColumn"`
	YColumn           string           `json:"yColumn"`
	ShadeBelow        bool             `json:"shadeBelow"`
	Position          string           `json:"position"`
}

// XYViewProperties represents options for line, bar, step, or stacked view in Chronograf
type XYViewProperties struct {
	Queries           []DashboardQuery `json:"queries"`
	Axes              map[string]Axis  `json:"axes"`
	Type              string           `json:"type"`
	Legend            Legend           `json:"legend"`
	Geom              string           `json:"geom"` // Either "line", "step", "stacked", or "bar"
	ViewColors        []ViewColor      `json:"colors"`
	Note              string           `json:"note"`
	ShowNoteWhenEmpty bool             `json:"showNoteWhenEmpty"`
	XColumn           string           `json:"xColumn"`
	YColumn           string           `json:"yColumn"`
	ShadeBelow        bool             `json:"shadeBelow"`
	Position          string           `json:"position"`
	TimeFormat        string           `json:"timeFormat"`
}

// CheckViewProperties represents options for a view representing a check
type CheckViewProperties struct {
	Type       string           `json:"type"`
	CheckID    string           `json:"checkID"`
	Queries    []DashboardQuery `json:"queries"`
	ViewColors []string         `json:"colors"`
}

// SingleStatViewProperties represents options for single stat view in Chronograf
type SingleStatViewProperties struct {
	Type              string           `json:"type"`
	Queries           []DashboardQuery `json:"queries"`
	Prefix            string           `json:"prefix"`
	Suffix            string           `json:"suffix"`
	ViewColors        []ViewColor      `json:"colors"`
	DecimalPlaces     DecimalPlaces    `json:"decimalPlaces"`
	Note              string           `json:"note"`
	ShowNoteWhenEmpty bool             `json:"showNoteWhenEmpty"`
}

// HistogramViewProperties represents options for histogram view in Chronograf
type HistogramViewProperties struct {
	Type              string           `json:"type"`
	Queries           []DashboardQuery `json:"queries"`
	ViewColors        []ViewColor      `json:"colors"`
	XColumn           string           `json:"xColumn"`
	FillColumns       []string         `json:"fillColumns"`
	XDomain           []float64        `json:"xDomain,omitempty"`
	XAxisLabel        string           `json:"xAxisLabel"`
	Position          string           `json:"position"`
	BinCount          int              `json:"binCount"`
	Note              string           `json:"note"`
	ShowNoteWhenEmpty bool             `json:"showNoteWhenEmpty"`
}

// HeatmapViewProperties represents options for heatmap view in Chronograf
type HeatmapViewProperties struct {
	Type              string           `json:"type"`
	Queries           []DashboardQuery `json:"queries"`
	ViewColors        []string         `json:"colors"`
	BinSize           int32            `json:"binSize"`
	XColumn           string           `json:"xColumn"`
	YColumn           string           `json:"yColumn"`
	XDomain           []float64        `json:"xDomain,omitempty"`
	YDomain           []float64        `json:"yDomain,omitempty"`
	XAxisLabel        string           `json:"xAxisLabel"`
	YAxisLabel        string           `json:"yAxisLabel"`
	XPrefix           string           `json:"xPrefix"`
	XSuffix           string           `json:"xSuffix"`
	YPrefix           string           `json:"yPrefix"`
	YSuffix           string           `json:"ySuffix"`
	Note              string           `json:"note"`
	ShowNoteWhenEmpty bool             `json:"showNoteWhenEmpty"`
	TimeFormat        string           `json:"timeFormat"`
}

// ScatterViewProperties represents options for scatter view in Chronograf
type ScatterViewProperties struct {
	Type              string           `json:"type"`
	Queries           []DashboardQuery `json:"queries"`
	ViewColors        []string         `json:"colors"`
	FillColumns       []string         `json:"fillColumns"`
	SymbolColumns     []string         `json:"symbolColumns"`
	XColumn           string           `json:"xColumn"`
	YColumn           string           `json:"yColumn"`
	XDomain           []float64        `json:"xDomain,omitempty"`
	YDomain           []float64        `json:"yDomain,omitempty"`
	XAxisLabel        string           `json:"xAxisLabel"`
	YAxisLabel        string           `json:"yAxisLabel"`
	XPrefix           string           `json:"xPrefix"`
	XSuffix           string           `json:"xSuffix"`
	YPrefix           string           `json:"yPrefix"`
	YSuffix           string           `json:"ySuffix"`
	Note              string           `json:"note"`
	ShowNoteWhenEmpty bool             `json:"showNoteWhenEmpty"`
	TimeFormat        string           `json:"timeFormat"`
}

// GaugeViewProperties represents options for gauge view in Chronograf
type GaugeViewProperties struct {
	Type              string           `json:"type"`
	Queries           []DashboardQuery `json:"queries"`
	Prefix            string           `json:"prefix"`
	TickPrefix        string           `json:"tickPrefix"`
	Suffix            string           `json:"suffix"`
	TickSuffix        string           `json:"tickSuffix"`
	ViewColors        []ViewColor      `json:"colors"`
	DecimalPlaces     DecimalPlaces    `json:"decimalPlaces"`
	Note              string           `json:"note"`
	ShowNoteWhenEmpty bool             `json:"showNoteWhenEmpty"`
}

// TableViewProperties represents options for table view in Chronograf
type TableViewProperties struct {
	Type              string           `json:"type"`
	Queries           []DashboardQuery `json:"queries"`
	ViewColors        []ViewColor      `json:"colors"`
	TableOptions      TableOptions     `json:"tableOptions"`
	FieldOptions      []RenamableField `json:"fieldOptions"`
	TimeFormat        string           `json:"timeFormat"`
	DecimalPlaces     DecimalPlaces    `json:"decimalPlaces"`
	Note              string           `json:"note"`
	ShowNoteWhenEmpty bool             `json:"showNoteWhenEmpty"`
}

type MarkdownViewProperties struct {
	Type string `json:"type"`
	Note string `json:"note"`
}

// LogViewProperties represents options for log viewer in Chronograf.
type LogViewProperties struct {
	Type    string            `json:"type"`
	Columns []LogViewerColumn `json:"columns"`
}

// LogViewerColumn represents a specific column in a Log Viewer.
type LogViewerColumn struct {
	Name     string             `json:"name"`
	Position int32              `json:"position"`
	Settings []LogColumnSetting `json:"settings"`
}

// LogColumnSetting represent the settings for a specific column of a Log Viewer.
type LogColumnSetting struct {
	Type  string `json:"type"`
	Value string `json:"value"`
	Name  string `json:"name,omitempty"`
}

func (XYViewProperties) viewProperties()             {}
func (LinePlusSingleStatProperties) viewProperties() {}
func (SingleStatViewProperties) viewProperties()     {}
func (HistogramViewProperties) viewProperties()      {}
func (HeatmapViewProperties) viewProperties()        {}
func (ScatterViewProperties) viewProperties()        {}
func (GaugeViewProperties) viewProperties()          {}
func (TableViewProperties) viewProperties()          {}
func (MarkdownViewProperties) viewProperties()       {}
func (LogViewProperties) viewProperties()            {}
func (CheckViewProperties) viewProperties()          {}

func (v XYViewProperties) GetType() string             { return v.Type }
func (v LinePlusSingleStatProperties) GetType() string { return v.Type }
func (v SingleStatViewProperties) GetType() string     { return v.Type }
func (v HistogramViewProperties) GetType() string      { return v.Type }
func (v HeatmapViewProperties) GetType() string        { return v.Type }
func (v ScatterViewProperties) GetType() string        { return v.Type }
func (v GaugeViewProperties) GetType() string          { return v.Type }
func (v TableViewProperties) GetType() string          { return v.Type }
func (v MarkdownViewProperties) GetType() string       { return v.Type }
func (v LogViewProperties) GetType() string            { return v.Type }
func (v CheckViewProperties) GetType() string          { return v.Type }

/////////////////////////////
// Old Chronograf Types
/////////////////////////////

// DashboardQuery represents a query used in a dashboard cell
type DashboardQuery struct {
	Text          string        `json:"text"`
	EditMode      string        `json:"editMode"` // Either "builder" or "advanced"
	Name          string        `json:"name"`     // Term or phrase that refers to the query
	BuilderConfig BuilderConfig `json:"builderConfig"`
}

type BuilderConfig struct {
	Buckets []string `json:"buckets"`
	Tags    []struct {
		Key    string   `json:"key"`
		Values []string `json:"values"`
	} `json:"tags"`
	Functions []struct {
		Name string `json:"name"`
	} `json:"functions"`
	AggregateWindow struct {
		Period string `json:"period"`
	} `json:"aggregateWindow"`
}

// MarshalJSON is necessary for the time being. UI keeps breaking
// b/c it relies on these slices being populated/not nil. Other
// consumers may have same issue.
func (b BuilderConfig) MarshalJSON() ([]byte, error) {
	type alias BuilderConfig
	copyCfg := alias(b)
	if copyCfg.Buckets == nil {
		copyCfg.Buckets = []string{}
	}
	if copyCfg.Tags == nil {
		copyCfg.Tags = []struct {
			Key    string   `json:"key"`
			Values []string `json:"values"`
		}{}
	}
	if copyCfg.Functions == nil {
		copyCfg.Functions = []struct {
			Name string `json:"name"`
		}{}
	}
	return json.Marshal(copyCfg)
}

// NewBuilderTag is a constructor for the builder config types. This
// isn't technically required, but working with struct literals with embedded
// struct tags is really painful. This is to get around that bit. Would be nicer
// to have these as actual types maybe.
func NewBuilderTag(key string, values ...string) struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
} {
	return struct {
		Key    string   `json:"key"`
		Values []string `json:"values"`
	}{
		Key:    key,
		Values: values,
	}
}

// Axis represents the visible extents of a visualization
type Axis struct {
	Bounds       []string `json:"bounds"` // bounds are an arbitrary list of client-defined strings that specify the viewport for a View
	LegacyBounds [2]int64 `json:"-"`      // legacy bounds are for testing a migration from an earlier version of axis
	Label        string   `json:"label"`  // label is a description of this Axis
	Prefix       string   `json:"prefix"` // Prefix represents a label prefix for formatting axis values
	Suffix       string   `json:"suffix"` // Suffix represents a label suffix for formatting axis values
	Base         string   `json:"base"`   // Base represents the radix for formatting axis values
	Scale        string   `json:"scale"`  // Scale is the axis formatting scale. Supported: "log", "linear"
}

// ViewColor represents the encoding of data into visualizations
type ViewColor struct {
	ID    string  `json:"id"`    // ID is the unique id of the View color
	Type  string  `json:"type"`  // Type is how the color is used. Accepted (min,max,threshold)
	Hex   string  `json:"hex"`   // Hex is the hex number of the color
	Name  string  `json:"name"`  // Name is the user-facing name of the hex color
	Value float64 `json:"value"` // Value is the data value mapped to this color
}

// Legend represents the encoding of data into a legend
type Legend struct {
	Type        string `json:"type,omitempty"`
	Orientation string `json:"orientation,omitempty"`
}

// TableOptions is a type of options for a DashboardView with type Table
type TableOptions struct {
	VerticalTimeAxis bool           `json:"verticalTimeAxis"`
	SortBy           RenamableField `json:"sortBy"`
	Wrapping         string         `json:"wrapping"`
	FixFirstColumn   bool           `json:"fixFirstColumn"`
}

// RenamableField is a column/row field in a DashboardView of type Table
type RenamableField struct {
	InternalName string `json:"internalName"`
	DisplayName  string `json:"displayName"`
	Visible      bool   `json:"visible"`
}

// DecimalPlaces indicates whether decimal places should be enforced, and how many digits it should show.
type DecimalPlaces struct {
	IsEnforced bool  `json:"isEnforced"`
	Digits     int32 `json:"digits"`
}
