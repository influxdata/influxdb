package influxdb

import (
	"context"
	"encoding/json"
	"fmt"
)

// ErrViewNotFound is the error msg for a missing View.
const ErrViewNotFound = "view not found"

// ops for view.
const (
	OpFindViewByID = "FindViewByID"
	OpFindViews    = "FindViews"
	OpCreateView   = "CreateView"
	OpUpdateView   = "UpdateView"
	OpDeleteView   = "DeleteView"
)

// NOTE: This service has been DEPRECATED and should be removed. Views are now
// resources that are nested beneath dashboards.
//
// ViewService represents a service for managing View data.
type ViewService interface {
	// FindViewByID returns a single View by ID.
	FindViewByID(ctx context.Context, id ID) (*View, error)

	// FindViews returns a list of Views that match filter and the total count of matching Views.
	// Additional options provide pagination & sorting.
	FindViews(ctx context.Context, filter ViewFilter) ([]*View, int, error)

	// CreateView creates a new View and sets b.ID with the new identifier.
	CreateView(ctx context.Context, b *View) error

	// UpdateView updates a single View with changeset.
	// Returns the new View state after update.
	UpdateView(ctx context.Context, id ID, upd ViewUpdate) (*View, error)

	// DeleteView removes a View by ID.
	DeleteView(ctx context.Context, id ID) error
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
		case "xy":
			var xyv XYViewProperties
			if err := json.Unmarshal(v.B, &xyv); err != nil {
				return nil, err
			}
			vis = xyv
		case "single-stat":
			var ssv SingleStatViewProperties
			if err := json.Unmarshal(v.B, &ssv); err != nil {
				return nil, err
			}
			vis = ssv
		case "gauge":
			var gv GaugeViewProperties
			if err := json.Unmarshal(v.B, &gv); err != nil {
				return nil, err
			}
			vis = gv
		case "table":
			var tv TableViewProperties
			if err := json.Unmarshal(v.B, &tv); err != nil {
				return nil, err
			}
			vis = tv
		case "markdown":
			var mv MarkdownViewProperties
			if err := json.Unmarshal(v.B, &mv); err != nil {
				return nil, err
			}
			vis = mv
		case "log-viewer": // happens in log viewer stays in log viewer.
			var lv LogViewProperties
			if err := json.Unmarshal(v.B, &lv); err != nil {
				return nil, err
			}
			vis = lv
		case "line-plus-single-stat":
			var lv LinePlusSingleStatProperties
			if err := json.Unmarshal(v.B, &lv); err != nil {
				return nil, err
			}
			vis = lv
		}
	case "empty":
		var ev EmptyViewProperties
		if err := json.Unmarshal(v.B, &ev); err != nil {
			return nil, err
		}
		vis = ev
	default:
		return nil, fmt.Errorf("unknown type %v", t.Shape)
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
func (c View) MarshalJSON() ([]byte, error) {
	vis, err := MarshalViewPropertiesJSON(c.Properties)
	if err != nil {
		return nil, err
	}

	return json.Marshal(struct {
		ViewContents
		ViewProperties json.RawMessage `json:"properties"`
	}{
		ViewContents:   c.ViewContents,
		ViewProperties: vis,
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

// GaugeViewProperties represents options for gauge view in Chronograf
type GaugeViewProperties struct {
	Type              string           `json:"type"`
	Queries           []DashboardQuery `json:"queries"`
	Prefix            string           `json:"prefix"`
	Suffix            string           `json:"suffix"`
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
func (GaugeViewProperties) viewProperties()          {}
func (TableViewProperties) viewProperties()          {}
func (MarkdownViewProperties) viewProperties()       {}
func (LogViewProperties) viewProperties()            {}

func (v XYViewProperties) GetType() string             { return v.Type }
func (v LinePlusSingleStatProperties) GetType() string { return v.Type }
func (v SingleStatViewProperties) GetType() string     { return v.Type }
func (v GaugeViewProperties) GetType() string          { return v.Type }
func (v TableViewProperties) GetType() string          { return v.Type }
func (v MarkdownViewProperties) GetType() string       { return v.Type }
func (v LogViewProperties) GetType() string            { return v.Type }

/////////////////////////////
// Old Chronograf Types
/////////////////////////////

// DashboardQuery represents a query used in a dashboard cell
type DashboardQuery struct {
	Text          string        `json:"text"`
	Type          string        `json:"type"`
	SourceID      string        `json:"sourceID"`
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
