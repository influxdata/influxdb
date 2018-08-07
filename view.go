package platform

import (
	"context"
	"encoding/json"
	"fmt"
)

// ErrViewNotFound is the error for a missing View.
const ErrViewNotFound = Error("View not found")

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
func (u ViewUpdate) Valid() error {
	_, ok := u.Properties.(EmptyViewProperties)
	if u.Name == nil && ok {
		return fmt.Errorf("expected at least one attribute to be updated")
	}

	return nil
}

// ViewContentsUpdate is a struct for updating the non properties content of a View.
type ViewContentsUpdate struct {
	Name *string `json:"name"`
}

// ViewFilter represents a set of filter that restrict the returned results.
type ViewFilter struct {
	ID *ID
}

// View holds positional and visual information for a View.
type View struct {
	ViewContents
	Properties ViewProperties
}

type ViewContents struct {
	ID   ID     `json:"id"`
	Name string `json:"name"`
}

type ViewProperties interface {
	ViewProperties()
}

// EmptyViewProperties is visualization that has no values
type EmptyViewProperties struct{}

func (v EmptyViewProperties) ViewProperties() {}

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
	}

	if err := json.Unmarshal(v.B, &t); err != nil {
		return nil, err
	}

	var vis ViewProperties
	switch t.Shape {
	case "chronograf-v1":
		var qv V1ViewProperties
		if err := json.Unmarshal(v.B, &qv); err != nil {
			return nil, err
		}
		vis = qv
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

func MarshalViewPropertiesJSON(v ViewProperties) ([]byte, error) {
	var s interface{}
	switch vis := v.(type) {
	case V1ViewProperties:
		s = struct {
			Shape string `json:"shape"`
			V1ViewProperties
		}{
			Shape:            "chronograf-v1",
			V1ViewProperties: vis,
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

type V1ViewProperties struct {
	Queries       []DashboardQuery `json:"queries"`
	Axes          map[string]Axis  `json:"axes"`
	Type          string           `json:"type"`
	ViewColors    []ViewColor      `json:"colors"`
	Legend        Legend           `json:"legend"`
	TableOptions  TableOptions     `json:"tableOptions,omitempty"`
	FieldOptions  []RenamableField `json:"fieldOptions"`
	TimeFormat    string           `json:"timeFormat"`
	DecimalPlaces DecimalPlaces    `json:"decimalPlaces"`
}

func (V1ViewProperties) ViewProperties() {}

/////////////////////////////
// Old Chronograf Types
/////////////////////////////

// DashboardQuery includes state for the query builder.  This is a transition
// struct while we move to the full InfluxQL AST
type DashboardQuery struct {
	Command     string      `json:"query"`                 // Command is the query itself
	Label       string      `json:"label,omitempty"`       // Label is the Y-Axis label for the data
	Range       *Range      `json:"range,omitempty"`       // Range is the default Y-Axis range for the data
	QueryConfig QueryConfig `json:"queryConfig,omitempty"` // QueryConfig represents the query state that is understood by the data explorer
	// TODO(desa): this should be platform.ID
	Source string      `json:"source"` // Source is the optional URI to the data source for this queryConfig
	Shifts []TimeShift `json:"-"`      // Shifts represents shifts to apply to an influxql query's time range.  Clients expect the shift to be in the generated QueryConfig
}

// Range represents an upper and lower bound for data
type Range struct {
	Upper int64 `json:"upper"` // Upper is the upper bound
	Lower int64 `json:"lower"` // Lower is the lower bound
}

// QueryConfig represents UI query from the data explorer
type QueryConfig struct {
	ID              string              `json:"id,omitempty"`
	Database        string              `json:"database"`
	Measurement     string              `json:"measurement"`
	RetentionPolicy string              `json:"retentionPolicy"`
	Fields          []Field             `json:"fields"`
	Tags            map[string][]string `json:"tags"`
	GroupBy         GroupBy             `json:"groupBy"`
	AreTagsAccepted bool                `json:"areTagsAccepted"`
	Fill            string              `json:"fill,omitempty"`
	RawText         *string             `json:"rawText"`
	Range           *DurationRange      `json:"range"`
	Shifts          []TimeShift         `json:"shifts"`
}

// TimeShift represents a shift to apply to an influxql query's time range
type TimeShift struct {
	Label    string `json:"label"`    // Label user facing description
	Unit     string `json:"unit"`     // Unit influxql time unit representation i.e. ms, s, m, h, d
	Quantity string `json:"quantity"` // Quantity number of units
}

// Field represent influxql fields and functions from the UI
type Field struct {
	Value interface{} `json:"value"`
	Type  string      `json:"type"`
	Alias string      `json:"alias"`
	Args  []Field     `json:"args,omitempty"`
}

// GroupBy represents influxql group by tags from the UI
type GroupBy struct {
	Time string   `json:"time"`
	Tags []string `json:"tags"`
}

// DurationRange represents the lower and upper durations of the query config
type DurationRange struct {
	Upper string `json:"upper"`
	Lower string `json:"lower"`
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
	ID    string `json:"id"`    // ID is the unique id of the View color
	Type  string `json:"type"`  // Type is how the color is used. Accepted (min,max,threshold)
	Hex   string `json:"hex"`   // Hex is the hex number of the color
	Name  string `json:"name"`  // Name is the user-facing name of the hex color
	Value string `json:"value"` // Value is the data value mapped to this color
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
