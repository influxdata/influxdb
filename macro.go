package influxdb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
)

// ErrMacroNotFound is the error msg for a missing macro.
const ErrMacroNotFound = "macro not found"

// ops for macro error.
const (
	OpFindMacroByID = "FindMacroByID"
	OpFindMacros    = "FindMacros"
	OpCreateMacro   = "CreateMacro"
	OpUpdateMacro   = "UpdateMacro"
	OpReplaceMacro  = "ReplaceMacro"
	OpDeleteMacro   = "DeleteMacro"
)

// MacroService describes a service for managing Macros
type MacroService interface {
	// FindMacro finds a single macro from the store by its ID
	FindMacroByID(ctx context.Context, id ID) (*Macro, error)

	// FindMacros returns all macros in the store
	FindMacros(ctx context.Context, filter MacroFilter, opt ...FindOptions) ([]*Macro, error)

	// CreateMacro creates a new macro and assigns it an ID
	CreateMacro(ctx context.Context, m *Macro) error

	// UpdateMacro updates a single macro with a changeset
	UpdateMacro(ctx context.Context, id ID, update *MacroUpdate) (*Macro, error)

	// ReplaceMacro replaces a single macro
	ReplaceMacro(ctx context.Context, macro *Macro) error

	// DeleteMacro removes a macro from the store
	DeleteMacro(ctx context.Context, id ID) error
}

// A Macro describes a keyword that can be expanded into several possible
// values when used in an InfluxQL or Flux query
type Macro struct {
	ID             ID              `json:"id,omitempty"`
	OrganizationID ID              `json:"org_id,omitempty"`
	Name           string          `json:"name"`
	Selected       []string        `json:"selected"`
	Arguments      *MacroArguments `json:"arguments"`
}

// DefaultMacroFindOptions are the default find options for macros.
var DefaultMacroFindOptions = FindOptions{}

// MacroFilter represents a set of filter that restrict the returned results.
type MacroFilter struct {
	ID             *ID
	OrganizationID *ID
	Organization   *string
}

// QueryParams implements PagingFilter.
//
// It converts MacroFilter fields to url query params.
func (f MacroFilter) QueryParams() map[string][]string {
	qp := url.Values{}
	if f.ID != nil {
		qp.Add("id", f.ID.String())
	}

	if f.OrganizationID != nil {
		qp.Add("orgID", f.OrganizationID.String())
	}

	if f.Organization != nil {
		qp.Add("org", *f.Organization)
	}

	return qp
}

// A MacroUpdate describes a set of changes that can be applied to a Macro
type MacroUpdate struct {
	Name      string          `json:"name"`
	Selected  []string        `json:"selected"`
	Arguments *MacroArguments `json:"arguments"`
}

// A MacroArguments contains arguments used when expanding a Macro
type MacroArguments struct {
	Type   string      `json:"type"`   // "constant", "map", or "query"
	Values interface{} `json:"values"` // either MacroQueryValues, MacroConstantValues, MacroMapValues
}

// MacroQueryValues contains a query used when expanding a query-based Macro
type MacroQueryValues struct {
	Query    string `json:"query"`
	Language string `json:"language"` // "influxql" or "flux"
}

// MacroConstantValues are the data for expanding a constants-based Macro
type MacroConstantValues []string

// MacroMapValues are the data for expanding a map-based Macro
type MacroMapValues map[string]string

// Valid returns an error if a Macro contains invalid data
func (m *Macro) Valid() error {
	// todo(leodido) > check it org ID validity?

	if m.Name == "" {
		return fmt.Errorf("missing macro name")
	}

	validTypes := map[string]bool{
		"constant": true,
		"map":      true,
		"query":    true,
	}

	if _, prs := validTypes[m.Arguments.Type]; !prs {
		return fmt.Errorf("invalid arguments type")
	}

	if len(m.Selected) == 0 {
		return fmt.Errorf("no selected values")
	}

	return nil
}

// Valid returns an error if a Macro changeset is not valid
func (u *MacroUpdate) Valid() error {
	if u.Name == "" && u.Selected == nil && u.Arguments == nil {
		return fmt.Errorf("no fields supplied in update")
	}

	return nil
}

// Apply applies non-zero fields from a MacroUpdate to a Macro
func (u *MacroUpdate) Apply(m *Macro) error {
	if u.Name != "" {
		m.Name = u.Name
	}

	if u.Selected != nil {
		m.Selected = u.Selected
	}

	if u.Arguments != nil {
		m.Arguments = u.Arguments
	}

	return nil
}

// UnmarshalJSON unmarshals json into a MacroArguments struct, using the `Type`
// field to assign the approriate struct to the `Values` field
func (a *MacroArguments) UnmarshalJSON(data []byte) error {
	type Alias MacroArguments
	aux := struct{ *Alias }{Alias: (*Alias)(a)}

	err := json.Unmarshal(data, &aux)
	if err != nil {
		return err
	}

	// Decode the polymorphic MacroArguments.Values field into the approriate struct
	switch aux.Type {
	case "constant":
		values, ok := aux.Values.([]interface{})
		if !ok {
			return fmt.Errorf("error parsing %v as MacroConstantArguments", aux.Values)
		}

		macroValues := make(MacroConstantValues, len(values))
		for i, v := range values {
			if _, ok := v.(string); !ok {
				return fmt.Errorf("expected macro constant value to be string but received %T", v)
			}
			macroValues[i] = v.(string)
		}

		a.Values = macroValues
	case "map":
		values, ok := aux.Values.(map[string]interface{})
		if !ok {
			return fmt.Errorf("error parsing %v as MacroMapArguments", aux.Values)
		}

		macroValues := MacroMapValues{}
		for k, v := range values {
			if _, ok := v.(string); !ok {
				return fmt.Errorf("expected macro map value to be string but received %T", v)
			}
			macroValues[k] = v.(string)
		}

		a.Values = macroValues
	case "query":
		values, ok := aux.Values.(map[string]interface{})
		if !ok {
			return fmt.Errorf("error parsing %v as MacroQueryArguments", aux.Values)
		}

		macroValues := MacroQueryValues{}

		query, prs := values["query"]
		if !prs {
			return fmt.Errorf("\"query\" key not present in MacroQueryArguments")
		}
		if _, ok := query.(string); !ok {
			return fmt.Errorf("expected \"query\" to be string but received %T", query)
		}

		language, prs := values["language"]
		if !prs {
			return fmt.Errorf("\"language\" key not present in MacroQueryArguments")
		}
		if _, ok := language.(string); !ok {
			return fmt.Errorf("expected \"language\" to be string but received %T", language)
		}

		macroValues.Query = query.(string)
		macroValues.Language = language.(string)
		a.Values = macroValues
	default:
		return fmt.Errorf("unknown MacroArguments type %s", aux.Type)
	}

	return nil
}
