package influxdb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

// ErrVariableNotFound is the error msg for a missing variable.
const ErrVariableNotFound = "variable not found"

// ops for variable error.
const (
	OpFindVariableByID = "FindVariableByID"
	OpFindVariables    = "FindVariables"
	OpCreateVariable   = "CreateVariable"
	OpUpdateVariable   = "UpdateVariable"
	OpReplaceVariable  = "ReplaceVariable"
	OpDeleteVariable   = "DeleteVariable"
)

// VariableService describes a service for managing Variables
type VariableService interface {
	// FindVariableByID finds a single variable from the store by its ID
	FindVariableByID(ctx context.Context, id platform.ID) (*Variable, error)

	// FindVariables returns all variables in the store
	FindVariables(ctx context.Context, filter VariableFilter, opt ...FindOptions) ([]*Variable, error)

	// CreateVariable creates a new variable and assigns it an ID
	CreateVariable(ctx context.Context, m *Variable) error

	// UpdateVariable updates a single variable with a changeset
	UpdateVariable(ctx context.Context, id platform.ID, update *VariableUpdate) (*Variable, error)

	// ReplaceVariable replaces a single variable
	ReplaceVariable(ctx context.Context, variable *Variable) error

	// DeleteVariable removes a variable from the store
	DeleteVariable(ctx context.Context, id platform.ID) error
}

// A Variable describes a keyword that can be expanded into several possible
// values when used in an InfluxQL or Flux query
type Variable struct {
	ID             platform.ID        `json:"id,omitempty"`
	OrganizationID platform.ID        `json:"orgID,omitempty"`
	Name           string             `json:"name"`
	Description    string             `json:"description"`
	Selected       []string           `json:"selected"`
	Arguments      *VariableArguments `json:"arguments"`
	CRUDLog
}

// DefaultVariableFindOptions are the default find options for variables.
var DefaultVariableFindOptions = FindOptions{}

// VariableFilter represents a set of filter that restrict the returned results.
type VariableFilter struct {
	ID             *platform.ID
	OrganizationID *platform.ID
	Organization   *string
}

// QueryParams implements PagingFilter.
//
// It converts VariableFilter fields to url query params.
func (f VariableFilter) QueryParams() map[string][]string {
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

// A VariableUpdate describes a set of changes that can be applied to a Variable
type VariableUpdate struct {
	Name        string             `json:"name"`
	Selected    []string           `json:"selected"`
	Description string             `json:"description"`
	Arguments   *VariableArguments `json:"arguments"`
}

// A VariableArguments contains arguments used when expanding a Variable
type VariableArguments struct {
	Type   string      `json:"type"`   // "constant", "map", or "query"
	Values interface{} `json:"values"` // either VariableQueryValues, VariableConstantValues, VariableMapValues
}

// VariableQueryValues contains a query used when expanding a query-based Variable
type VariableQueryValues struct {
	Query    string `json:"query"`
	Language string `json:"language"` // "influxql" or "flux"
}

// VariableConstantValues are the data for expanding a constants-based Variable
type VariableConstantValues []string

// VariableMapValues are the data for expanding a map-based Variable
type VariableMapValues map[string]string

// Valid returns an error if a Variable contains invalid data
func (m *Variable) Valid() error {
	// todo(leodido) > check it org ID validity?

	if m.Name == "" {
		return fmt.Errorf("missing variable name")
	}

	// variable name must start with a letter to be a valid identifier in Flux
	if !regexp.MustCompile(`^[a-zA-Z_].*`).MatchString(m.Name) {
		return fmt.Errorf("variable name must start with a letter")
	}

	validTypes := map[string]bool{
		"constant": true,
		"map":      true,
		"query":    true,
	}

	if m.Arguments == nil || !validTypes[m.Arguments.Type] {
		return fmt.Errorf("invalid arguments type")
	}

	inValidNames := [11]string{"and", "import", "not", "return", "option", "test", "empty", "in", "or", "package", "builtin"}

	for x := range inValidNames {

		if m.Name == inValidNames[x] {
			return fmt.Errorf("%q is a protected variable name", inValidNames[x])
		}
	}

	return nil
}

// Valid returns an error if a Variable changeset is not valid
func (u *VariableUpdate) Valid() error {
	if u.Name == "" && u.Description == "" && u.Selected == nil && u.Arguments == nil {
		return fmt.Errorf("no fields supplied in update")
	}

	return nil
}

// Apply applies non-zero fields from a VariableUpdate to a Variable
func (u *VariableUpdate) Apply(m *Variable) {
	if u.Name != "" {
		m.Name = u.Name
	}

	if u.Selected != nil {
		m.Selected = u.Selected
	}

	if u.Arguments != nil {
		m.Arguments = u.Arguments
	}

	if u.Description != "" {
		m.Description = u.Description
	}
}

// UnmarshalJSON unmarshals json into a VariableArguments struct, using the `Type`
// field to assign the approriate struct to the `Values` field
func (a *VariableArguments) UnmarshalJSON(data []byte) error {
	type Alias VariableArguments
	aux := struct{ *Alias }{Alias: (*Alias)(a)}

	err := json.Unmarshal(data, &aux)
	if err != nil {
		return err
	}

	// Decode the polymorphic VariableArguments.Values field into the appropriate struct
	switch aux.Type {
	case "constant":
		values, ok := aux.Values.([]interface{})
		if !ok {
			return fmt.Errorf("error parsing %v as VariableConstantArguments", aux.Values)
		}

		variableValues := make(VariableConstantValues, len(values))
		for i, v := range values {
			if _, ok := v.(string); !ok {
				return fmt.Errorf("expected variable constant value to be string but received %T", v)
			}
			variableValues[i] = v.(string)
		}

		a.Values = variableValues
	case "map":
		values, ok := aux.Values.(map[string]interface{})
		if !ok {
			return fmt.Errorf("error parsing %v as VariableMapArguments", aux.Values)
		}

		variableValues := VariableMapValues{}
		for k, v := range values {
			if _, ok := v.(string); !ok {
				return fmt.Errorf("expected variable map value to be string but received %T", v)
			}
			variableValues[k] = v.(string)
		}

		a.Values = variableValues
	case "query":
		values, ok := aux.Values.(map[string]interface{})
		if !ok {
			return fmt.Errorf("error parsing %v as VariableQueryArguments", aux.Values)
		}

		variableValues := VariableQueryValues{}

		query, prs := values["query"]
		if !prs {
			return fmt.Errorf("\"query\" key not present in VariableQueryArguments")
		}
		if _, ok := query.(string); !ok {
			return fmt.Errorf("expected \"query\" to be string but received %T", query)
		}

		language, prs := values["language"]
		if !prs {
			return fmt.Errorf("\"language\" key not present in VariableQueryArguments")
		}
		if _, ok := language.(string); !ok {
			return fmt.Errorf("expected \"language\" to be string but received %T", language)
		}

		variableValues.Query = query.(string)
		variableValues.Language = language.(string)
		a.Values = variableValues
	default:
		return fmt.Errorf("unknown VariableArguments type %s", aux.Type)
	}

	return nil
}
