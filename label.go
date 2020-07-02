package influxdb

import (
	"context"
)

// ErrLabelNotFound is the error for a missing Label.
const ErrLabelNotFound = "label not found"

const (
	OpFindLabels         = "FindLabels"
	OpFindLabelByID      = "FindLabelByID"
	OpFindLabelMapping   = "FindLabelMapping"
	OpCreateLabel        = "CreateLabel"
	OpCreateLabelMapping = "CreateLabelMapping"
	OpUpdateLabel        = "UpdateLabel"
	OpDeleteLabel        = "DeleteLabel"
	OpDeleteLabelMapping = "DeleteLabelMapping"
)

// errors on label
var (
	// ErrLabelNameisEmpty is error when org name is empty
	ErrLabelNameisEmpty = &Error{
		Code: EInvalid,
		Msg:  "label name is empty",
	}

	// ErrLabelExistsOnResource is used when attempting to add a label to a resource
	// when that label already exists on the resource
	ErrLabelExistsOnResource = &Error{
		Code: EConflict,
		Msg:  "Cannot add label, label already exists on resource",
	}
)

// LabelService represents a service for managing resource labels
type LabelService interface {
	// FindLabelByID a single label by ID.
	FindLabelByID(ctx context.Context, id ID) (*Label, error)

	// FindLabels returns a list of labels that match a filter
	FindLabels(ctx context.Context, filter LabelFilter, opt ...FindOptions) ([]*Label, error)

	// FindResourceLabels returns a list of labels that belong to a resource
	FindResourceLabels(ctx context.Context, filter LabelMappingFilter) ([]*Label, error)

	// CreateLabel creates a new label
	CreateLabel(ctx context.Context, l *Label) error

	// CreateLabelMapping maps a resource to an existing label
	CreateLabelMapping(ctx context.Context, m *LabelMapping) error

	// UpdateLabel updates a label with a changeset.
	UpdateLabel(ctx context.Context, id ID, upd LabelUpdate) (*Label, error)

	// DeleteLabel deletes a label
	DeleteLabel(ctx context.Context, id ID) error

	// DeleteLabelMapping deletes a label mapping
	DeleteLabelMapping(ctx context.Context, m *LabelMapping) error
}

// Label is a tag set on a resource, typically used for filtering on a UI.
type Label struct {
	ID          ID                `json:"id,omitempty"`
	OrgID       ID                `json:"orgID,omitempty"`
	Name        string            `json:"name"`
	Properties  map[string]string `json:"properties,omitempty"`
	Annotations Annotations       `json:"annotations"`
}

// Validate returns an error if the label is invalid.
func (l *Label) Validate() error {
	if l.Name == "" {
		return &Error{
			Code: EInvalid,
			Msg:  "label name is required",
		}
	}

	if !l.OrgID.Valid() {
		return &Error{
			Code: EInvalid,
			Msg:  "orgID is required",
		}
	}

	return nil
}

// LabelMapping is used to map resource to its labels.
// It should not be shared directly over the HTTP API.
type LabelMapping struct {
	LabelID      ID `json:"labelID"`
	ResourceID   ID `json:"resourceID,omitempty"`
	ResourceType `json:"resourceType"`
}

// Validate returns an error if the mapping is invalid.
func (l *LabelMapping) Validate() error {
	if !l.LabelID.Valid() {
		return &Error{
			Code: EInvalid,
			Msg:  "label id is required",
		}
	}
	if !l.ResourceID.Valid() {
		return &Error{
			Code: EInvalid,
			Msg:  "resource id is required",
		}
	}
	if err := l.ResourceType.Valid(); err != nil {
		return &Error{
			Code: EInvalid,
			Err:  err,
		}
	}

	return nil
}

// LabelUpdate represents a changeset for a label.
// Only the properties specified are updated.
type LabelUpdate struct {
	Name        string            `json:"name,omitempty"`
	Properties  map[string]string `json:"properties,omitempty"`
	Annotations *Annotations      `json:"annotations,omitempty"`
}

// LabelFilter represents a set of filters that restrict the returned results.
type LabelFilter struct {
	Name  string
	OrgID *ID
}

// LabelMappingFilter represents a set of filters that restrict the returned results.
type LabelMappingFilter struct {
	ResourceID ID
	ResourceType
}
