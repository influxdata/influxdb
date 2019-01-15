package influxdb

import (
	"context"
)

// ErrLabelNotFound is the error for a missing Label.
const ErrLabelNotFound = ChronografError("label not found")

const (
	OpFindLabels    = "FindLabels"
	OpFindLabelByID = "FindLabelByID"
	OpCreateLabel   = "CreateLabel"
	OpUpdateLabel   = "UpdateLabel"
	OpDeleteLabel   = "DeleteLabel"
)

// LabelService represents a service for managing resource labels
type LabelService interface {
	// FindLabels returns a list of labels that match a filter
	FindLabels(ctx context.Context, filter LabelFilter, opt ...FindOptions) ([]*Label, error)

	// FindResourceLabels returns a list of labels that belong to a resource
	FindResourceLabels(ctx context.Context, filter LabelMappingFilter) ([]*Label, error)

	// CreateLabel creates a new label
	CreateLabel(ctx context.Context, l *Label) error

	// UpdateLabel updates a label with a changeset.
	UpdateLabel(ctx context.Context, l *Label, upd LabelUpdate) (*Label, error)

	// DeleteLabel deletes a label
	DeleteLabel(ctx context.Context, id ID) error
}

// Label is a tag set on a resource, typically used for filtering on a UI.
type Label struct {
	ID         ID                `json:"id,omitempty"`
	Name       string            `json:"name"`
	Properties map[string]string `json:"properties,omitempty"`
}

// Validate returns an error if the label is invalid.
func (l *Label) Validate() error {
	if l.Name == "" {
		return &Error{
			Code: EInvalid,
			Msg:  "label name is required",
		}
	}

	return nil
}

// LabelMapping is used to map resource to its labels.
// It should not be shared directly over the HTTP API.
type LabelMapping struct {
	LabelID    ID
	ResourceID ID
}

// LabelUpdate represents a changeset for a label.
// Only fields which are set are updated.
type LabelUpdate struct {
	Properties map[string]string `json:"properties,omitempty"`
}

// LabelFilter represents a set of filters that restrict the returned results.
type LabelFilter struct {
	ID   ID
	Name string
}

// LabelMappingFilter represents a set of filters that restrict the returned results.
type LabelMappingFilter struct {
	ResourceID ID
}
