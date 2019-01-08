package influxdb

import (
	"context"
)

// ErrLabelNotFound is the error for a missing Label.
const ErrLabelNotFound = ChronografError("label not found")

const (
	OpFindLabels  = "FindLabels"
	OpCreateLabel = "CreateLabel"
	OpUpdateLabel = "UpdateLabel"
	OpDeleteLabel = "DeleteLabel"
)

type LabelService interface {
	// FindLabels returns a list of labels that match a filter
	FindLabels(ctx context.Context, filter LabelFilter, opt ...FindOptions) ([]*Label, error)

	// CreateLabel creates a new label
	CreateLabel(ctx context.Context, l *Label) error

	// UpdateLabel updates a label with a changeset.
	UpdateLabel(ctx context.Context, l *Label, upd LabelUpdate) (*Label, error)

	// DeleteLabel deletes a label
	DeleteLabel(ctx context.Context, l Label) error
}

type Label struct {
	ResourceID ID                `json:"resourceID"`
	Name       string            `json:"name"`
	Properties map[string]string `json:"properties"`
}

// Validate returns an error if the label is invalid.
func (l *Label) Validate() error {
	if !l.ResourceID.Valid() {
		return &Error{
			Code: EInvalid,
			Msg:  "resourceID is required",
		}
	}

	if l.Name == "" {
		return &Error{
			Code: EInvalid,
			Msg:  "label name is required",
		}
	}

	return nil
}

// LabelUpdate represents a changeset for a label.
// Only fields which are set are updated.
type LabelUpdate struct {
	Properties map[string]string `json:"properties,omitempty"`
}

type LabelFilter struct {
	ResourceID ID
	Name       string
}
