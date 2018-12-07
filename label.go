// Resource types that can have labels:
// Dashboards, Users, Tokens, Buckets

package platform

import (
	"context"
	"errors"
)

// ErrLabelNotFound is the error for a missing Label.
const ErrLabelNotFound = ChronografError("label not found")

type LabelService interface {
	// FindLabels returns a list of labels that match a filter
	FindLabels(ctx context.Context, filter LabelFilter, opt ...FindOptions) ([]*Label, error)

	// CreateLabel creates a new label
	CreateLabel(ctx context.Context, l *Label) error

	// DeleteLabel deletes a label
	DeleteLabel(ctx context.Context, l Label) error
}

type Label struct {
	ResourceID ID     `json:"resource_id"`
	Name       string `json:"name"`
}

// Validate returns an error if the label is invalid.
func (l *Label) Validate() error {
	if !l.ResourceID.Valid() {
		return errors.New("resourceID is required")
	}

	if l.Name == "" {
		return errors.New("label name is required")
	}

	return nil
}

type LabelFilter struct {
	ResourceID ID
	Name       string
}
