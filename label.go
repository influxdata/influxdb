package platform

import (
	"context"
	"errors"
	"regexp"
)

// ErrLabelNotFound is the error for a missing Label.
const ErrLabelNotFound = ChronografError("label not found")

var colorPattern = regexp.MustCompile(`^([A-Fa-f0-9]{6})$`)

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
	ResourceID ID     `json:"resource_id"`
	Name       string `json:"name"`
	Color      string `json:"color"`
}

// Validate returns an error if the label is invalid.
func (l *Label) Validate() error {
	if !l.ResourceID.Valid() {
		return errors.New("resourceID is required")
	}

	if l.Name == "" {
		return errors.New("label name is required")
	}

	if l.Color != "" && !colorPattern.MatchString(l.Color) {
		return errors.New("label color must be valid hex string")
	}

	return nil
}

// LabelUpdate represents a changeset for a label.
// Only fields which are set are updated.
type LabelUpdate struct {
	Name  *string `json:"name,omitempty"`
	Color *string `json:"color,omitempty"`
}

type LabelFilter struct {
	ResourceID ID
	Name       string
}
