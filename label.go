// Resource types that can have labels:
// Dashboards, Users, Tokens, Buckets

// Other implementation thoughts:
// - check for duplicate values for a resource when creating new ones
//   - should it do nothing and return success, or is it better to return a failure?
//   - a failure is probably more semantically correct, because it doesn't actually create the resource; the frontend can handle it how it wants to
// - should we swallow deletion if a key doesn't exist?
// - ensure that proper bucket cleanup happens if a resource is deleted, or if a single label is removed
//
// Say we want to filter on all dashboards that match a tag. What would the interface look like?
// probably something like /dashboards?tag=baz

package platform

import (
	"context"
	"errors"
)

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
