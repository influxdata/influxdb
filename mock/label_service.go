package mock

import (
	"context"

	platform "github.com/influxdata/influxdb"
)

var _ platform.LabelService = &LabelService{}

// LabelService is a mock implementation of platform.LabelService
type LabelService struct {
	FindLabelsFn  func(context.Context, platform.LabelFilter) ([]*platform.Label, error)
	CreateLabelFn func(context.Context, *platform.Label) error
	UpdateLabelFn func(context.Context, *platform.Label, platform.LabelUpdate) (*platform.Label, error)
	DeleteLabelFn func(context.Context, platform.Label) error
}

// NewLabelService returns a mock of LabelService
// where its methods will return zero values.
func NewLabelService() *LabelService {
	return &LabelService{
		FindLabelsFn: func(context.Context, platform.LabelFilter) ([]*platform.Label, error) {
			return nil, nil
		},
		CreateLabelFn: func(context.Context, *platform.Label) error { return nil },
		UpdateLabelFn: func(context.Context, *platform.Label, platform.LabelUpdate) (*platform.Label, error) { return nil, nil },
		DeleteLabelFn: func(context.Context, platform.Label) error { return nil },
	}
}

// FindLabels finds mappings that match a given filter.
func (s *LabelService) FindLabels(ctx context.Context, filter platform.LabelFilter, opt ...platform.FindOptions) ([]*platform.Label, error) {
	return s.FindLabelsFn(ctx, filter)
}

// CreateLabel creates a new Label.
func (s *LabelService) CreateLabel(ctx context.Context, l *platform.Label) error {
	return s.CreateLabelFn(ctx, l)
}

// UpdateLabel updates a label.
func (s *LabelService) UpdateLabel(ctx context.Context, l *platform.Label, upd platform.LabelUpdate) (*platform.Label, error) {
	return s.UpdateLabelFn(ctx, l, upd)
}

// DeleteLabel removes a Label.
func (s *LabelService) DeleteLabel(ctx context.Context, l platform.Label) error {
	return s.DeleteLabelFn(ctx, l)
}
