package mock

import (
	"context"

	"github.com/influxdata/platform"
)

var _ platform.LabelService = &LabelService{}

// LabelService is a mock implementation of platform.LabelService
type LabelService struct {
	FindLabelsFn  func(context.Context, platform.LabelFilter) ([]*platform.Label, error)
	CreateLabelFn func(context.Context, *platform.Label) error
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
		DeleteLabelFn: func(context.Context, platform.Label) error { return nil },
	}
}

// FindLabels finds mappings that match a given filter.
func (s *LabelService) FindLabels(ctx context.Context, filter platform.LabelFilter, opt ...platform.FindOptions) ([]*platform.Label, error) {
	return s.FindLabelsFn(ctx, filter)
}

// CreateLabel creates a new Label.
func (s *LabelService) CreateLabel(ctx context.Context, m *platform.Label) error {
	return s.CreateLabelFn(ctx, m)
}

// DeleteLabel removes a Label.
func (s *LabelService) DeleteLabel(ctx context.Context, l platform.Label) error {
	return s.DeleteLabelFn(ctx, l)
}
