package mock

import (
	"context"

	platform "github.com/influxdata/influxdb"
)

var _ platform.LabelService = &LabelService{}

// LabelService is a mock implementation of platform.LabelService
type LabelService struct {
	FindLabelByIDFn      func(ctx context.Context, id platform.ID) (*platform.Label, error)
	FindLabelsFn         func(context.Context, platform.LabelFilter) ([]*platform.Label, error)
	FindResourceLabelsFn func(context.Context, platform.LabelMappingFilter) ([]*platform.Label, error)
	CreateLabelFn        func(context.Context, *platform.Label) error
	CreateLabelMappingFn func(context.Context, *platform.LabelMapping) error
	UpdateLabelFn        func(context.Context, platform.ID, platform.LabelUpdate) (*platform.Label, error)
	DeleteLabelFn        func(context.Context, platform.ID) error
	DeleteLabelMappingFn func(context.Context, *platform.LabelMapping) error
}

// NewLabelService returns a mock of LabelService
// where its methods will return zero values.
func NewLabelService() *LabelService {
	return &LabelService{
		FindLabelByIDFn: func(ctx context.Context, id platform.ID) (*platform.Label, error) {
			return nil, nil
		},
		FindLabelsFn: func(context.Context, platform.LabelFilter) ([]*platform.Label, error) {
			return nil, nil
		},
		FindResourceLabelsFn: func(context.Context, platform.LabelMappingFilter) ([]*platform.Label, error) {
			return []*platform.Label{}, nil
		},
		CreateLabelFn:        func(context.Context, *platform.Label) error { return nil },
		CreateLabelMappingFn: func(context.Context, *platform.LabelMapping) error { return nil },
		UpdateLabelFn:        func(context.Context, platform.ID, platform.LabelUpdate) (*platform.Label, error) { return nil, nil },
		DeleteLabelFn:        func(context.Context, platform.ID) error { return nil },
		DeleteLabelMappingFn: func(context.Context, *platform.LabelMapping) error { return nil },
	}
}

// FindLabelByID finds mappings by their ID
func (s *LabelService) FindLabelByID(ctx context.Context, id platform.ID) (*platform.Label, error) {
	return s.FindLabelByIDFn(ctx, id)
}

// FindLabels finds mappings that match a given filter.
func (s *LabelService) FindLabels(ctx context.Context, filter platform.LabelFilter, opt ...platform.FindOptions) ([]*platform.Label, error) {
	return s.FindLabelsFn(ctx, filter)
}

// FindResourceLabels finds mappings that match a given filter.
func (s *LabelService) FindResourceLabels(ctx context.Context, filter platform.LabelMappingFilter) ([]*platform.Label, error) {
	return s.FindResourceLabelsFn(ctx, filter)
}

// CreateLabel creates a new Label.
func (s *LabelService) CreateLabel(ctx context.Context, l *platform.Label) error {
	return s.CreateLabelFn(ctx, l)
}

// CreateLabelMapping creates a new Label mapping.
func (s *LabelService) CreateLabelMapping(ctx context.Context, m *platform.LabelMapping) error {
	return s.CreateLabelMappingFn(ctx, m)
}

// UpdateLabel updates a label.
func (s *LabelService) UpdateLabel(ctx context.Context, id platform.ID, upd platform.LabelUpdate) (*platform.Label, error) {
	return s.UpdateLabelFn(ctx, id, upd)
}

// DeleteLabel removes a Label.
func (s *LabelService) DeleteLabel(ctx context.Context, id platform.ID) error {
	return s.DeleteLabelFn(ctx, id)
}

// DeleteLabelMapping removes a Label mapping.
func (s *LabelService) DeleteLabelMapping(ctx context.Context, m *platform.LabelMapping) error {
	return s.DeleteLabelMappingFn(ctx, m)
}
