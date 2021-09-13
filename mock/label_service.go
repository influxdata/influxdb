package mock

import (
	"context"

	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
)

var _ platform.LabelService = &LabelService{}

// LabelService is a mock implementation of platform.LabelService
type LabelService struct {
	CreateLabelFn           func(context.Context, *platform.Label) error
	CreateLabelCalls        SafeCount
	DeleteLabelFn           func(context.Context, platform2.ID) error
	DeleteLabelCalls        SafeCount
	FindLabelByIDFn         func(ctx context.Context, id platform2.ID) (*platform.Label, error)
	FindLabelByIDCalls      SafeCount
	FindLabelsFn            func(context.Context, platform.LabelFilter) ([]*platform.Label, error)
	FindLabelsCalls         SafeCount
	FindResourceLabelsFn    func(context.Context, platform.LabelMappingFilter) ([]*platform.Label, error)
	FindResourceLabelsCalls SafeCount
	UpdateLabelFn           func(context.Context, platform2.ID, platform.LabelUpdate) (*platform.Label, error)
	UpdateLabelCalls        SafeCount
	CreateLabelMappingFn    func(context.Context, *platform.LabelMapping) error
	CreateLabelMappingCalls SafeCount
	DeleteLabelMappingFn    func(context.Context, *platform.LabelMapping) error
	DeleteLabelMappingCalls SafeCount
}

// NewLabelService returns a mock of LabelService
// where its methods will return zero values.
func NewLabelService() *LabelService {
	return &LabelService{
		FindLabelByIDFn: func(ctx context.Context, id platform2.ID) (*platform.Label, error) {
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
		UpdateLabelFn:        func(context.Context, platform2.ID, platform.LabelUpdate) (*platform.Label, error) { return nil, nil },
		DeleteLabelFn:        func(context.Context, platform2.ID) error { return nil },
		DeleteLabelMappingFn: func(context.Context, *platform.LabelMapping) error { return nil },
	}
}

// FindLabelByID finds mappings by their ID
func (s *LabelService) FindLabelByID(ctx context.Context, id platform2.ID) (*platform.Label, error) {
	defer s.FindLabelByIDCalls.IncrFn()()
	return s.FindLabelByIDFn(ctx, id)
}

// FindLabels finds mappings that match a given filter.
func (s *LabelService) FindLabels(ctx context.Context, filter platform.LabelFilter, opt ...platform.FindOptions) ([]*platform.Label, error) {
	defer s.FindLabelsCalls.IncrFn()()
	return s.FindLabelsFn(ctx, filter)
}

// FindResourceLabels finds mappings that match a given filter.
func (s *LabelService) FindResourceLabels(ctx context.Context, filter platform.LabelMappingFilter) ([]*platform.Label, error) {
	defer s.FindResourceLabelsCalls.IncrFn()()
	return s.FindResourceLabelsFn(ctx, filter)
}

// CreateLabel creates a new Label.
func (s *LabelService) CreateLabel(ctx context.Context, l *platform.Label) error {
	defer s.CreateLabelCalls.IncrFn()()
	return s.CreateLabelFn(ctx, l)
}

// CreateLabelMapping creates a new Label mapping.
func (s *LabelService) CreateLabelMapping(ctx context.Context, m *platform.LabelMapping) error {
	defer s.CreateLabelMappingCalls.IncrFn()()
	return s.CreateLabelMappingFn(ctx, m)
}

// UpdateLabel updates a label.
func (s *LabelService) UpdateLabel(ctx context.Context, id platform2.ID, upd platform.LabelUpdate) (*platform.Label, error) {
	defer s.UpdateLabelCalls.IncrFn()()
	return s.UpdateLabelFn(ctx, id, upd)
}

// DeleteLabel removes a Label.
func (s *LabelService) DeleteLabel(ctx context.Context, id platform2.ID) error {
	defer s.DeleteLabelCalls.IncrFn()()
	return s.DeleteLabelFn(ctx, id)
}

// DeleteLabelMapping removes a Label mapping.
func (s *LabelService) DeleteLabelMapping(ctx context.Context, m *platform.LabelMapping) error {
	defer s.DeleteLabelMappingCalls.IncrFn()()
	return s.DeleteLabelMappingFn(ctx, m)
}
