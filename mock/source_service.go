package mock

import (
	"context"

	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
)

var _ platform.SourceService = (*SourceService)(nil)

// SourceService is a mock implementation of platform.SourceService.
type SourceService struct {
	DefaultSourceFn  func(context.Context) (*platform.Source, error)
	FindSourceByIDFn func(context.Context, platform2.ID) (*platform.Source, error)
	FindSourcesFn    func(context.Context, platform.FindOptions) ([]*platform.Source, int, error)
	CreateSourceFn   func(context.Context, *platform.Source) error
	UpdateSourceFn   func(context.Context, platform2.ID, platform.SourceUpdate) (*platform.Source, error)
	DeleteSourceFn   func(context.Context, platform2.ID) error
}

// NewSourceService returns a mock of SourceService where its methods will return zero values.
func NewSourceService() *SourceService {
	return &SourceService{
		DefaultSourceFn:  func(context.Context) (*platform.Source, error) { return nil, nil },
		FindSourceByIDFn: func(context.Context, platform2.ID) (*platform.Source, error) { return nil, nil },
		CreateSourceFn:   func(context.Context, *platform.Source) error { return nil },
		UpdateSourceFn:   func(context.Context, platform2.ID, platform.SourceUpdate) (*platform.Source, error) { return nil, nil },
		DeleteSourceFn:   func(context.Context, platform2.ID) error { return nil },
		FindSourcesFn: func(context.Context, platform.FindOptions) ([]*platform.Source, int, error) {
			return nil, 0, nil
		},
	}
}

// DefaultSource retrieves the default source.
func (s *SourceService) DefaultSource(ctx context.Context) (*platform.Source, error) {
	return s.DefaultSourceFn(ctx)
}

// FindSourceByID retrieves a source by its ID.
func (s *SourceService) FindSourceByID(ctx context.Context, id platform2.ID) (*platform.Source, error) {
	return s.FindSourceByIDFn(ctx, id)
}

// FindSources returns a list of all sources.
func (s *SourceService) FindSources(ctx context.Context, opts platform.FindOptions) ([]*platform.Source, int, error) {
	return s.FindSourcesFn(ctx, opts)
}

// CreateSource sets the sources ID and stores it.
func (s *SourceService) CreateSource(ctx context.Context, source *platform.Source) error {
	return s.CreateSourceFn(ctx, source)
}

// DeleteSource removes the source.
func (s *SourceService) DeleteSource(ctx context.Context, id platform2.ID) error {
	return s.DeleteSourceFn(ctx, id)
}

// UpdateSource updates the source.
func (s *SourceService) UpdateSource(ctx context.Context, id platform2.ID, upd platform.SourceUpdate) (*platform.Source, error) {
	return s.UpdateSourceFn(ctx, id, upd)
}
