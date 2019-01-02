package mock

import (
	"context"

	"github.com/influxdata/platform"
)

var _ platform.ScraperTargetStoreService = (*ScraperTargetStoreService)(nil)

// ScraperTargetStoreService is a mock implementation of platform.ScraperTargetStoreService.
type ScraperTargetStoreService struct {
	ListTargetsFn   func(ctx context.Context) ([]platform.ScraperTarget, error)
	AddTargetFn     func(ctx context.Context, t *platform.ScraperTarget) error
	GetTargetByIDFn func(ctx context.Context, id platform.ID) (*platform.ScraperTarget, error)
	RemoveTargetFn  func(ctx context.Context, id platform.ID) error
	UpdateTargetFn  func(ctx context.Context, t *platform.ScraperTarget) (*platform.ScraperTarget, error)
}

// NewScraperTargetStoreService returns a mock of ScraperTargetStoreService where its methods will return zero values.
func NewScraperTargetStoreService() *ScraperTargetStoreService {
	return &ScraperTargetStoreService{
		ListTargetsFn:   func(ctx context.Context) ([]platform.ScraperTarget, error) { return nil, nil },
		AddTargetFn:     func(ctx context.Context, t *platform.ScraperTarget) error { return nil },
		GetTargetByIDFn: func(ctx context.Context, id platform.ID) (*platform.ScraperTarget, error) { return nil, nil },
		RemoveTargetFn:  func(ctx context.Context, id platform.ID) error { return nil },
		UpdateTargetFn:  func(ctx context.Context, t *platform.ScraperTarget) (*platform.ScraperTarget, error) { return nil, nil },
	}
}

// ListTargets returns a list of targets
func (s *ScraperTargetStoreService) ListTargets(ctx context.Context) ([]platform.ScraperTarget, error) {
	return s.ListTargetsFn(ctx)
}

// AddTarget adds a new target.
func (s *ScraperTargetStoreService) AddTarget(ctx context.Context, t *platform.ScraperTarget) error {
	return s.AddTargetFn(ctx, t)
}

// UpdateTarget updates a target.
func (s *ScraperTargetStoreService) UpdateTarget(ctx context.Context, t *platform.ScraperTarget) (*platform.ScraperTarget, error) {
	return s.UpdateTargetFn(ctx, t)
}

// GetTargetByID returns a single target by ID.
func (s *ScraperTargetStoreService) GetTargetByID(ctx context.Context, id platform.ID) (*platform.ScraperTarget, error) {
	return s.GetTargetByIDFn(ctx, id)
}

// RemoveTarget removes a target by ID.
func (s *ScraperTargetStoreService) RemoveTarget(ctx context.Context, id platform.ID) error {
	return s.RemoveTargetFn(ctx, id)
}
