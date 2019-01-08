package mock

import (
	"context"

	platform "github.com/influxdata/influxdb"
)

var _ platform.ScraperTargetStoreService = &ScraperTargetStoreService{}

// ScraperTargetStoreService is a mock implementation of a platform.ScraperTargetStoreService.
type ScraperTargetStoreService struct {
	ListTargetsF   func(ctx context.Context) ([]platform.ScraperTarget, error)
	AddTargetF     func(ctx context.Context, t *platform.ScraperTarget) error
	GetTargetByIDF func(ctx context.Context, id platform.ID) (*platform.ScraperTarget, error)
	RemoveTargetF  func(ctx context.Context, id platform.ID) error
	UpdateTargetF  func(ctx context.Context, t *platform.ScraperTarget) (*platform.ScraperTarget, error)
}

// ListTargets lists all the scraper targets.
func (s *ScraperTargetStoreService) ListTargets(ctx context.Context) ([]platform.ScraperTarget, error) {
	return s.ListTargetsF(ctx)
}

// AddTarget adds a scraper target.
func (s *ScraperTargetStoreService) AddTarget(ctx context.Context, t *platform.ScraperTarget) error {
	return s.AddTargetF(ctx, t)
}

// GetTargetByID retrieves a scraper target by id.
func (s *ScraperTargetStoreService) GetTargetByID(ctx context.Context, id platform.ID) (*platform.ScraperTarget, error) {
	return s.GetTargetByIDF(ctx, id)
}

// RemoveTarget deletes a scraper target.
func (s *ScraperTargetStoreService) RemoveTarget(ctx context.Context, id platform.ID) error {
	return s.RemoveTargetF(ctx, id)
}

// UpdateTarget updates a scraper target.
func (s *ScraperTargetStoreService) UpdateTarget(ctx context.Context, t *platform.ScraperTarget) (*platform.ScraperTarget, error) {
	return s.UpdateTargetF(ctx, t)
}
