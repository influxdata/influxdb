package mock

import (
	"context"

	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
)

var _ platform.ScraperTargetStoreService = &ScraperTargetStoreService{}

// ScraperTargetStoreService is a mock implementation of a platform.ScraperTargetStoreService.
type ScraperTargetStoreService struct {
	UserResourceMappingService
	OrganizationService
	ListTargetsF   func(ctx context.Context, filter platform.ScraperTargetFilter) ([]platform.ScraperTarget, error)
	AddTargetF     func(ctx context.Context, t *platform.ScraperTarget, userID platform2.ID) error
	GetTargetByIDF func(ctx context.Context, id platform2.ID) (*platform.ScraperTarget, error)
	RemoveTargetF  func(ctx context.Context, id platform2.ID) error
	UpdateTargetF  func(ctx context.Context, t *platform.ScraperTarget, userID platform2.ID) (*platform.ScraperTarget, error)
}

// ListTargets lists all the scraper targets.
func (s *ScraperTargetStoreService) ListTargets(ctx context.Context, filter platform.ScraperTargetFilter) ([]platform.ScraperTarget, error) {
	return s.ListTargetsF(ctx, filter)
}

// AddTarget adds a scraper target.
func (s *ScraperTargetStoreService) AddTarget(ctx context.Context, t *platform.ScraperTarget, userID platform2.ID) error {
	return s.AddTargetF(ctx, t, userID)
}

// GetTargetByID retrieves a scraper target by id.
func (s *ScraperTargetStoreService) GetTargetByID(ctx context.Context, id platform2.ID) (*platform.ScraperTarget, error) {
	return s.GetTargetByIDF(ctx, id)
}

// RemoveTarget deletes a scraper target.
func (s *ScraperTargetStoreService) RemoveTarget(ctx context.Context, id platform2.ID) error {
	return s.RemoveTargetF(ctx, id)
}

// UpdateTarget updates a scraper target.
func (s *ScraperTargetStoreService) UpdateTarget(ctx context.Context, t *platform.ScraperTarget, userID platform2.ID) (*platform.ScraperTarget, error) {
	return s.UpdateTargetF(ctx, t, userID)
}
