package authorizer

import (
	"context"

	"github.com/influxdata/influxdb"
)

var _ influxdb.ScraperTargetStoreService = (*ScraperTargetStoreService)(nil)

// ScraperTargetStoreService wraps a influxdb.ScraperTargetStoreService and authorizes actions
// against it appropriately.
type ScraperTargetStoreService struct {
	s influxdb.ScraperTargetStoreService
}

// NewScraperTargetStoreService constructs an instance of an authorizing scraper target store serivce.
func NewScraperTargetStoreService(s influxdb.ScraperTargetStoreService) *ScraperTargetStoreService {
	return &ScraperTargetStoreService{
		s: s,
	}
}

func newScraperPermission(a influxdb.Action, orgID, id influxdb.ID) (*influxdb.Permission, error) {
	return influxdb.NewPermissionAtID(id, a, influxdb.ScraperResourceType, orgID)
}

func authorizeReadScraper(ctx context.Context, orgID, id influxdb.ID) error {
	p, err := newScraperPermission(influxdb.ReadAction, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeWriteScraper(ctx context.Context, orgID, id influxdb.ID) error {
	p, err := newScraperPermission(influxdb.WriteAction, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

// GetTargetByID checks to see if the authorizer on context has read access to the id provided.
func (s *ScraperTargetStoreService) GetTargetByID(ctx context.Context, id influxdb.ID) (*influxdb.ScraperTarget, error) {
	st, err := s.s.GetTargetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeReadScraper(ctx, st.OrgID, id); err != nil {
		return nil, err
	}

	return st, nil
}

// ListTargets retrieves all scraper targets that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *ScraperTargetStoreService) ListTargets(ctx context.Context) ([]influxdb.ScraperTarget, error) {
	// TODO: we'll likely want to push this operation into the database eventually since fetching the whole list of data
	// will likely be expensive.
	ss, err := s.s.ListTargets(ctx)
	if err != nil {
		return nil, err
	}

	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	scrapers := ss[:0]
	for _, st := range ss {
		err := authorizeReadScraper(ctx, st.OrgID, st.ID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, err
		}

		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}

		scrapers = append(scrapers, st)
	}

	return scrapers, nil
}

// AddTarget checks to see if the authorizer on context has write access to the global scraper target resource.
func (s *ScraperTargetStoreService) AddTarget(ctx context.Context, st *influxdb.ScraperTarget) error {
	p, err := influxdb.NewPermission(influxdb.WriteAction, influxdb.ScraperResourceType, st.OrgID)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return s.s.AddTarget(ctx, st)
}

// UpdateTarget checks to see if the authorizer on context has write access to the scraper target provided.
func (s *ScraperTargetStoreService) UpdateTarget(ctx context.Context, upd *influxdb.ScraperTarget) (*influxdb.ScraperTarget, error) {
	st, err := s.s.GetTargetByID(ctx, upd.ID)
	if err != nil {
		return nil, err
	}

	if err := authorizeWriteScraper(ctx, st.OrgID, upd.ID); err != nil {
		return nil, err
	}

	return s.s.UpdateTarget(ctx, upd)
}

// RemoveTarget checks to see if the authorizer on context has write access to the scraper target provided.
func (s *ScraperTargetStoreService) RemoveTarget(ctx context.Context, id influxdb.ID) error {
	st, err := s.s.GetTargetByID(ctx, id)
	if err != nil {
		return err
	}

	if err := authorizeWriteScraper(ctx, st.OrgID, id); err != nil {
		return err
	}

	return s.s.RemoveTarget(ctx, id)
}
