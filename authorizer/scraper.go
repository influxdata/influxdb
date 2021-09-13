package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

var _ influxdb.ScraperTargetStoreService = (*ScraperTargetStoreService)(nil)

// ScraperTargetStoreService wraps a influxdb.ScraperTargetStoreService and authorizes actions
// against it appropriately.
type ScraperTargetStoreService struct {
	influxdb.UserResourceMappingService
	influxdb.OrganizationService
	s influxdb.ScraperTargetStoreService
}

// NewScraperTargetStoreService constructs an instance of an authorizing scraper target store service.
func NewScraperTargetStoreService(s influxdb.ScraperTargetStoreService,
	urm influxdb.UserResourceMappingService,
	org influxdb.OrganizationService,
) *ScraperTargetStoreService {
	return &ScraperTargetStoreService{
		UserResourceMappingService: urm,
		s:                          s,
	}
}

// GetTargetByID checks to see if the authorizer on context has read access to the id provided.
func (s *ScraperTargetStoreService) GetTargetByID(ctx context.Context, id platform.ID) (*influxdb.ScraperTarget, error) {
	st, err := s.s.GetTargetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.ScraperResourceType, id, st.OrgID); err != nil {
		return nil, err
	}
	return st, nil
}

// ListTargets retrieves all scraper targets that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *ScraperTargetStoreService) ListTargets(ctx context.Context, filter influxdb.ScraperTargetFilter) ([]influxdb.ScraperTarget, error) {
	// TODO: we'll likely want to push this operation into the database eventually since fetching the whole list of data
	// will likely be expensive.
	ss, err := s.s.ListTargets(ctx, filter)
	if err != nil {
		return nil, err
	}
	ss, _, err = AuthorizeFindScrapers(ctx, ss)
	return ss, err
}

// AddTarget checks to see if the authorizer on context has write access to the global scraper target resource.
func (s *ScraperTargetStoreService) AddTarget(ctx context.Context, st *influxdb.ScraperTarget, userID platform.ID) error {
	if _, _, err := AuthorizeCreate(ctx, influxdb.ScraperResourceType, st.OrgID); err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.BucketsResourceType, st.BucketID, st.OrgID); err != nil {
		return err
	}
	return s.s.AddTarget(ctx, st, userID)
}

// UpdateTarget checks to see if the authorizer on context has write access to the scraper target provided.
func (s *ScraperTargetStoreService) UpdateTarget(ctx context.Context, upd *influxdb.ScraperTarget, userID platform.ID) (*influxdb.ScraperTarget, error) {
	st, err := s.s.GetTargetByID(ctx, upd.ID)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.ScraperResourceType, upd.ID, st.OrgID); err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.BucketsResourceType, st.BucketID, st.OrgID); err != nil {
		return nil, err
	}
	return s.s.UpdateTarget(ctx, upd, userID)
}

// RemoveTarget checks to see if the authorizer on context has write access to the scraper target provided.
func (s *ScraperTargetStoreService) RemoveTarget(ctx context.Context, id platform.ID) error {
	st, err := s.s.GetTargetByID(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.ScraperResourceType, st.ID, st.OrgID); err != nil {
		return err
	}
	return s.s.RemoveTarget(ctx, id)
}
