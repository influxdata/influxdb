package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

var _ influxdb.CheckService = (*CheckService)(nil)

// CheckService wraps a influxdb.CheckService and authorizes actions
// against it appropriately.
type CheckService struct {
	s influxdb.CheckService
	influxdb.UserResourceMappingService
	influxdb.OrganizationService
	influxdb.TaskService
}

// NewCheckService constructs an instance of an authorizing check serivce.
func NewCheckService(s influxdb.CheckService, urm influxdb.UserResourceMappingService, org influxdb.OrganizationService) *CheckService {
	return &CheckService{
		s:                          s,
		UserResourceMappingService: urm,
		OrganizationService:        org,
	}
}

// FindCheckByID checks to see if the authorizer on context has read access to the id provided.
func (s *CheckService) FindCheckByID(ctx context.Context, id influxdb.ID) (influxdb.Check, error) {
	chk, err := s.s.FindCheckByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.ChecksResourceType, chk.GetID(), chk.GetOrgID()); err != nil {
		return nil, err
	}
	return chk, nil
}

// FindChecks retrieves all checks that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *CheckService) FindChecks(ctx context.Context, filter influxdb.CheckFilter, opt ...influxdb.FindOptions) ([]influxdb.Check, int, error) {
	if _, _, err := AuthorizeOrgReadResource(ctx, influxdb.ChecksResourceType, filter.OrgID); err != nil {
		return nil, 0, err
	}

	return s.s.FindChecks(ctx, filter, opt...)
}

// FindCheck will return the check.
func (s *CheckService) FindCheck(ctx context.Context, filter influxdb.CheckFilter) (influxdb.Check, error) {
	chk, err := s.s.FindCheck(ctx, filter)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.ChecksResourceType, chk.GetID(), chk.GetOrgID()); err != nil {
		return nil, err
	}
	return chk, nil
}

// CreateCheck checks to see if the authorizer on context has write access to the global check resource.
func (s *CheckService) CreateCheck(ctx context.Context, chk influxdb.CheckCreate, userID influxdb.ID) error {
	orgID := chk.GetOrgID()
	if _, _, err := AuthorizeCreate(ctx, influxdb.ChecksResourceType, &orgID); err != nil {
		return err
	}
	return s.s.CreateCheck(ctx, chk, userID)
}

// UpdateCheck checks to see if the authorizer on context has write access to the check provided.
func (s *CheckService) UpdateCheck(ctx context.Context, id influxdb.ID, upd influxdb.CheckCreate) (influxdb.Check, error) {
	chk, err := s.FindCheckByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.ChecksResourceType, chk.GetID(), chk.GetOrgID()); err != nil {
		return nil, err
	}
	return s.s.UpdateCheck(ctx, id, upd)
}

// PatchCheck checks to see if the authorizer on context has write access to the check provided.
func (s *CheckService) PatchCheck(ctx context.Context, id influxdb.ID, upd influxdb.CheckUpdate) (influxdb.Check, error) {
	chk, err := s.FindCheckByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.ChecksResourceType, chk.GetID(), chk.GetOrgID()); err != nil {
		return nil, err
	}
	return s.s.PatchCheck(ctx, id, upd)
}

// DeleteCheck checks to see if the authorizer on context has write access to the check provided.
func (s *CheckService) DeleteCheck(ctx context.Context, id influxdb.ID) error {
	chk, err := s.FindCheckByID(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.ChecksResourceType, chk.GetID(), chk.GetOrgID()); err != nil {
		return err
	}
	return s.s.DeleteCheck(ctx, id)
}
