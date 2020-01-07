package authorizer

import (
	"context"

	"github.com/influxdata/influxdb"
)

var _ influxdb.NotificationEndpointService = (*NotificationEndpointService)(nil)

// NotificationEndpointService wraps a influxdb.NotificationEndpointService and authorizes actions
// against it appropriately.
type NotificationEndpointService struct {
	s influxdb.NotificationEndpointService
	influxdb.UserResourceMappingService
	influxdb.OrganizationService
}

// NewNotificationEndpointService constructs an instance of an authorizing notification endpoint serivce.
func NewNotificationEndpointService(
	s influxdb.NotificationEndpointService,
	urm influxdb.UserResourceMappingService,
	org influxdb.OrganizationService,
) *NotificationEndpointService {
	return &NotificationEndpointService{
		s:                          s,
		UserResourceMappingService: urm,
		OrganizationService:        org,
	}
}

// FindByID checks to see if the authorizer on context has read access to the id provided.
func (s *NotificationEndpointService) FindByID(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
	edp, err := s.s.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeReadOrg(ctx, edp.GetOrgID()); err != nil {
		return nil, err
	}

	return edp, nil
}

// Find retrieves all notification endpoints that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *NotificationEndpointService) Find(ctx context.Context, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, error) {
	// TODO: This is a temporary fix as to not fetch the entire collection when no filter is provided.
	if !filter.UserID.Valid() && filter.OrgID == nil {
		return nil, &influxdb.Error{
			Code: influxdb.EUnauthorized,
			Msg:  "cannot process a request without a org or user filter",
		}
	}

	// TODO: we'll likely want to push this operation into the database eventually since fetching the whole list of data
	// will likely be expensive.
	edps, err := s.s.Find(ctx, filter, opt...)
	if err != nil {
		return nil, err
	}

	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	endpoints := edps[:0]
	for _, edp := range edps {
		err := authorizeReadOrg(ctx, edp.GetOrgID())
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, err
		}

		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}

		endpoints = append(endpoints, edp)
	}

	return endpoints, nil
}

// Create checks to see if the authorizer on context has write access to the global notification endpoint resource.
func (s *NotificationEndpointService) Create(ctx context.Context, userID influxdb.ID, edp influxdb.NotificationEndpoint) error {
	p, err := influxdb.NewPermission(influxdb.WriteAction, influxdb.NotificationEndpointResourceType, edp.GetOrgID())
	if err != nil {
		return err
	}

	pOrg, err := newOrgPermission(influxdb.WriteAction, edp.GetOrgID())
	if err != nil {
		return err
	}

	err0 := IsAllowed(ctx, *p)
	err1 := IsAllowed(ctx, *pOrg)

	if err0 != nil && err1 != nil {
		return err0
	}

	return s.s.Create(ctx, userID, edp)
}

// Update checks to see if the authorizer on context has write access to the notification endpoint provided.
func (s *NotificationEndpointService) Update(ctx context.Context, update influxdb.EndpointUpdate) (influxdb.NotificationEndpoint, error) {
	edp, err := s.FindByID(ctx, update.ID)
	if err != nil {
		return nil, err
	}

	if err := authorizeWriteOrg(ctx, edp.GetOrgID()); err != nil {
		return nil, err
	}

	return s.s.Update(ctx, update)
}

// Delete checks to see if the authorizer on context has write access to the notification endpoint provided.
func (s *NotificationEndpointService) Delete(ctx context.Context, id influxdb.ID) error {
	edp, err := s.FindByID(ctx, id)
	if err != nil {
		return err
	}

	if err := authorizeWriteOrg(ctx, edp.GetOrgID()); err != nil {
		return err
	}

	return s.s.Delete(ctx, id)
}
