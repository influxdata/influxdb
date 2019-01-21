package authorizer

import (
	"context"

	"github.com/influxdata/influxdb"
)

type OrganizationService interface {
	FindResourceOrganizationID(ctx context.Context, rt influxdb.ResourceType, id influxdb.ID) (influxdb.ID, error)
}

type URMService struct {
	s          influxdb.UserResourceMappingService
	orgService OrganizationService
}

func NewURMService(orgSvc OrganizationService, s influxdb.UserResourceMappingService) *URMService {
	return &URMService{
		s:          s,
		orgService: orgSvc,
	}
}

func newURMPermission(a influxdb.Action, rt influxdb.ResourceType, orgID, id influxdb.ID) (*influxdb.Permission, error) {
	return influxdb.NewPermissionAtID(id, a, rt, orgID)
}

func authorizeReadURM(ctx context.Context, rt influxdb.ResourceType, orgID, id influxdb.ID) error {
	p, err := newURMPermission(influxdb.ReadAction, rt, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeWriteURM(ctx context.Context, rt influxdb.ResourceType, orgID, id influxdb.ID) error {
	p, err := newURMPermission(influxdb.WriteAction, rt, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func (s *URMService) FindUserResourceMappings(ctx context.Context, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
	urms, _, err := s.s.FindUserResourceMappings(ctx, filter, opt...)
	if err != nil {
		return nil, 0, err
	}

	mappings := urms[:0]
	for _, urm := range urms {
		orgID, err := s.orgService.FindResourceOrganizationID(ctx, urm.ResourceType, urm.ResourceID)
		if err != nil {
			return nil, 0, err
		}

		if err := authorizeReadURM(ctx, urm.ResourceType, orgID, urm.ResourceID); err != nil {
			continue
		}

		mappings = append(mappings, urm)
	}

	return mappings, len(mappings), nil
}

func (s *URMService) CreateUserResourceMapping(ctx context.Context, m *influxdb.UserResourceMapping) error {
	orgID, err := s.orgService.FindResourceOrganizationID(ctx, m.ResourceType, m.ResourceID)
	if err != nil {
		return err
	}

	if err := authorizeWriteURM(ctx, m.ResourceType, orgID, m.ResourceID); err != nil {
		return err
	}

	return s.s.CreateUserResourceMapping(ctx, m)
}

func (s *URMService) DeleteUserResourceMapping(ctx context.Context, resourceID influxdb.ID, userID influxdb.ID) error {
	f := influxdb.UserResourceMappingFilter{ResourceID: resourceID, UserID: userID}
	urms, _, err := s.s.FindUserResourceMappings(ctx, f)
	if err != nil {
		return err
	}

	for _, urm := range urms {
		orgID, err := s.orgService.FindResourceOrganizationID(ctx, urm.ResourceType, urm.ResourceID)
		if err != nil {
			return err
		}

		if err := authorizeWriteURM(ctx, urm.ResourceType, orgID, urm.ResourceID); err != nil {
			return err
		}

		if err := s.s.DeleteUserResourceMapping(ctx, urm.ResourceID, urm.UserID); err != nil {
			return err
		}
	}

	return nil
}
