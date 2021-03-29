package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
)

type OrgIDResolver interface {
	FindResourceOrganizationID(ctx context.Context, rt influxdb.ResourceType, id platform.ID) (platform.ID, error)
}

type URMService struct {
	s             influxdb.UserResourceMappingService
	orgIDResolver OrgIDResolver
}

func NewURMService(orgIDResolver OrgIDResolver, s influxdb.UserResourceMappingService) *URMService {
	return &URMService{
		s:             s,
		orgIDResolver: orgIDResolver,
	}
}

func (s *URMService) FindUserResourceMappings(ctx context.Context, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
	urms, _, err := s.s.FindUserResourceMappings(ctx, filter, opt...)
	if err != nil {
		return nil, 0, err
	}
	return AuthorizeFindUserResourceMappings(ctx, s.orgIDResolver, urms)
}

func (s *URMService) CreateUserResourceMapping(ctx context.Context, m *influxdb.UserResourceMapping) error {
	orgID, err := s.orgIDResolver.FindResourceOrganizationID(ctx, m.ResourceType, m.ResourceID)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, m.ResourceType, m.ResourceID, orgID); err != nil {
		return err
	}
	return s.s.CreateUserResourceMapping(ctx, m)
}

func (s *URMService) DeleteUserResourceMapping(ctx context.Context, resourceID platform.ID, userID platform.ID) error {
	f := influxdb.UserResourceMappingFilter{ResourceID: resourceID, UserID: userID}
	urms, _, err := s.s.FindUserResourceMappings(ctx, f)
	if err != nil {
		return err
	}

	for _, urm := range urms {
		orgID, err := s.orgIDResolver.FindResourceOrganizationID(ctx, urm.ResourceType, urm.ResourceID)
		if err != nil {
			return err
		}
		if _, _, err := AuthorizeWrite(ctx, urm.ResourceType, urm.ResourceID, orgID); err != nil {
			return err
		}
		if err := s.s.DeleteUserResourceMapping(ctx, urm.ResourceID, urm.UserID); err != nil {
			return err
		}
	}
	return nil
}
