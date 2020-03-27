package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
)

type AuthedURMService struct {
	s          influxdb.UserResourceMappingService
	orgService influxdb.OrganizationService
}

func NewAuthedURMService(orgSvc influxdb.OrganizationService, s influxdb.UserResourceMappingService) *AuthedURMService {
	return &AuthedURMService{
		s:          s,
		orgService: orgSvc,
	}
}

func (s *AuthedURMService) FindUserResourceMappings(ctx context.Context, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
	urms, _, err := s.s.FindUserResourceMappings(ctx, filter, opt...)
	if err != nil {
		return nil, 0, err
	}

	authedUrms := urms[:0]
	for _, urm := range urms {
		orgID := orgIDFromContext(ctx)
		if orgID != nil {
			if _, _, err := authorizer.AuthorizeRead(ctx, urm.ResourceType, urm.ResourceID, *orgID); err != nil {
				continue
			}
		} else {
			if _, _, err := authorizer.AuthorizeReadResource(ctx, urm.ResourceType, urm.ResourceID); err != nil {
				continue
			}
		}
		authedUrms = append(authedUrms, urm)
	}

	return authedUrms, len(authedUrms), nil
}

func (s *AuthedURMService) CreateUserResourceMapping(ctx context.Context, m *influxdb.UserResourceMapping) error {
	orgID := orgIDFromContext(ctx)
	if orgID != nil {
		if _, _, err := authorizer.AuthorizeWrite(ctx, m.ResourceType, m.ResourceID, *orgID); err != nil {
			return err
		}
	} else {
		if _, _, err := authorizer.AuthorizeWriteResource(ctx, m.ResourceType, m.ResourceID); err != nil {
			return err
		}
	}

	return s.s.CreateUserResourceMapping(ctx, m)
}

func (s *AuthedURMService) DeleteUserResourceMapping(ctx context.Context, resourceID influxdb.ID, userID influxdb.ID) error {
	if !resourceID.Valid() || !userID.Valid() {
		return ErrInvalidURMID
	}

	f := influxdb.UserResourceMappingFilter{ResourceID: resourceID, UserID: userID}
	urms, _, err := s.s.FindUserResourceMappings(ctx, f)
	if err != nil {
		return err
	}

	// There should only be one because resourceID and userID are used to create the primary key for urms
	for _, urm := range urms {
		orgID := orgIDFromContext(ctx)
		if orgID != nil {
			if _, _, err := authorizer.AuthorizeWrite(ctx, urm.ResourceType, urm.ResourceID, *orgID); err != nil {
				return err
			}
		} else {
			if _, _, err := authorizer.AuthorizeWriteResource(ctx, urm.ResourceType, urm.ResourceID); err != nil {
				return err
			}
		}

		if err := s.s.DeleteUserResourceMapping(ctx, urm.ResourceID, urm.UserID); err != nil {
			return err
		}
	}
	return nil
}
