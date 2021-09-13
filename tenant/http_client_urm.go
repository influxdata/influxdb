package tenant

import (
	"context"
	"path"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
)

type UserResourceMappingClient struct {
	Client *httpc.Client
}

// CreateUserResourceMapping will create a user resource mapping
func (s *UserResourceMappingClient) CreateUserResourceMapping(ctx context.Context, m *influxdb.UserResourceMapping) error {
	if err := m.Validate(); err != nil {
		return err
	}

	urlPath := resourceIDPath(m.ResourceType, m.ResourceID, string(m.UserType)+"s")
	return s.Client.
		PostJSON(influxdb.User{ID: m.UserID}, urlPath).
		DecodeJSON(m).
		Do(ctx)
}

// FindUserResourceMappings returns the user resource mappings
func (s *UserResourceMappingClient) FindUserResourceMappings(ctx context.Context, f influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
	var results resourceUsersResponse
	err := s.Client.
		Get(resourceIDPath(f.ResourceType, f.ResourceID, string(f.UserType)+"s")).
		DecodeJSON(&results).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	urs := make([]*influxdb.UserResourceMapping, len(results.Users))
	for k, item := range results.Users {
		urs[k] = &influxdb.UserResourceMapping{
			ResourceID:   f.ResourceID,
			ResourceType: f.ResourceType,
			UserID:       item.User.ID,
			UserType:     item.Role,
		}
	}
	return urs, len(urs), nil
}

// DeleteUserResourceMapping will delete user resource mapping based in criteria.
func (s *UserResourceMappingClient) DeleteUserResourceMapping(ctx context.Context, resourceID platform.ID, userID platform.ID) error {
	urlPath := resourceIDUserPath(influxdb.OrgsResourceType, resourceID, influxdb.Member, userID)
	return s.Client.
		Delete(urlPath).
		Do(ctx)
}

// SpecificURMSvc returns a urm service with specific resource and user types.
// this will help us stay compatible with the existing service contract but also allow for urm deletes to go through the correct
// api
func (s *UserResourceMappingClient) SpecificURMSvc(rt influxdb.ResourceType, ut influxdb.UserType) *SpecificURMSvc {
	return &SpecificURMSvc{
		Client: s.Client,
		rt:     rt,
		ut:     ut,
	}
}

// SpecificURMSvc is a URM client that speaks to a specific resource with a specified user type
type SpecificURMSvc struct {
	Client *httpc.Client
	rt     influxdb.ResourceType
	ut     influxdb.UserType
}

// FindUserResourceMappings returns the user resource mappings
func (s *SpecificURMSvc) FindUserResourceMappings(ctx context.Context, f influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
	var results resourceUsersResponse
	err := s.Client.
		Get(resourceIDPath(s.rt, f.ResourceID, string(s.ut)+"s")).
		DecodeJSON(&results).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	urs := make([]*influxdb.UserResourceMapping, len(results.Users))
	for k, item := range results.Users {
		urs[k] = &influxdb.UserResourceMapping{
			ResourceID:   f.ResourceID,
			ResourceType: f.ResourceType,
			UserID:       item.User.ID,
			UserType:     item.Role,
		}
	}
	return urs, len(urs), nil
}

// CreateUserResourceMapping will create a user resource mapping
func (s *SpecificURMSvc) CreateUserResourceMapping(ctx context.Context, m *influxdb.UserResourceMapping) error {
	if err := m.Validate(); err != nil {
		return err
	}

	urlPath := resourceIDPath(s.rt, m.ResourceID, string(s.ut)+"s")
	return s.Client.
		PostJSON(influxdb.User{ID: m.UserID}, urlPath).
		DecodeJSON(m).
		Do(ctx)
}

// DeleteUserResourceMapping will delete user resource mapping based in criteria.
func (s *SpecificURMSvc) DeleteUserResourceMapping(ctx context.Context, resourceID platform.ID, userID platform.ID) error {
	urlPath := resourceIDUserPath(s.rt, resourceID, s.ut, userID)
	return s.Client.
		Delete(urlPath).
		Do(ctx)
}

func resourceIDPath(resourceType influxdb.ResourceType, resourceID platform.ID, p string) string {
	return path.Join("/api/v2/", string(resourceType), resourceID.String(), p)
}

func resourceIDUserPath(resourceType influxdb.ResourceType, resourceID platform.ID, userType influxdb.UserType, userID platform.ID) string {
	return path.Join("/api/v2/", string(resourceType), resourceID.String(), string(userType)+"s", userID.String())
}
