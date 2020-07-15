package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

// FindUserResourceMappings returns a list of UserResourceMappings that match filter and the total count of matching mappings.
func (s *Service) FindUserResourceMappings(ctx context.Context, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
	var urms []*influxdb.UserResourceMapping
	err := s.store.View(ctx, func(tx kv.Tx) error {
		u, err := s.store.ListURMs(ctx, tx, filter, opt...)
		if err != nil {
			return err
		}
		urms = u
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return urms, len(urms), nil
}

// CreateUserResourceMapping creates a user resource mapping.
func (s *Service) CreateUserResourceMapping(ctx context.Context, m *influxdb.UserResourceMapping) error {
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		return s.store.CreateURM(ctx, tx, m)
	})
	return err
}

// DeleteUserResourceMapping deletes a user resource mapping.
func (s *Service) DeleteUserResourceMapping(ctx context.Context, resourceID, userID influxdb.ID) error {
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		_, err := s.store.GetURM(ctx, tx, resourceID, userID)
		if err != nil {
			return ErrURMNotFound
		}
		return s.store.DeleteURM(ctx, tx, resourceID, userID)
	})
	return err
}

// removeResourceRelations allows us to clean up any resource relationship that would have normally been left over after a delete action of a resource.
func (s *Service) removeResourceRelations(ctx context.Context, tx kv.Tx, resourceID influxdb.ID) error {
	urms, err := s.store.ListURMs(ctx, tx, influxdb.UserResourceMappingFilter{
		ResourceID: resourceID,
	})
	if err != nil {
		return err
	}
	for _, urm := range urms {
		err := s.store.DeleteURM(ctx, tx, urm.ResourceID, urm.UserID)
		if err != nil && err != ErrURMNotFound {
			return err
		}
	}
	return nil
}

func permissionFromMapping(mappings []*influxdb.UserResourceMapping) ([]influxdb.Permission, error) {
	ps := make([]influxdb.Permission, 0, len(mappings))
	for _, m := range mappings {
		p, err := m.ToPermissions()
		if err != nil {
			return nil, &influxdb.Error{
				Err: err,
			}
		}

		ps = append(ps, p...)
	}

	return ps, nil
}
