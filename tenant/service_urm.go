package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
)

type URMSvc struct {
	store *Store
	svc   *Service
}

func NewUserResourceMappingSvc(st *Store, svc *Service) *URMSvc {
	return &URMSvc{
		store: st,
		svc:   svc,
	}
}

// FindUserResourceMappings returns a list of UserResourceMappings that match filter and the total count of matching mappings.
func (s *URMSvc) FindUserResourceMappings(ctx context.Context, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
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
func (s *URMSvc) CreateUserResourceMapping(ctx context.Context, m *influxdb.UserResourceMapping) error {
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		return s.store.CreateURM(ctx, tx, m)
	})
	return err
}

// DeleteUserResourceMapping deletes a user resource mapping.
func (s *URMSvc) DeleteUserResourceMapping(ctx context.Context, resourceID, userID platform.ID) error {
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		_, err := s.store.GetURM(ctx, tx, resourceID, userID)
		if err != nil {
			return ErrURMNotFound
		}
		return s.store.DeleteURM(ctx, tx, resourceID, userID)
	})
	return err
}
