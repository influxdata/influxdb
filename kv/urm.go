package kv

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb"
)

var (
	urmBucket = []byte("userresourcemappingsv1")
)

func (s *Service) initializeURMs(ctx context.Context, tx Tx) error {
	if _, err := tx.Bucket(urmBucket); err != nil {
		return err
	}
	return nil
}

func filterMappingsFn(filter influxdb.UserResourceMappingFilter) func(m *influxdb.UserResourceMapping) bool {
	return func(mapping *influxdb.UserResourceMapping) bool {
		return (!filter.UserID.Valid() || (filter.UserID == mapping.UserID)) &&
			(!filter.ResourceID.Valid() || (filter.ResourceID == mapping.ResourceID)) &&
			(filter.UserType == "" || (filter.UserType == mapping.UserType)) &&
			(filter.ResourceType == "" || (filter.ResourceType == mapping.ResourceType))
	}
}

// FindUserResourceMappings returns a list of UserResourceMappings that match filter and the total count of matching mappings.
func (s *Service) FindUserResourceMappings(ctx context.Context, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
	ms := []*influxdb.UserResourceMapping{}
	err := s.kv.View(func(tx Tx) error {
		mappings, err := s.findUserResourceMappings(ctx, tx, filter)
		if err != nil {
			return err
		}
		ms = mappings
		return nil
	})

	if err != nil {
		return nil, 0, err
	}

	return ms, len(ms), nil
}

func (s *Service) findUserResourceMappings(ctx context.Context, tx Tx, filter influxdb.UserResourceMappingFilter) ([]*influxdb.UserResourceMapping, error) {
	ms := []*influxdb.UserResourceMapping{}
	filterFn := filterMappingsFn(filter)
	err := s.forEachUserResourceMapping(ctx, tx, func(m *influxdb.UserResourceMapping) bool {
		if filterFn(m) {
			ms = append(ms, m)
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	return ms, nil
}

func (s *Service) findUserResourceMapping(ctx context.Context, tx Tx, filter influxdb.UserResourceMappingFilter) (*influxdb.UserResourceMapping, error) {
	ms, err := s.findUserResourceMappings(ctx, tx, filter)
	if err != nil {
		return nil, err
	}

	if len(ms) == 0 {
		return nil, fmt.Errorf("userResource mapping not found")
	}

	return ms[0], nil
}

func (s *Service) CreateUserResourceMapping(ctx context.Context, m *influxdb.UserResourceMapping) error {
	return s.kv.Update(func(tx Tx) error {
		if err := s.createUserResourceMapping(ctx, tx, m); err != nil {
			return err
		}

		if m.ResourceType == influxdb.OrgsResourceType {
			return s.createOrgDependentMappings(ctx, tx, m)
		}

		return nil
	})
}

func (s *Service) createUserResourceMapping(ctx context.Context, tx Tx, m *influxdb.UserResourceMapping) error {
	unique := s.uniqueUserResourceMapping(ctx, tx, m)

	if !unique {
		return fmt.Errorf("mapping for user %s already exists", m.UserID.String())
	}

	v, err := json.Marshal(m)
	if err != nil {
		return err
	}

	key, err := userResourceKey(m)
	if err != nil {
		return err
	}

	b, err := tx.Bucket(urmBucket)
	if err != nil {
		return err
	}

	if err := b.Put(key, v); err != nil {
		return err
	}

	return nil
}

// This method creates the user/resource mappings for resources that belong to an organization.
func (s *Service) createOrgDependentMappings(ctx context.Context, tx Tx, m *influxdb.UserResourceMapping) error {
	bf := influxdb.BucketFilter{OrganizationID: &m.ResourceID}
	bs, err := s.findBuckets(ctx, tx, bf)
	if err != nil {
		return err
	}
	for _, b := range bs {
		m := &influxdb.UserResourceMapping{
			ResourceType: influxdb.BucketsResourceType,
			ResourceID:   b.ID,
			UserType:     m.UserType,
			UserID:       m.UserID,
		}
		if err := s.createUserResourceMapping(ctx, tx, m); err != nil {
			return err
		}
		// TODO(desa): add support for all other resource types.
	}

	return nil
}

func userResourceKey(m *influxdb.UserResourceMapping) ([]byte, error) {
	encodedResourceID, err := m.ResourceID.Encode()
	if err != nil {
		return nil, err
	}

	encodedUserID, err := m.UserID.Encode()
	if err != nil {
		return nil, err
	}

	key := make([]byte, len(encodedResourceID)+len(encodedUserID))
	copy(key, encodedResourceID)
	copy(key[len(encodedResourceID):], encodedUserID)

	return key, nil
}

func (s *Service) forEachUserResourceMapping(ctx context.Context, tx Tx, fn func(*influxdb.UserResourceMapping) bool) error {
	b, err := tx.Bucket(urmBucket)
	if err != nil {
		return err
	}

	cur, err := b.Cursor()
	if err != nil {
		return err
	}

	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		m := &influxdb.UserResourceMapping{}
		if err := json.Unmarshal(v, m); err != nil {
			return err
		}
		if !fn(m) {
			break
		}
	}

	return nil
}

func (s *Service) uniqueUserResourceMapping(ctx context.Context, tx Tx, m *influxdb.UserResourceMapping) bool {
	key, err := userResourceKey(m)
	if err != nil {
		return false
	}

	b, err := tx.Bucket(urmBucket)
	if err != nil {
		return false
	}

	_, err = b.Get(key)
	return IsNotFound(err)
}

// DeleteUserResourceMapping deletes a user resource mapping.
func (s *Service) DeleteUserResourceMapping(ctx context.Context, resourceID influxdb.ID, userID influxdb.ID) error {
	return s.kv.Update(func(tx Tx) error {
		m, err := s.findUserResourceMapping(ctx, tx, influxdb.UserResourceMappingFilter{
			ResourceID: resourceID,
			UserID:     userID,
		})
		if err != nil {
			return err
		}

		if err := s.deleteUserResourceMapping(ctx, tx, influxdb.UserResourceMappingFilter{
			ResourceID: resourceID,
			UserID:     userID,
		}); err != nil {
			return err
		}

		if m.ResourceType == influxdb.OrgsResourceType {
			return s.deleteOrgDependentMappings(ctx, tx, m)
		}

		return nil
	})
}

func (s *Service) deleteUserResourceMapping(ctx context.Context, tx Tx, filter influxdb.UserResourceMappingFilter) error {
	ms, err := s.findUserResourceMappings(ctx, tx, filter)
	if err != nil {
		return err
	}
	if len(ms) == 0 {
		return fmt.Errorf("userResource mapping not found")
	}

	key, err := userResourceKey(ms[0])
	if err != nil {
		return err
	}

	b, err := tx.Bucket(urmBucket)
	if err != nil {
		return err
	}

	return b.Delete(key)
}

func (s *Service) deleteUserResourceMappings(ctx context.Context, tx Tx, filter influxdb.UserResourceMappingFilter) error {
	ms, err := s.findUserResourceMappings(ctx, tx, filter)
	if err != nil {
		return err
	}
	for _, m := range ms {
		key, err := userResourceKey(m)
		if err != nil {
			return err
		}

		b, err := tx.Bucket(urmBucket)
		if err != nil {
			return err
		}

		if err = b.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// This method deletes the user/resource mappings for resources that belong to an organization.
func (s *Service) deleteOrgDependentMappings(ctx context.Context, tx Tx, m *influxdb.UserResourceMapping) error {
	bf := influxdb.BucketFilter{OrganizationID: &m.ResourceID}
	bs, err := s.findBuckets(ctx, tx, bf)
	if err != nil {
		return err
	}
	for _, b := range bs {
		if err := s.deleteUserResourceMapping(ctx, tx, influxdb.UserResourceMappingFilter{
			ResourceType: influxdb.BucketsResourceType,
			ResourceID:   b.ID,
			UserID:       m.UserID,
		}); err != nil {
			return err
		}
		// TODO(desa): add support for all other resource types.
	}

	return nil
}
