package inmem

import (
	"context"
	"fmt"
	"path"

	"github.com/influxdata/platform"
)

func encodeUserResourceMappingKey(resourceID, userID platform.ID) string {
	return path.Join(resourceID.String(), userID.String())
}

func (s *Service) loadUserResourceMapping(ctx context.Context, resourceID, userID platform.ID) (*platform.UserResourceMapping, error) {
	i, ok := s.userResourceMappingKV.Load(encodeUserResourceMappingKey(resourceID, userID))
	if !ok {
		return nil, fmt.Errorf("userResource mapping not found")
	}

	m, ok := i.(platform.UserResourceMapping)
	if !ok {
		return nil, fmt.Errorf("type %T is not an userResource mapping", i)
	}

	return &m, nil
}

func (s *Service) FindUserResourceBy(ctx context.Context, resourceID, userID platform.ID) (*platform.UserResourceMapping, error) {
	return s.loadUserResourceMapping(ctx, resourceID, userID)
}

func (s *Service) forEachUserResourceMapping(ctx context.Context, fn func(m *platform.UserResourceMapping) bool) error {
	var err error
	s.userResourceMappingKV.Range(func(k, v interface{}) bool {
		m, ok := v.(platform.UserResourceMapping)
		if !ok {
			err = fmt.Errorf("type %T is not a userResource mapping", v)
			return false
		}
		return fn(&m)
	})

	return err
}

func (s *Service) filterUserResourceMappings(ctx context.Context, fn func(m *platform.UserResourceMapping) bool) ([]*platform.UserResourceMapping, error) {
	mappings := []*platform.UserResourceMapping{}
	err := s.forEachUserResourceMapping(ctx, func(m *platform.UserResourceMapping) bool {
		if fn(m) {
			mappings = append(mappings, m)
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	return mappings, nil
}

func (s *Service) FindUserResourceMappings(ctx context.Context, filter platform.UserResourceMappingFilter, opt ...platform.FindOptions) ([]*platform.UserResourceMapping, int, error) {
	if filter.ResourceID.Valid() && filter.UserID.Valid() {
		m, err := s.FindUserResourceBy(ctx, filter.ResourceID, filter.UserID)
		if err != nil {
			return nil, 0, err
		}
		return []*platform.UserResourceMapping{m}, 1, nil
	}

	filterFunc := func(mapping *platform.UserResourceMapping) bool {
		return (!filter.UserID.Valid() || (filter.UserID == mapping.UserID)) &&
			(!filter.ResourceID.Valid() || (filter.ResourceID == mapping.ResourceID)) &&
			(filter.UserType == "" || (filter.UserType == mapping.UserType)) &&
			(filter.ResourceType == "" || (filter.ResourceType == mapping.ResourceType))
	}

	mappings, err := s.filterUserResourceMappings(ctx, filterFunc)
	if err != nil {
		return nil, 0, err
	}

	return mappings, len(mappings), nil
}

func (s *Service) CreateUserResourceMapping(ctx context.Context, m *platform.UserResourceMapping) error {
	mapping, _ := s.FindUserResourceBy(ctx, m.ResourceID, m.UserID)
	if mapping != nil {
		return fmt.Errorf("mapping for user %s already exists", m.UserID)
	}

	s.userResourceMappingKV.Store(encodeUserResourceMappingKey(m.ResourceID, m.UserID), *m)
	return nil
}

func (s *Service) PutUserResourceMapping(ctx context.Context, m *platform.UserResourceMapping) error {
	s.userResourceMappingKV.Store(encodeUserResourceMappingKey(m.ResourceID, m.UserID), *m)
	return nil
}

func (s *Service) DeleteUserResourceMapping(ctx context.Context, resourceID, userID platform.ID) error {
	mapping, err := s.FindUserResourceBy(ctx, resourceID, userID)
	if mapping == nil && err != nil {
		return err
	}

	s.userResourceMappingKV.Delete(encodeUserResourceMappingKey(resourceID, userID))
	return nil
}

func (s *Service) deleteUserResourceMapping(ctx context.Context, filter platform.UserResourceMappingFilter) error {
	mappings, _, err := s.FindUserResourceMappings(ctx, filter)
	if mappings == nil && err != nil {
		return err
	}
	for _, m := range mappings {
		s.userResourceMappingKV.Delete(encodeUserResourceMappingKey(m.ResourceID, m.UserID))
	}

	return nil
}
