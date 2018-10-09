package bolt

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/coreos/bbolt"
	"github.com/influxdata/platform"
)

var (
	userResourceMappingBucket = []byte("userresourcemappingsv1")
)

func (c *Client) initializeUserResourceMappings(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(userResourceMappingBucket)); err != nil {
		return err
	}
	return nil
}

func filterMappingsFn(filter platform.UserResourceMappingFilter) func(m *platform.UserResourceMapping) bool {
	return func(mapping *platform.UserResourceMapping) bool {
		return (filter.UserID == nil || (filter.UserID.String()) == mapping.UserID.String()) &&
			(filter.ResourceID == nil || (filter.ResourceID.String()) == mapping.ResourceID.String()) &&
			(filter.UserType == "" || (filter.UserType == mapping.UserType)) &&
			(filter.ResourceType == "" || (filter.ResourceType == mapping.ResourceType))
	}
}

func (c *Client) FindUserResourceMappings(ctx context.Context, filter platform.UserResourceMappingFilter, opt ...platform.FindOptions) ([]*platform.UserResourceMapping, int, error) {
	ms := []*platform.UserResourceMapping{}
	err := c.db.View(func(tx *bolt.Tx) error {
		mappings, err := c.findUserResourceMappings(ctx, tx, filter)
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

func (c *Client) findUserResourceMappings(ctx context.Context, tx *bolt.Tx, filter platform.UserResourceMappingFilter) ([]*platform.UserResourceMapping, error) {
	ms := []*platform.UserResourceMapping{}
	filterFn := filterMappingsFn(filter)
	err := c.forEachUserResourceMapping(ctx, tx, func(m *platform.UserResourceMapping) bool {
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

func (c *Client) findUserResourceMapping(ctx context.Context, tx *bolt.Tx, resourceID platform.ID, userID platform.ID) (*platform.UserResourceMapping, error) {
	var m platform.UserResourceMapping

	key := userResourceKey(&platform.UserResourceMapping{
		ResourceID: resourceID,
		UserID:     userID,
	})

	v := tx.Bucket(userResourceMappingBucket).Get(key)

	if len(v) == 0 {
		return nil, fmt.Errorf("userResource mapping not found")
	}

	if err := json.Unmarshal(v, &m); err != nil {
		return nil, err
	}

	return &m, nil
}

func (c *Client) CreateUserResourceMapping(ctx context.Context, m *platform.UserResourceMapping) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		unique := c.uniqueUserResourceMapping(ctx, tx, m)

		if !unique {
			return fmt.Errorf("mapping for user %s already exists", m.UserID.String())
		}

		v, err := json.Marshal(m)
		if err != nil {
			return err
		}

		if err := tx.Bucket(userResourceMappingBucket).Put(userResourceKey(m), v); err != nil {
			return err
		}

		return nil
	})
}

func userResourceKey(m *platform.UserResourceMapping) []byte {
	k := make([]byte, len(m.ResourceID)+len(m.UserID))
	copy(k, m.ResourceID)
	copy(k[len(m.ResourceID):], []byte(m.UserID))
	return k
}

func (c *Client) forEachUserResourceMapping(ctx context.Context, tx *bolt.Tx, fn func(*platform.UserResourceMapping) bool) error {
	cur := tx.Bucket(userResourceMappingBucket).Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		m := &platform.UserResourceMapping{}
		if err := json.Unmarshal(v, m); err != nil {
			return err
		}
		if !fn(m) {
			break
		}
	}

	return nil
}

func (c *Client) uniqueUserResourceMapping(ctx context.Context, tx *bolt.Tx, m *platform.UserResourceMapping) bool {
	v := tx.Bucket(userResourceMappingBucket).Get(userResourceKey(m))
	return len(v) == 0
}

func (c *Client) DeleteUserResourceMapping(ctx context.Context, resourceID platform.ID, userID platform.ID) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.deleteUserResourceMapping(ctx, tx, resourceID, userID)
	})
}

func (c *Client) deleteUserResourceMapping(ctx context.Context, tx *bolt.Tx, resourceID platform.ID, userID platform.ID) error {
	m, err := c.findUserResourceMapping(ctx, tx, resourceID, userID)
	if err != nil {
		return err
	}

	return tx.Bucket(userResourceMappingBucket).Delete(userResourceKey(m))
}
