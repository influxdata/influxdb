package tenant

import (
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
)

var urmBucket = []byte("userresourcemappingsv1")

// NOTE(affo): On URM creation, we check that the user exists.
// We do not check that the resource it is pointing to exists.
// This decision takes into account that different resources could not be in the same store.
// To perform that kind of check, we must rely on the service layer.
// However, we do not want having the storage layer depend on the service layer above.
func (s *Store) CreateURM(ctx context.Context, tx kv.Tx, urm *influxdb.UserResourceMapping) error {
	if _, err := s.GetUser(ctx, tx, urm.UserID); err != nil {
		return err
	}
	if err := s.uniqueUserResourceMapping(ctx, tx, urm); err != nil {
		return err
	}

	v, err := json.Marshal(urm)
	if err != nil {
		return ErrUnprocessableMapping(err)
	}

	key, err := userResourceKey(urm.ResourceID, urm.UserID)
	if err != nil {
		return err
	}

	b, err := tx.Bucket(urmBucket)
	if err != nil {
		return UnavailableURMServiceError(err)
	}

	if err := b.Put(key, v); err != nil {
		return UnavailableURMServiceError(err)
	}

	// insert urm into by user index
	userID, err := urm.UserID.Encode()
	if err != nil {
		return err
	}
	if err := s.urmByUserIndex.Insert(tx, userID, key); err != nil {
		return err
	}

	return nil
}

func (s *Store) ListURMs(ctx context.Context, tx kv.Tx, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, error) {
	ms := []*influxdb.UserResourceMapping{}

	b, err := tx.Bucket(urmBucket)
	if err != nil {
		return nil, UnavailableURMServiceError(err)
	}

	filterFn := func(m *influxdb.UserResourceMapping) bool {
		return (!filter.UserID.Valid() || (filter.UserID == m.UserID)) &&
			(!filter.ResourceID.Valid() || (filter.ResourceID == m.ResourceID)) &&
			(filter.UserType == "" || (filter.UserType == m.UserType)) &&
			(filter.ResourceType == "" || (filter.ResourceType == m.ResourceType))
	}

	if filter.UserID.Valid() {
		var (
			// urm by user index lookup
			userID, _ = filter.UserID.Encode()
			seen      int
		)

		err := s.urmByUserIndex.Walk(ctx, tx, userID, func(k, v []byte) (bool, error) {
			m := &influxdb.UserResourceMapping{}
			if err := json.Unmarshal(v, m); err != nil {
				return false, CorruptURMError(err)
			}

			// respect offset parameter
			reachedOffset := (len(opt) == 0 || seen >= opt[0].Offset)
			if reachedOffset && filterFn(m) {
				ms = append(ms, m)
			}

			seen++

			return (len(opt) == 0 || opt[0].Limit <= 0 || len(ms) < opt[0].Limit), nil
		})

		return ms, err
	}

	// for now the best we can do is use the resourceID if we have that as a forward cursor option
	var prefix []byte
	var cursorOptions []kv.CursorOption

	if filter.ResourceID.Valid() {
		p, err := userResourcePrefixKey(filter.ResourceID)
		if err != nil {
			return nil, err
		}
		prefix = p
		cursorOptions = append(cursorOptions, kv.WithCursorPrefix(p))
	}
	cur, err := b.ForwardCursor(prefix, cursorOptions...)
	if err != nil {
		return nil, err
	}
	defer cur.Close()

	for k, v := cur.Next(); k != nil; k, v = cur.Next() {
		m := &influxdb.UserResourceMapping{}
		if err := json.Unmarshal(v, m); err != nil {
			return nil, CorruptURMError(err)
		}

		// check to see if it matches the filter
		if filterFn(m) {
			ms = append(ms, m)
		}

		if len(opt) > 0 && opt[0].Limit > 0 && len(ms) >= opt[0].Limit {
			break
		}
	}

	return ms, cur.Err()
}

func (s *Store) GetURM(ctx context.Context, tx kv.Tx, resourceID, userID platform.ID) (*influxdb.UserResourceMapping, error) {
	key, err := userResourceKey(resourceID, userID)
	if err != nil {
		return nil, err
	}

	b, err := tx.Bucket(urmBucket)
	if err != nil {
		return nil, UnavailableURMServiceError(err)
	}

	val, err := b.Get(key)
	if err != nil {
		return nil, err
	}

	m := &influxdb.UserResourceMapping{}
	if err := json.Unmarshal(val, m); err != nil {
		return nil, CorruptURMError(err)
	}
	return m, nil
}

func (s *Store) DeleteURM(ctx context.Context, tx kv.Tx, resourceID, userID platform.ID) error {
	key, err := userResourceKey(resourceID, userID)
	if err != nil {
		return err
	}

	b, err := tx.Bucket(urmBucket)
	if err != nil {
		return err
	}

	// remove user resource mapping from by user index
	uid, err := userID.Encode()
	if err != nil {
		return err
	}

	if err := s.urmByUserIndex.Delete(tx, uid, key); err != nil {
		return err
	}

	return b.Delete(key)
}

func userResourcePrefixKey(resourceID platform.ID) ([]byte, error) {
	encodedResourceID, err := resourceID.Encode()
	if err != nil {
		return nil, ErrInvalidURMID
	}
	return encodedResourceID, nil
}

func userResourceKey(resourceID, userID platform.ID) ([]byte, error) {
	encodedResourceID, err := resourceID.Encode()
	if err != nil {
		return nil, ErrInvalidURMID
	}

	encodedUserID, err := userID.Encode()
	if err != nil {
		return nil, ErrInvalidURMID
	}

	key := make([]byte, len(encodedResourceID)+len(encodedUserID))
	copy(key, encodedResourceID)
	copy(key[len(encodedResourceID):], encodedUserID)

	return key, nil
}

func (s *Store) uniqueUserResourceMapping(ctx context.Context, tx kv.Tx, m *influxdb.UserResourceMapping) error {
	key, err := userResourceKey(m.ResourceID, m.UserID)
	if err != nil {
		return err
	}

	b, err := tx.Bucket(urmBucket)
	if err != nil {
		return UnavailableURMServiceError(err)
	}

	_, err = b.Get(key)
	if !kv.IsNotFound(err) {
		return NonUniqueMappingError(m.UserID)
	}

	return nil
}
