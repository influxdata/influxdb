package tenant

import (
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
)

var urmBucket = []byte("userresourcemappingsv1")

func (s *Store) CreateURM(ctx context.Context, tx *Tx, urm *influxdb.UserResourceMapping) error {
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

	return nil
}

func (s *Store) ListURMs(ctx context.Context, tx *Tx, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, error) {
	ms := []*influxdb.UserResourceMapping{}

	b, err := tx.Bucket(urmBucket)
	if err != nil {
		return nil, UnavailableURMServiceError(err)
	}

	// TODO(compute): Once we have an index we should be able to use it somewhere in here
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

	for k, v := cur.Next(); k != nil; k, v = cur.Next() {
		m := &influxdb.UserResourceMapping{}
		if err := json.Unmarshal(v, m); err != nil {
			return nil, CorruptURMError(err)
		}

		// check to see if it matches the filter
		if (!filter.UserID.Valid() || (filter.UserID == m.UserID)) &&
			(!filter.ResourceID.Valid() || (filter.ResourceID == m.ResourceID)) &&
			(filter.UserType == "" || (filter.UserType == m.UserType)) &&
			(filter.ResourceType == "" || (filter.ResourceType == m.ResourceType)) {
			ms = append(ms, m)
		}

		if len(opt) > 0 && len(ms) >= opt[0].Limit {
			break
		}
	}

	return ms, err
}

func (s *Store) GetURM(ctx context.Context, tx *Tx, resourceID, userID influxdb.ID) (*influxdb.UserResourceMapping, error) {
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

func (s *Store) DeleteURM(ctx context.Context, tx *Tx, resourceID, userID influxdb.ID) error {
	key, err := userResourceKey(resourceID, userID)
	if err != nil {
		return err
	}

	b, err := tx.Bucket(urmBucket)
	if err != nil {
		return err
	}

	return b.Delete(key)
}

func userResourcePrefixKey(resourceID influxdb.ID) ([]byte, error) {
	encodedResourceID, err := resourceID.Encode()
	if err != nil {
		return nil, ErrInvalidURMID
	}
	return encodedResourceID, nil
}

func userResourceKey(resourceID, userID influxdb.ID) ([]byte, error) {
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

func (s *Store) uniqueUserResourceMapping(ctx context.Context, tx *Tx, m *influxdb.UserResourceMapping) error {
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
