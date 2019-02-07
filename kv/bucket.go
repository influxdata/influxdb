package kv

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb"
)

var (
	bucketBucket = []byte("buckets/v1")
	bucketIndex  = []byte("bucketindex/v1")
)

var _ influxdb.BucketService = (*Service)(nil)

//var _ influxdb.BucketOperationLogService = (*Client)(nil)

func (s *Service) initializeBuckets(ctx context.Context, tx Tx) error {
	if _, err := tx.Bucket(bucketBucket); err != nil {
		return err
	}
	if _, err := tx.Bucket(bucketIndex); err != nil {
		return err
	}
	return nil
}

func (s *Service) setOrganizationOnBucket(ctx context.Context, tx Tx, b *influxdb.Bucket) error {
	o, err := s.findOrganizationByID(ctx, tx, b.OrganizationID)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	b.Organization = o.Name
	return nil
}

// FindBucketByID retrieves a bucket by id.
func (s *Service) FindBucketByID(ctx context.Context, id influxdb.ID) (*influxdb.Bucket, error) {
	var b *influxdb.Bucket
	var err error

	err = s.kv.View(func(tx Tx) error {
		bkt, pe := s.findBucketByID(ctx, tx, id)
		if pe != nil {
			err = pe
			return err
		}
		b = bkt
		return nil
	})

	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *Service) findBucketByID(ctx context.Context, tx Tx, id influxdb.ID) (*influxdb.Bucket, error) {
	var b influxdb.Bucket

	encodedID, err := id.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	bkt, err := tx.Bucket(bucketBucket)
	if err != nil {
		return nil, err
	}

	v, err := bkt.Get(encodedID)
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "bucket not found",
		}
	}

	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(v, &b); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	if err := s.setOrganizationOnBucket(ctx, tx, &b); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return &b, nil
}

// FindBucketByName returns a bucket by name for a particular organization.
// TODO: have method for finding bucket using organization name and bucket name.
func (s *Service) FindBucketByName(ctx context.Context, orgID influxdb.ID, n string) (*influxdb.Bucket, error) {
	var b *influxdb.Bucket
	var err error

	err = s.kv.View(func(tx Tx) error {
		bkt, pe := s.findBucketByName(ctx, tx, orgID, n)
		if pe != nil {
			err = pe
			return err
		}
		b = bkt
		return nil
	})

	return b, err
}

func (s *Service) findBucketByName(ctx context.Context, tx Tx, orgID influxdb.ID, n string) (*influxdb.Bucket, error) {
	b := &influxdb.Bucket{
		OrganizationID: orgID,
		Name:           n,
	}
	key, err := bucketIndexKey(b)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	idx, err := tx.Bucket(bucketIndex)
	if err != nil {
		return nil, err
	}

	buf, err := idx.Get(key)
	if buf == nil {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "bucket not found",
		}
	}

	var id influxdb.ID
	if err := id.Decode(buf); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}
	return s.findBucketByID(ctx, tx, id)
}

// FindBucket retrives a bucket using an arbitrary bucket filter.
// Filters using ID, or OrganizationID and bucket Name should be efficient.
// Other filters will do a linear scan across buckets until it finds a match.
func (s *Service) FindBucket(ctx context.Context, filter influxdb.BucketFilter) (*influxdb.Bucket, error) {
	var b *influxdb.Bucket
	var err error

	if filter.ID != nil {
		b, err = s.FindBucketByID(ctx, *filter.ID)
		if err != nil {
			return nil, &influxdb.Error{
				Err: err,
			}
		}
		return b, nil
	}

	if filter.Name != nil && filter.OrganizationID != nil {
		return s.FindBucketByName(ctx, *filter.OrganizationID, *filter.Name)
	}

	err = s.kv.View(func(tx Tx) error {
		if filter.Organization != nil {
			o, err := s.findOrganizationByName(ctx, tx, *filter.Organization)
			if err != nil {
				return err
			}
			filter.OrganizationID = &o.ID
		}

		filterFn := filterBucketsFn(filter)
		return s.forEachBucket(ctx, tx, false, func(bkt *influxdb.Bucket) bool {
			if filterFn(bkt) {
				b = bkt
				return false
			}
			return true
		})
	})

	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	if b == nil {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "bucket not found",
		}
	}

	return b, nil
}

func filterBucketsFn(filter influxdb.BucketFilter) func(b *influxdb.Bucket) bool {
	if filter.ID != nil {
		return func(b *influxdb.Bucket) bool {
			return b.ID == *filter.ID
		}
	}

	if filter.Name != nil && filter.OrganizationID != nil {
		return func(b *influxdb.Bucket) bool {
			return b.Name == *filter.Name && b.OrganizationID == *filter.OrganizationID
		}
	}

	if filter.Name != nil {
		return func(b *influxdb.Bucket) bool {
			return b.Name == *filter.Name
		}
	}

	if filter.OrganizationID != nil {
		return func(b *influxdb.Bucket) bool {
			return b.OrganizationID == *filter.OrganizationID
		}
	}

	return func(b *influxdb.Bucket) bool { return true }
}

// FindBuckets retrives all buckets that match an arbitrary bucket filter.
// Filters using ID, or OrganizationID and bucket Name should be efficient.
// Other filters will do a linear scan across all buckets searching for a match.
func (s *Service) FindBuckets(ctx context.Context, filter influxdb.BucketFilter, opts ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
	if filter.ID != nil {
		b, err := s.FindBucketByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}

		return []*influxdb.Bucket{b}, 1, nil
	}

	if filter.Name != nil && filter.OrganizationID != nil {
		b, err := s.FindBucketByName(ctx, *filter.OrganizationID, *filter.Name)
		if err != nil {
			return nil, 0, err
		}

		return []*influxdb.Bucket{b}, 1, nil
	}

	bs := []*influxdb.Bucket{}
	err := s.kv.View(func(tx Tx) error {
		bkts, err := s.findBuckets(ctx, tx, filter, opts...)
		if err != nil {
			return err
		}
		bs = bkts
		return nil
	})

	if err != nil {
		return nil, 0, err
	}

	return bs, len(bs), nil
}

func (s *Service) findBuckets(ctx context.Context, tx Tx, filter influxdb.BucketFilter, opts ...influxdb.FindOptions) ([]*influxdb.Bucket, error) {
	bs := []*influxdb.Bucket{}
	if filter.Organization != nil {
		o, err := s.findOrganizationByName(ctx, tx, *filter.Organization)
		if err != nil {
			return nil, &influxdb.Error{
				Err: err,
			}
		}
		filter.OrganizationID = &o.ID
	}

	var offset, limit, count int
	var descending bool
	if len(opts) > 0 {
		offset = opts[0].Offset
		limit = opts[0].Limit
		descending = opts[0].Descending
	}

	filterFn := filterBucketsFn(filter)
	err := s.forEachBucket(ctx, tx, descending, func(b *influxdb.Bucket) bool {
		if filterFn(b) {
			if count >= offset {
				bs = append(bs, b)
			}
			count++
		}

		if limit > 0 && len(bs) >= limit {
			return false
		}

		return true
	})

	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return bs, nil
}

// CreateBucket creates a influxdb bucket and sets b.ID.
func (s *Service) CreateBucket(ctx context.Context, b *influxdb.Bucket) error {
	return s.kv.Update(func(tx Tx) error {
		if b.OrganizationID.Valid() {
			_, pe := s.findOrganizationByID(ctx, tx, b.OrganizationID)
			if pe != nil {
				return &influxdb.Error{
					Err: pe,
				}
			}
		} else {
			o, pe := s.findOrganizationByName(ctx, tx, b.Organization)
			if pe != nil {
				return &influxdb.Error{
					Err: pe,
				}
			}
			b.OrganizationID = o.ID
		}

		unique := s.uniqueBucketName(ctx, tx, b)

		if !unique {
			// TODO: make standard error
			return &influxdb.Error{
				Code: influxdb.EConflict,
				Msg:  fmt.Sprintf("bucket with name %s already exists", b.Name),
			}
		}

		b.ID = s.IDGenerator.ID()

		//if err = s.appendBucketEventToLog(ctx, tx, b.ID, bucketCreatedEvent); err != nil {
		//	return &influxdb.Error{
		//		Err: err,
		//	}
		//}

		if err := s.putBucket(ctx, tx, b); err != nil {
			return err
		}

		//if pe := s.createBucketUserResourceMappings(ctx, tx, b); pe != nil {
		//	err = pe
		//}
		return nil
	})
}

// PutBucket will put a bucket without setting an ID.
func (s *Service) PutBucket(ctx context.Context, b *influxdb.Bucket) error {
	return s.kv.Update(func(tx Tx) error {
		var err error
		pe := s.putBucket(ctx, tx, b)
		if pe != nil {
			err = pe
		}
		return err
	})
}

//func (s *Service) createBucketUserResourceMappings(ctx context.Context, tx Tx, b *influxdb.Bucket) *influxdb.Error {
//	ms, err := s.findUserResourceMappings(ctx, tx, influxdb.UserResourceMappingFilter{
//		ResourceType: influxdb.OrgsResourceType,
//		ResourceID:   b.OrganizationID,
//	})
//	if err != nil {
//		return &influxdb.Error{
//			Err: err,
//		}
//	}
//
//	for _, m := range ms {
//		if err := s.createUserResourceMapping(ctx, tx, &influxdb.UserResourceMapping{
//			ResourceType: influxdb.BucketsResourceType,
//			ResourceID:   b.ID,
//			UserID:       m.UserID,
//			UserType:     m.UserType,
//		}); err != nil {
//			return &influxdb.Error{
//				Err: err,
//			}
//		}
//	}
//
//	return nil
//}

func (s *Service) putBucket(ctx context.Context, tx Tx, b *influxdb.Bucket) error {
	b.Organization = ""
	v, err := json.Marshal(b)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	encodedID, err := b.ID.Encode()
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	key, pe := bucketIndexKey(b)
	if err != nil {
		return pe
	}

	idx, err := tx.Bucket(bucketIndex)
	if err != nil {
		return err
	}

	if err := idx.Put(key, encodedID); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	bkt, err := tx.Bucket(bucketBucket)
	if bkt.Put(encodedID, v); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	return s.setOrganizationOnBucket(ctx, tx, b)
}

func bucketIndexKey(b *influxdb.Bucket) ([]byte, error) {
	orgID, err := b.OrganizationID.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	k := make([]byte, influxdb.IDLength+len(b.Name))
	copy(k, orgID)
	copy(k[influxdb.IDLength:], []byte(b.Name))
	return k, nil
}

// forEachBucket will iterate through all buckets while fn returns true.
func (s *Service) forEachBucket(ctx context.Context, tx Tx, descending bool, fn func(*influxdb.Bucket) bool) error {
	bkt, err := tx.Bucket(bucketBucket)
	if err != nil {
		return err
	}

	cur, err := bkt.Cursor()
	if err != nil {
		return err
	}

	var k, v []byte
	if descending {
		k, v = cur.Last()
	} else {
		k, v = cur.First()
	}

	for k != nil {
		b := &influxdb.Bucket{}
		if err := json.Unmarshal(v, b); err != nil {
			return err
		}
		if err := s.setOrganizationOnBucket(ctx, tx, b); err != nil {
			return err
		}
		if !fn(b) {
			break
		}

		if descending {
			k, v = cur.Prev()
		} else {
			k, v = cur.Next()
		}
	}

	return nil
}

func (s *Service) uniqueBucketName(ctx context.Context, tx Tx, b *influxdb.Bucket) bool {
	key, err := bucketIndexKey(b)
	if err != nil {
		return false
	}
	idx, err := tx.Bucket(bucketIndex)
	if err != nil {
		return false
	}

	_, err = idx.Get(key)

	return IsNotFound(err)
}

// UpdateBucket updates a bucket according the parameters set on upd.
func (s *Service) UpdateBucket(ctx context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	var b *influxdb.Bucket
	err := s.kv.Update(func(tx Tx) error {
		bkt, err := s.updateBucket(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		b = bkt
		return nil
	})

	return b, err
}

func (s *Service) updateBucket(ctx context.Context, tx Tx, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	b, err := s.findBucketByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.RetentionPeriod != nil {
		b.RetentionPeriod = *upd.RetentionPeriod
	}

	if upd.Name != nil {
		key, err := bucketIndexKey(b)
		if err != nil {
			return nil, err
		}
		idx, err := tx.Bucket(bucketIndex)
		if err != nil {
			return nil, err
		}
		// Buckets are indexed by name and so the bucket index must be pruned when name is modified.
		if err := idx.Delete(key); err != nil {
			return nil, err
		}
		b.Name = *upd.Name
	}

	//if err := s.appendBucketEventToLog(ctx, tx, b.ID, bucketUpdatedEvent); err != nil {
	//	return nil, err
	//}

	if err := s.putBucket(ctx, tx, b); err != nil {
		return nil, err
	}

	if err := s.setOrganizationOnBucket(ctx, tx, b); err != nil {
		return nil, err
	}

	return b, nil
}

// DeleteBucket deletes a bucket and prunes it from the index.
func (s *Service) DeleteBucket(ctx context.Context, id influxdb.ID) error {
	return s.kv.Update(func(tx Tx) error {
		var err error
		if pe := s.deleteBucket(ctx, tx, id); pe != nil {
			err = pe
		}
		return err
	})
}

func (s *Service) deleteBucket(ctx context.Context, tx Tx, id influxdb.ID) error {
	b, pe := s.findBucketByID(ctx, tx, id)
	if pe != nil {
		return pe
	}

	key, pe := bucketIndexKey(b)
	if pe != nil {
		return pe
	}

	idx, err := tx.Bucket(bucketIndex)
	if err != nil {
		return err
	}

	if err := idx.Delete(key); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	encodedID, err := id.Encode()
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	bkt, err := tx.Bucket(bucketBucket)
	if err != nil {
		return err
	}

	if err := bkt.Delete(encodedID); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	//if err := s.deleteUserResourceMappings(ctx, tx, influxdb.UserResourceMappingFilter{
	//	ResourceID:   id,
	//	ResourceType: influxdb.BucketsResourceType,
	//}); err != nil {
	//	return &influxdb.Error{
	//		Err: err,
	//	}
	//}

	return nil
}

//const bucketOperationLogKeyPrefix = "bucket"
//
//func encodeBucketOperationLogKey(id influxdb.ID) ([]byte, error) {
//	buf, err := id.Encode()
//	if err != nil {
//		return nil, err
//	}
//	return append([]byte(bucketOperationLogKeyPrefix), buf...), nil
//}
//
//// GetBucketOperationLog retrieves a buckets operation log.
//func (s *Service) GetBucketOperationLog(ctx context.Context, id influxdb.ID, opts influxdb.FindOptions) ([]*influxdb.OperationLogEntry, int, error) {
//	// TODO(desa): might be worthwhile to allocate a slice of size opts.Limit
//	log := []*influxdb.OperationLogEntry{}
//
//	err := s.kv.View(func(tx Tx) error {
//		key, err := encodeBucketOperationLogKey(id)
//		if err != nil {
//			return err
//		}
//
//		return s.forEachLogEntry(ctx, tx, key, opts, func(v []byte, t time.Time) error {
//			e := &influxdb.OperationLogEntry{}
//			if err := json.Unmarshal(v, e); err != nil {
//				return err
//			}
//			e.Time = t
//
//			log = append(log, e)
//
//			return nil
//		})
//	})
//
//	if err != nil {
//		return nil, 0, err
//	}
//
//	return log, len(log), nil
//}
//
//// TODO(desa): what do we want these to be?
//const (
//	bucketCreatedEvent = "Bucket Created"
//	bucketUpdatedEvent = "Bucket Updated"
//)
//
//func (s *Service) appendBucketEventToLog(ctx context.Context, tx Tx, id influxdb.ID, s string) error {
//	e := &influxdb.OperationLogEntry{
//		Description: s,
//	}
//	// TODO(desa): this is fragile and non explicit since it requires an authorizer to be on context. It should be
//	//             replaced with a higher level transaction so that adding to the log can take place in the http handler
//	//             where the userID will exist explicitly.
//	a, err := influxdbcontext.GetAuthorizer(ctx)
//	if err == nil {
//		// Add the user to the log if you can, but don't error if its not there.
//		e.UserID = a.GetUserID()
//	}
//
//	v, err := json.Marshal(e)
//	if err != nil {
//		return err
//	}
//
//	k, err := encodeBucketOperationLogKey(id)
//	if err != nil {
//		return err
//	}
//
//	return s.addLogEntry(ctx, tx, k, v, s.time())
//}
