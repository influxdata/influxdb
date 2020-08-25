package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/resource"
)

var (
	bucketBucket = []byte("bucketsv1")
	bucketIndex  = []byte("bucketindexv1")
)

var _ influxdb.BucketService = (*Service)(nil)
var _ influxdb.BucketOperationLogService = (*Service)(nil)

func (s *Service) bucketsBucket(tx Tx) (Bucket, error) {
	b, err := tx.Bucket(bucketBucket)
	if err != nil {
		return nil, UnexpectedBucketError(err)
	}

	return b, nil
}

func (s *Service) bucketsIndexBucket(tx Tx) (Bucket, error) {
	b, err := tx.Bucket(bucketIndex)
	if err != nil {
		return nil, UnexpectedBucketIndexError(err)
	}

	return b, nil
}

// FindBucketByID retrieves a bucket by id.
func (s *Service) FindBucketByID(ctx context.Context, id influxdb.ID) (*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var b *influxdb.Bucket
	var err error

	err = s.kv.View(ctx, func(tx Tx) error {
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
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var b influxdb.Bucket

	encodedID, err := id.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	bkt, err := s.bucketsBucket(tx)
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

	return &b, nil
}

// FindBucketByName returns a bucket by name for a particular organization.
// TODO: have method for finding bucket using organization name and bucket name.
func (s *Service) FindBucketByName(ctx context.Context, orgID influxdb.ID, n string) (*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var b *influxdb.Bucket
	err := s.kv.View(ctx, func(tx Tx) error {
		bkt, pe := s.findBucketByName(ctx, tx, orgID, n)
		if pe != nil {
			return pe
		}

		b = bkt
		return nil
	})

	return b, err
}

// CreateSystemBuckets for an organization
func (s *Service) CreateSystemBuckets(ctx context.Context, o *influxdb.Organization) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.createSystemBuckets(ctx, tx, o)
	})
}

// createSystemBuckets creates the task and monitoring system buckets for an organization
func (s *Service) createSystemBuckets(ctx context.Context, tx Tx, o *influxdb.Organization) error {
	tb := &influxdb.Bucket{
		OrgID:           o.ID,
		Type:            influxdb.BucketTypeSystem,
		Name:            influxdb.TasksSystemBucketName,
		RetentionPeriod: influxdb.TasksSystemBucketRetention,
		Description:     "System bucket for task logs",
	}

	if err := s.createBucket(ctx, tx, tb); err != nil {
		return err
	}

	mb := &influxdb.Bucket{
		OrgID:           o.ID,
		Type:            influxdb.BucketTypeSystem,
		Name:            influxdb.MonitoringSystemBucketName,
		RetentionPeriod: influxdb.MonitoringSystemBucketRetention,
		Description:     "System bucket for monitoring logs",
	}

	return s.createBucket(ctx, tx, mb)
}

func (s *Service) findBucketByName(ctx context.Context, tx Tx, orgID influxdb.ID, n string) (*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b := &influxdb.Bucket{
		OrgID: orgID,
		Name:  n,
	}
	key, err := bucketIndexKey(b)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	idx, err := s.bucketsIndexBucket(tx)
	if err != nil {
		return nil, err
	}

	buf, err := idx.Get(key)
	if IsNotFound(err) {
		switch n {
		case influxdb.TasksSystemBucketName:
			return &influxdb.Bucket{
				ID:              influxdb.TasksSystemBucketID,
				Type:            influxdb.BucketTypeSystem,
				Name:            influxdb.TasksSystemBucketName,
				RetentionPeriod: influxdb.TasksSystemBucketRetention,
				Description:     "System bucket for task logs",
				OrgID:           orgID,
			}, nil
		case influxdb.MonitoringSystemBucketName:
			return &influxdb.Bucket{
				ID:              influxdb.MonitoringSystemBucketID,
				Type:            influxdb.BucketTypeSystem,
				Name:            influxdb.MonitoringSystemBucketName,
				RetentionPeriod: influxdb.MonitoringSystemBucketRetention,
				Description:     "System bucket for monitoring logs",
				OrgID:           orgID,
			}, nil
		default:
			return nil, &influxdb.Error{
				Code: influxdb.ENotFound,
				Msg:  fmt.Sprintf("bucket %q not found", n),
			}
		}
	}

	if err != nil {
		return nil, err
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
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

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

	err = s.kv.View(ctx, func(tx Tx) error {
		if filter.Org != nil {
			o, err := s.findOrganizationByName(ctx, tx, *filter.Org)
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
			return b.Name == *filter.Name && b.OrgID == *filter.OrganizationID
		}
	}

	if filter.Name != nil {
		return func(b *influxdb.Bucket) bool {
			return b.Name == *filter.Name
		}
	}

	if filter.OrganizationID != nil {
		return func(b *influxdb.Bucket) bool {
			return b.OrgID == *filter.OrganizationID
		}
	}

	return func(b *influxdb.Bucket) bool { return true }
}

// FindBuckets retrives all buckets that match an arbitrary bucket filter.
// Filters using ID, or OrganizationID and bucket Name should be efficient.
// Other filters will do a linear scan across all buckets searching for a match.
func (s *Service) FindBuckets(ctx context.Context, filter influxdb.BucketFilter, opts ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

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
	err := s.kv.View(ctx, func(tx Tx) error {
		bkts, err := s.findBuckets(ctx, tx, filter, opts...)
		if err != nil {
			return err
		}

		bs = bkts

		return nil
	})

	// Don't append system buckets if Name is set. Users who don't have real
	// system buckets won't get mocked buckets if they query for a bucket by name
	// without the orgID, but this is a vanishing small number of users and has
	// limited utility anyways. Can be removed once mock system code is ripped out.
	if filter.Name != nil {
		return bs, len(bs), nil
	}

	needsSystemBuckets := true
	for _, b := range bs {
		if b.Type == influxdb.BucketTypeSystem {
			needsSystemBuckets = false
			break
		}
	}

	if needsSystemBuckets {
		tb := &influxdb.Bucket{
			ID:              influxdb.TasksSystemBucketID,
			Type:            influxdb.BucketTypeSystem,
			Name:            influxdb.TasksSystemBucketName,
			RetentionPeriod: influxdb.TasksSystemBucketRetention,
			Description:     "System bucket for task logs",
		}

		bs = append(bs, tb)

		mb := &influxdb.Bucket{
			ID:              influxdb.MonitoringSystemBucketID,
			Type:            influxdb.BucketTypeSystem,
			Name:            influxdb.MonitoringSystemBucketName,
			RetentionPeriod: influxdb.MonitoringSystemBucketRetention,
			Description:     "System bucket for monitoring logs",
		}

		bs = append(bs, mb)
	}

	if err != nil {
		return nil, 0, err
	}

	return bs, len(bs), nil
}

func (s *Service) findBuckets(ctx context.Context, tx Tx, filter influxdb.BucketFilter, opts ...influxdb.FindOptions) ([]*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	bs := []*influxdb.Bucket{}
	if filter.Org != nil {
		o, err := s.findOrganizationByName(ctx, tx, *filter.Org)
		if err != nil {
			return nil, &influxdb.Error{
				Err: err,
			}
		}
		filter.OrganizationID = &o.ID
	}

	var (
		offset, limit, count int
		descending           bool
	)

	after := func(*influxdb.Bucket) bool {
		return true
	}

	if len(opts) > 0 {
		offset = opts[0].Offset
		limit = opts[0].Limit
		descending = opts[0].Descending
		if opts[0].After != nil {
			after = func(b *influxdb.Bucket) bool {
				if descending {
					return b.ID < *opts[0].After
				}

				return b.ID > *opts[0].After
			}
		}
	}

	filterFn := filterBucketsFn(filter)
	err := s.forEachBucket(ctx, tx, descending, func(b *influxdb.Bucket) bool {
		if filterFn(b) {
			if count >= offset && after(b) {
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
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return s.kv.Update(ctx, func(tx Tx) error {
		return s.createBucket(ctx, tx, b)
	})
}

// CreateBucketTx is used when importing kv as a library
func (s *Service) CreateBucketTx(ctx context.Context, tx Tx, b *influxdb.Bucket) (err error) {
	return s.createBucket(ctx, tx, b)
}

func (s *Service) createBucket(ctx context.Context, tx Tx, b *influxdb.Bucket) (err error) {
	if b.OrgID.Valid() {
		span, ctx := tracing.StartSpanFromContext(ctx)
		defer span.Finish()

		_, pe := s.findOrganizationByID(ctx, tx, b.OrgID)
		if pe != nil {
			return &influxdb.Error{
				Err: pe,
			}
		}
	}

	if err := s.uniqueBucketName(ctx, tx, b); err != nil {
		return err
	}

	if err := validBucketName(b.Name, b.Type); err != nil {
		return err
	}

	if b.ID, err = s.generateBucketID(ctx, tx); err != nil {
		return err
	}

	b.CreatedAt = s.Now()
	b.UpdatedAt = s.Now()

	if err := s.appendBucketEventToLog(ctx, tx, b.ID, bucketCreatedEvent); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	v, err := json.Marshal(b)
	if err != nil {
		return influxdb.ErrInternalBucketServiceError(influxdb.OpCreateBucket, err)
	}
	if err := s.putBucket(ctx, tx, b, v); err != nil {
		return err
	}

	if err := s.createUserResourceMappingForOrg(ctx, tx, b.OrgID, b.ID, influxdb.BucketsResourceType); err != nil {
		return err
	}

	uid, _ := icontext.GetUserID(ctx)
	return s.audit.Log(resource.Change{
		Type:           resource.Create,
		ResourceID:     b.ID,
		ResourceType:   influxdb.BucketsResourceType,
		OrganizationID: b.OrgID,
		UserID:         uid,
		ResourceBody:   v,
		Time:           time.Now(),
	})
}

func (s *Service) generateBucketID(ctx context.Context, tx Tx) (influxdb.ID, error) {
	return s.generateSafeID(ctx, tx, bucketBucket, s.BucketIDs)
}

// PutBucket will put a bucket without setting an ID.
func (s *Service) PutBucket(ctx context.Context, b *influxdb.Bucket) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		v, err := json.Marshal(b)
		if err != nil {
			return influxdb.ErrInternalBucketServiceError(influxdb.OpPutBucket, err)
		}

		if err := s.putBucket(ctx, tx, b, v); err != nil {
			return err
		}

		uid, _ := icontext.GetUserID(ctx)
		return s.audit.Log(resource.Change{
			Type:           resource.Put,
			ResourceID:     b.ID,
			ResourceType:   influxdb.BucketsResourceType,
			OrganizationID: b.OrgID,
			UserID:         uid,
			ResourceBody:   v,
			Time:           time.Now(),
		})
	})
}

func (s *Service) putBucket(ctx context.Context, tx Tx, b *influxdb.Bucket, v []byte) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encodedID, err := b.ID.Encode()
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	key, err := bucketIndexKey(b)
	if err != nil {
		return err
	}

	idx, err := s.bucketsIndexBucket(tx)
	if err != nil {
		return err
	}

	if err := idx.Put(key, encodedID); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	bkt, err := s.bucketsBucket(tx)
	if bkt.Put(encodedID, v); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	return nil
}

// bucketIndexKey is a combination of the orgID and the bucket name.
func bucketIndexKey(b *influxdb.Bucket) ([]byte, error) {
	orgID, err := b.OrgID.Encode()
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
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	bkt, err := s.bucketsBucket(tx)
	if err != nil {
		return err
	}

	direction := CursorAscending
	if descending {
		direction = CursorDescending
	}

	cur, err := bkt.ForwardCursor(nil, WithCursorDirection(direction))
	if err != nil {
		return err
	}

	for k, v := cur.Next(); k != nil; k, v = cur.Next() {
		b := &influxdb.Bucket{}
		if err := json.Unmarshal(v, b); err != nil {
			return err
		}

		if !fn(b) {
			break
		}
	}

	return nil
}

func (s *Service) uniqueBucketName(ctx context.Context, tx Tx, b *influxdb.Bucket) error {
	key, err := bucketIndexKey(b)
	if err != nil {
		return err
	}

	// if the bucket name is not unique for this organization, then, do not
	// allow creation.
	err = s.unique(ctx, tx, bucketIndex, key)
	if err == NotUniqueError {
		return BucketAlreadyExistsError(b)
	}

	return err
}

// validBucketName reports any errors with bucket names
func validBucketName(name string, typ influxdb.BucketType) error {
	// names starting with an underscore are reserved for system buckets
	if strings.HasPrefix(name, "_") && typ != influxdb.BucketTypeSystem {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("bucket name %s is invalid. Buckets may not start with underscore", name),
			Op:   influxdb.OpCreateBucket,
		}
	}
	// quotation marks will cause queries to fail
	if strings.Contains(name, "\"") {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("bucket name %s is invalid. Bucket names may not include quotation marks", name),
			Op:   influxdb.OpCreateBucket,
		}
	}
	return nil
}

// UpdateBucket updates a bucket according the parameters set on upd.
func (s *Service) UpdateBucket(ctx context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var b *influxdb.Bucket
	err := s.kv.Update(ctx, func(tx Tx) error {
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
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b, err := s.findBucketByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil && b.Type == influxdb.BucketTypeSystem {
		err = &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "system buckets cannot be renamed",
		}

		return nil, err
	}

	if upd.RetentionPeriod != nil {
		b.RetentionPeriod = *upd.RetentionPeriod
	}

	if upd.Description != nil {
		b.Description = *upd.Description
	}

	if upd.Name != nil {
		b0, err := s.findBucketByName(ctx, tx, b.OrgID, *upd.Name)
		if err == nil && b0.ID != id {
			return nil, &influxdb.Error{
				Code: influxdb.EConflict,
				Msg:  "bucket name is not unique",
			}
		}

		if err := validBucketName(*upd.Name, b.Type); err != nil {
			return nil, err
		}

		key, err := bucketIndexKey(b)
		if err != nil {
			return nil, err
		}
		idx, err := s.bucketsIndexBucket(tx)
		if err != nil {
			return nil, err
		}
		// Buckets are indexed by name and so the bucket index must be pruned when name is modified.
		if err := idx.Delete(key); err != nil {
			return nil, err
		}
		b.Name = *upd.Name
	}

	b.UpdatedAt = s.Now()

	if err := s.appendBucketEventToLog(ctx, tx, b.ID, bucketUpdatedEvent); err != nil {
		return nil, err
	}

	v, err := json.Marshal(b)
	if err != nil {
		return nil, influxdb.ErrInternalBucketServiceError(influxdb.OpUpdateBucket, err)
	}

	if err := s.putBucket(ctx, tx, b, v); err != nil {
		return nil, err
	}

	uid, _ := icontext.GetUserID(ctx)
	if err := s.audit.Log(resource.Change{
		Type:           resource.Update,
		ResourceID:     b.ID,
		ResourceType:   influxdb.BucketsResourceType,
		OrganizationID: b.OrgID,
		UserID:         uid,
		ResourceBody:   v,
		Time:           time.Now(),
	}); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return b, nil
}

// DeleteBucket deletes a bucket and prunes it from the index.
func (s *Service) DeleteBucket(ctx context.Context, id influxdb.ID) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		bucket, err := s.findBucketByID(ctx, tx, id)
		if err != nil && !IsNotFound(err) {
			return err
		}

		if !IsNotFound(err) && bucket.Type == influxdb.BucketTypeSystem {
			return &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "system buckets cannot be deleted",
			}
		}

		if err := s.deleteBucket(ctx, tx, id); err != nil {
			return err
		}

		uid, _ := icontext.GetUserID(ctx)
		return s.audit.Log(resource.Change{
			Type:           resource.Delete,
			ResourceID:     id,
			ResourceType:   influxdb.BucketsResourceType,
			OrganizationID: bucket.OrgID,
			UserID:         uid,
			Time:           time.Now(),
		})
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

	idx, err := s.bucketsIndexBucket(tx)
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

	bkt, err := s.bucketsBucket(tx)
	if err != nil {
		return err
	}

	if err := bkt.Delete(encodedID); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	if err := s.deleteUserResourceMappings(ctx, tx, influxdb.UserResourceMappingFilter{
		ResourceID:   id,
		ResourceType: influxdb.BucketsResourceType,
	}); err != nil {
		return err
	}

	return nil
}

const bucketOperationLogKeyPrefix = "bucket"

func encodeBucketOperationLogKey(id influxdb.ID) ([]byte, error) {
	buf, err := id.Encode()
	if err != nil {
		return nil, err
	}
	return append([]byte(bucketOperationLogKeyPrefix), buf...), nil
}

// GetBucketOperationLog retrieves a buckets operation log.
func (s *Service) GetBucketOperationLog(ctx context.Context, id influxdb.ID, opts influxdb.FindOptions) ([]*influxdb.OperationLogEntry, int, error) {
	// TODO(desa): might be worthwhile to allocate a slice of size opts.Limit
	log := []*influxdb.OperationLogEntry{}

	err := s.kv.View(ctx, func(tx Tx) error {
		key, err := encodeBucketOperationLogKey(id)
		if err != nil {
			return err
		}

		return s.forEachLogEntry(ctx, tx, key, opts, func(v []byte, t time.Time) error {
			e := &influxdb.OperationLogEntry{}
			if err := json.Unmarshal(v, e); err != nil {
				return err
			}
			e.Time = t

			log = append(log, e)

			return nil
		})
	})

	if err != nil && err != errKeyValueLogBoundsNotFound {
		return nil, 0, err
	}

	return log, len(log), nil
}

// TODO(desa): what do we want these to be?
const (
	bucketCreatedEvent = "Bucket Created"
	bucketUpdatedEvent = "Bucket Updated"
)

func (s *Service) appendBucketEventToLog(ctx context.Context, tx Tx, id influxdb.ID, st string) error {
	e := &influxdb.OperationLogEntry{
		Description: st,
	}
	// TODO(desa): this is fragile and non explicit since it requires an authorizer to be on context. It should be
	//             replaced with a higher level transaction so that adding to the log can take place in the http handler
	//             where the userID will exist explicitly.
	a, err := icontext.GetAuthorizer(ctx)
	if err == nil {
		// Add the user to the log if you can, but don't error if its not there.
		e.UserID = a.GetUserID()
	}

	v, err := json.Marshal(e)
	if err != nil {
		return err
	}

	k, err := encodeBucketOperationLogKey(id)
	if err != nil {
		return err
	}

	return s.addLogEntry(ctx, tx, k, v, s.Now())
}

// UnexpectedBucketError is used when the error comes from an internal system.
func UnexpectedBucketError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving bucket's bucket; Err %v", err),
		Op:   "kv/bucketBucket",
	}
}

// UnexpectedBucketIndexError is used when the error comes from an internal system.
func UnexpectedBucketIndexError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving bucket index; Err: %v", err),
		Op:   "kv/bucketIndex",
	}
}

// BucketAlreadyExistsError is used when creating a bucket with a name
// that already exists within an organization.
func BucketAlreadyExistsError(b *influxdb.Bucket) error {
	return &influxdb.Error{
		Code: influxdb.EConflict,
		Op:   "kv/bucket",
		Msg:  fmt.Sprintf("bucket with name %s already exists", b.Name),
	}
}
