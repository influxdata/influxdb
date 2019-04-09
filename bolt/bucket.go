package bolt

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bolt "github.com/coreos/bbolt"
	platform "github.com/influxdata/influxdb"
	platformcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/kit/tracing"
)

var (
	bucketBucket = []byte("bucketsv1")
	bucketIndex  = []byte("bucketindexv1")
)

var _ platform.BucketService = (*Client)(nil)
var _ platform.BucketOperationLogService = (*Client)(nil)

func (c *Client) initializeBuckets(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(bucketBucket)); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists([]byte(bucketIndex)); err != nil {
		return err
	}
	return nil
}

func (c *Client) setOrganizationOnBucket(ctx context.Context, tx *bolt.Tx, b *platform.Bucket) *platform.Error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	o, err := c.findOrganizationByID(ctx, tx, b.OrganizationID)
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	b.Org = o.Name
	return nil
}

// FindBucketByID retrieves a bucket by id.
func (c *Client) FindBucketByID(ctx context.Context, id platform.ID) (*platform.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var b *platform.Bucket
	var err error

	err = c.db.View(func(tx *bolt.Tx) error {
		bkt, pe := c.findBucketByID(ctx, tx, id)
		if pe != nil {
			pe.Op = getOp(platform.OpFindBucketByID)
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

func (c *Client) findBucketByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.Bucket, *platform.Error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var b platform.Bucket

	encodedID, err := id.Encode()
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	v := tx.Bucket(bucketBucket).Get(encodedID)
	if len(v) == 0 {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  "bucket not found",
		}
	}

	if err := json.Unmarshal(v, &b); err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}

	if err := c.setOrganizationOnBucket(ctx, tx, &b); err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}

	return &b, nil
}

// FindBucketByName returns a bucket by name for a particular organization.
// TODO: have method for finding bucket using organization name and bucket name.
func (c *Client) FindBucketByName(ctx context.Context, orgID platform.ID, n string) (*platform.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var b *platform.Bucket
	var err error

	err = c.db.View(func(tx *bolt.Tx) error {
		bkt, pe := c.findBucketByName(ctx, tx, orgID, n)
		if pe != nil {
			pe.Op = getOp(platform.OpFindBucket)
			err = pe
			return err
		}
		b = bkt
		return nil
	})

	return b, err
}

func (c *Client) findBucketByName(ctx context.Context, tx *bolt.Tx, orgID platform.ID, n string) (*platform.Bucket, *platform.Error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b := &platform.Bucket{
		OrganizationID: orgID,
		Name:           n,
	}
	key, err := bucketIndexKey(b)
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	buf := tx.Bucket(bucketIndex).Get(key)
	if buf == nil {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  fmt.Sprintf("bucket %q not found", n),
		}
	}

	var id platform.ID
	if err := id.Decode(buf); err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}
	return c.findBucketByID(ctx, tx, id)
}

// FindBucket retrives a bucket using an arbitrary bucket filter.
// Filters using ID, or OrganizationID and bucket Name should be efficient.
// Other filters will do a linear scan across buckets until it finds a match.
func (c *Client) FindBucket(ctx context.Context, filter platform.BucketFilter) (*platform.Bucket, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var b *platform.Bucket
	var err error

	if filter.ID != nil {
		b, err = c.FindBucketByID(ctx, *filter.ID)
		if err != nil {
			return nil, &platform.Error{
				Op:  getOp(platform.OpFindBucket),
				Err: err,
			}
		}
		return b, nil
	}

	if filter.Name != nil && filter.OrganizationID != nil {
		return c.FindBucketByName(ctx, *filter.OrganizationID, *filter.Name)
	}

	err = c.db.View(func(tx *bolt.Tx) error {
		if filter.Org != nil {
			o, err := c.findOrganizationByName(ctx, tx, *filter.Org)
			if err != nil {
				return err
			}
			filter.OrganizationID = &o.ID
		}

		filterFn := filterBucketsFn(filter)
		return c.forEachBucket(ctx, tx, false, func(bkt *platform.Bucket) bool {
			if filterFn(bkt) {
				b = bkt
				return false
			}
			return true
		})
	})

	if err != nil {
		return nil, &platform.Error{
			Op:  getOp(platform.OpFindBucket),
			Err: err,
		}
	}

	if b == nil {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  "bucket not found",
		}
	}

	return b, nil
}

func filterBucketsFn(filter platform.BucketFilter) func(b *platform.Bucket) bool {
	if filter.ID != nil {
		return func(b *platform.Bucket) bool {
			return b.ID == *filter.ID
		}
	}

	if filter.Name != nil && filter.OrganizationID != nil {
		return func(b *platform.Bucket) bool {
			return b.Name == *filter.Name && b.OrganizationID == *filter.OrganizationID
		}
	}

	if filter.Name != nil {
		return func(b *platform.Bucket) bool {
			return b.Name == *filter.Name
		}
	}

	if filter.OrganizationID != nil {
		return func(b *platform.Bucket) bool {
			return b.OrganizationID == *filter.OrganizationID
		}
	}

	return func(b *platform.Bucket) bool { return true }
}

// FindBuckets retrives all buckets that match an arbitrary bucket filter.
// Filters using ID, or OrganizationID and bucket Name should be efficient.
// Other filters will do a linear scan across all buckets searching for a match.
func (c *Client) FindBuckets(ctx context.Context, filter platform.BucketFilter, opts ...platform.FindOptions) ([]*platform.Bucket, int, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if filter.ID != nil {
		b, err := c.FindBucketByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.Bucket{b}, 1, nil
	}

	if filter.Name != nil && filter.OrganizationID != nil {
		b, err := c.FindBucketByName(ctx, *filter.OrganizationID, *filter.Name)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.Bucket{b}, 1, nil
	}

	bs := []*platform.Bucket{}
	err := c.db.View(func(tx *bolt.Tx) error {
		bkts, err := c.findBuckets(ctx, tx, filter, opts...)
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

func (c *Client) findBuckets(ctx context.Context, tx *bolt.Tx, filter platform.BucketFilter, opts ...platform.FindOptions) ([]*platform.Bucket, *platform.Error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	bs := []*platform.Bucket{}
	if filter.Org != nil {
		o, err := c.findOrganizationByName(ctx, tx, *filter.Org)
		if err != nil {
			return nil, &platform.Error{
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
	err := c.forEachBucket(ctx, tx, descending, func(b *platform.Bucket) bool {
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
		return nil, &platform.Error{
			Err: err,
		}
	}

	return bs, nil
}

// CreateBucket creates a platform bucket and sets b.ID.
func (c *Client) CreateBucket(ctx context.Context, b *platform.Bucket) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var err error
	op := getOp(platform.OpCreateBucket)
	return c.db.Update(func(tx *bolt.Tx) error {
		if b.OrganizationID.Valid() {
			_, pe := c.findOrganizationByID(ctx, tx, b.OrganizationID)
			if pe != nil {
				return &platform.Error{
					Err: pe,
					Op:  op,
				}
			}
		} else {
			o, pe := c.findOrganizationByName(ctx, tx, b.Org)
			if pe != nil {
				return &platform.Error{
					Err: pe,
					Op:  op,
				}
			}
			b.OrganizationID = o.ID
		}

		unique := c.uniqueBucketName(ctx, tx, b)

		if !unique {
			// TODO: make standard error
			return &platform.Error{
				Code: platform.EConflict,
				Op:   op,
				Msg:  fmt.Sprintf("bucket with name %s already exists", b.Name),
			}
		}

		b.ID = c.IDGenerator.ID()

		if err = c.appendBucketEventToLog(ctx, tx, b.ID, bucketCreatedEvent); err != nil {
			return &platform.Error{
				Op:  op,
				Err: err,
			}
		}

		if pe := c.putBucket(ctx, tx, b); pe != nil {
			pe.Op = op
			err = pe
		}

		if pe := c.createBucketUserResourceMappings(ctx, tx, b); pe != nil {
			pe.Op = op
			err = pe
		}
		return nil
	})
}

// PutBucket will put a bucket without setting an ID.
func (c *Client) PutBucket(ctx context.Context, b *platform.Bucket) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		var err error
		pe := c.putBucket(ctx, tx, b)
		if pe != nil {
			err = pe
		}
		return err
	})
}

func (c *Client) createBucketUserResourceMappings(ctx context.Context, tx *bolt.Tx, b *platform.Bucket) *platform.Error {
	ms, err := c.findUserResourceMappings(ctx, tx, platform.UserResourceMappingFilter{
		ResourceType: platform.OrgsResourceType,
		ResourceID:   b.OrganizationID,
	})
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	for _, m := range ms {
		if err := c.createUserResourceMapping(ctx, tx, &platform.UserResourceMapping{
			ResourceType: platform.BucketsResourceType,
			ResourceID:   b.ID,
			UserID:       m.UserID,
			UserType:     m.UserType,
		}); err != nil {
			return &platform.Error{
				Err: err,
			}
		}
	}

	return nil
}

func (c *Client) putBucket(ctx context.Context, tx *bolt.Tx, b *platform.Bucket) *platform.Error {
	b.Org = ""
	v, err := json.Marshal(b)
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	encodedID, err := b.ID.Encode()
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	key, pe := bucketIndexKey(b)
	if err != nil {
		return pe
	}

	if err := tx.Bucket(bucketIndex).Put(key, encodedID); err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	if err := tx.Bucket(bucketBucket).Put(encodedID, v); err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	return c.setOrganizationOnBucket(ctx, tx, b)
}

func bucketIndexKey(b *platform.Bucket) ([]byte, *platform.Error) {
	orgID, err := b.OrganizationID.Encode()
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}
	k := make([]byte, platform.IDLength+len(b.Name))
	copy(k, orgID)
	copy(k[platform.IDLength:], []byte(b.Name))
	return k, nil
}

// forEachBucket will iterate through all buckets while fn returns true.
func (c *Client) forEachBucket(ctx context.Context, tx *bolt.Tx, descending bool, fn func(*platform.Bucket) bool) error {
	cur := tx.Bucket(bucketBucket).Cursor()

	var k, v []byte
	if descending {
		k, v = cur.Last()
	} else {
		k, v = cur.First()
	}

	for k != nil {
		b := &platform.Bucket{}
		if err := json.Unmarshal(v, b); err != nil {
			return err
		}
		if err := c.setOrganizationOnBucket(ctx, tx, b); err != nil {
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

func (c *Client) uniqueBucketName(ctx context.Context, tx *bolt.Tx, b *platform.Bucket) bool {
	key, err := bucketIndexKey(b)
	if err != nil {
		return false
	}
	v := tx.Bucket(bucketIndex).Get(key)
	return len(v) == 0
}

// UpdateBucket updates a bucket according the parameters set on upd.
func (c *Client) UpdateBucket(ctx context.Context, id platform.ID, upd platform.BucketUpdate) (*platform.Bucket, error) {
	var b *platform.Bucket
	err := c.db.Update(func(tx *bolt.Tx) error {
		bkt, err := c.updateBucket(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		b = bkt
		return nil
	})

	return b, err
}

func (c *Client) updateBucket(ctx context.Context, tx *bolt.Tx, id platform.ID, upd platform.BucketUpdate) (*platform.Bucket, error) {
	b, err := c.findBucketByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.RetentionPeriod != nil {
		b.RetentionPeriod = *upd.RetentionPeriod
	}

	if upd.Name != nil {
		b0, err := c.findBucketByName(ctx, tx, b.OrganizationID, *upd.Name)
		if err == nil && b0.ID != id {
			return nil, &platform.Error{
				Code: platform.EConflict,
				Msg:  "bucket name is not unique",
			}
		}

		key, err := bucketIndexKey(b)
		if err != nil {
			return nil, err
		}
		// Buckets are indexed by name and so the bucket index must be pruned when name is modified.
		if err := tx.Bucket(bucketIndex).Delete(key); err != nil {
			return nil, err
		}
		b.Name = *upd.Name
	}

	if err := c.appendBucketEventToLog(ctx, tx, b.ID, bucketUpdatedEvent); err != nil {
		return nil, err
	}

	if err := c.putBucket(ctx, tx, b); err != nil {
		return nil, err
	}

	if err := c.setOrganizationOnBucket(ctx, tx, b); err != nil {
		return nil, err
	}

	return b, nil
}

// DeleteBucket deletes a bucket and prunes it from the index.
func (c *Client) DeleteBucket(ctx context.Context, id platform.ID) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		var err error
		if pe := c.deleteBucket(ctx, tx, id); pe != nil {
			pe.Op = getOp(platform.OpDeleteBucket)
			err = pe
		}
		return err
	})
}

func (c *Client) deleteBucket(ctx context.Context, tx *bolt.Tx, id platform.ID) *platform.Error {
	b, pe := c.findBucketByID(ctx, tx, id)
	if pe != nil {
		return pe
	}
	key, pe := bucketIndexKey(b)
	if pe != nil {
		return pe
	}
	// make lowercase deleteBucket with tx
	if err := tx.Bucket(bucketIndex).Delete(key); err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	encodedID, err := id.Encode()
	if err != nil {
		return &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}
	if err := tx.Bucket(bucketBucket).Delete(encodedID); err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	if err := c.deleteUserResourceMappings(ctx, tx, platform.UserResourceMappingFilter{
		ResourceID:   id,
		ResourceType: platform.BucketsResourceType,
	}); err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	return nil
}

const bucketOperationLogKeyPrefix = "bucket"

func encodeBucketOperationLogKey(id platform.ID) ([]byte, error) {
	buf, err := id.Encode()
	if err != nil {
		return nil, err
	}
	return append([]byte(bucketOperationLogKeyPrefix), buf...), nil
}

// GetBucketOperationLog retrieves a buckets operation log.
func (c *Client) GetBucketOperationLog(ctx context.Context, id platform.ID, opts platform.FindOptions) ([]*platform.OperationLogEntry, int, error) {
	// TODO(desa): might be worthwhile to allocate a slice of size opts.Limit
	log := []*platform.OperationLogEntry{}

	err := c.db.View(func(tx *bolt.Tx) error {
		key, err := encodeBucketOperationLogKey(id)
		if err != nil {
			return err
		}

		return c.forEachLogEntry(ctx, tx, key, opts, func(v []byte, t time.Time) error {
			e := &platform.OperationLogEntry{}
			if err := json.Unmarshal(v, e); err != nil {
				return err
			}
			e.Time = t

			log = append(log, e)

			return nil
		})
	})

	if err != nil {
		return nil, 0, err
	}

	return log, len(log), nil
}

// TODO(desa): what do we want these to be?
const (
	bucketCreatedEvent = "Bucket Created"
	bucketUpdatedEvent = "Bucket Updated"
)

func (c *Client) appendBucketEventToLog(ctx context.Context, tx *bolt.Tx, id platform.ID, s string) error {
	e := &platform.OperationLogEntry{
		Description: s,
	}
	// TODO(desa): this is fragile and non explicit since it requires an authorizer to be on context. It should be
	//             replaced with a higher level transaction so that adding to the log can take place in the http handler
	//             where the userID will exist explicitly.
	a, err := platformcontext.GetAuthorizer(ctx)
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

	return c.addLogEntry(ctx, tx, k, v, c.time())
}
