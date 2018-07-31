package bolt

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/coreos/bbolt"
	"github.com/influxdata/platform"
)

var (
	bucketBucket = []byte("bucketsv1")
	bucketIndex  = []byte("bucketindexv1")
)

func (c *Client) initializeBuckets(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(bucketBucket)); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists([]byte(bucketIndex)); err != nil {
		return err
	}
	return nil
}

func (c *Client) setOrganizationOnBucket(ctx context.Context, tx *bolt.Tx, b *platform.Bucket) error {
	o, err := c.findOrganizationByID(ctx, tx, b.OrganizationID)
	if err != nil {
		return err
	}
	b.Organization = o.Name
	return nil
}

// FindBucketByID retrieves a bucket by id.
func (c *Client) FindBucketByID(ctx context.Context, id platform.ID) (*platform.Bucket, error) {
	var b *platform.Bucket

	err := c.db.View(func(tx *bolt.Tx) error {
		bkt, err := c.findBucketByID(ctx, tx, id)
		if err != nil {
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

func (c *Client) findBucketByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.Bucket, error) {
	var b platform.Bucket

	encodedID, err := id.Encode()
	if err != nil {
		return nil, err
	}

	v := tx.Bucket(bucketBucket).Get(encodedID)

	if len(v) == 0 {
		// TODO: Make standard error
		return nil, fmt.Errorf("bucket not found")
	}

	if err := json.Unmarshal(v, &b); err != nil {
		return nil, err
	}

	if err := c.setOrganizationOnBucket(ctx, tx, &b); err != nil {
		return nil, err
	}

	return &b, nil
}

// FindBucketByName returns a bucket by name for a particular organization.
// TODO: have method for finding bucket using organization name and bucket name.
func (c *Client) FindBucketByName(ctx context.Context, orgID platform.ID, n string) (*platform.Bucket, error) {
	var b *platform.Bucket

	err := c.db.View(func(tx *bolt.Tx) error {
		bkt, err := c.findBucketByName(ctx, tx, orgID, n)
		if err != nil {
			return err
		}
		b = bkt
		return nil
	})

	return b, err
}

func (c *Client) findBucketByName(ctx context.Context, tx *bolt.Tx, orgID platform.ID, n string) (*platform.Bucket, error) {
	b := &platform.Bucket{
		OrganizationID: orgID,
		Name:           n,
	}
	key, err := bucketIndexKey(b)
	if err != nil {
		return nil, err
	}

	var id platform.ID
	if err := id.Decode(tx.Bucket(bucketIndex).Get(key)); err != nil {
		return nil, err
	}
	return c.findBucketByID(ctx, tx, id)
}

// FindBucket retrives a bucket using an arbitrary bucket filter.
// Filters using ID, or OrganizationID and bucket Name should be efficient.
// Other filters will do a linear scan across buckets until it finds a match.
func (c *Client) FindBucket(ctx context.Context, filter platform.BucketFilter) (*platform.Bucket, error) {
	if filter.ID != nil {
		return c.FindBucketByID(ctx, *filter.ID)
	}

	if filter.Name != nil && filter.OrganizationID != nil {
		return c.FindBucketByName(ctx, *filter.OrganizationID, *filter.Name)
	}

	var b *platform.Bucket
	err := c.db.View(func(tx *bolt.Tx) error {
		if filter.Organization != nil {
			o, err := c.findOrganizationByName(ctx, tx, *filter.Organization)
			if err != nil {
				return err
			}
			filter.OrganizationID = &o.ID
		}

		filterFn := filterBucketsFn(filter)
		return c.forEachBucket(ctx, tx, func(bkt *platform.Bucket) bool {
			if filterFn(bkt) {
				b = bkt
				return false
			}
			return true
		})
	})

	if err != nil {
		return nil, err
	}

	if b == nil {
		return nil, fmt.Errorf("bucket not found")
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
func (c *Client) FindBuckets(ctx context.Context, filter platform.BucketFilter, opt ...platform.FindOptions) ([]*platform.Bucket, int, error) {
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
		bkts, err := c.findBuckets(ctx, tx, filter)
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

func (c *Client) findBuckets(ctx context.Context, tx *bolt.Tx, filter platform.BucketFilter) ([]*platform.Bucket, error) {
	bs := []*platform.Bucket{}
	if filter.Organization != nil {
		o, err := c.findOrganizationByName(ctx, tx, *filter.Organization)
		if err != nil {
			return nil, err
		}
		filter.OrganizationID = &o.ID
	}

	filterFn := filterBucketsFn(filter)
	err := c.forEachBucket(ctx, tx, func(b *platform.Bucket) bool {
		if filterFn(b) {
			bs = append(bs, b)
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	return bs, nil
}

// CreateBucket creates a platform bucket and sets b.ID.
func (c *Client) CreateBucket(ctx context.Context, b *platform.Bucket) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		if !b.OrganizationID.Valid() {
			o, err := c.findOrganizationByName(ctx, tx, b.Organization)
			if err != nil {
				return err
			}
			b.OrganizationID = o.ID
		}

		unique := c.uniqueBucketName(ctx, tx, b)

		if !unique {
			// TODO: make standard error
			return fmt.Errorf("bucket with name %s already exists", b.Name)
		}

		b.ID = c.IDGenerator.ID()

		return c.putBucket(ctx, tx, b)
	})
}

// PutBucket will put a bucket without setting an ID.
func (c *Client) PutBucket(ctx context.Context, b *platform.Bucket) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.putBucket(ctx, tx, b)
	})
}

func (c *Client) putBucket(ctx context.Context, tx *bolt.Tx, b *platform.Bucket) error {
	b.Organization = ""
	v, err := json.Marshal(b)
	if err != nil {
		return err
	}

	encodedID, err := b.ID.Encode()
	if err != nil {
		return err
	}
	key, err := bucketIndexKey(b)
	if err != nil {
		return err
	}

	if err := tx.Bucket(bucketIndex).Put(key, encodedID); err != nil {
		return err
	}
	if err := tx.Bucket(bucketBucket).Put(encodedID, v); err != nil {
		return err
	}
	return c.setOrganizationOnBucket(ctx, tx, b)
}

func bucketIndexKey(b *platform.Bucket) ([]byte, error) {
	orgID, err := b.OrganizationID.Encode()
	if err != nil {
		return nil, err
	}
	k := make([]byte, platform.IDStringLength+len(b.Name))
	copy(k, orgID)
	copy(k[platform.IDStringLength:], []byte(b.Name))
	return k, nil
}

// forEachBucket will iterate through all buckets while fn returns true.
func (c *Client) forEachBucket(ctx context.Context, tx *bolt.Tx, fn func(*platform.Bucket) bool) error {
	cur := tx.Bucket(bucketBucket).Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
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
		return c.deleteBucket(ctx, tx, id)
	})
}

func (c *Client) deleteBucket(ctx context.Context, tx *bolt.Tx, id platform.ID) error {
	b, err := c.findBucketByID(ctx, tx, id)
	if err != nil {
		return err
	}
	key, err := bucketIndexKey(b)
	if err != nil {
		return err
	}
	// make lowercase deleteBucket with tx
	if err := tx.Bucket(bucketIndex).Delete(key); err != nil {
		return err
	}
	encodedID, err := id.Encode()
	if err != nil {
		return err
	}
	return tx.Bucket(bucketBucket).Delete(encodedID)
}
