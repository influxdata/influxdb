package inmem

import (
	"context"
	"fmt"

	"github.com/influxdata/platform"
)

var (
	errBucketNotFound = fmt.Errorf("bucket not found")
)

func (c *Service) loadBucket(ctx context.Context, id platform.ID) (*platform.Bucket, error) {
	i, ok := c.bucketKV.Load(id.String())
	if !ok {
		return nil, errBucketNotFound
	}

	b, ok := i.(platform.Bucket)
	if !ok {
		return nil, fmt.Errorf("type %T is not a bucket", i)
	}

	if err := c.setOrganizationNameOnBucket(ctx, &b); err != nil {
		return nil, err
	}

	return &b, nil
}

func (c *Service) setOrganizationNameOnBucket(ctx context.Context, b *platform.Bucket) error {
	o, err := c.loadOrganization(b.OrganizationID)
	if err != nil {
		return err
	}

	b.Organization = o.Name
	return nil
}

// FindBucketByID returns a single bucket by ID.
func (s *Service) FindBucketByID(ctx context.Context, id platform.ID) (*platform.Bucket, error) {
	return s.loadBucket(ctx, id)
}

func (c *Service) forEachBucket(ctx context.Context, fn func(b *platform.Bucket) bool) error {
	var err error
	c.bucketKV.Range(func(k, v interface{}) bool {
		b, ok := v.(platform.Bucket)
		if !ok {
			err = fmt.Errorf("type %T is not a bucket", v)
			return false
		}
		return fn(&b)
	})

	return err
}

func (s *Service) filterBuckets(ctx context.Context, fn func(b *platform.Bucket) bool) ([]*platform.Bucket, error) {
	buckets := []*platform.Bucket{}
	err := s.forEachBucket(ctx, func(b *platform.Bucket) bool {
		if fn(b) {
			buckets = append(buckets, b)
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	return buckets, nil
}

// FindBucket returns the first bucket that matches filter.
func (s *Service) FindBucket(ctx context.Context, filter platform.BucketFilter) (*platform.Bucket, error) {
	if filter.ID == nil && filter.Name == nil && filter.OrganizationID == nil {
		return nil, fmt.Errorf("no filter parameters provided")
	}

	// filter by bucket id
	if filter.ID != nil {
		return s.FindBucketByID(ctx, *filter.ID)
	}

	bs, n, err := s.FindBuckets(ctx, filter)
	if err != nil {
		return nil, err
	}

	if n < 1 {
		return nil, fmt.Errorf("bucket not found")
	}

	return bs[0], nil
}

func (s *Service) findBuckets(ctx context.Context, filter platform.BucketFilter, opt ...platform.FindOptions) ([]*platform.Bucket, error) {
	// filter by bucket id
	if filter.ID != nil {
		b, err := s.FindBucketByID(ctx, *filter.ID)
		if err != nil {
			return nil, err
		}

		return []*platform.Bucket{b}, nil
	}

	if filter.Organization != nil {
		o, err := s.findOrganizationByName(ctx, *filter.Organization)
		if err != nil {
			return nil, err
		}
		filter.OrganizationID = &o.ID
	}

	filterFunc := func(b *platform.Bucket) bool { return true }

	if filter.Name != nil && filter.OrganizationID != nil {
		filterFunc = func(b *platform.Bucket) bool {
			return b.Name == *filter.Name && b.OrganizationID == *filter.OrganizationID
		}
	} else if filter.Name != nil {
		// filter by bucket name
		filterFunc = func(b *platform.Bucket) bool {
			return b.Name == *filter.Name
		}
	} else if filter.OrganizationID != nil {
		// filter by organization id
		filterFunc = func(b *platform.Bucket) bool {
			return b.OrganizationID == *filter.OrganizationID
		}
	}

	bs, err := s.filterBuckets(ctx, filterFunc)
	if err != nil {
		return nil, err
	}
	return bs, nil
}

// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
// Additional options provide pagination & sorting.
func (s *Service) FindBuckets(ctx context.Context, filter platform.BucketFilter, opt ...platform.FindOptions) ([]*platform.Bucket, int, error) {
	bs, err := s.findBuckets(ctx, filter, opt...)
	if err != nil {
		return nil, 0, err
	}
	for _, b := range bs {
		if err := s.setOrganizationNameOnBucket(ctx, b); err != nil {
			return nil, 0, err
		}
	}
	return bs, len(bs), nil
}

// CreateBucket creates a new bucket and sets b.ID with the new identifier.
func (s *Service) CreateBucket(ctx context.Context, b *platform.Bucket) error {
	if !b.OrganizationID.Valid() {
		o, err := s.findOrganizationByName(ctx, b.Organization)
		if err != nil {
			return err
		}
		b.OrganizationID = o.ID
	}
	filter := platform.BucketFilter{
		Name:           &b.Name,
		OrganizationID: &b.OrganizationID,
	}
	if _, err := s.FindBucket(ctx, filter); err == nil {
		return fmt.Errorf("bucket with name %s already exists", b.Name)
	}
	b.ID = s.IDGenerator.ID()
	return s.PutBucket(ctx, b)
}

// PutBucket sets bucket with the current ID.
func (s *Service) PutBucket(ctx context.Context, b *platform.Bucket) error {
	s.bucketKV.Store(b.ID.String(), *b)
	return nil
}

// UpdateBucket updates a single bucket with changeset.
// Returns the new bucket state after update.
func (s *Service) UpdateBucket(ctx context.Context, id platform.ID, upd platform.BucketUpdate) (*platform.Bucket, error) {
	b, err := s.FindBucketByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		b.Name = *upd.Name
	}

	if upd.RetentionPeriod != nil {
		b.RetentionPeriod = *upd.RetentionPeriod
	}

	s.bucketKV.Store(b.ID.String(), b)

	return b, nil
}

// DeleteBucket removes a bucket by ID.
func (s *Service) DeleteBucket(ctx context.Context, id platform.ID) error {
	if _, err := s.FindBucketByID(ctx, id); err != nil {
		return err
	}
	s.bucketKV.Delete(id.String())
	return nil

}

// DeleteOrganizationBuckets removes all the buckets for a given org
func (s *Service) DeleteOrganizationBuckets(ctx context.Context, id platform.ID) error {
	bucks, err := s.findBuckets(ctx, platform.BucketFilter{
		OrganizationID: &id,
	})
	if err != nil {
		return err
	}
	for _, buck := range bucks {
		s.bucketKV.Delete(buck.ID.String())
	}
	return nil
}
