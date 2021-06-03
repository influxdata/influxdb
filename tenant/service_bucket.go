package tenant

import (
	"context"
	"fmt"
	"strings"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

type BucketSvc struct {
	store *Store
	svc   *Service
}

func NewBucketSvc(st *Store, svc *Service) *BucketSvc {
	return &BucketSvc{
		store: st,
		svc:   svc,
	}
}

// FindBucketByID returns a single bucket by ID.
func (s *BucketSvc) FindBucketByID(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
	var bucket *influxdb.Bucket
	err := s.store.View(ctx, func(tx kv.Tx) error {
		b, err := s.store.GetBucket(ctx, tx, id)
		if err != nil {
			return err
		}
		bucket = b
		return nil
	})

	if err != nil {
		return nil, err
	}

	return bucket, nil
}

func (s *BucketSvc) FindBucketByName(ctx context.Context, orgID platform.ID, name string) (*influxdb.Bucket, error) {
	var bucket *influxdb.Bucket
	err := s.store.View(ctx, func(tx kv.Tx) error {
		b, err := s.store.GetBucketByName(ctx, tx, orgID, name)
		if err != nil {
			return err
		}
		bucket = b
		return nil
	})

	if err != nil {
		return nil, err
	}

	return bucket, nil

}

// FindBucket returns the first bucket that matches filter.
func (s *BucketSvc) FindBucket(ctx context.Context, filter influxdb.BucketFilter) (*influxdb.Bucket, error) {
	if filter.ID != nil {
		return s.FindBucketByID(ctx, *filter.ID)
	}

	if filter.Name != nil && filter.OrganizationID != nil {
		return s.FindBucketByName(ctx, *filter.OrganizationID, *filter.Name)
	}

	buckets, _, err := s.FindBuckets(ctx, filter, influxdb.FindOptions{
		Limit: 1,
	})

	if err != nil {
		return nil, err
	}

	if len(buckets) < 1 {
		return nil, ErrBucketNotFound
	}

	return buckets[0], nil
}

// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
// Additional options provide pagination & sorting.
func (s *BucketSvc) FindBuckets(ctx context.Context, filter influxdb.BucketFilter, opt ...influxdb.FindOptions) ([]*influxdb.Bucket, int, error) {
	if filter.ID != nil {
		b, err := s.FindBucketByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}
		return []*influxdb.Bucket{b}, 1, nil
	}
	if filter.OrganizationID == nil && filter.Org != nil {
		org, err := s.svc.FindOrganization(ctx, influxdb.OrganizationFilter{Name: filter.Org})
		if err != nil {
			return nil, 0, err
		}
		filter.OrganizationID = &org.ID
	}

	var buckets []*influxdb.Bucket
	err := s.store.View(ctx, func(tx kv.Tx) error {
		if filter.Name != nil && filter.OrganizationID != nil {
			b, err := s.store.GetBucketByName(ctx, tx, *filter.OrganizationID, *filter.Name)
			if err != nil {
				return err
			}
			buckets = []*influxdb.Bucket{b}
			return nil
		}

		bs, err := s.store.ListBuckets(ctx, tx, BucketFilter{
			Name:           filter.Name,
			OrganizationID: filter.OrganizationID,
		}, opt...)
		if err != nil {
			return err
		}
		buckets = bs
		return nil
	})

	if err != nil {
		return nil, 0, err
	}

	return buckets, len(buckets), nil
}

// CreateBucket creates a new bucket and sets b.ID with the new identifier.
func (s *BucketSvc) CreateBucket(ctx context.Context, b *influxdb.Bucket) error {
	if !b.OrgID.Valid() {
		// we need a valid org id
		return ErrOrgNotFound
	}

	if err := validBucketName(b.Name, b.Type); err != nil {
		return err
	}

	// make sure the org exists
	if _, err := s.svc.FindOrganizationByID(ctx, b.OrgID); err != nil {
		return err
	}

	return s.store.Update(ctx, func(tx kv.Tx) error {
		return s.store.CreateBucket(ctx, tx, b)
	})
}

// UpdateBucket updates a single bucket with changeset.
// Returns the new bucket state after update.
func (s *BucketSvc) UpdateBucket(ctx context.Context, id platform.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
	var bucket *influxdb.Bucket
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		b, err := s.store.UpdateBucket(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		bucket = b
		return nil
	})

	if err != nil {
		return nil, err
	}

	return bucket, nil
}

// DeleteBucket removes a bucket by ID.
func (s *BucketSvc) DeleteBucket(ctx context.Context, id platform.ID) error {
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		bucket, err := s.store.GetBucket(ctx, tx, id)
		if err != nil {
			return err
		}
		if bucket.Type == influxdb.BucketTypeSystem && !isInternal(ctx) {
			// TODO: I think we should allow bucket deletes but maybe im wrong.
			return errDeleteSystemBucket
		}

		if err := s.store.DeleteBucket(ctx, tx, id); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return s.removeResourceRelations(ctx, id)
}

// removeResourceRelations allows us to clean up any resource relationship that would have normally been left over after a delete action of a resource.
func (s *BucketSvc) removeResourceRelations(ctx context.Context, resourceID platform.ID) error {
	urms, _, err := s.svc.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
		ResourceID: resourceID,
	})
	if err != nil {
		return err
	}
	for _, urm := range urms {
		err := s.svc.DeleteUserResourceMapping(ctx, urm.ResourceID, urm.UserID)
		if err != nil && err != ErrURMNotFound {
			return err
		}
	}
	return nil
}

// validBucketName reports any errors with bucket names
func validBucketName(name string, typ influxdb.BucketType) error {
	// names starting with an underscore are reserved for system buckets
	if strings.HasPrefix(name, "_") && typ != influxdb.BucketTypeSystem {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  fmt.Sprintf("bucket name %s is invalid. Buckets may not start with underscore", name),
			Op:   influxdb.OpCreateBucket,
		}
	}
	// quotation marks will cause queries to fail
	if strings.Contains(name, "\"") {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  fmt.Sprintf("bucket name %s is invalid. Bucket names may not include quotation marks", name),
			Op:   influxdb.OpCreateBucket,
		}
	}
	return nil
}
