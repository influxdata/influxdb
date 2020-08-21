package tenant

import (
	"context"
	"fmt"
	"strings"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

type BucketSvc struct {
	store *Store
	svc   *Service
}

const NumLegacyBuckets = 2

func NewBucketSvc(st *Store, svc *Service) *BucketSvc {
	return &BucketSvc{
		store: st,
		svc:   svc,
	}
}

// FindBucketByID returns a single bucket by ID.
func (s *BucketSvc) FindBucketByID(ctx context.Context, id influxdb.ID) (*influxdb.Bucket, error) {
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

func (s *BucketSvc) FindBucketByName(ctx context.Context, orgID influxdb.ID, name string) (*influxdb.Bucket, error) {
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

	if len(opt) == 0 {
		opt = append(opt, influxdb.FindOptions{
			Limit: influxdb.DefaultPageSize,
		})
	}
	o := opt[0]
	if o.Limit > influxdb.MaxPageSize || o.Limit == 0 {
		o.Limit = influxdb.MaxPageSize
	}

	// handle legacy buckets
	needsLegacyTasks := false
	needsLegacyMonitoring := false
	var err error
	var buckets []*influxdb.Bucket

	if filter.OrganizationID != nil && filter.Name == nil {
		err = s.store.View(ctx, func(tx kv.Tx) error {
			_, e := s.store.GetBucketByName(ctx, tx, *filter.OrganizationID, influxdb.TasksSystemBucketName)
			if e != nil && e.Error() == ErrBucketNotFoundByName(influxdb.TasksSystemBucketName).Error() {
				needsLegacyTasks = true
			} else if e != nil {
				return e
			}

			_, e = s.store.GetBucketByName(ctx, tx, *filter.OrganizationID, influxdb.MonitoringSystemBucketName)
			if e != nil && e.Error() == ErrBucketNotFoundByName(influxdb.MonitoringSystemBucketName).Error() {
				needsLegacyMonitoring = true
			} else if e != nil {
				return e
			}

			return nil
		})
	}

	if err != nil {
		return []*influxdb.Bucket{}, 0, err
	}

	if needsLegacyTasks {
		tb := &influxdb.Bucket{
			ID:              influxdb.TasksSystemBucketID,
			Type:            influxdb.BucketTypeSystem,
			Name:            influxdb.TasksSystemBucketName,
			RetentionPeriod: influxdb.TasksSystemBucketRetention,
			Description:     "System bucket for task logs",
		}

		// if request includes the first bucket, then add the legacy system bucket to
		// the beginning of response and adjust the limit
		// because this is the first bucket in the list, only return it if there is no After field
		// the offset is after the first bucket, then decrement offset to account for the "extra" bucket on first page
		if o.Offset < NumLegacyBuckets && o.After == nil {
			buckets = append(buckets, tb)
			o.Limit = o.Limit - 1
		} else {
			o.Offset = o.Offset - 1
		}
	}

	if needsLegacyMonitoring {
		mb := &influxdb.Bucket{
			ID:              influxdb.MonitoringSystemBucketID,
			Type:            influxdb.BucketTypeSystem,
			Name:            influxdb.MonitoringSystemBucketName,
			RetentionPeriod: influxdb.MonitoringSystemBucketRetention,
			Description:     "System bucket for monitoring logs",
		}

		// because this is the second bucket in the list, if there is an After field it will only be returned
		// if it's After the Task bucket
		if o.Offset < NumLegacyBuckets+1 {
			if o.After == nil || *o.After == influxdb.TasksSystemBucketID {
				buckets = append(buckets, mb)
				o.Limit = o.Limit - 1
			}
		} else {
			o.Offset = o.Offset - 1
		}
	}

	err = s.store.View(ctx, func(tx kv.Tx) error {
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
		}, o)
		if err != nil {
			return err
		}
		buckets = append(buckets, bs...)
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
func (s *BucketSvc) UpdateBucket(ctx context.Context, id influxdb.ID, upd influxdb.BucketUpdate) (*influxdb.Bucket, error) {
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
func (s *BucketSvc) DeleteBucket(ctx context.Context, id influxdb.ID) error {
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
func (s *BucketSvc) removeResourceRelations(ctx context.Context, resourceID influxdb.ID) error {
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
