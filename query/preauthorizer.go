package query

import (
	"github.com/pkg/errors"
	"github.com/influxdata/platform"
	"context"
)

// PreAuthorizer provides a method for ensuring that the buckets accessed by a query spec
// are allowed access by the given Authorization.  This is a pre-check provided as a way for
// callers to fail early for operations that are not allowed.  However, it's still possible
// for authorization to be denied at runtime even if this check passes.
type PreAuthorizer interface {
	PreAuthorize(ctx context.Context, spec *Spec, auth *platform.Authorization) error
}

// NewPreAuthorizer creates a new PreAuthorizer
func NewPreAuthorizer(bucketService platform.BucketService) PreAuthorizer {
	return &preAuthorizer{bucketService: bucketService}
}

type preAuthorizer struct {
	bucketService platform.BucketService
}

// PreAuthorize finds all the buckets read and written by the given spec, and ensures that execution is allowed
// given the Authorization.  Returns nil on success, and an error with an appropriate message otherwise.
func (a *preAuthorizer) PreAuthorize(ctx context.Context, spec *Spec, auth *platform.Authorization) error {

	readBuckets, writeBuckets, err := spec.BucketsAccessed()

	if err != nil {
		return errors.Wrap(err, "Could not retrieve buckets for query.Spec")
	}

	for _, readBucketFilter := range readBuckets {
		bucket, err := a.bucketService.FindBucket(ctx, readBucketFilter)
		if err != nil {
			return errors.Wrapf(err, "Bucket service error")
		} else if bucket == nil {
			return errors.New("Bucket service returned nil bucket")
		}

		reqPerm := platform.ReadBucketPermission(bucket.ID)
		if ! platform.Allowed(reqPerm, auth) {
			return errors.New("No read permission for bucket: \"" + bucket.Name + "\"")
		}
	}

	for _, writeBucketFilter := range writeBuckets {
		bucket, err := a.bucketService.FindBucket(context.Background(), writeBucketFilter)
		if err != nil {
			return errors.Wrapf(err, "Could not find bucket %v", writeBucketFilter)
		}

		reqPerm := platform.WriteBucketPermission(bucket.ID)
		if ! platform.Allowed(reqPerm, auth) {
			return errors.New("No write permission for bucket: \"" + bucket.Name + "\"")
		}
	}

	return nil
}
