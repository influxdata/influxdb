package query

import (
	"context"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query/id"
)

// FromBucketService wraps an platform.BucketService in the BucketLookup interface.
func FromBucketService(srv platform.BucketService) *BucketLookup {
	return &BucketLookup{
		BucketService: srv,
	}
}

// BucketLookup converts IFQL bucket lookups into platform.BucketService calls.
type BucketLookup struct {
	BucketService platform.BucketService
}

// Lookup returns the bucket id and its existence given an org id and bucket name.
func (b *BucketLookup) Lookup(orgID id.ID, name string) (id.ID, bool) {
	oid := platform.ID(orgID)
	filter := platform.BucketFilter{
		OrganizationID: &oid,
		Name:           &name,
	}
	bucket, err := b.BucketService.FindBucket(context.Background(), filter)
	if err != nil {
		return nil, false
	}
	return id.ID(bucket.ID), true
}
