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

// BucketLookup converts Flux bucket lookups into platform.BucketService calls.
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

// FromOrganizationService wraps a platform.OrganizationService in the OrganizationLookup interface.
func FromOrganizationService(srv platform.OrganizationService) *OrganizationLookup {
	return &OrganizationLookup{OrganizationService: srv}
}

// OrganizationLookup converts organization name lookups into platform.OrganizationService calls.
type OrganizationLookup struct {
	OrganizationService platform.OrganizationService
}

// Lookup returns the organization ID and its existence given an organization name.
func (o *OrganizationLookup) Lookup(ctx context.Context, name string) (platform.ID, bool) {
	org, err := o.OrganizationService.FindOrganization(
		ctx,
		platform.OrganizationFilter{Name: &name},
	)

	if err != nil {
		return nil, false
	}
	return org.ID, true
}
