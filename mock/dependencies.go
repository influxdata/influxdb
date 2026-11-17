package mock

import (
	"context"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
)

// BucketLookup implements the BucketLookup interface needed by flux "from" and "to".
type BucketLookup struct{}

func (BucketLookup) Lookup(_ context.Context, orgID platform2.ID, name string) (platform2.ID, bool) {
	if name == "my-bucket" {
		return platform2.ID(1), true
	}
	return platform2.InvalidID(), false
}

func (BucketLookup) LookupByID(_ context.Context, orgID platform2.ID, id platform2.ID) (platform2.ID, bool) {
	if id == 1 {
		return platform2.ID(1), true
	}
	return platform2.InvalidID(), false
}

func (BucketLookup) LookupName(_ context.Context, orgID platform2.ID, id platform2.ID) string {
	if id == 1 {
		return "my-bucket"
	}
	return ""
}

// BucketLookupFn implements the BucketLookup interface needed by flux "from" and "to"
// by delegating to its function fields. A nil function field reports the bucket as not found.
type BucketLookupFn struct {
	LookupFn     func(ctx context.Context, orgID platform2.ID, name string) (platform2.ID, bool)
	LookupByIDFn func(ctx context.Context, orgID platform2.ID, id platform2.ID) (platform2.ID, bool)
}

func (b BucketLookupFn) Lookup(ctx context.Context, orgID platform2.ID, name string) (platform2.ID, bool) {
	if b.LookupFn == nil {
		return platform2.InvalidID(), false
	}
	return b.LookupFn(ctx, orgID, name)
}

func (b BucketLookupFn) LookupByID(ctx context.Context, orgID platform2.ID, id platform2.ID) (platform2.ID, bool) {
	if b.LookupByIDFn == nil {
		return platform2.InvalidID(), false
	}
	return b.LookupByIDFn(ctx, orgID, id)
}

// OrganizationLookup implements the OrganizationLookup interface needed by flux "from" and "to".
type OrganizationLookup struct{}

func (OrganizationLookup) Lookup(_ context.Context, name string) (platform2.ID, bool) {
	if name == "my-org" {
		return platform2.ID(2), true
	}
	return platform2.InvalidID(), false
}

func (OrganizationLookup) LookupName(_ context.Context, id platform2.ID) string {
	if id == 2 {
		return "my-org"
	}
	return ""
}
