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

func (BucketLookup) LookupName(_ context.Context, orgID platform2.ID, id platform2.ID) string {
	if id == 1 {
		return "my-bucket"
	}
	return ""
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
