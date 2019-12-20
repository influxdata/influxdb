package mock

import (
	"context"

	platform "github.com/influxdata/influxdb/v2"
)

// BucketLookup implements the BucketLookup interface needed by flux "from" and "to".
type BucketLookup struct{}

func (BucketLookup) Lookup(_ context.Context, orgID platform.ID, name string) (platform.ID, bool) {
	if name == "my-bucket" {
		return platform.ID(1), true
	}
	return platform.InvalidID(), false
}

func (BucketLookup) LookupName(_ context.Context, orgID platform.ID, id platform.ID) string {
	if id == 1 {
		return "my-bucket"
	}
	return ""
}

// OrganizationLookup implements the OrganizationLookup interface needed by flux "from" and "to".
type OrganizationLookup struct{}

func (OrganizationLookup) Lookup(_ context.Context, name string) (platform.ID, bool) {
	if name == "my-org" {
		return platform.ID(2), true
	}
	return platform.InvalidID(), false
}

func (OrganizationLookup) LookupName(_ context.Context, id platform.ID) string {
	if id == 2 {
		return "my-org"
	}
	return ""
}
