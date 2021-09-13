package query

import (
	"context"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

// FromBucketService wraps an influxdb.BucketService in the BucketLookup interface.
func FromBucketService(srv influxdb.BucketService) *BucketLookup {
	return &BucketLookup{
		BucketService: srv,
	}
}

// BucketLookup converts Flux bucket lookups into influxdb.BucketService calls.
type BucketLookup struct {
	BucketService influxdb.BucketService
}

// Lookup returns the bucket id and its existence given an org id and bucket name.
func (b *BucketLookup) Lookup(ctx context.Context, orgID platform.ID, name string) (platform.ID, bool) {
	filter := influxdb.BucketFilter{
		OrganizationID: &orgID,
		Name:           &name,
	}
	bucket, err := b.BucketService.FindBucket(ctx, filter)
	if err != nil {
		return platform.InvalidID(), false
	}
	return bucket.ID, true
}

// LookupName returns an bucket name given its organization ID and its bucket ID.
func (b *BucketLookup) LookupName(ctx context.Context, orgID platform.ID, id platform.ID) string {
	filter := influxdb.BucketFilter{
		OrganizationID: &orgID,
		ID:             &id,
	}
	bucket, err := b.BucketService.FindBucket(ctx, filter)
	if err != nil || bucket == nil {
		return ""
	}
	return bucket.Name
}

func (b *BucketLookup) FindAllBuckets(ctx context.Context, orgID platform.ID) ([]*influxdb.Bucket, int) {
	filter := influxdb.BucketFilter{
		OrganizationID: &orgID,
	}

	var allBuckets []*influxdb.Bucket
	opt := influxdb.FindOptions{Limit: 20}
	for ; ; opt.Offset += opt.Limit {
		buckets, _, err := b.BucketService.FindBuckets(ctx, filter, opt)
		if err != nil {
			return nil, len(buckets)
		}
		allBuckets = append(allBuckets, buckets...)
		if len(buckets) < opt.Limit {
			break
		}
	}
	return allBuckets, len(allBuckets)
}

// FromOrganizationService wraps a influxdb.OrganizationService in the OrganizationLookup interface.
func FromOrganizationService(srv influxdb.OrganizationService) *OrganizationLookup {
	return &OrganizationLookup{OrganizationService: srv}
}

// OrganizationLookup converts organization name lookups into influxdb.OrganizationService calls.
type OrganizationLookup struct {
	OrganizationService influxdb.OrganizationService
}

// Lookup returns the organization ID and its existence given an organization name.
func (o *OrganizationLookup) Lookup(ctx context.Context, name string) (platform.ID, bool) {
	org, err := o.OrganizationService.FindOrganization(
		ctx,
		influxdb.OrganizationFilter{Name: &name},
	)

	if err != nil {
		return platform.InvalidID(), false
	}
	return org.ID, true
}

// LookupName returns an organization name given its ID.
func (o *OrganizationLookup) LookupName(ctx context.Context, id platform.ID) string {
	id = platform.ID(id)
	org, err := o.OrganizationService.FindOrganization(
		ctx,
		influxdb.OrganizationFilter{
			ID: &id,
		},
	)

	if err != nil || org == nil {
		return ""
	}
	return org.Name
}

// SecretLookup wraps the influxdb.SecretService to perform lookups based on the organization
// in the context.
type SecretLookup struct {
	SecretService influxdb.SecretService
}

// FromSecretService wraps a influxdb.OrganizationService in the OrganizationLookup interface.
func FromSecretService(srv influxdb.SecretService) *SecretLookup {
	return &SecretLookup{SecretService: srv}
}

// LoadSecret loads the secret associated with the key in the current organization context.
func (s *SecretLookup) LoadSecret(ctx context.Context, key string) (string, error) {
	req := RequestFromContext(ctx)
	if req == nil {
		return "", &flux.Error{
			Code: codes.Internal,
			Msg:  "missing request on context",
		}
	}

	orgID := req.OrganizationID
	return s.SecretService.LoadSecret(ctx, orgID, key)
}
