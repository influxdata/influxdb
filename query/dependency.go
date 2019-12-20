package query

import (
	"context"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	platform "github.com/influxdata/influxdb/v2"
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
func (b *BucketLookup) Lookup(ctx context.Context, orgID platform.ID, name string) (platform.ID, bool) {
	oid := platform.ID(orgID)
	filter := platform.BucketFilter{
		OrganizationID: &oid,
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
	oid := platform.ID(orgID)
	id = platform.ID(id)
	filter := platform.BucketFilter{
		OrganizationID: &oid,
		ID:             &id,
	}
	bucket, err := b.BucketService.FindBucket(ctx, filter)
	if err != nil || bucket == nil {
		return ""
	}
	return bucket.Name
}

func (b *BucketLookup) FindAllBuckets(ctx context.Context, orgID platform.ID) ([]*platform.Bucket, int) {
	oid := platform.ID(orgID)
	filter := platform.BucketFilter{
		OrganizationID: &oid,
	}
	buckets, count, err := b.BucketService.FindBuckets(ctx, filter)
	if err != nil {
		return nil, count
	}
	return buckets, count

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
		return platform.InvalidID(), false
	}
	return org.ID, true
}

// LookupName returns an organization name given its ID.
func (o *OrganizationLookup) LookupName(ctx context.Context, id platform.ID) string {
	id = platform.ID(id)
	org, err := o.OrganizationService.FindOrganization(
		ctx,
		platform.OrganizationFilter{
			ID: &id,
		},
	)

	if err != nil || org == nil {
		return ""
	}
	return org.Name
}

// SecretLookup wraps the platform.SecretService to perform lookups based on the organization
// in the context.
type SecretLookup struct {
	SecretService platform.SecretService
}

// FromSecretService wraps a platform.OrganizationService in the OrganizationLookup interface.
func FromSecretService(srv platform.SecretService) *SecretLookup {
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
