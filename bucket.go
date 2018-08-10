package platform

import (
	"context"
	"time"
)

// Bucket is a bucket. 🎉
type Bucket struct {
	ID                  ID            `json:"id,omitempty"`
	OrganizationID      ID            `json:"organizationID,omitempty"`
	Organization        string        `json:"organization,omitempty"`
	Name                string        `json:"name"`
	RetentionPolicyName string        `json:"rp,omitempty"` // This to support v1 sources
	RetentionPeriod     time.Duration `json:"retentionPeriod"`
}

// BucketService represents a service for managing bucket data.
type BucketService interface {
	// FindBucketByID returns a single bucket by ID.
	FindBucketByID(ctx context.Context, id ID) (*Bucket, error)

	// FindBucket returns the first bucket that matches filter.
	FindBucket(ctx context.Context, filter BucketFilter) (*Bucket, error)

	// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
	// Additional options provide pagination & sorting.
	FindBuckets(ctx context.Context, filter BucketFilter, opt ...FindOptions) ([]*Bucket, int, error)

	// CreateBucket creates a new bucket and sets b.ID with the new identifier.
	CreateBucket(ctx context.Context, b *Bucket) error

	// UpdateBucket updates a single bucket with changeset.
	// Returns the new bucket state after update.
	UpdateBucket(ctx context.Context, id ID, upd BucketUpdate) (*Bucket, error)

	// DeleteBucket removes a bucket by ID.
	DeleteBucket(ctx context.Context, id ID) error
}

// BucketUpdate represents updates to a bucket.
// Only fields which are set are updated.
type BucketUpdate struct {
	Name            *string        `json:"name,omitempty"`
	RetentionPeriod *time.Duration `json:"retentionPeriod,omitempty"`
}

// BucketFilter represents a set of filter that restrict the returned results.
type BucketFilter struct {
	ID             *ID
	Name           *string
	OrganizationID *ID
	Organization   *string
}

// FindOptions represents options passed to all find methods with multiple results.
type FindOptions struct {
	Limit      int
	Offset     int
	SortBy     string
	Descending bool
}
