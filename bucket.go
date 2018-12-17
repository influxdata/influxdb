package platform

import (
	"context"
	"fmt"
	"time"
)

// BucketType defines known system-buckets.
type BucketType int

const (
	// BucketTypeLogs defines the bucket ID of the system logs.
	BucketTypeLogs = BucketType(iota + 10)
)

// InfiniteRetention is default infinite retention period.
const InfiniteRetention = 0

// Bucket is a bucket. 🎉
type Bucket struct {
	ID                  ID            `json:"id,omitempty"`
	OrganizationID      ID            `json:"organizationID,omitempty"`
	Organization        string        `json:"organization,omitempty"`
	Name                string        `json:"name"`
	RetentionPolicyName string        `json:"rp,omitempty"` // This to support v1 sources
	RetentionPeriod     time.Duration `json:"retentionPeriod"`
}

// ops for buckets error and buckets op logs.
var (
	OpFindBucketByID = "FindBucketByID"
	OpFindBucket     = "FindBucket"
	OpFindBuckets    = "FindBuckets"
	OpCreateBucket   = "CreateBucket"
	OpUpdateBucket   = "UpdateBucket"
	OpDeleteBucket   = "DeleteBucket"
)

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

// QueryParams Converts BucketFilter fields to url query params.
func (f BucketFilter) QueryParams() map[string][]string {
	qp := map[string][]string{}
	if f.ID != nil {
		qp["id"] = []string{f.ID.String()}
	}

	if f.Name != nil {
		qp["name"] = []string{*f.Name}
	}

	if f.OrganizationID != nil {
		qp["orgID"] = []string{f.OrganizationID.String()}
	}

	if f.Organization != nil {
		qp["org"] = []string{*f.Organization}
	}

	return qp
}

// InternalBucketID returns the ID for an organization's specified internal bucket
func InternalBucketID(t BucketType) (*ID, error) {
	return IDFromString(fmt.Sprintf("%d", t))
}
