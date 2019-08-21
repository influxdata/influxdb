package influxdb

import (
	"context"
	"strings"
	"time"
)

// BucketType defines known system-buckets.
type BucketType int

// String converts a BucketType into a human-readable string.
func (bt BucketType) String() string {
	if bt == 0 {
		return "user"
	}

	return [...]string{"logs", "tasks", "monitoring"}[bt-10]
}

const (
	// ~*~ User Buckets ~*~
	BucketTypeUser = BucketType(0) // BucketTypeUser describes a user-created data bucket. Its use of a zero value ensures that it is the default type.

	// ~*~ System Buckets ~*~
	BucketTypeLogs       BucketType = iota + 10 // BucketTypeLogs defines the bucket ID of the system logs.
	BucketTypeTasks                             // BucketTypeTasks defines the bucket ID of task run logs.
	BucketTypeMonitoring                        // BucketTypeMonitoring defines the bucket ID of monitoring records.
)

// InfiniteRetention is default infinite retention period.
const InfiniteRetention = 0

// Bucket is a bucket. 🎉
type Bucket struct {
	ID                  ID            `json:"id,omitempty"`
	OrgID               ID            `json:"orgID,omitempty"`
	Type                BucketType    `json:"type"`
	Name                string        `json:"name"`
	Description         string        `json:"description"`
	RetentionPolicyName string        `json:"rp,omitempty"` // This to support v1 sources
	RetentionPeriod     time.Duration `json:"retentionPeriod"`
	CRUDLog
}

// IsSystem returns true if the bucket is a reserved system bucket.
func (b *Bucket) IsSystem() bool {
	return b.Type != 0
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
	Description     *string        `json:"description,omitempty"`
	RetentionPeriod *time.Duration `json:"retentionPeriod,omitempty"`
}

// BucketFilter represents a set of filter that restrict the returned results.
type BucketFilter struct {
	ID             *ID
	Name           *string
	OrganizationID *ID
	Org            *string
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

	if f.Org != nil {
		qp["org"] = []string{*f.Org}
	}

	return qp
}

// String returns a human-readable string of the BucketFilter,
// particularly useful for error messages.
func (f BucketFilter) String() string {
	// There should always be exactly 2 fields set, but if it's somehow more, that's fine.
	parts := make([]string, 0, 2)
	if f.ID != nil {
		parts = append(parts, "Bucket ID: "+f.ID.String())
	}
	if f.Name != nil {
		parts = append(parts, "Bucket Name: "+*f.Name)
	}
	if f.OrganizationID != nil {
		parts = append(parts, "Org ID: "+f.OrganizationID.String())
	}
	if f.Org != nil {
		parts = append(parts, "Org Name: "+*f.Org)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}
