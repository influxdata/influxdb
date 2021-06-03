package influxdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

const (
	// BucketTypeUser is a user created bucket
	BucketTypeUser = BucketType(0)
	// BucketTypeSystem is an internally created bucket that cannot be deleted/renamed.
	BucketTypeSystem = BucketType(1)
	// MonitoringSystemBucketRetention is the time we should retain monitoring system bucket information
	MonitoringSystemBucketRetention = time.Hour * 24 * 7
	// TasksSystemBucketRetention is the time we should retain task system bucket information
	TasksSystemBucketRetention = time.Hour * 24 * 3
)

// Bucket names constants
const (
	TasksSystemBucketName      = "_tasks"
	MonitoringSystemBucketName = "_monitoring"
)

// InfiniteRetention is default infinite retention period.
const InfiniteRetention = 0

// Bucket is a bucket. 🎉
type Bucket struct {
	ID                  platform.ID   `json:"id,omitempty"`
	OrgID               platform.ID   `json:"orgID,omitempty"`
	Type                BucketType    `json:"type"`
	Name                string        `json:"name"`
	Description         string        `json:"description"`
	RetentionPolicyName string        `json:"rp,omitempty"` // This to support v1 sources
	RetentionPeriod     time.Duration `json:"retentionPeriod"`
	ShardGroupDuration  time.Duration `json:"shardGroupDuration"`
	CRUDLog
}

// Clone returns a shallow copy of b.
func (b *Bucket) Clone() *Bucket {
	other := *b
	return &other
}

// BucketType differentiates system buckets from user buckets.
type BucketType int

// String converts a BucketType into a human-readable string.
func (bt BucketType) String() string {
	if bt == BucketTypeSystem {
		return "system"
	}
	return "user"
}

// ParseBucketType parses a bucket type from a string
func ParseBucketType(s string) BucketType {
	if s == "system" {
		return BucketTypeSystem
	}
	return BucketTypeUser
}

// ops for buckets error and buckets op logs.
var (
	OpFindBucketByID = "FindBucketByID"
	OpFindBucket     = "FindBucket"
	OpFindBuckets    = "FindBuckets"
	OpCreateBucket   = "CreateBucket"
	OpPutBucket      = "PutBucket"
	OpUpdateBucket   = "UpdateBucket"
	OpDeleteBucket   = "DeleteBucket"
)

// BucketService represents a service for managing bucket data.
type BucketService interface {
	// FindBucketByID returns a single bucket by ID.
	FindBucketByID(ctx context.Context, id platform.ID) (*Bucket, error)

	// FindBucket returns the first bucket that matches filter.
	FindBucket(ctx context.Context, filter BucketFilter) (*Bucket, error)

	// FindBuckets returns a list of buckets that match filter and the total count of matching buckets.
	// Additional options provide pagination & sorting.
	FindBuckets(ctx context.Context, filter BucketFilter, opt ...FindOptions) ([]*Bucket, int, error)

	// CreateBucket creates a new bucket and sets b.ID with the new identifier.
	CreateBucket(ctx context.Context, b *Bucket) error

	// UpdateBucket updates a single bucket with changeset.
	// Returns the new bucket state after update.
	UpdateBucket(ctx context.Context, id platform.ID, upd BucketUpdate) (*Bucket, error)

	// DeleteBucket removes a bucket by ID.
	DeleteBucket(ctx context.Context, id platform.ID) error
	FindBucketByName(ctx context.Context, orgID platform.ID, name string) (*Bucket, error)
}

// BucketUpdate represents updates to a bucket.
// Only fields which are set are updated.
type BucketUpdate struct {
	Name               *string
	Description        *string
	RetentionPeriod    *time.Duration
	ShardGroupDuration *time.Duration
}

// BucketFilter represents a set of filter that restrict the returned results.
type BucketFilter struct {
	ID             *platform.ID
	Name           *string
	OrganizationID *platform.ID
	Org            *string
}

// QueryParams Converts BucketFilter fields to url query params.
func (f BucketFilter) QueryParams() map[string][]string {
	qp := map[string][]string{}
	if f.ID != nil {
		qp["id"] = []string{f.ID.String()}
	}

	if f.Name != nil {
		qp["bucket"] = []string{*f.Name}
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

func ErrInternalBucketServiceError(op string, err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  fmt.Sprintf("unexpected error in buckets; Err: %v", err),
		Op:   op,
		Err:  err,
	}
}
