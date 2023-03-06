package influxdb

import (
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

const (
	MinReplicationMaxQueueSizeBytes     int64 = 33554430 // 32 MiB
	DefaultReplicationMaxQueueSizeBytes       = 2 * MinReplicationMaxQueueSizeBytes
	DefaultReplicationMaxAge            int64 = 604800 // 1 week, in seconds
)

var ErrMaxQueueSizeTooSmall = errors.Error{
	Code: errors.EInvalid,
	Msg:  fmt.Sprintf("maxQueueSize too small, must be at least %d", MinReplicationMaxQueueSizeBytes),
}

// Replication contains all info about a replication that should be returned to users.
type Replication struct {
	ID                       platform.ID  `json:"id" db:"id"`
	OrgID                    platform.ID  `json:"orgID" db:"org_id"`
	Name                     string       `json:"name" db:"name"`
	Description              *string      `json:"description,omitempty" db:"description"`
	RemoteID                 platform.ID  `json:"remoteID" db:"remote_id"`
	LocalBucketID            platform.ID  `json:"localBucketID" db:"local_bucket_id"`
	RemoteBucketID           *platform.ID `json:"remoteBucketID" db:"remote_bucket_id"`
	RemoteBucketName         string       `json:"RemoteBucketName" db:"remote_bucket_name"`
	MaxQueueSizeBytes        int64        `json:"maxQueueSizeBytes" db:"max_queue_size_bytes"`
	CurrentQueueSizeBytes    int64        `json:"currentQueueSizeBytes"`
	RemainingBytesToBeSynced int64        `json:"remainingBytesToBeSynced"`
	LatestResponseCode       *int32       `json:"latestResponseCode,omitempty" db:"latest_response_code"`
	LatestErrorMessage       *string      `json:"latestErrorMessage,omitempty" db:"latest_error_message"`
	DropNonRetryableData     bool         `json:"dropNonRetryableData" db:"drop_non_retryable_data"`
	MaxAgeSeconds            int64        `json:"maxAgeSeconds" db:"max_age_seconds"`
}

// ReplicationListFilter is a selection filter for listing replications.
type ReplicationListFilter struct {
	OrgID         platform.ID
	Name          *string
	RemoteID      *platform.ID
	LocalBucketID *platform.ID
}

// Replications is a collection of metadata about replications.
type Replications struct {
	Replications []Replication `json:"replications"`
}

// TrackedReplication defines a replication stream which is currently being tracked via sqlite.
type TrackedReplication struct {
	MaxQueueSizeBytes int64
	MaxAgeSeconds     int64
	OrgID             platform.ID
	LocalBucketID     platform.ID
}

// CreateReplicationRequest contains all info needed to establish a new replication
// to a remote InfluxDB bucket.
type CreateReplicationRequest struct {
	OrgID                platform.ID `json:"orgID"`
	Name                 string      `json:"name"`
	Description          *string     `json:"description,omitempty"`
	RemoteID             platform.ID `json:"remoteID"`
	LocalBucketID        platform.ID `json:"localBucketID"`
	RemoteBucketID       platform.ID `json:"remoteBucketID"`
	RemoteBucketName     string      `json:"remoteBucketName"`
	MaxQueueSizeBytes    int64       `json:"maxQueueSizeBytes,omitempty"`
	DropNonRetryableData bool        `json:"dropNonRetryableData,omitempty"`
	MaxAgeSeconds        int64       `json:"maxAgeSeconds,omitempty"`
}

func (r *CreateReplicationRequest) OK() error {
	if r.MaxQueueSizeBytes < MinReplicationMaxQueueSizeBytes {
		return &ErrMaxQueueSizeTooSmall
	}

	return nil
}

// UpdateReplicationRequest contains a partial update to existing info about a replication.
type UpdateReplicationRequest struct {
	Name                 *string      `json:"name,omitempty"`
	Description          *string      `json:"description,omitempty"`
	RemoteID             *platform.ID `json:"remoteID,omitempty"`
	RemoteBucketID       *platform.ID `json:"remoteBucketID,omitempty"`
	RemoteBucketName     *string      `json:"remoteBucketName,omitempty"`
	MaxQueueSizeBytes    *int64       `json:"maxQueueSizeBytes,omitempty"`
	DropNonRetryableData *bool        `json:"dropNonRetryableData,omitempty"`
	MaxAgeSeconds        *int64       `json:"maxAgeSeconds,omitempty"`
}

func (r *UpdateReplicationRequest) OK() error {
	if r.MaxQueueSizeBytes == nil {
		return nil
	}

	if *r.MaxQueueSizeBytes < MinReplicationMaxQueueSizeBytes {
		return &ErrMaxQueueSizeTooSmall
	}

	return nil
}

// ReplicationHTTPConfig contains all info needed by a client to make HTTP requests against the
// remote bucket targeted by a replication.
type ReplicationHTTPConfig struct {
	RemoteURL            string       `db:"remote_url"`
	RemoteToken          string       `db:"remote_api_token"`
	RemoteOrgID          *platform.ID `db:"remote_org_id"`
	AllowInsecureTLS     bool         `db:"allow_insecure_tls"`
	RemoteBucketID       *platform.ID `db:"remote_bucket_id"`
	RemoteBucketName     string       `db:"remote_bucket_name"`
	DropNonRetryableData bool         `db:"drop_non_retryable_data"`
}
