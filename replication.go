package influxdb

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

const (
	MinReplicationMaxQueueSizeBytes     int64 = 33554430 // 32 MiB
	DefaultReplicationMaxQueueSizeBytes       = 2 * MinReplicationMaxQueueSizeBytes
)

var ErrMaxQueueSizeTooSmall = errors.Error{
	Code: errors.EInvalid,
	Msg:  fmt.Sprintf("maxQueueSize too small, must be at least %d", MinReplicationMaxQueueSizeBytes),
}

type ReplicationService interface {
	// ListReplications returns all info about registered replications matching a filter.
	ListReplications(context.Context, ReplicationListFilter) (*Replications, error)

	// CreateReplication registers a new replication stream.
	CreateReplication(context.Context, CreateReplicationRequest) (*Replication, error)

	// ValidateNewReplication validates that the given settings for a replication are usable,
	// without persisting the configuration.
	ValidateNewReplication(context.Context, CreateReplicationRequest) error

	// GetReplication returns metadata about the replication with the given ID.
	GetReplication(context.Context, platform.ID) (*Replication, error)

	// UpdateReplication updates the settings for the replication with the given ID.
	UpdateReplication(context.Context, platform.ID, UpdateReplicationRequest) (*Replication, error)

	// ValidateUpdatedReplication valdiates that a replication is still usable after applying the
	// given update, without persisting the new configuration.
	ValidateUpdatedReplication(context.Context, platform.ID, UpdateReplicationRequest) error

	// DeleteReplication deletes all info for the replication with the given ID.
	DeleteReplication(context.Context, platform.ID) error

	// ValidateReplication checks that the replication with the given ID is still usable with its
	// persisted settings.
	ValidateReplication(context.Context, platform.ID) error
}

// Replication contains all info about a replication that should be returned to users.
type Replication struct {
	ID                    platform.ID `json:"id" db:"id"`
	OrgID                 platform.ID `json:"orgID" db:"org_id"`
	Name                  string      `json:"name" db:"name"`
	Description           *string     `json:"description,omitempty" db:"description"`
	RemoteID              platform.ID `json:"remoteID" db:"remote_id"`
	LocalBucketID         platform.ID `json:"localBucketID" db:"local_bucket_id"`
	RemoteBucketID        platform.ID `json:"remoteBucketID" db:"remote_bucket_id"`
	MaxQueueSizeBytes     int64       `json:"maxQueueSizeBytes" db:"max_queue_size_bytes"`
	CurrentQueueSizeBytes int64       `json:"currentQueueSizeBytes" db:"current_queue_size_bytes"`
	LatestResponseCode    *int32      `json:"latestResponseCode,omitempty" db:"latest_response_code"`
	LatestErrorMessage    *string     `json:"latestErrorMessage,omitempty" db:"latest_error_message"`
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

// CreateReplicationRequest contains all info needed to establish a new replication
// to a remote InfluxDB bucket.
type CreateReplicationRequest struct {
	OrgID             platform.ID `json:"orgID"`
	Name              string      `json:"name"`
	Description       *string     `json:"description,omitempty"`
	RemoteID          platform.ID `json:"remoteID"`
	LocalBucketID     platform.ID `json:"localBucketID"`
	RemoteBucketID    platform.ID `json:"remoteBucketID"`
	MaxQueueSizeBytes int64       `json:"maxQueueSizeBytes,omitempty"`
}

func (r *CreateReplicationRequest) OK() error {
	if r.MaxQueueSizeBytes < MinReplicationMaxQueueSizeBytes {
		return &ErrMaxQueueSizeTooSmall
	}

	return nil
}

// UpdateReplicationRequest contains a partial update to existing info about a replication.
type UpdateReplicationRequest struct {
	Name              *string      `json:"name,omitempty"`
	Description       *string      `json:"description,omitempty"`
	RemoteID          *platform.ID `json:"remoteID,omitempty"`
	RemoteBucketID    *platform.ID `json:"remoteBucketID,omitempty"`
	MaxQueueSizeBytes *int64       `json:"maxQueueSizeBytes,omitempty"`
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
