package influxdb

import (
	"context"
	"time"
)

// OperationLogEntry is a record in an operation log.
type OperationLogEntry struct {
	Description string    `json:"description"`
	UserID      ID        `json:"userID,omitempty"`
	Time        time.Time `json:"time,omitempty"`
}

// DashboardOperationLogService is an interface for retrieving the operation log for a dashboard.
type DashboardOperationLogService interface {
	// GetDashboardOperationLog retrieves the operation log for the dashboard with the provided id.
	GetDashboardOperationLog(ctx context.Context, id ID, opts FindOptions) ([]*OperationLogEntry, int, error)
}

// BucketOperationLogService is an interface for retrieving the operation log for a bucket.
type BucketOperationLogService interface {
	// GetBucketOperationLog retrieves the operation log for the bucket with the provided id.
	GetBucketOperationLog(ctx context.Context, id ID, opts FindOptions) ([]*OperationLogEntry, int, error)
}

// UserOperationLogService is an interface for retrieving the operation log for a user.
type UserOperationLogService interface {
	// GetUserOperationLog retrieves the operation log for the user with the provided id.
	GetUserOperationLog(ctx context.Context, id ID, opts FindOptions) ([]*OperationLogEntry, int, error)
}

// OrganizationOperationLogService is an interface for retrieving the operation log for an org.
type OrganizationOperationLogService interface {
	// GetOrganizationOperationLog retrieves the operation log for the org with the provided id.
	GetOrganizationOperationLog(ctx context.Context, id ID, opts FindOptions) ([]*OperationLogEntry, int, error)
}

// DefaultOperationLogFindOptions are the default options for the operation log.
var DefaultOperationLogFindOptions = FindOptions{
	Descending: true,
	Limit:      100,
}
