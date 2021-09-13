package mock

import (
	"context"

	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
)

var _ platform.BucketOperationLogService = (*BucketOperationLogService)(nil)
var _ platform.DashboardOperationLogService = (*DashboardOperationLogService)(nil)
var _ platform.OrganizationOperationLogService = (*OrganizationOperationLogService)(nil)
var _ platform.UserOperationLogService = (*UserOperationLogService)(nil)

// NewBucketOperationLogService returns a mock of BucketOperationLogService.
func NewBucketOperationLogService() *BucketOperationLogService {
	return &BucketOperationLogService{
		GetBucketOperationLogFn: func(context.Context, platform2.ID, platform.FindOptions) ([]*platform.OperationLogEntry, int, error) {
			return nil, 0, nil
		},
	}
}

// NewDashboardOperationLogService returns a mock of DashboardOperationLogService.
func NewDashboardOperationLogService() *DashboardOperationLogService {
	return &DashboardOperationLogService{
		GetDashboardOperationLogFn: func(context.Context, platform2.ID, platform.FindOptions) ([]*platform.OperationLogEntry, int, error) {
			return nil, 0, nil
		},
	}
}

// NewOrganizationOperationLogService returns a mock of OrganizationOperationLogService.
func NewOrganizationOperationLogService() *OrganizationOperationLogService {
	return &OrganizationOperationLogService{
		GetOrganizationOperationLogFn: func(context.Context, platform2.ID, platform.FindOptions) ([]*platform.OperationLogEntry, int, error) {
			return nil, 0, nil
		},
	}
}

// NewUserOperationLogService returns a mock of UserOperationLogService.
func NewUserOperationLogService() *UserOperationLogService {
	return &UserOperationLogService{
		GetUserOperationLogFn: func(context.Context, platform2.ID, platform.FindOptions) ([]*platform.OperationLogEntry, int, error) {
			return nil, 0, nil
		},
	}
}

// BucketOperationLogService is a mock implementation of platform.BucketOperationLogService.
type BucketOperationLogService struct {
	GetBucketOperationLogFn func(context.Context, platform2.ID, platform.FindOptions) ([]*platform.OperationLogEntry, int, error)
}

// DashboardOperationLogService is a mock implementation of platform.DashboardOperationLogService.
type DashboardOperationLogService struct {
	GetDashboardOperationLogFn func(context.Context, platform2.ID, platform.FindOptions) ([]*platform.OperationLogEntry, int, error)
}

// OrganizationOperationLogService is a mock implementation of platform.OrganizationOperationLogService.
type OrganizationOperationLogService struct {
	GetOrganizationOperationLogFn func(context.Context, platform2.ID, platform.FindOptions) ([]*platform.OperationLogEntry, int, error)
}

// UserOperationLogService is a mock implementation of platform.UserOperationLogService.
type UserOperationLogService struct {
	GetUserOperationLogFn func(context.Context, platform2.ID, platform.FindOptions) ([]*platform.OperationLogEntry, int, error)
}

// GetBucketOperationLog retrieves the operation log for the bucket with the provided id.
func (s *BucketOperationLogService) GetBucketOperationLog(ctx context.Context, id platform2.ID, opts platform.FindOptions) ([]*platform.OperationLogEntry, int, error) {
	return s.GetBucketOperationLogFn(ctx, id, opts)
}

// GetDashboardOperationLog retrieves the operation log for the dashboard with the provided id.
func (s *DashboardOperationLogService) GetDashboardOperationLog(ctx context.Context, id platform2.ID, opts platform.FindOptions) ([]*platform.OperationLogEntry, int, error) {
	return s.GetDashboardOperationLogFn(ctx, id, opts)
}

// GetOrganizationOperationLog retrieves the operation log for the org with the provided id.
func (s *OrganizationOperationLogService) GetOrganizationOperationLog(ctx context.Context, id platform2.ID, opts platform.FindOptions) ([]*platform.OperationLogEntry, int, error) {
	return s.GetOrganizationOperationLogFn(ctx, id, opts)
}

// GetUserOperationLog retrieves the operation log for the user with the provided id.
func (s *UserOperationLogService) GetUserOperationLog(ctx context.Context, id platform2.ID, opts platform.FindOptions) ([]*platform.OperationLogEntry, int, error) {
	return s.GetUserOperationLogFn(ctx, id, opts)
}
