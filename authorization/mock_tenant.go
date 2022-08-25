package authorization

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

// tenantService is a mock implementation of an authorization.tenantService
type tenantService struct {
	FindUserByIDFn        func(context.Context, platform.ID) (*influxdb.User, error)
	FindUserFn            func(context.Context, influxdb.UserFilter) (*influxdb.User, error)
	FindOrganizationByIDF func(ctx context.Context, id platform.ID) (*influxdb.Organization, error)
	FindOrganizationF     func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error)
	FindBucketByIDFn      func(context.Context, platform.ID) (*influxdb.Bucket, error)
}

// FindUserByID returns a single User by ID.
func (s *tenantService) FindUserByID(ctx context.Context, id platform.ID) (*influxdb.User, error) {
	return s.FindUserByIDFn(ctx, id)
}

// FindUsers returns a list of Users that match filter and the total count of matching Users.
func (s *tenantService) FindUser(ctx context.Context, filter influxdb.UserFilter) (*influxdb.User, error) {
	return s.FindUserFn(ctx, filter)
}

// FindOrganizationByID calls FindOrganizationByIDF.
func (s *tenantService) FindOrganizationByID(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
	return s.FindOrganizationByIDF(ctx, id)
}

// FindOrganization calls FindOrganizationF.
func (s *tenantService) FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
	return s.FindOrganizationF(ctx, filter)
}

func (s *tenantService) FindBucketByID(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
	return s.FindBucketByIDFn(ctx, id)
}
