package launcher_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	icontext "github.com/influxdata/influxdb/v2/context"
)

func testTenant(t *testing.T, args ...string) {
	l := launcher.RunTestLauncherOrFail(t, ctx, args...)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	ctx := context.Background()
	ctx = icontext.SetAuthorizer(ctx, l.Auth)

	o := &influxdb.Organization{Name: "a-org"}
	if err := l.OrganizationService().CreateOrganization(ctx, o); err != nil {
		t.Error(err)
	}
	u := &influxdb.User{Name: "a-user"}
	if err := l.UserService().CreateUser(ctx, u); err != nil {
		t.Error(err)
	}
	if err := l.BucketService(t).CreateBucket(ctx, &influxdb.Bucket{Name: "a-bucket", OrgID: o.ID}); err != nil {
		t.Error(err)
	}
	if err := l.UserResourceMappingService().CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
		UserID:       u.ID,
		UserType:     influxdb.Owner,
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   o.ID,
	}); err != nil {
		t.Error(err)
	}

	if _, _, err := l.BucketService(t).FindBuckets(ctx, influxdb.BucketFilter{}); err != nil {
		t.Error(err)
	}
	if _, _, err := l.OrganizationService().FindOrganizations(ctx, influxdb.OrganizationFilter{}); err != nil {
		t.Error(err)
	}
	if _, _, err := l.UserService().FindUsers(ctx, influxdb.UserFilter{}); err != nil {
		t.Error(err)
	}
	if _, _, err := l.UserResourceMappingService().FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{}); err != nil {
		t.Error(err)
	}
}

func Test_Tenant(t *testing.T) {
	t.Run("tenant service", func(t *testing.T) {
		testTenant(t)
	})
}
