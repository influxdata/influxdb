package tenant_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/pkg/testing/assert"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	influxdb "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestBoltOnboardingService(t *testing.T) {
	influxdbtesting.OnboardInitialUser(initBoltOnboardingService, t)
}

func initBoltOnboardingService(f influxdbtesting.OnboardingFields, t *testing.T) (influxdb.OnboardingService, func()) {
	s, closeStore, err := NewTestInmemStore(t)
	if err != nil {
		t.Fatalf("failed to create new bolt kv store: %v", err)
	}

	svc := initOnboardingService(s, f, t)
	return svc, func() {
		closeStore()
	}
}

func initOnboardingService(s kv.Store, f influxdbtesting.OnboardingFields, t *testing.T) influxdb.OnboardingService {
	storage := tenant.NewStore(s)
	ten := tenant.NewService(storage)

	authStore, err := authorization.NewStore(s)
	require.NoError(t, err)
	authSvc := authorization.NewService(authStore, ten)

	// we will need an auth service as well
	svc := tenant.NewOnboardService(ten, authSvc)

	ctx := context.Background()

	t.Logf("Onboarding: %v", f.IsOnboarding)
	if !f.IsOnboarding {
		// create a dummy so so we can no longer onboard
		err := ten.CreateUser(ctx, &influxdb.User{Name: "dummy", Status: influxdb.Active})
		if err != nil {
			t.Fatal(err)
		}
	}

	return svc
}

func TestOnboardURM(t *testing.T) {
	s, _, _ := NewTestInmemStore(t)
	storage := tenant.NewStore(s)
	ten := tenant.NewService(storage)

	authStore, err := authorization.NewStore(s)
	require.NoError(t, err)
	authSvc := authorization.NewService(authStore, ten)

	svc := tenant.NewOnboardService(ten, authSvc)

	ctx := icontext.SetAuthorizer(context.Background(), &influxdb.Authorization{
		UserID: 123,
	})

	onboard, err := svc.OnboardUser(ctx, &influxdb.OnboardingRequest{
		User:   "name",
		Org:    "name",
		Bucket: "name",
	})

	if err != nil {
		t.Fatal(err)
	}

	urms, _, err := ten.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{ResourceID: onboard.Org.ID})
	if err != nil {
		t.Fatal(err)
	}

	if len(urms) > 1 {
		t.Fatal("additional URMs created")
	}
	if urms[0].UserID != onboard.User.ID {
		t.Fatal("org assigned to the wrong user")
	}
}

func TestOnboardAuth(t *testing.T) {
	s, _, _ := NewTestInmemStore(t)
	storage := tenant.NewStore(s)
	ten := tenant.NewService(storage)

	authStore, err := authorization.NewStore(s)
	require.NoError(t, err)
	authSvc := authorization.NewService(authStore, ten)

	svc := tenant.NewOnboardService(ten, authSvc)

	ctx := icontext.SetAuthorizer(context.Background(), &influxdb.Authorization{
		UserID: 123,
	})

	onboard, err := svc.OnboardUser(ctx, &influxdb.OnboardingRequest{
		User:   "name",
		Org:    "name",
		Bucket: "name",
	})

	if err != nil {
		t.Fatal(err)
	}

	auth := onboard.Auth
	expectedPerm := []influxdb.Permission{
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.AuthorizationsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.AuthorizationsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.BucketsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.BucketsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.DashboardsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.DashboardsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{ID: &onboard.Org.ID, Type: influxdb.OrgsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{ID: &onboard.Org.ID, Type: influxdb.OrgsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.SourcesResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.SourcesResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.TasksResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.TasksResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.TelegrafsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.TelegrafsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.UsersResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.UsersResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.VariablesResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.VariablesResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.ScraperResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.ScraperResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.SecretsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.SecretsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.LabelsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.LabelsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.ViewsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.ViewsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.DocumentsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.DocumentsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.NotificationRuleResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.NotificationRuleResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.NotificationEndpointResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.NotificationEndpointResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.ChecksResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.ChecksResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.DBRPResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{OrgID: &onboard.Org.ID, Type: influxdb.DBRPResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{ID: &onboard.User.ID, Type: influxdb.UsersResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{ID: &onboard.User.ID, Type: influxdb.UsersResourceType}},
	}
	if !cmp.Equal(auth.Permissions, expectedPerm) {
		t.Fatalf("unequal permissions: \n %+v", cmp.Diff(auth.Permissions, expectedPerm))
	}

}

func TestOnboardService_RetentionPolicy(t *testing.T) {
	s, _, _ := NewTestInmemStore(t)
	storage := tenant.NewStore(s)
	ten := tenant.NewService(storage)

	authStore, err := authorization.NewStore(s)
	require.NoError(t, err)

	authSvc := authorization.NewService(authStore, ten)

	// we will need an auth service as well
	svc := tenant.NewOnboardService(ten, authSvc)

	ctx := icontext.SetAuthorizer(context.Background(), &influxdb.Authorization{
		UserID: 123,
	})

	retention := 72 * time.Hour
	onboard, err := svc.OnboardInitialUser(ctx, &influxdb.OnboardingRequest{
		User:            "name",
		Org:             "name",
		Bucket:          "name",
		RetentionPeriod: retention,
	})

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, onboard.Bucket.RetentionPeriod, retention, "Retention policy should pass through")
}
