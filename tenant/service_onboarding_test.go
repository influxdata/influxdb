package tenant_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/pkg/testing/assert"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/influxdb/v2"
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
	s := influxdbtesting.NewTestInmemStore(t)
	svc := initOnboardingService(s, f, t)
	return svc, func() {}
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
	s := influxdbtesting.NewTestInmemStore(t)
	storage := tenant.NewStore(s)
	ten := tenant.NewService(storage)

	authStore, err := authorization.NewStore(s)
	require.NoError(t, err)
	authSvc := authorization.NewService(authStore, ten)

	svc := tenant.NewOnboardService(ten, authSvc)

	ctx := icontext.SetAuthorizer(context.Background(), &influxdb.Authorization{
		UserID: 123,
	})

	onboard, err := svc.OnboardInitialUser(ctx, &influxdb.OnboardingRequest{
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
	s := influxdbtesting.NewTestInmemStore(t)
	storage := tenant.NewStore(s)
	ten := tenant.NewService(storage)

	authStore, err := authorization.NewStore(s)
	require.NoError(t, err)
	authSvc := authorization.NewService(authStore, ten)

	svc := tenant.NewOnboardService(ten, authSvc)

	ctx := icontext.SetAuthorizer(context.Background(), &influxdb.Authorization{
		UserID: 123,
	})

	onboard, err := svc.OnboardInitialUser(ctx, &influxdb.OnboardingRequest{
		User:   "name",
		Org:    "name",
		Bucket: "name",
	})

	if err != nil {
		t.Fatal(err)
	}

	auth := onboard.Auth
	expectedPerm := []influxdb.Permission{
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.AuthorizationsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.AuthorizationsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.BucketsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.BucketsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.DashboardsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.DashboardsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.OrgsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.OrgsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.SourcesResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.SourcesResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TasksResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.TelegrafsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.TelegrafsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.UsersResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.UsersResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.VariablesResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.VariablesResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.ScraperResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.ScraperResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.SecretsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.SecretsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.LabelsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.LabelsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.ViewsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.ViewsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.DocumentsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.DocumentsResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.NotificationRuleResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.NotificationRuleResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.NotificationEndpointResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.NotificationEndpointResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.ChecksResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.ChecksResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.DBRPResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.DBRPResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.NotebooksResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.NotebooksResourceType}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.AnnotationsResourceType}},
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.AnnotationsResourceType}},
	}
	if !cmp.Equal(auth.Permissions, expectedPerm) {
		t.Fatalf("unequal permissions: \n %+v", cmp.Diff(auth.Permissions, expectedPerm))
	}

}

func TestOnboardService_RetentionPolicy(t *testing.T) {
	s := influxdbtesting.NewTestInmemStore(t)
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

	var retention int64 = 72 * 3600 // 72h
	onboard, err := svc.OnboardInitialUser(ctx, &influxdb.OnboardingRequest{
		User:                   "name",
		Org:                    "name",
		Bucket:                 "name",
		RetentionPeriodSeconds: retention,
	})

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, onboard.Bucket.RetentionPeriod, time.Duration(retention)*time.Second, "Retention policy should pass through")
}

func TestOnboardService_RetentionPolicyDeprecated(t *testing.T) {
	s := influxdbtesting.NewTestInmemStore(t)
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
		User:                      "name",
		Org:                       "name",
		Bucket:                    "name",
		RetentionPeriodDeprecated: retention,
	})

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, onboard.Bucket.RetentionPeriod, retention, "Retention policy should pass through")
}

func TestOnboardService_WeakPassword(t *testing.T) {
	s := influxdbtesting.NewTestInmemStore(t)
	storage := tenant.NewStore(s)
	ten := tenant.NewService(storage)

	authStore, err := authorization.NewStore(s)
	require.NoError(t, err)

	authSvc := authorization.NewService(authStore, ten)
	svc := tenant.NewOnboardService(ten, authSvc)

	ctx := icontext.SetAuthorizer(context.Background(), &influxdb.Authorization{
		UserID: 123,
	})

	_, err = svc.OnboardInitialUser(ctx, &influxdb.OnboardingRequest{
		User:     "name",
		Password: "short",
		Org:      "name",
		Bucket:   "name",
	})
	assert.Equal(t, err, tenant.EShortPassword)
}
