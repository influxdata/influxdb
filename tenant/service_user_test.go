package tenant_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestBoltUserService(t *testing.T) {
	influxdbtesting.UserService(initBoltUserService, t)
}

func initBoltUserService(f influxdbtesting.UserFields, t *testing.T) (influxdb.UserService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initUserService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initUserService(s kv.Store, f influxdbtesting.UserFields, t *testing.T) (influxdb.UserService, string, func()) {
	storage := tenant.NewStore(s)
	svc := tenant.NewService(storage)

	for _, u := range f.Users {
		if err := svc.CreateUser(context.Background(), u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	return svc, "tenant", func() {
		for _, u := range f.Users {
			if err := svc.DeleteUser(context.Background(), u.ID); err != nil {
				t.Logf("failed to remove users: %v", err)
			}
		}
	}
}

func TestBoltPasswordService(t *testing.T) {
	influxdbtesting.PasswordsService(initBoltPasswordsService, t)
}

func initBoltPasswordsService(f influxdbtesting.PasswordFields, t *testing.T) (influxdb.PasswordsService, func()) {
	s, closeStore, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new bolt kv store: %v", err)
	}

	svc, closeSvc := initPasswordsService(s, f, t)
	return svc, func() {
		closeSvc()
		closeStore()
	}
}

func initPasswordsService(s kv.Store, f influxdbtesting.PasswordFields, t *testing.T) (influxdb.PasswordsService, func()) {
	storage := tenant.NewStore(s)
	svc := tenant.NewService(storage)

	for _, u := range f.Users {
		if err := svc.CreateUser(context.Background(), u); err != nil {
			t.Fatalf("error populating users: %v", err)
		}
	}

	for i := range f.Passwords {
		if err := svc.SetPassword(context.Background(), f.Users[i].ID, f.Passwords[i]); err != nil {
			t.Fatalf("error setting passsword user, %s %s: %v", f.Users[i].Name, f.Passwords[i], err)
		}
	}

	return svc, func() {
		for _, u := range f.Users {
			if err := svc.DeleteUser(context.Background(), u.ID); err != nil {
				t.Logf("error removing users: %v", err)
			}
		}
	}
}

func TestFindPermissionsFromUser(t *testing.T) {
	s, _, err := NewTestInmemStore(t)
	if err != nil {
		t.Fatal(err)
	}
	storage := tenant.NewStore(s)
	svc := tenant.NewService(storage)

	// createUser
	u := &influxdb.User{
		Name:   "rockstar",
		Status: influxdb.Active,
	}

	if err := svc.CreateUser(context.Background(), u); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// createSomeURMS
	err = svc.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
		UserID:       u.ID,
		UserType:     influxdb.Member,
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   1,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = svc.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
		UserID:       u.ID,
		UserType:     influxdb.Owner,
		ResourceType: influxdb.BucketsResourceType,
		ResourceID:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	// pull the permissions for this user
	perms, err := svc.FindPermissionForUser(ctx, u.ID)
	if err != nil {
		t.Fatal(err)
	}

	orgID := platform.ID(1)
	expected := influxdb.PermissionSet{
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.AuthorizationsResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.BucketsResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.DashboardsResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{ID: &orgID, Type: influxdb.OrgsResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.SourcesResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.TasksResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.TelegrafsResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.UsersResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.VariablesResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.ScraperResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.SecretsResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.LabelsResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.ViewsResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.DocumentsResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.NotificationRuleResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.NotificationEndpointResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.ChecksResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.DBRPResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.NotebooksResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{OrgID: &orgID, Type: influxdb.AnnotationsResourceType}},
		influxdb.Permission{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.UsersResourceType, ID: &u.ID}},
		influxdb.Permission{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.UsersResourceType, ID: &u.ID}},
	}
	if !cmp.Equal(perms, expected) {
		t.Fatalf("inequal response for find params %+v", cmp.Diff(perms, expected))
	}
}
