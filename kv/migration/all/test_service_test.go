package all

import (
	"context"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration"
	"github.com/influxdata/influxdb/v2/tenant"
	"go.uber.org/zap/zaptest"
)

type testService struct {
	Store   kv.SchemaStore
	Service *kv.Service
	Org     influxdb.Organization
	User    influxdb.User
	Auth    influxdb.Authorization
	Clock   clock.Clock
}

func newService(t *testing.T, ctx context.Context, endMigration int) *testService {
	t.Helper()

	var (
		ts = &testService{
			Store: inmem.NewKVStore(),
		}
		logger = zaptest.NewLogger(t)
	)

	// apply migrations up to (but not including) this one
	migrator, err := migration.NewMigrator(logger, ts.Store, Migrations[:endMigration]...)
	if err != nil {
		t.Fatal(err)
	}

	if err := migrator.Up(ctx); err != nil {
		t.Fatal(err)
	}

	store := tenant.NewStore(ts.Store)
	tenantSvc := tenant.NewService(store)

	authStore, err := authorization.NewStore(ts.Store)
	if err != nil {
		t.Fatal(err)
	}
	authSvc := authorization.NewService(authStore, tenantSvc)

	ts.Service = kv.NewService(logger, ts.Store, tenantSvc)

	ts.User = influxdb.User{Name: t.Name() + "-user"}
	if err := tenantSvc.CreateUser(ctx, &ts.User); err != nil {
		t.Fatal(err)
	}
	ts.Org = influxdb.Organization{Name: t.Name() + "-org"}
	if err := tenantSvc.CreateOrganization(ctx, &ts.Org); err != nil {
		t.Fatal(err)
	}

	if err := tenantSvc.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   ts.Org.ID,
		UserID:       ts.User.ID,
		UserType:     influxdb.Owner,
	}); err != nil {
		t.Fatal(err)
	}

	ts.Auth = influxdb.Authorization{
		OrgID:       ts.Org.ID,
		UserID:      ts.User.ID,
		Permissions: influxdb.OperPermissions(),
	}
	if err := authSvc.CreateAuthorization(context.Background(), &ts.Auth); err != nil {
		t.Fatal(err)
	}

	return ts
}
