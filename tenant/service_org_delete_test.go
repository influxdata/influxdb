package tenant_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	icontext "github.com/influxdata/influxdb/v2/context"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"github.com/influxdata/influxdb/v2/tenant"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestDeleteOrganization_PurgesTasksV1(t *testing.T) {
	ctx := context.Background()
	store, closeStore := itesting.NewTestBoltStore(t)
	t.Cleanup(closeStore)

	ts := tenant.NewService(tenant.NewStore(store))

	authStore, err := authorization.NewStore(ctx, store, false)
	require.NoError(t, err)
	authSvc := authorization.NewService(authStore, ts)

	kvSvc := kv.NewService(zaptest.NewLogger(t), store, ts, kv.ServiceConfig{
		FluxLanguageService: fluxlang.DefaultService,
	})
	ts.Apply(tenant.WithTaskService(kvSvc))

	cases := []struct {
		name      string
		taskCount int
	}{
		{"zero tasks", 0},
		{"five tasks", 5},
		{"multi-page", taskmodel.TaskDefaultPageSize + 50},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			org, user, authCtx := seedOrgWithAuth(ctx, t, ts, authSvc)
			ids := seedTasks(authCtx, t, kvSvc, org.ID, user.ID, tc.taskCount)

			require.NoError(t, ts.DeleteOrganization(ctx, org.ID))

			after, _, err := kvSvc.FindTasks(authCtx, taskmodel.TaskFilter{
				OrganizationID: &org.ID,
				Limit:          taskmodel.TaskMaxPageSize,
			})
			require.NoError(t, err)
			require.Empty(t, after)

			assertTaskBucketsClean(ctx, t, store, org.ID, ids)
		})
	}
}

func seedOrgWithAuth(ctx context.Context, t *testing.T, ts *tenant.Service, authSvc influxdb.AuthorizationService) (*influxdb.Organization, *influxdb.User, context.Context) {
	t.Helper()

	user := &influxdb.User{Name: t.Name() + "-user"}
	require.NoError(t, ts.CreateUser(ctx, user))

	org := &influxdb.Organization{Name: t.Name() + "-org"}
	require.NoError(t, ts.CreateOrganization(ctx, org))

	require.NoError(t, ts.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   org.ID,
		UserID:       user.ID,
		UserType:     influxdb.Owner,
	}))

	auth := &influxdb.Authorization{
		OrgID:       org.ID,
		UserID:      user.ID,
		Permissions: influxdb.OperPermissions(),
	}
	require.NoError(t, authSvc.CreateAuthorization(ctx, auth))

	return org, user, icontext.SetAuthorizer(ctx, auth)
}

func seedTasks(ctx context.Context, t *testing.T, svc *kv.Service, orgID, ownerID platform.ID, n int) []platform.ID {
	t.Helper()
	ids := make([]platform.ID, 0, n)
	for i := range n {
		task, err := svc.CreateTask(ctx, taskmodel.TaskCreate{
			Flux:           fmt.Sprintf(`option task = {name: "task-%d", every: 1h} from(bucket:"b") |> range(start:-1h)`, i),
			OrganizationID: orgID,
			OwnerID:        ownerID,
			Status:         string(taskmodel.TaskActive),
		})
		require.NoError(t, err)
		ids = append(ids, task.ID)
	}
	return ids
}

func assertTaskBucketsClean(ctx context.Context, t *testing.T, store kv.Store, orgID platform.ID, ids []platform.ID) {
	t.Helper()
	require.NoError(t, store.View(ctx, func(tx kv.Tx) error {
		taskBkt, err := tx.Bucket([]byte("tasksv1"))
		require.NoError(t, err)
		for _, id := range ids {
			key, err := id.Encode()
			require.NoError(t, err)
			_, err = taskBkt.Get(key)
			require.Truef(t, kv.IsNotFound(err), "task %s still in tasksv1", id.String())
		}

		idxBkt, err := tx.Bucket([]byte("taskIndexsv1"))
		require.NoError(t, err)
		orgPrefix, err := orgID.Encode()
		require.NoError(t, err)
		c, err := idxBkt.ForwardCursor(orgPrefix, kv.WithCursorPrefix(orgPrefix))
		require.NoError(t, err)
		defer c.Close()
		var remaining [][]byte
		for k, _ := c.Next(); k != nil; k, _ = c.Next() {
			remaining = append(remaining, append([]byte(nil), k...))
		}
		require.NoError(t, c.Err())
		require.Emptyf(t, remaining, "taskIndexsv1 still has entries for org %s: %x", orgID.String(), remaining)
		return nil
	}))
}
