package tenant_test

import (
	"bytes"
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

	tenantStore := tenant.NewStore(store)
	ts := tenant.NewService(tenantStore)

	authStore, err := authorization.NewStore(ctx, store, false)
	require.NoError(t, err)
	authSvc := authorization.NewService(authStore, ts)

	kvSvc := kv.NewService(zaptest.NewLogger(t), store, ts, kv.ServiceConfig{
		FluxLanguageService: fluxlang.DefaultService,
	})
	ts.Apply(tenant.WithTaskService(kvSvc))

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
	authCtx := icontext.SetAuthorizer(ctx, auth)

	var createdIDs []platform.ID
	for i := range 3 {
		task, err := kvSvc.CreateTask(authCtx, taskmodel.TaskCreate{
			Flux:           fmt.Sprintf(`option task = {name: "task-%d", every: 1h} from(bucket:"b") |> range(start:-1h)`, i),
			OrganizationID: org.ID,
			OwnerID:        user.ID,
			Status:         string(taskmodel.TaskActive),
		})
		require.NoError(t, err)
		createdIDs = append(createdIDs, task.ID)
	}

	before, _, err := kvSvc.FindTasks(authCtx, taskmodel.TaskFilter{OrganizationID: &org.ID})
	require.NoError(t, err)
	require.Len(t, before, 3)

	require.NoError(t, ts.DeleteOrganization(ctx, org.ID))

	after, _, err := kvSvc.FindTasks(authCtx, taskmodel.TaskFilter{OrganizationID: &org.ID})
	require.NoError(t, err)
	require.Empty(t, after)

	require.NoError(t, store.View(ctx, func(tx kv.Tx) error {
		taskBkt, err := tx.Bucket([]byte("tasksv1"))
		if err != nil {
			return err
		}
		for _, id := range createdIDs {
			key, err := id.Encode()
			require.NoError(t, err)
			v, err := taskBkt.Get(key)
			if !kv.IsNotFound(err) {
				require.NoError(t, err)
				require.Failf(t, "task still in tasksv1", "id=%s value=%q", id.String(), string(v))
			}
		}

		idxBkt, err := tx.Bucket([]byte("taskIndexsv1"))
		if err != nil {
			return err
		}
		orgPrefix, err := org.ID.Encode()
		require.NoError(t, err)
		c, err := idxBkt.ForwardCursor(orgPrefix, kv.WithCursorPrefix(orgPrefix))
		require.NoError(t, err)
		defer c.Close()
		var remaining [][]byte
		for k, _ := c.Next(); k != nil; k, _ = c.Next() {
			if bytes.HasPrefix(k, orgPrefix) {
				remaining = append(remaining, append([]byte(nil), k...))
			}
		}
		require.NoError(t, c.Err())
		require.Emptyf(t, remaining, "taskIndexsv1 still has entries for deleted org: %x", remaining)
		return nil
	}))
}
