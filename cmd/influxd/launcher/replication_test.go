package launcher_test

import (
	"testing"

	"github.com/influxdata/influx-cli/v2/api"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/stretchr/testify/require"
)

func TestValidateReplication_Valid(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t, func(o *launcher.InfluxdOpts) {
		o.FeatureFlags = map[string]string{feature.ReplicationStreamBackend().Key(): "true"}
	})
	defer l.ShutdownOrFail(t, ctx)
	client := l.APIClient(t)

	// Create a "remote" connection to the launcher from itself.
	remote, err := client.RemoteConnectionsApi.PostRemoteConnection(ctx).
		RemoteConnectionCreationRequest(api.RemoteConnectionCreationRequest{
			Name:             "self",
			OrgID:            l.Org.ID.String(),
			RemoteURL:        l.URL().String(),
			RemoteAPIToken:   l.Auth.Token,
			RemoteOrgID:      l.Org.ID.String(),
			AllowInsecureTLS: false,
		}).Execute()
	require.NoError(t, err)

	// Validate the replication before creating it.
	createReq := api.ReplicationCreationRequest{
		Name:              "test",
		OrgID:             l.Org.ID.String(),
		RemoteID:          remote.Id,
		LocalBucketID:     l.Bucket.ID.String(),
		RemoteBucketID:    l.Bucket.ID.String(),
		MaxQueueSizeBytes: influxdb.DefaultReplicationMaxQueueSizeBytes,
	}
	_, err = client.ReplicationsApi.PostReplication(ctx).ReplicationCreationRequest(createReq).Validate(true).Execute()
	require.NoError(t, err)

	// Create the replication.
	replication, err := client.ReplicationsApi.PostReplication(ctx).ReplicationCreationRequest(createReq).Execute()
	require.NoError(t, err)

	// Ensure the replication is marked as valid.
	require.NoError(t, client.ReplicationsApi.PostValidateReplicationByID(ctx, replication.Id).Execute())

	// Create a new auth token that can only write to the bucket.
	auth := influxdb.Authorization{
		Status: "active",
		OrgID:  l.Org.ID,
		UserID: l.User.ID,
		Permissions: []influxdb.Permission{{
			Action: "write",
			Resource: influxdb.Resource{
				Type:  influxdb.BucketsResourceType,
				ID:    &l.Bucket.ID,
				OrgID: &l.Org.ID,
			},
		}},
		CRUDLog: influxdb.CRUDLog{},
	}
	require.NoError(t, l.AuthorizationService(t).CreateAuthorization(ctx, &auth))

	// Update the remote to use the new token.
	_, err = client.RemoteConnectionsApi.PatchRemoteConnectionByID(ctx, remote.Id).
		RemoteConnenctionUpdateRequest(api.RemoteConnenctionUpdateRequest{RemoteAPIToken: &auth.Token}).
		Execute()
	require.NoError(t, err)

	// Ensure the replication is still valid.
	require.NoError(t, client.ReplicationsApi.PostValidateReplicationByID(ctx, replication.Id).Execute())
}

func TestValidateReplication_Invalid(t *testing.T) {
	l := launcher.RunAndSetupNewLauncherOrFail(ctx, t, func(o *launcher.InfluxdOpts) {
		o.FeatureFlags = map[string]string{feature.ReplicationStreamBackend().Key(): "true"}
	})
	defer l.ShutdownOrFail(t, ctx)
	client := l.APIClient(t)

	// Create a "remote" connection to the launcher from itself,
	// but with a bad auth token.
	remote, err := client.RemoteConnectionsApi.PostRemoteConnection(ctx).
		RemoteConnectionCreationRequest(api.RemoteConnectionCreationRequest{
			Name:             "self",
			OrgID:            l.Org.ID.String(),
			RemoteURL:        l.URL().String(),
			RemoteAPIToken:   "foo",
			RemoteOrgID:      l.Org.ID.String(),
			AllowInsecureTLS: false,
		}).Execute()
	require.NoError(t, err)

	// Validate the replication before creating it. This should fail because of the bad
	// auth token in the linked remote.
	createReq := api.ReplicationCreationRequest{
		Name:              "test",
		OrgID:             l.Org.ID.String(),
		RemoteID:          remote.Id,
		LocalBucketID:     l.Bucket.ID.String(),
		RemoteBucketID:    l.Bucket.ID.String(),
		MaxQueueSizeBytes: influxdb.DefaultReplicationMaxQueueSizeBytes,
	}
	_, err = client.ReplicationsApi.PostReplication(ctx).ReplicationCreationRequest(createReq).Validate(true).Execute()
	require.Error(t, err)

	// Create the replication even though it failed validation.
	replication, err := client.ReplicationsApi.PostReplication(ctx).ReplicationCreationRequest(createReq).Execute()
	require.NoError(t, err)

	// Ensure the replication is marked as invalid.
	require.Error(t, client.ReplicationsApi.PostValidateReplicationByID(ctx, replication.Id).Execute())

	// Create a new auth token that can only write to the bucket.
	auth := influxdb.Authorization{
		Status: "active",
		OrgID:  l.Org.ID,
		UserID: l.User.ID,
		Permissions: []influxdb.Permission{{
			Action: "write",
			Resource: influxdb.Resource{
				Type:  influxdb.BucketsResourceType,
				ID:    &l.Bucket.ID,
				OrgID: &l.Org.ID,
			},
		}},
		CRUDLog: influxdb.CRUDLog{},
	}
	require.NoError(t, l.AuthorizationService(t).CreateAuthorization(ctx, &auth))

	// Update the remote to use the new token.
	_, err = client.RemoteConnectionsApi.PatchRemoteConnectionByID(ctx, remote.Id).
		RemoteConnenctionUpdateRequest(api.RemoteConnenctionUpdateRequest{RemoteAPIToken: &auth.Token}).
		Execute()
	require.NoError(t, err)

	// Ensure the replication is now valid.
	require.NoError(t, client.ReplicationsApi.PostValidateReplicationByID(ctx, replication.Id).Execute())

	// Create a new bucket.
	bucket2 := influxdb.Bucket{
		OrgID:               l.Org.ID,
		Name:                "bucket2",
		RetentionPeriod:     0,
		ShardGroupDuration:  0,
	}
	require.NoError(t, l.BucketService(t).CreateBucket(ctx, &bucket2))
	bucket2Id := bucket2.ID.String()

	// Updating the replication to point at the new bucket should fail validation.
	_, err = client.ReplicationsApi.PatchReplicationByID(ctx, replication.Id).
		ReplicationUpdateRequest(api.ReplicationUpdateRequest{RemoteBucketID: &bucket2Id}).
		Validate(true).
		Execute()
	require.Error(t, err)
}
