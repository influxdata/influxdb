package internal

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/sqlite"
	"github.com/influxdata/influxdb/v2/sqlite/migrations"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

var (
	ctx         = context.Background()
	initID      = platform.ID(1)
	desc        = "testing testing"
	replication = influxdb.Replication{
		ID:                initID,
		OrgID:             platform.ID(10),
		Name:              "test",
		Description:       &desc,
		RemoteID:          platform.ID(100),
		LocalBucketID:     platform.ID(1000),
		RemoteBucketID:    idPointer(99999),
		MaxQueueSizeBytes: 3 * influxdb.DefaultReplicationMaxQueueSizeBytes,
		MaxAgeSeconds:     0,
	}
	createReq = influxdb.CreateReplicationRequest{
		OrgID:             replication.OrgID,
		Name:              replication.Name,
		Description:       replication.Description,
		RemoteID:          replication.RemoteID,
		LocalBucketID:     replication.LocalBucketID,
		RemoteBucketID:    *replication.RemoteBucketID,
		MaxQueueSizeBytes: replication.MaxQueueSizeBytes,
		MaxAgeSeconds:     replication.MaxAgeSeconds,
	}
	httpConfig = influxdb.ReplicationHTTPConfig{
		RemoteURL:        fmt.Sprintf("http://%s.cloud", replication.RemoteID),
		RemoteToken:      replication.RemoteID.String(),
		RemoteOrgID:      idPointer(888888),
		AllowInsecureTLS: true,
		RemoteBucketID:   replication.RemoteBucketID,
	}
	newQueueSize = influxdb.MinReplicationMaxQueueSizeBytes
	updateReq    = influxdb.UpdateReplicationRequest{
		RemoteID:             idPointer(200),
		MaxQueueSizeBytes:    &newQueueSize,
		DropNonRetryableData: boolPointer(true),
	}
	updatedReplication = influxdb.Replication{
		ID:                   replication.ID,
		OrgID:                replication.OrgID,
		Name:                 replication.Name,
		Description:          replication.Description,
		RemoteID:             *updateReq.RemoteID,
		LocalBucketID:        replication.LocalBucketID,
		RemoteBucketID:       replication.RemoteBucketID,
		MaxQueueSizeBytes:    *updateReq.MaxQueueSizeBytes,
		DropNonRetryableData: true,
		MaxAgeSeconds:        replication.MaxAgeSeconds,
	}
)

func idPointer(id int) *platform.ID {
	p := platform.ID(id)
	return &p
}

func TestCreateAndGetReplication(t *testing.T) {
	t.Parallel()

	testStore := newTestStore(t)

	insertRemote(t, testStore, replication.RemoteID)

	// Getting an invalid ID should return an error.
	got, err := testStore.GetReplication(ctx, initID)
	require.Equal(t, errReplicationNotFound, err)
	require.Nil(t, got)

	// Create a replication, check the results.
	created, err := testStore.CreateReplication(ctx, initID, createReq)
	require.NoError(t, err)
	require.Equal(t, replication, *created)

	// Read the created replication and assert it matches the creation response.
	got, err = testStore.GetReplication(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, replication, *got)
}

func TestCreateAndGetReplicationName(t *testing.T) {
	t.Parallel()

	testStore := newTestStore(t)

	insertRemote(t, testStore, replication.RemoteID)

	// Getting an invalid ID should return an error.
	got, err := testStore.GetReplication(ctx, initID)
	require.Equal(t, errReplicationNotFound, err)
	require.Nil(t, got)

	req := createReq
	req.RemoteBucketID = platform.ID(0)
	req.RemoteBucketName = "testbucket"
	expected := replication
	expected.RemoteBucketName = "testbucket"
	expected.RemoteBucketID = nil

	// Create a replication, check the results.
	created, err := testStore.CreateReplication(ctx, initID, req)
	require.NoError(t, err)
	require.Equal(t, expected, *created)

	// Read the created replication and assert it matches the creation response.
	got, err = testStore.GetReplication(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, expected, *got)
}

func TestCreateAndGetReplicationNameAndID(t *testing.T) {
	t.Parallel()

	testStore := newTestStore(t)

	insertRemote(t, testStore, replication.RemoteID)

	// Getting an invalid ID should return an error.
	got, err := testStore.GetReplication(ctx, initID)
	require.Equal(t, errReplicationNotFound, err)
	require.Nil(t, got)

	req := createReq
	req.RemoteBucketID = platform.ID(100)
	req.RemoteBucketName = "testbucket"
	expected := replication
	expected.RemoteBucketName = ""
	expected.RemoteBucketID = idPointer(100)

	// Create a replication, check the results.
	created, err := testStore.CreateReplication(ctx, initID, req)
	require.NoError(t, err)
	require.Equal(t, expected, *created)

	// Read the created replication and assert it matches the creation response.
	got, err = testStore.GetReplication(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, expected, *got)
}

func TestCreateAndGetReplicationNameError(t *testing.T) {
	t.Parallel()

	testStore := newTestStore(t)

	insertRemote(t, testStore, replication.RemoteID)

	// Getting an invalid ID should return an error.
	got, err := testStore.GetReplication(ctx, initID)
	require.Equal(t, errReplicationNotFound, err)
	require.Nil(t, got)

	req := createReq
	req.RemoteBucketID = platform.ID(0)
	req.RemoteBucketName = ""

	// Create a replication, should fail due to missing params
	created, err := testStore.CreateReplication(ctx, initID, req)
	require.Equal(t, errMissingIDName, err)
	require.Nil(t, created)
}

func TestCreateMissingRemote(t *testing.T) {
	t.Parallel()

	testStore := newTestStore(t)

	created, err := testStore.CreateReplication(ctx, initID, createReq)
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("remote %q not found", createReq.RemoteID))
	require.Nil(t, created)

	// Make sure nothing was persisted.
	got, err := testStore.GetReplication(ctx, initID)
	require.Equal(t, errReplicationNotFound, err)
	require.Nil(t, got)
}

func TestUpdateAndGetReplication(t *testing.T) {
	t.Parallel()

	testStore := newTestStore(t)

	insertRemote(t, testStore, replication.RemoteID)
	insertRemote(t, testStore, updatedReplication.RemoteID)

	// Updating a nonexistent ID fails.
	updated, err := testStore.UpdateReplication(ctx, initID, updateReq)
	require.Equal(t, errReplicationNotFound, err)
	require.Nil(t, updated)

	// Create a replication.
	created, err := testStore.CreateReplication(ctx, initID, createReq)
	require.NoError(t, err)
	require.Equal(t, replication, *created)

	// Update the replication.
	updated, err = testStore.UpdateReplication(ctx, created.ID, updateReq)
	require.NoError(t, err)
	require.Equal(t, updatedReplication, *updated)
}

func TestUpdateResponseInfo(t *testing.T) {
	t.Parallel()

	testStore := newTestStore(t)

	insertRemote(t, testStore, replication.RemoteID)
	insertRemote(t, testStore, updatedReplication.RemoteID)

	testCode := http.StatusBadRequest
	testMsg := "some error message"

	// Updating a nonexistent ID fails.
	err := testStore.UpdateResponseInfo(ctx, initID, testCode, testMsg)
	require.Equal(t, errReplicationNotFound, err)

	// Create a replication.
	created, err := testStore.CreateReplication(ctx, initID, createReq)
	require.NoError(t, err)
	require.Equal(t, replication, *created)

	// Update the replication response info.
	err = testStore.UpdateResponseInfo(ctx, initID, testCode, testMsg)
	require.NoError(t, err)

	// Check the updated response code and error message.
	got, err := testStore.GetReplication(ctx, initID)
	require.NoError(t, err)
	require.Equal(t, int32(testCode), *got.LatestResponseCode)
	require.Equal(t, testMsg, *got.LatestErrorMessage)
}

func TestUpdateMissingRemote(t *testing.T) {
	t.Parallel()

	testStore := newTestStore(t)

	insertRemote(t, testStore, replication.RemoteID)

	// Create a replication.
	created, err := testStore.CreateReplication(ctx, initID, createReq)
	require.NoError(t, err)
	require.Equal(t, replication, *created)

	// Attempt to update the replication to point at a nonexistent remote.
	updated, err := testStore.UpdateReplication(ctx, created.ID, updateReq)
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("remote %q not found", *updateReq.RemoteID))
	require.Nil(t, updated)

	// Make sure nothing changed in the DB.
	got, err := testStore.GetReplication(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, replication, *got)
}

func TestUpdateNoop(t *testing.T) {
	t.Parallel()

	testStore := newTestStore(t)

	insertRemote(t, testStore, replication.RemoteID)

	// Create a replication.
	created, err := testStore.CreateReplication(ctx, initID, createReq)
	require.NoError(t, err)
	require.Equal(t, replication, *created)

	// Send a no-op update, assert nothing changed.
	updated, err := testStore.UpdateReplication(ctx, created.ID, influxdb.UpdateReplicationRequest{})
	require.NoError(t, err)
	require.Equal(t, replication, *updated)
}

func TestDeleteReplication(t *testing.T) {
	t.Parallel()

	testStore := newTestStore(t)

	insertRemote(t, testStore, replication.RemoteID)

	// Deleting a nonexistent ID should return an error.
	require.Equal(t, errReplicationNotFound, testStore.DeleteReplication(ctx, initID))

	// Create a replication, then delete it.
	created, err := testStore.CreateReplication(ctx, initID, createReq)
	require.NoError(t, err)
	require.Equal(t, replication, *created)
	require.NoError(t, testStore.DeleteReplication(ctx, created.ID))

	// Looking up the ID should again produce an error.
	got, err := testStore.GetReplication(ctx, created.ID)
	require.Equal(t, errReplicationNotFound, err)
	require.Nil(t, got)
}

func TestDeleteReplications(t *testing.T) {
	t.Parallel()

	testStore := newTestStore(t)

	// Deleting when there is no bucket is OK.
	_, err := testStore.DeleteBucketReplications(ctx, replication.LocalBucketID)
	require.NoError(t, err)

	// Register a handful of replications.
	createReq2, createReq3 := createReq, createReq
	createReq2.Name, createReq3.Name = "test2", "test3"
	createReq2.LocalBucketID = platform.ID(77777)
	createReq3.RemoteID = updatedReplication.RemoteID
	insertRemote(t, testStore, createReq.RemoteID)
	insertRemote(t, testStore, createReq3.RemoteID)

	for _, req := range []influxdb.CreateReplicationRequest{createReq, createReq2, createReq3} {
		_, err := testStore.CreateReplication(ctx, snowflake.NewIDGenerator().ID(), req)
		require.NoError(t, err)
	}

	listed, err := testStore.ListReplications(ctx, influxdb.ReplicationListFilter{OrgID: replication.OrgID})
	require.NoError(t, err)
	require.Len(t, listed.Replications, 3)

	// Delete 2/3 by bucket ID.
	deleted, err := testStore.DeleteBucketReplications(ctx, createReq.LocalBucketID)
	require.NoError(t, err)
	require.Len(t, deleted, 2)

	// Ensure they were deleted.
	listed, err = testStore.ListReplications(ctx, influxdb.ReplicationListFilter{OrgID: replication.OrgID})
	require.NoError(t, err)
	require.Len(t, listed.Replications, 1)
	require.Equal(t, createReq2.LocalBucketID, listed.Replications[0].LocalBucketID)
}

func TestListReplications(t *testing.T) {
	t.Parallel()

	createReq2, createReq3 := createReq, createReq
	createReq2.Name, createReq3.Name = "test2", "test3"
	createReq2.LocalBucketID = platform.ID(77777)
	createReq3.RemoteID = updatedReplication.RemoteID

	setup := func(t *testing.T, testStore *Store) []influxdb.Replication {
		insertRemote(t, testStore, createReq.RemoteID)
		insertRemote(t, testStore, createReq3.RemoteID)

		var allReplications []influxdb.Replication
		for _, req := range []influxdb.CreateReplicationRequest{createReq, createReq2, createReq3} {
			created, err := testStore.CreateReplication(ctx, snowflake.NewIDGenerator().ID(), req)
			require.NoError(t, err)
			allReplications = append(allReplications, *created)
		}
		return allReplications
	}

	t.Run("list all for org", func(t *testing.T) {
		t.Parallel()

		testStore := newTestStore(t)
		allRepls := setup(t, testStore)

		listed, err := testStore.ListReplications(ctx, influxdb.ReplicationListFilter{OrgID: createReq.OrgID})
		require.NoError(t, err)

		// The order from sqlite is not the same, so for simplicity we sort both lists before comparing.
		sort.Slice(allRepls, func(i int, j int) bool {
			return allRepls[i].ID < allRepls[j].ID
		})
		sort.Slice(listed.Replications, func(i int, j int) bool {
			return listed.Replications[i].ID < listed.Replications[j].ID
		})
		require.Equal(t, influxdb.Replications{Replications: allRepls}, *listed)
	})

	t.Run("list all with empty filter", func(t *testing.T) {
		t.Parallel()

		testStore := newTestStore(t)
		allRepls := setup(t, testStore)

		otherOrgReq := createReq
		otherOrgReq.OrgID = platform.ID(12345)
		created, err := testStore.CreateReplication(ctx, snowflake.NewIDGenerator().ID(), otherOrgReq)
		require.NoError(t, err)
		allRepls = append(allRepls, *created)

		listed, err := testStore.ListReplications(ctx, influxdb.ReplicationListFilter{})
		require.NoError(t, err)
		require.Equal(t, influxdb.Replications{Replications: allRepls}, *listed)
	})

	t.Run("list by name", func(t *testing.T) {
		t.Parallel()

		testStore := newTestStore(t)
		allRepls := setup(t, testStore)

		listed, err := testStore.ListReplications(ctx, influxdb.ReplicationListFilter{
			OrgID: createReq.OrgID,
			Name:  &createReq2.Name,
		})
		require.NoError(t, err)
		require.Equal(t, influxdb.Replications{Replications: allRepls[1:2]}, *listed)
	})

	t.Run("list by remote ID", func(t *testing.T) {
		t.Parallel()

		testStore := newTestStore(t)
		allRepls := setup(t, testStore)

		listed, err := testStore.ListReplications(ctx, influxdb.ReplicationListFilter{
			OrgID:    createReq.OrgID,
			RemoteID: &createReq.RemoteID,
		})
		require.NoError(t, err)
		require.Equal(t, influxdb.Replications{Replications: allRepls[0:2]}, *listed)
	})

	t.Run("list by bucket ID", func(t *testing.T) {
		t.Parallel()

		testStore := newTestStore(t)
		allRepls := setup(t, testStore)

		listed, err := testStore.ListReplications(ctx, influxdb.ReplicationListFilter{
			OrgID:         createReq.OrgID,
			LocalBucketID: &createReq.LocalBucketID,
		})
		require.NoError(t, err)
		require.Equal(t, influxdb.Replications{Replications: append(allRepls[0:1], allRepls[2:]...)}, *listed)
	})

	t.Run("list by other org ID", func(t *testing.T) {
		t.Parallel()

		testStore := newTestStore(t)

		listed, err := testStore.ListReplications(ctx, influxdb.ReplicationListFilter{OrgID: platform.ID(2)})
		require.NoError(t, err)
		require.Equal(t, influxdb.Replications{}, *listed)
	})
}

func TestMigrateDownFromReplicationsWithName(t *testing.T) {
	t.Parallel()

	testStore := newTestStore(t)

	insertRemote(t, testStore, replication.RemoteID)

	req := createReq
	req.RemoteBucketID = platform.ID(100)
	_, err := testStore.CreateReplication(ctx, platform.ID(10), req)
	require.NoError(t, err)

	req.RemoteBucketID = platform.ID(0)
	req.RemoteBucketName = "testbucket"
	req.Name = "namedrepl"
	_, err = testStore.CreateReplication(ctx, platform.ID(20), req)
	require.NoError(t, err)

	replications, err := testStore.ListReplications(context.Background(), influxdb.ReplicationListFilter{OrgID: replication.OrgID})
	require.NoError(t, err)
	require.Equal(t, 2, len(replications.Replications))

	logger := zaptest.NewLogger(t)
	sqliteMigrator := sqlite.NewMigrator(testStore.sqlStore, logger)
	require.NoError(t, sqliteMigrator.Down(ctx, 5, migrations.AllDown))

	// Can't use ListReplications because it expects the `remote_bucket_name` column to be there in this version of influx.
	q := sq.Select(
		"id", "org_id", "name", "description", "remote_id", "local_bucket_id", "remote_bucket_id",
		"max_queue_size_bytes", "latest_response_code", "latest_error_message", "drop_non_retryable_data",
		"max_age_seconds").
		From("replications")

	q = q.Where(sq.Eq{"org_id": replication.OrgID})

	query, args, err := q.ToSql()
	require.NoError(t, err)
	var rs influxdb.Replications
	if err := testStore.sqlStore.DB.SelectContext(ctx, &rs.Replications, query, args...); err != nil {
		require.NoError(t, err)
	}
	require.Equal(t, 1, len(rs.Replications))
	require.Equal(t, platform.ID(10), rs.Replications[0].ID)
}

func TestMigrateUpToRemotesNullRemoteOrg(t *testing.T) {
	sqlStore := sqlite.NewTestStore(t)
	logger := zaptest.NewLogger(t)
	sqliteMigrator := sqlite.NewMigrator(sqlStore, logger)
	require.NoError(t, sqliteMigrator.UpUntil(ctx, 7, migrations.AllUp))

	// Make sure foreign-key checking is enabled.
	_, err := sqlStore.DB.Exec("PRAGMA foreign_keys = ON;")
	require.NoError(t, err)

	testStore := NewStore(sqlStore)

	insertRemote(t, testStore, replication.RemoteID)

	req := createReq
	req.RemoteBucketID = platform.ID(100)
	_, err = testStore.CreateReplication(ctx, platform.ID(10), req)
	require.NoError(t, err)

	req.RemoteBucketID = platform.ID(0)
	req.RemoteBucketName = "testbucket"
	req.Name = "namedrepl"
	_, err = testStore.CreateReplication(ctx, platform.ID(20), req)
	require.NoError(t, err)

	replications, err := testStore.ListReplications(context.Background(), influxdb.ReplicationListFilter{OrgID: replication.OrgID})
	require.NoError(t, err)
	require.Equal(t, 2, len(replications.Replications))

	require.NoError(t, sqliteMigrator.UpUntil(ctx, 8, migrations.AllUp))
}

func TestGetFullHTTPConfig(t *testing.T) {
	t.Parallel()

	testStore := newTestStore(t)

	// Does not exist returns the appropriate error
	_, err := testStore.GetFullHTTPConfig(ctx, initID)
	require.Equal(t, errReplicationNotFound, err)

	// Valid result
	insertRemote(t, testStore, replication.RemoteID)
	created, err := testStore.CreateReplication(ctx, initID, createReq)
	require.NoError(t, err)
	require.Equal(t, replication, *created)

	conf, err := testStore.GetFullHTTPConfig(ctx, initID)
	require.NoError(t, err)
	require.Equal(t, httpConfig, *conf)
}

func TestPopulateRemoteHTTPConfig(t *testing.T) {
	t.Parallel()

	testStore := newTestStore(t)

	emptyConfig := &influxdb.ReplicationHTTPConfig{RemoteOrgID: idPointer(0)}

	// Remote not found returns the appropriate error
	target := &influxdb.ReplicationHTTPConfig{}
	err := testStore.PopulateRemoteHTTPConfig(ctx, replication.RemoteID, target)
	require.Equal(t, errRemoteNotFound(replication.RemoteID, nil), err)
	require.Equal(t, emptyConfig, target)

	// Valid result
	want := influxdb.ReplicationHTTPConfig{
		RemoteURL:        httpConfig.RemoteURL,
		RemoteToken:      httpConfig.RemoteToken,
		RemoteOrgID:      httpConfig.RemoteOrgID,
		AllowInsecureTLS: httpConfig.AllowInsecureTLS,
	}
	insertRemote(t, testStore, replication.RemoteID)
	err = testStore.PopulateRemoteHTTPConfig(ctx, replication.RemoteID, target)
	require.NoError(t, err)
	require.Equal(t, want, *target)
}

func newTestStore(t *testing.T) *Store {
	sqlStore := sqlite.NewTestStore(t)
	logger := zaptest.NewLogger(t)
	sqliteMigrator := sqlite.NewMigrator(sqlStore, logger)
	require.NoError(t, sqliteMigrator.Up(ctx, migrations.AllUp))

	// Make sure foreign-key checking is enabled.
	_, err := sqlStore.DB.Exec("PRAGMA foreign_keys = ON;")
	require.NoError(t, err)

	return NewStore(sqlStore)
}

func insertRemote(t *testing.T, store *Store, id platform.ID) {
	sqlStore := store.sqlStore

	sqlStore.Mu.Lock()
	defer sqlStore.Mu.Unlock()

	q := sq.Insert("remotes").SetMap(sq.Eq{
		"id":                 id,
		"org_id":             replication.OrgID,
		"name":               fmt.Sprintf("foo-%s", id),
		"remote_url":         fmt.Sprintf("http://%s.cloud", id),
		"remote_api_token":   id.String(),
		"remote_org_id":      platform.ID(888888),
		"allow_insecure_tls": true,
		"created_at":         "datetime('now')",
		"updated_at":         "datetime('now')",
	})
	query, args, err := q.ToSql()
	require.NoError(t, err)

	_, err = sqlStore.DB.Exec(query, args...)
	require.NoError(t, err)
}

func boolPointer(b bool) *bool {
	return &b
}
