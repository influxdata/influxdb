package replications

import (
	"context"
	"errors"
	"fmt"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/replications/internal"
	replicationsMock "github.com/influxdata/influxdb/v2/replications/mock"
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
		RemoteBucketID:    platform.ID(99999),
		MaxQueueSizeBytes: 3 * influxdb.DefaultReplicationMaxQueueSizeBytes,
	}
	createReq = influxdb.CreateReplicationRequest{
		OrgID:             replication.OrgID,
		Name:              replication.Name,
		Description:       replication.Description,
		RemoteID:          replication.RemoteID,
		LocalBucketID:     replication.LocalBucketID,
		RemoteBucketID:    replication.RemoteBucketID,
		MaxQueueSizeBytes: replication.MaxQueueSizeBytes,
	}
	httpConfig = internal.ReplicationHTTPConfig{
		RemoteURL:        fmt.Sprintf("http://%s.cloud", replication.RemoteID),
		RemoteToken:      replication.RemoteID.String(),
		RemoteOrgID:      platform.ID(888888),
		AllowInsecureTLS: true,
		RemoteBucketID:   replication.RemoteBucketID,
	}
	newRemoteID  = platform.ID(200)
	newQueueSize = influxdb.MinReplicationMaxQueueSizeBytes
	updateReq    = influxdb.UpdateReplicationRequest{
		RemoteID:          &newRemoteID,
		MaxQueueSizeBytes: &newQueueSize,
	}
	updatedReplication = influxdb.Replication{
		ID:                replication.ID,
		OrgID:             replication.OrgID,
		Name:              replication.Name,
		Description:       replication.Description,
		RemoteID:          *updateReq.RemoteID,
		LocalBucketID:     replication.LocalBucketID,
		RemoteBucketID:    replication.RemoteBucketID,
		MaxQueueSizeBytes: *updateReq.MaxQueueSizeBytes,
	}
	updatedHttpConfig = internal.ReplicationHTTPConfig{
		RemoteURL:        fmt.Sprintf("http://%s.cloud", updatedReplication.RemoteID),
		RemoteToken:      updatedReplication.RemoteID.String(),
		RemoteOrgID:      platform.ID(888888),
		AllowInsecureTLS: true,
		RemoteBucketID:   updatedReplication.RemoteBucketID,
	}
)

func TestCreateAndGetReplication(t *testing.T) {
	t.Parallel()

	svc, mocks, clean := newTestService(t)
	defer clean(t)

	insertRemote(t, svc.store, replication.RemoteID)
	mocks.bucketSvc.EXPECT().RLock()
	mocks.bucketSvc.EXPECT().RUnlock()
	mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq.LocalBucketID).
		Return(&influxdb.Bucket{}, nil)

	// Getting or validating an invalid ID should return an error.
	got, err := svc.GetReplication(ctx, initID)
	require.Equal(t, errReplicationNotFound, err)
	require.Nil(t, got)
	require.Equal(t, errReplicationNotFound, svc.ValidateReplication(ctx, initID))

	// Create a replication, check the results.
	created, err := svc.CreateReplication(ctx, createReq)
	require.NoError(t, err)
	require.Equal(t, replication, *created)

	// Read the created replication and assert it matches the creation response.
	got, err = svc.GetReplication(ctx, initID)
	require.NoError(t, err)
	require.Equal(t, replication, *got)

	// Validate the replication; this is mostly a no-op for this test, but it allows
	// us to check that our sql for extracting the linked remote's parameters is correct.
	fakeErr := errors.New("O NO")
	mocks.validator.EXPECT().ValidateReplication(gomock.Any(), &httpConfig).Return(fakeErr)
	require.Contains(t, svc.ValidateReplication(ctx, initID).Error(), fakeErr.Error())
}

func TestCreateMissingBucket(t *testing.T) {
	t.Parallel()

	svc, mocks, clean := newTestService(t)
	defer clean(t)

	insertRemote(t, svc.store, replication.RemoteID)
	bucketNotFound := errors.New("bucket not found")
	mocks.bucketSvc.EXPECT().RLock()
	mocks.bucketSvc.EXPECT().RUnlock()
	mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq.LocalBucketID).
		Return(nil, bucketNotFound)

	created, err := svc.CreateReplication(ctx, createReq)
	require.Equal(t, errLocalBucketNotFound(createReq.LocalBucketID, bucketNotFound), err)
	require.Nil(t, created)

	// Make sure nothing was persisted.
	got, err := svc.GetReplication(ctx, initID)
	require.Equal(t, errReplicationNotFound, err)
	require.Nil(t, got)
}

func TestCreateMissingRemote(t *testing.T) {
	t.Parallel()

	svc, mocks, clean := newTestService(t)
	defer clean(t)

	mocks.bucketSvc.EXPECT().RLock()
	mocks.bucketSvc.EXPECT().RUnlock()
	mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq.LocalBucketID).
		Return(&influxdb.Bucket{}, nil)

	created, err := svc.CreateReplication(ctx, createReq)
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("remote %q not found", createReq.RemoteID))
	require.Nil(t, created)

	// Make sure nothing was persisted.
	got, err := svc.GetReplication(ctx, initID)
	require.Equal(t, errReplicationNotFound, err)
	require.Nil(t, got)
}

func TestValidateReplicationWithoutPersisting(t *testing.T) {
	t.Parallel()

	t.Run("missing bucket", func(t *testing.T) {
		svc, mocks, clean := newTestService(t)
		defer clean(t)

		bucketNotFound := errors.New("bucket not found")
		mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq.LocalBucketID).Return(nil, bucketNotFound)

		require.Equal(t, errLocalBucketNotFound(createReq.LocalBucketID, bucketNotFound),
			svc.ValidateNewReplication(ctx, createReq))

		got, err := svc.GetReplication(ctx, initID)
		require.Equal(t, errReplicationNotFound, err)
		require.Nil(t, got)
	})

	t.Run("missing remote", func(t *testing.T) {
		svc, mocks, clean := newTestService(t)
		defer clean(t)

		mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq.LocalBucketID).Return(&influxdb.Bucket{}, nil)

		require.Contains(t, svc.ValidateNewReplication(ctx, createReq).Error(),
			fmt.Sprintf("remote %q not found", createReq.RemoteID))

		got, err := svc.GetReplication(ctx, initID)
		require.Equal(t, errReplicationNotFound, err)
		require.Nil(t, got)
	})

	t.Run("validation error", func(t *testing.T) {
		svc, mocks, clean := newTestService(t)
		defer clean(t)

		mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq.LocalBucketID).Return(&influxdb.Bucket{}, nil)
		insertRemote(t, svc.store, createReq.RemoteID)

		fakeErr := errors.New("O NO")
		mocks.validator.EXPECT().ValidateReplication(gomock.Any(), &httpConfig).Return(fakeErr)

		require.Contains(t, svc.ValidateNewReplication(ctx, createReq).Error(), fakeErr.Error())

		got, err := svc.GetReplication(ctx, initID)
		require.Equal(t, errReplicationNotFound, err)
		require.Nil(t, got)
	})

	t.Run("no error", func(t *testing.T) {
		svc, mocks, clean := newTestService(t)
		defer clean(t)

		mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq.LocalBucketID).Return(&influxdb.Bucket{}, nil)
		insertRemote(t, svc.store, createReq.RemoteID)

		mocks.validator.EXPECT().ValidateReplication(gomock.Any(), &httpConfig).Return(nil)

		require.NoError(t, svc.ValidateNewReplication(ctx, createReq))

		got, err := svc.GetReplication(ctx, initID)
		require.Equal(t, errReplicationNotFound, err)
		require.Nil(t, got)
	})
}

func TestUpdateAndGetReplication(t *testing.T) {
	t.Parallel()

	svc, mocks, clean := newTestService(t)
	defer clean(t)

	insertRemote(t, svc.store, replication.RemoteID)
	insertRemote(t, svc.store, updatedReplication.RemoteID)
	mocks.bucketSvc.EXPECT().RLock()
	mocks.bucketSvc.EXPECT().RUnlock()
	mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq.LocalBucketID).
		Return(&influxdb.Bucket{}, nil)

	// Updating a nonexistent ID fails.
	updated, err := svc.UpdateReplication(ctx, initID, updateReq)
	require.Equal(t, errReplicationNotFound, err)
	require.Nil(t, updated)

	// Create a replication.
	created, err := svc.CreateReplication(ctx, createReq)
	require.NoError(t, err)
	require.Equal(t, replication, *created)

	// Update the replication.
	updated, err = svc.UpdateReplication(ctx, initID, updateReq)
	require.NoError(t, err)
	require.Equal(t, updatedReplication, *updated)
}

func TestUpdateMissingRemote(t *testing.T) {
	t.Parallel()

	svc, mocks, clean := newTestService(t)
	defer clean(t)

	insertRemote(t, svc.store, replication.RemoteID)
	mocks.bucketSvc.EXPECT().RLock()
	mocks.bucketSvc.EXPECT().RUnlock()
	mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq.LocalBucketID).
		Return(&influxdb.Bucket{}, nil)

	// Create a replication.
	created, err := svc.CreateReplication(ctx, createReq)
	require.NoError(t, err)
	require.Equal(t, replication, *created)

	// Attempt to update the replication to point at a nonexistent remote.
	updated, err := svc.UpdateReplication(ctx, initID, updateReq)
	require.Error(t, err)
	require.Contains(t, err.Error(), fmt.Sprintf("remote %q not found", *updateReq.RemoteID))
	require.Nil(t, updated)

	// Make sure nothing changed in the DB.
	got, err := svc.GetReplication(ctx, initID)
	require.NoError(t, err)
	require.Equal(t, replication, *got)
}

func TestUpdateNoop(t *testing.T) {
	t.Parallel()

	svc, mocks, clean := newTestService(t)
	defer clean(t)

	insertRemote(t, svc.store, replication.RemoteID)
	mocks.bucketSvc.EXPECT().RLock()
	mocks.bucketSvc.EXPECT().RUnlock()
	mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq.LocalBucketID).
		Return(&influxdb.Bucket{}, nil)

	// Create a replication.
	created, err := svc.CreateReplication(ctx, createReq)
	require.NoError(t, err)
	require.Equal(t, replication, *created)

	// Send a no-op update, assert nothing changed.
	updated, err := svc.UpdateReplication(ctx, initID, influxdb.UpdateReplicationRequest{})
	require.NoError(t, err)
	require.Equal(t, replication, *updated)
}

func TestValidateUpdatedReplicationWithoutPersisting(t *testing.T) {
	t.Parallel()

	t.Run("bad remote", func(t *testing.T) {
		t.Parallel()

		svc, mocks, clean := newTestService(t)
		defer clean(t)

		insertRemote(t, svc.store, replication.RemoteID)
		mocks.bucketSvc.EXPECT().RLock()
		mocks.bucketSvc.EXPECT().RUnlock()
		mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq.LocalBucketID).
			Return(&influxdb.Bucket{}, nil)

		// Create a replication.
		created, err := svc.CreateReplication(ctx, createReq)
		require.NoError(t, err)
		require.Equal(t, replication, *created)

		// Attempt to update the replication to point at a nonexistent remote.
		require.Contains(t, svc.ValidateUpdatedReplication(ctx, initID, updateReq).Error(),
			fmt.Sprintf("remote %q not found", *updateReq.RemoteID))

		// Make sure nothing changed in the DB.
		got, err := svc.GetReplication(ctx, initID)
		require.NoError(t, err)
		require.Equal(t, replication, *got)
	})

	t.Run("validation error", func(t *testing.T) {
		t.Parallel()

		svc, mocks, clean := newTestService(t)
		defer clean(t)

		insertRemote(t, svc.store, replication.RemoteID)
		insertRemote(t, svc.store, updatedReplication.RemoteID)
		mocks.bucketSvc.EXPECT().RLock()
		mocks.bucketSvc.EXPECT().RUnlock()
		mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq.LocalBucketID).
			Return(&influxdb.Bucket{}, nil)

		// Create a replication.
		created, err := svc.CreateReplication(ctx, createReq)
		require.NoError(t, err)
		require.Equal(t, replication, *created)

		// Check updating to a failing remote, assert error is returned.
		fakeErr := errors.New("O NO")
		mocks.validator.EXPECT().ValidateReplication(gomock.Any(), &updatedHttpConfig).Return(fakeErr)

		require.Contains(t, svc.ValidateUpdatedReplication(ctx, initID, updateReq).Error(), fakeErr.Error())

		// Make sure nothing changed in the DB.
		got, err := svc.GetReplication(ctx, initID)
		require.NoError(t, err)
		require.Equal(t, replication, *got)
	})

	t.Run("no error", func(t *testing.T) {
		t.Parallel()

		svc, mocks, clean := newTestService(t)
		defer clean(t)

		insertRemote(t, svc.store, replication.RemoteID)
		insertRemote(t, svc.store, updatedReplication.RemoteID)
		mocks.bucketSvc.EXPECT().RLock()
		mocks.bucketSvc.EXPECT().RUnlock()
		mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq.LocalBucketID).
			Return(&influxdb.Bucket{}, nil)

		// Create a replication.
		created, err := svc.CreateReplication(ctx, createReq)
		require.NoError(t, err)
		require.Equal(t, replication, *created)

		// Check updating to a remote that passes validation, assert no error.
		mocks.validator.EXPECT().ValidateReplication(gomock.Any(), &updatedHttpConfig).Return(nil)

		require.NoError(t, svc.ValidateUpdatedReplication(ctx, initID, updateReq))

		// Make sure nothing changed in the DB.
		got, err := svc.GetReplication(ctx, initID)
		require.NoError(t, err)
		require.Equal(t, replication, *got)
	})
}

func TestDeleteReplication(t *testing.T) {
	t.Parallel()

	svc, mocks, clean := newTestService(t)
	defer clean(t)

	insertRemote(t, svc.store, replication.RemoteID)
	mocks.bucketSvc.EXPECT().RLock()
	mocks.bucketSvc.EXPECT().RUnlock()
	mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq.LocalBucketID).
		Return(&influxdb.Bucket{}, nil)

	// Deleting a nonexistent ID should return an error.
	require.Equal(t, errReplicationNotFound, svc.DeleteReplication(ctx, initID))

	// Create a replication, then delete it.
	created, err := svc.CreateReplication(ctx, createReq)
	require.NoError(t, err)
	require.Equal(t, replication, *created)
	require.NoError(t, svc.DeleteReplication(ctx, initID))

	// Looking up the ID should again produce an error.
	got, err := svc.GetReplication(ctx, initID)
	require.Equal(t, errReplicationNotFound, err)
	require.Nil(t, got)
}

func TestListReplications(t *testing.T) {
	t.Parallel()

	createReq2, createReq3 := createReq, createReq
	createReq2.Name, createReq3.Name = "test2", "test3"
	createReq2.LocalBucketID = platform.ID(77777)
	createReq3.RemoteID = updatedReplication.RemoteID

	setup := func(t *testing.T, svc *service, mocks mocks) []influxdb.Replication {
		mocks.bucketSvc.EXPECT().RLock().Times(3)
		mocks.bucketSvc.EXPECT().RUnlock().Times(3)
		mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq.LocalBucketID).Return(&influxdb.Bucket{}, nil).Times(2)
		mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), createReq2.LocalBucketID).Return(&influxdb.Bucket{}, nil)
		insertRemote(t, svc.store, createReq.RemoteID)
		insertRemote(t, svc.store, createReq3.RemoteID)

		var allReplications []influxdb.Replication
		for _, req := range []influxdb.CreateReplicationRequest{createReq, createReq2, createReq3} {
			created, err := svc.CreateReplication(ctx, req)
			require.NoError(t, err)
			allReplications = append(allReplications, *created)
		}
		return allReplications
	}

	t.Run("list all", func(t *testing.T) {
		t.Parallel()

		svc, mocks, clean := newTestService(t)
		defer clean(t)
		allRepls := setup(t, svc, mocks)

		listed, err := svc.ListReplications(ctx, influxdb.ReplicationListFilter{OrgID: createReq.OrgID})
		require.NoError(t, err)
		require.Equal(t, influxdb.Replications{Replications: allRepls}, *listed)
	})

	t.Run("list by name", func(t *testing.T) {
		t.Parallel()

		svc, mocks, clean := newTestService(t)
		defer clean(t)
		allRepls := setup(t, svc, mocks)

		listed, err := svc.ListReplications(ctx, influxdb.ReplicationListFilter{
			OrgID: createReq.OrgID,
			Name:  &createReq2.Name,
		})
		require.NoError(t, err)
		require.Equal(t, influxdb.Replications{Replications: allRepls[1:2]}, *listed)
	})

	t.Run("list by remote ID", func(t *testing.T) {
		t.Parallel()

		svc, mocks, clean := newTestService(t)
		defer clean(t)
		allRepls := setup(t, svc, mocks)

		listed, err := svc.ListReplications(ctx, influxdb.ReplicationListFilter{
			OrgID:    createReq.OrgID,
			RemoteID: &createReq.RemoteID,
		})
		require.NoError(t, err)
		require.Equal(t, influxdb.Replications{Replications: allRepls[0:2]}, *listed)
	})

	t.Run("list by bucket ID", func(t *testing.T) {
		t.Parallel()

		svc, mocks, clean := newTestService(t)
		defer clean(t)
		allRepls := setup(t, svc, mocks)

		listed, err := svc.ListReplications(ctx, influxdb.ReplicationListFilter{
			OrgID:         createReq.OrgID,
			LocalBucketID: &createReq.LocalBucketID,
		})
		require.NoError(t, err)
		require.Equal(t, influxdb.Replications{Replications: append(allRepls[0:1], allRepls[2:]...)}, *listed)
	})

	t.Run("list by other org ID", func(t *testing.T) {
		t.Parallel()

		svc, mocks, clean := newTestService(t)
		defer clean(t)
		setup(t, svc, mocks)

		listed, err := svc.ListReplications(ctx, influxdb.ReplicationListFilter{OrgID: platform.ID(2)})
		require.NoError(t, err)
		require.Equal(t, influxdb.Replications{}, *listed)
	})
}

type mocks struct {
	bucketSvc *replicationsMock.MockBucketService
	validator *replicationsMock.MockReplicationValidator
}

func newTestService(t *testing.T) (*service, mocks, func(t *testing.T)) {
	store, clean := sqlite.NewTestStore(t)
	logger := zaptest.NewLogger(t)
	sqliteMigrator := sqlite.NewMigrator(store, logger)
	require.NoError(t, sqliteMigrator.Up(ctx, migrations.All))

	// Make sure foreign-key checking is enabled.
	_, err := store.DB.Exec("PRAGMA foreign_keys = ON;")
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	mocks := mocks{
		bucketSvc: replicationsMock.NewMockBucketService(ctrl),
		validator: replicationsMock.NewMockReplicationValidator(ctrl),
	}
	svc := service{
		store:         store,
		idGenerator:   mock.NewIncrementingIDGenerator(initID),
		bucketService: mocks.bucketSvc,
		validator:     mocks.validator,
	}

	return &svc, mocks, clean
}

func insertRemote(t *testing.T, store *sqlite.SqlStore, id platform.ID) {
	store.Mu.Lock()
	defer store.Mu.Unlock()

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

	_, err = store.DB.Exec(query, args...)
	require.NoError(t, err)
}
