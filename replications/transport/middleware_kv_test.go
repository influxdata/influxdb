package transport

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/replications/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func getCount(kvStore kv.Store, orgID platform.ID) (uint64, error) {
	var count uint64
	if err := kvStore.Update(context.Background(), func(tx kv.Tx) error {
		encodedID, err := orgID.Encode()
		if err != nil {
			return err
		}
		bucket, err := tx.Bucket([]byte("replicationsv2"))
		if err != nil {
			return err
		}
		c, err := bucket.Get(encodedID)
		if err != nil {
			return err
		}

		count = binary.BigEndian.Uint64(c)
		return nil
	}); err != nil {
		return 0, err
	}

	return count, nil
}

func TestReplicationCreateKVUpdate(t *testing.T) {
	kvStore := inmem.NewKVStore()
	gmock := gomock.NewController(t)
	defer gmock.Finish()
	mockRemote := mock.NewMockReplicationService(gmock)
	telemetry := newTelemetryCollectingService(kvStore, mockRemote)

	kvMigrator, err := migration.NewMigrator(
		zap.L(),
		kvStore,
		all.Migrations[:]...,
	)
	require.NoError(t, err)
	require.NoError(t, kvMigrator.Up(context.Background()))

	ctx := context.Background()
	req := influxdb.CreateReplicationRequest{
		OrgID:          platform.ID(1),
		Name:           "test1",
		RemoteBucketID: platform.ID(11),
		LocalBucketID:  platform.ID(22),
	}

	replication := influxdb.Replication{
		OrgID: platform.ID(1),
	}
	replications := influxdb.Replications{
		Replications: []influxdb.Replication{replication},
	}

	mockRemote.EXPECT().CreateReplication(ctx, req).Return(&replication, nil).Times(1)
	mockRemote.EXPECT().ListReplications(ctx, influxdb.ReplicationListFilter{OrgID: req.OrgID}).Return(&replications, nil).Times(1)

	repl, err := telemetry.CreateReplication(ctx, req)
	require.NoError(t, err)

	count, err := getCount(kvStore, repl.OrgID)
	require.NoError(t, err)
	require.Equal(t, int64(1), int64(count))
}

func TestReplicationDeleteKVUpdate(t *testing.T) {
	kvStore := inmem.NewKVStore()
	gmock := gomock.NewController(t)
	defer gmock.Finish()
	mockRemote := mock.NewMockReplicationService(gmock)
	telemetry := newTelemetryCollectingService(kvStore, mockRemote)

	ctx := context.Background()

	kvMigrator, err := migration.NewMigrator(
		zap.L(),
		kvStore,
		all.Migrations[:]...,
	)
	require.NoError(t, err)
	require.NoError(t, kvMigrator.Up(ctx))

	req := influxdb.CreateReplicationRequest{
		OrgID:          platform.ID(1),
		Name:           "test1",
		RemoteBucketID: platform.ID(11),
		LocalBucketID:  platform.ID(22),
	}
	req2 := req
	req2.Name = "test2"

	replication1 := influxdb.Replication{
		ID:    platform.ID(1),
		OrgID: platform.ID(1),
	}
	replication2 := replication1
	replication2.ID = platform.ID(2)

	remoteConnectionsPreDelete := influxdb.Replications{
		Replications: []influxdb.Replication{replication1, replication2},
	}

	remoteConnectionsPostDelete := influxdb.Replications{
		Replications: []influxdb.Replication{replication1},
	}

	mockRemote.EXPECT().CreateReplication(ctx, req).Return(&replication1, nil).Times(1)
	mockRemote.EXPECT().CreateReplication(ctx, req2).Return(&replication2, nil).Times(1)
	mockRemote.EXPECT().ListReplications(ctx, influxdb.ReplicationListFilter{OrgID: req.OrgID}).Return(&remoteConnectionsPreDelete, nil).Times(2)

	mockRemote.EXPECT().DeleteReplication(ctx, replication1.ID).Return(nil).Times(1)
	mockRemote.EXPECT().GetReplication(ctx, replication1.ID).Return(&replication1, nil).Times(1)
	mockRemote.EXPECT().ListReplications(ctx, influxdb.ReplicationListFilter{OrgID: req.OrgID}).Return(&remoteConnectionsPostDelete, nil).Times(1)

	_, err = telemetry.CreateReplication(ctx, req)
	require.NoError(t, err)

	repl, err := telemetry.CreateReplication(ctx, req2)
	require.NoError(t, err)

	err = telemetry.DeleteReplication(ctx, replication1.ID)
	require.NoError(t, err)

	count, err := getCount(kvStore, repl.OrgID)
	require.NoError(t, err)
	require.Equal(t, int64(1), int64(count))
}
