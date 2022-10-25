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
	"github.com/influxdata/influxdb/v2/remotes/mock"
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
		bucket, err := tx.Bucket([]byte("remotesv2"))
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

func TestRemoteCreateKVUpdate(t *testing.T) {
	kvStore := inmem.NewKVStore()
	gmock := gomock.NewController(t)
	defer gmock.Finish()
	mockRemote := mock.NewMockRemoteConnectionService(gmock)
	telemetry := newTelemetryCollectingService(kvStore, mockRemote)

	kvMigrator, err := migration.NewMigrator(
		zap.L(),
		kvStore,
		all.Migrations[:]...,
	)
	require.NoError(t, err)
	require.NoError(t, kvMigrator.Up(context.Background()))

	ctx := context.Background()
	remoteID := platform.ID(2)
	req := influxdb.CreateRemoteConnectionRequest{
		OrgID:       platform.ID(1),
		Name:        "test1",
		RemoteURL:   "cloud2.influxdata.com",
		RemoteToken: "testtoken",
		RemoteOrgID: &remoteID,
	}

	remoteConnction := influxdb.RemoteConnection{
		OrgID: platform.ID(1),
	}
	remoteConnections := influxdb.RemoteConnections{
		Remotes: []influxdb.RemoteConnection{remoteConnction},
	}

	mockRemote.EXPECT().CreateRemoteConnection(ctx, req).Return(&remoteConnction, nil).Times(1)
	mockRemote.EXPECT().ListRemoteConnections(ctx, influxdb.RemoteConnectionListFilter{OrgID: req.OrgID}).Return(&remoteConnections, nil).Times(1)

	remote, err := telemetry.CreateRemoteConnection(ctx, req)
	require.NoError(t, err)

	count, err := getCount(kvStore, remote.OrgID)
	require.NoError(t, err)
	require.Equal(t, int64(1), int64(count))
}

func TestRemoteDeleteKVUpdate(t *testing.T) {
	kvStore := inmem.NewKVStore()
	gmock := gomock.NewController(t)
	defer gmock.Finish()
	mockRemote := mock.NewMockRemoteConnectionService(gmock)
	telemetry := newTelemetryCollectingService(kvStore, mockRemote)

	ctx := context.Background()

	kvMigrator, err := migration.NewMigrator(
		zap.L(),
		kvStore,
		all.Migrations[:]...,
	)
	require.NoError(t, err)
	require.NoError(t, kvMigrator.Up(ctx))

	remoteID := platform.ID(2)
	req := influxdb.CreateRemoteConnectionRequest{
		OrgID:       platform.ID(1),
		Name:        "test1",
		RemoteURL:   "cloud2.influxdata.com",
		RemoteToken: "testtoken",
		RemoteOrgID: &remoteID,
	}
	req2 := req
	req2.Name = "test2"

	remoteConnection1 := influxdb.RemoteConnection{
		ID:    platform.ID(1),
		OrgID: platform.ID(1),
	}
	remoteConnection2 := remoteConnection1
	remoteConnection2.ID = platform.ID(2)

	remoteConnectionsPreDelete := influxdb.RemoteConnections{
		Remotes: []influxdb.RemoteConnection{remoteConnection1, remoteConnection2},
	}

	remoteConnectionsPostDelete := influxdb.RemoteConnections{
		Remotes: []influxdb.RemoteConnection{remoteConnection1},
	}

	mockRemote.EXPECT().CreateRemoteConnection(ctx, req).Return(&remoteConnection1, nil).Times(1)
	mockRemote.EXPECT().CreateRemoteConnection(ctx, req2).Return(&remoteConnection2, nil).Times(1)
	mockRemote.EXPECT().ListRemoteConnections(ctx, influxdb.RemoteConnectionListFilter{OrgID: req.OrgID}).Return(&remoteConnectionsPreDelete, nil).Times(2)

	mockRemote.EXPECT().DeleteRemoteConnection(ctx, remoteConnection1.ID).Return(nil).Times(1)
	mockRemote.EXPECT().GetRemoteConnection(ctx, remoteConnection1.ID).Return(&remoteConnection1, nil).Times(1)
	mockRemote.EXPECT().ListRemoteConnections(ctx, influxdb.RemoteConnectionListFilter{OrgID: req.OrgID}).Return(&remoteConnectionsPostDelete, nil).Times(1)

	_, err = telemetry.CreateRemoteConnection(ctx, req)
	require.NoError(t, err)

	remote, err := telemetry.CreateRemoteConnection(ctx, req2)
	require.NoError(t, err)

	err = telemetry.DeleteRemoteConnection(ctx, remoteConnection1.ID)
	require.NoError(t, err)

	count, err := getCount(kvStore, remote.OrgID)
	require.NoError(t, err)
	require.Equal(t, int64(1), int64(count))
}
