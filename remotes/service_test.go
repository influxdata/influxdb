package remotes

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/sqlite"
	"github.com/influxdata/influxdb/v2/sqlite/migrations"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

var (
	ctx        = context.Background()
	initID     = platform.ID(1)
	desc       = "testing testing"
	remoteID   = platform.ID(20)
	connection = influxdb.RemoteConnection{
		ID:               initID,
		OrgID:            platform.ID(10),
		Name:             "test",
		Description:      &desc,
		RemoteURL:        "https://influxdb.cloud",
		RemoteOrgID:      &remoteID,
		AllowInsecureTLS: true,
	}
	fakeToken = "abcdefghijklmnop"
	createReq = influxdb.CreateRemoteConnectionRequest{
		OrgID:            connection.OrgID,
		Name:             connection.Name,
		Description:      connection.Description,
		RemoteURL:        connection.RemoteURL,
		RemoteToken:      fakeToken,
		RemoteOrgID:      connection.RemoteOrgID,
		AllowInsecureTLS: connection.AllowInsecureTLS,
	}
	fakeToken2 = "qrstuvwxyz"
	fals       = false
	updateReq  = influxdb.UpdateRemoteConnectionRequest{
		RemoteToken:      &fakeToken2,
		AllowInsecureTLS: &fals,
	}
	updatedConnection = influxdb.RemoteConnection{
		ID:               connection.ID,
		OrgID:            connection.OrgID,
		Name:             connection.Name,
		Description:      connection.Description,
		RemoteURL:        connection.RemoteURL,
		RemoteOrgID:      connection.RemoteOrgID,
		AllowInsecureTLS: *updateReq.AllowInsecureTLS,
	}
)

func TestCreateAndGetConnection(t *testing.T) {
	t.Parallel()

	svc := newTestService(t)

	// Getting an invalid ID should return an error.
	got, err := svc.GetRemoteConnection(ctx, initID)
	require.Equal(t, errRemoteNotFound, err)
	require.Nil(t, got)

	// Create a connection, check the results.
	created, err := svc.CreateRemoteConnection(ctx, createReq)
	require.NoError(t, err)
	require.Equal(t, connection, *created)

	// Read the created connection and assert it matches the creation response.
	got, err = svc.GetRemoteConnection(ctx, initID)
	require.NoError(t, err)
	require.Equal(t, connection, *got)
}

func TestUpdateAndGetConnection(t *testing.T) {
	t.Parallel()

	svc := newTestService(t)

	// Updating a nonexistent ID fails.
	updated, err := svc.UpdateRemoteConnection(ctx, initID, updateReq)
	require.Equal(t, errRemoteNotFound, err)
	require.Nil(t, updated)

	// Create a connection.
	created, err := svc.CreateRemoteConnection(ctx, createReq)
	require.NoError(t, err)
	require.Equal(t, connection, *created)

	// Update the connection.
	updated, err = svc.UpdateRemoteConnection(ctx, initID, updateReq)
	require.NoError(t, err)
	require.Equal(t, updatedConnection, *updated)

	// Read the updated connection and assert it matches the updated response.
	got, err := svc.GetRemoteConnection(ctx, initID)
	require.NoError(t, err)
	require.Equal(t, updated, got)
}

func TestUpdateNoop(t *testing.T) {
	t.Parallel()

	svc := newTestService(t)

	// Create a connection.
	created, err := svc.CreateRemoteConnection(ctx, createReq)
	require.NoError(t, err)
	require.Equal(t, connection, *created)

	// Send a no-op update, assert nothing changed.
	updated, err := svc.UpdateRemoteConnection(ctx, initID, influxdb.UpdateRemoteConnectionRequest{})
	require.NoError(t, err)
	require.Equal(t, connection, *updated)

	// Read the updated connection and assert it matches the updated response.
	got, err := svc.GetRemoteConnection(ctx, initID)
	require.NoError(t, err)
	require.Equal(t, updated, got)
}

func TestDeleteConnection(t *testing.T) {
	t.Parallel()

	svc := newTestService(t)

	// Deleting a nonexistent ID should return an error.
	require.Equal(t, errRemoteNotFound, svc.DeleteRemoteConnection(ctx, initID))

	// Create a connection, then delete it.
	created, err := svc.CreateRemoteConnection(ctx, createReq)
	require.NoError(t, err)
	require.Equal(t, connection, *created)
	require.NoError(t, svc.DeleteRemoteConnection(ctx, initID))

	// Looking up the ID should again produce an error.
	got, err := svc.GetRemoteConnection(ctx, initID)
	require.Equal(t, errRemoteNotFound, err)
	require.Nil(t, got)
}

func TestListConnections(t *testing.T) {
	t.Parallel()

	createReq2, createReq3 := createReq, createReq
	createReq2.Name, createReq3.Name = "test2", "test3"
	altURL := "https://other.influxdb"
	createReq2.RemoteURL, createReq3.RemoteURL = altURL, altURL

	setup := func(t *testing.T, svc *service) []influxdb.RemoteConnection {
		var allConns []influxdb.RemoteConnection
		for _, req := range []influxdb.CreateRemoteConnectionRequest{createReq, createReq2, createReq3} {
			created, err := svc.CreateRemoteConnection(ctx, req)
			require.NoError(t, err)
			allConns = append(allConns, *created)
		}
		return allConns
	}

	t.Run("list all", func(t *testing.T) {
		t.Parallel()

		svc := newTestService(t)
		allConns := setup(t, svc)

		listed, err := svc.ListRemoteConnections(ctx, influxdb.RemoteConnectionListFilter{OrgID: connection.OrgID})
		require.NoError(t, err)
		require.Equal(t, influxdb.RemoteConnections{Remotes: allConns}, *listed)
	})

	t.Run("list by name", func(t *testing.T) {
		t.Parallel()

		svc := newTestService(t)
		allConns := setup(t, svc)

		listed, err := svc.ListRemoteConnections(ctx, influxdb.RemoteConnectionListFilter{
			OrgID: connection.OrgID,
			Name:  &createReq2.Name,
		})
		require.NoError(t, err)
		require.Equal(t, influxdb.RemoteConnections{Remotes: allConns[1:2]}, *listed)
	})

	t.Run("list by URL", func(t *testing.T) {
		t.Parallel()

		svc := newTestService(t)
		allConns := setup(t, svc)

		listed, err := svc.ListRemoteConnections(ctx, influxdb.RemoteConnectionListFilter{
			OrgID:     connection.OrgID,
			RemoteURL: &altURL,
		})
		require.NoError(t, err)
		require.Equal(t, influxdb.RemoteConnections{Remotes: allConns[1:]}, *listed)
	})

	t.Run("list by other org ID", func(t *testing.T) {
		t.Parallel()

		svc := newTestService(t)
		setup(t, svc)

		listed, err := svc.ListRemoteConnections(ctx, influxdb.RemoteConnectionListFilter{OrgID: platform.ID(1000)})
		require.NoError(t, err)
		require.Equal(t, influxdb.RemoteConnections{}, *listed)
	})
}

func newTestService(t *testing.T) *service {
	store := sqlite.NewTestStore(t)
	logger := zaptest.NewLogger(t)
	sqliteMigrator := sqlite.NewMigrator(store, logger)
	require.NoError(t, sqliteMigrator.Up(ctx, migrations.AllUp))

	svc := service{
		store:       store,
		idGenerator: mock.NewIncrementingIDGenerator(initID),
	}

	return &svc
}
