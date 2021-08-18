package remotes

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
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
	connection = influxdb.RemoteConnection{
		ID:               initID,
		OrgID:            platform.ID(10), //createReq.OrgID,
		Name:             "test",
		Description:      &desc,
		RemoteURL:        "https://influxdb.cloud",
		RemoteOrgID:      platform.ID(20),
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
	httpConfig = influxdb.RemoteConnectionHTTPConfig{
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
	updatedHttpConfig = influxdb.RemoteConnectionHTTPConfig{
		RemoteURL:        updatedConnection.RemoteURL,
		RemoteToken:      fakeToken2,
		RemoteOrgID:      updatedConnection.RemoteOrgID,
		AllowInsecureTLS: updatedConnection.AllowInsecureTLS,
	}
)

func TestCreateAndGetConnection(t *testing.T) {
	t.Parallel()

	svc, mockValidator, clean := newTestService(t)
	defer clean(t)

	// Getting or validating an invalid ID should return an error.
	got, err := svc.GetRemoteConnection(ctx, initID)
	require.ErrorIs(t, errRemoteNotFound, err)
	require.Nil(t, got)
	require.ErrorIs(t, errRemoteNotFound, svc.ValidateRemoteConnection(ctx, initID))

	// Create a connection, check the results.
	created, err := svc.CreateRemoteConnection(ctx, createReq)
	require.NoError(t, err)
	require.Equal(t, connection, *created)

	// Read the created notebook and assert it matches the creation response.
	got, err = svc.GetRemoteConnection(ctx, initID)
	require.NoError(t, err)
	require.Equal(t, connection, *got)

	// Validate the connection; this is mostly a no-op, for this test, but it allows
	// us to assert that the auth token was properly persisted.
	fakeErr := errors.New("O NO")
	mockValidator.EXPECT().ValidateRemoteConnectionHTTPConfig(gomock.Any(), &httpConfig).Return(fakeErr)
	require.ErrorIs(t, fakeErr, svc.ValidateRemoteConnection(ctx, initID))
}

func TestValidateConnectionWithoutPersisting(t *testing.T) {
	t.Parallel()

	t.Run("error", func(t *testing.T) {
		t.Parallel()

		svc, mockValidator, clean := newTestService(t)
		defer clean(t)

		fakeErr := errors.New("O NO")
		mockValidator.EXPECT().ValidateRemoteConnectionHTTPConfig(gomock.Any(), &httpConfig).Return(fakeErr)
		require.ErrorIs(t, fakeErr, svc.ValidateNewRemoteConnection(ctx, createReq))

		got, err := svc.GetRemoteConnection(ctx, initID)
		require.ErrorIs(t, err, errRemoteNotFound)
		require.Nil(t, got)
	})

	t.Run("no error", func(t *testing.T) {
		t.Parallel()

		svc, mockValidator, clean := newTestService(t)
		defer clean(t)

		mockValidator.EXPECT().ValidateRemoteConnectionHTTPConfig(gomock.Any(), &httpConfig).Return(nil)
		require.NoError(t, svc.ValidateNewRemoteConnection(ctx, createReq))

		got, err := svc.GetRemoteConnection(ctx, initID)
		require.ErrorIs(t, err, errRemoteNotFound)
		require.Nil(t, got)
	})
}

func TestUpdateAndGetConnection(t *testing.T) {
	t.Parallel()

	svc, mockValidator, clean := newTestService(t)
	defer clean(t)

	// Updating a nonexistent ID fails.
	updated, err := svc.UpdateRemoteConnection(ctx, initID, updateReq)
	require.ErrorIs(t, err, errRemoteNotFound)
	require.Nil(t, updated)

	// Create a connection.
	created, err := svc.CreateRemoteConnection(ctx, createReq)
	require.NoError(t, err)
	require.Equal(t, connection, *created)

	// Update the connection.
	updated, err = svc.UpdateRemoteConnection(ctx, initID, updateReq)
	require.NoError(t, err)
	require.Equal(t, updatedConnection, *updated)

	// Validate the updated connection to check that the new auth token persisted.
	mockValidator.EXPECT().ValidateRemoteConnectionHTTPConfig(gomock.Any(), &updatedHttpConfig).Return(nil)
	require.NoError(t, svc.ValidateRemoteConnection(ctx, initID))
}

func TestValidateUpdatedConnectionWithoutPersisting(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T, svc *service) {
		// Validating an update to a nonexistent ID fails.
		updated, err := svc.UpdateRemoteConnection(ctx, initID, updateReq)
		require.ErrorIs(t, err, errRemoteNotFound)
		require.Nil(t, updated)

		// Create a connection.
		created, err := svc.CreateRemoteConnection(ctx, createReq)
		require.NoError(t, err)
		require.Equal(t, connection, *created)
	}
	fakeErr := errors.New("O NO")

	t.Run("update causes error", func(t *testing.T) {
		t.Parallel()

		svc, mockValidator, clean := newTestService(t)
		defer clean(t)
		setup(t, svc)

		mockValidator.EXPECT().ValidateRemoteConnectionHTTPConfig(gomock.Any(), &updatedHttpConfig).Return(fakeErr)
		mockValidator.EXPECT().ValidateRemoteConnectionHTTPConfig(gomock.Any(), &httpConfig).Return(nil)

		require.ErrorIs(t, svc.ValidateUpdatedRemoteConnection(ctx, initID, updateReq), fakeErr)

		// Ensure the update wasn't applied.
		got, err := svc.GetRemoteConnection(ctx, initID)
		require.NoError(t, err)
		require.Equal(t, connection, *got)
		require.NoError(t, svc.ValidateRemoteConnection(ctx, initID))
	})

	t.Run("update fixes error", func(t *testing.T) {
		t.Parallel()

		svc, mockValidator, clean := newTestService(t)
		defer clean(t)
		setup(t, svc)

		mockValidator.EXPECT().ValidateRemoteConnectionHTTPConfig(gomock.Any(), &updatedHttpConfig).Return(nil)
		mockValidator.EXPECT().ValidateRemoteConnectionHTTPConfig(gomock.Any(), &httpConfig).Return(fakeErr)

		require.NoError(t, svc.ValidateUpdatedRemoteConnection(ctx, initID, updateReq))

		// Ensure the update wasn't applied.
		got, err := svc.GetRemoteConnection(ctx, initID)
		require.NoError(t, err)
		require.Equal(t, connection, *got)
		require.ErrorIs(t, svc.ValidateRemoteConnection(ctx, initID), fakeErr)
	})
}

func TestDeleteConnection(t *testing.T) {
	t.Parallel()

	svc, _, clean := newTestService(t)
	defer clean(t)

	// Deleting a nonexistent ID should return an error.
	require.ErrorIs(t, svc.DeleteRemoteConnection(ctx, initID), errRemoteNotFound)

	// Create a connection, then delete it.
	created, err := svc.CreateRemoteConnection(ctx, createReq)
	require.NoError(t, err)
	require.Equal(t, connection, *created)
	require.NoError(t, svc.DeleteRemoteConnection(ctx, initID))

	// Looking up the ID should again produce an error.
	got, err := svc.GetRemoteConnection(ctx, initID)
	require.ErrorIs(t, err, errRemoteNotFound)
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

		svc, _, clean := newTestService(t)
		defer clean(t)
		allConns := setup(t, svc)

		listed, err := svc.ListRemoteConnections(ctx, influxdb.RemoteConnectionListFilter{OrgID: connection.OrgID})
		require.NoError(t, err)
		require.Equal(t, influxdb.RemoteConnections{Remotes: allConns}, *listed)
	})

	t.Run("list by name", func(t *testing.T) {
		t.Parallel()

		svc, _, clean := newTestService(t)
		defer clean(t)
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

		svc, _, clean := newTestService(t)
		defer clean(t)
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

		svc, _, clean := newTestService(t)
		defer clean(t)
		setup(t, svc)

		listed, err := svc.ListRemoteConnections(ctx, influxdb.RemoteConnectionListFilter{OrgID: platform.ID(1000)})
		require.NoError(t, err)
		require.Equal(t, influxdb.RemoteConnections{}, *listed)
	})
}

func newTestService(t *testing.T) (*service, *mock.MockRemoteConnectionValidator, func(t *testing.T)) {
	store, clean := sqlite.NewTestStore(t)
	logger := zaptest.NewLogger(t)
	sqliteMigrator := sqlite.NewMigrator(store, logger)
	require.NoError(t, sqliteMigrator.Up(ctx, migrations.All))

	mockValidator := mock.NewMockRemoteConnectionValidator(gomock.NewController(t))
	svc := service{
		store:       store,
		log:         logger,
		idGenerator: mock.NewIncrementingIDGenerator(platform.ID(1)),
		validator:   mockValidator,
	}

	return &svc, mockValidator, clean
}
