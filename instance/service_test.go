package instance

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
	connection = influxdb.Instance{
		ID: initID,
	}
)

func TestCreateAndGetConnection(t *testing.T) {
	t.Parallel()

	svc, clean := newTestService(t)
	defer clean(t)

	// Getting an invalid ID should return an error.
	got, err := svc.GetInstance(ctx)
	require.Equal(t, errInstanceNotFound, err)
	require.Nil(t, got)

	// Create an instance, check the results.
	created, err := svc.CreateInstance(ctx)
	require.NoError(t, err)
	require.Equal(t, connection, *created)

	// Read the created connection and assert it matches the creation response.
	got, err = svc.GetInstance(ctx)
	require.NoError(t, err)
	require.Equal(t, connection, *got)
}

func newTestService(t *testing.T) (*service, func(t *testing.T)) {
	store, clean := sqlite.NewTestStore(t)
	logger := zaptest.NewLogger(t)
	sqliteMigrator := sqlite.NewMigrator(store, logger)
	require.NoError(t, sqliteMigrator.Up(ctx, migrations.AllUp))

	svc := service{
		store:       store,
		idGenerator: mock.NewIncrementingIDGenerator(initID),
	}

	return &svc, clean
}
