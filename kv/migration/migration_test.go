package migration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func newMigrator(t *testing.T, logger *zap.Logger, store kv.SchemaStore, now influxdbtesting.NowFunc) *migration.Migrator {
	migrator, err := migration.NewMigrator(logger, store)
	if err != nil {
		t.Fatal(err)
	}

	migration.MigratorSetNow(migrator, now)
	return migrator
}

func Test_Inmem_Migrator(t *testing.T) {
	influxdbtesting.Migrator(t, inmem.NewKVStore(), newMigrator)
}

func Test_Bolt_Migrator(t *testing.T) {
	store, closeBolt, err := newTestBoltStoreWithoutMigrations(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}
	defer closeBolt()

	influxdbtesting.Migrator(t, store, newMigrator)
}

func Test_Bolt_MigratorWithBackup(t *testing.T) {
	store, closeBolt, err := newTestBoltStoreWithoutMigrations(t)
	require.NoError(t, err)
	defer closeBolt()

	ctx := context.Background()
	migrator := newMigrator(t, zaptest.NewLogger(t), store, time.Now)
	backupPath := fmt.Sprintf("%s.bak", store.DB().Path())
	migrator.SetBackupPath(backupPath)

	// Run the first migration.
	migrator.AddMigrations(all.Migration0001_InitialMigration)
	require.NoError(t, migrator.Up(ctx))

	// List of applied migrations should now have length 1.
	ms, err := migrator.List(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(ms))

	// Backup file shouldn't exist because there was no previous state to back up.
	_, err = os.Stat(backupPath)
	require.True(t, os.IsNotExist(err))

	// Run a few more migrations.
	migrator.AddMigrations(all.Migrations[1:5]...)
	require.NoError(t, migrator.Up(ctx))

	// List of applied migrations should now have length 5.
	ms, err = migrator.List(ctx)
	require.NoError(t, err)
	require.Equal(t, 5, len(ms))

	// Backup file should now exist.
	_, err = os.Stat(backupPath)
	require.NoError(t, err)

	// Open a 2nd store using the backup file.
	backupStore := bolt.NewKVStore(zaptest.NewLogger(t), backupPath, bolt.WithNoSync)
	require.NoError(t, backupStore.Open(ctx))
	defer backupStore.Close()

	// List of applied migrations in the backup should be 1.
	backupMigrator := newMigrator(t, zaptest.NewLogger(t), backupStore, time.Now)
	backupMigrator.AddMigrations(all.Migration0001_InitialMigration)
	backupMs, err := backupMigrator.List(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(backupMs))

	// Run the other migrations on the backup.
	backupMigrator.AddMigrations(all.Migrations[1:5]...)
	require.NoError(t, backupMigrator.Up(ctx))

	// List of applied migrations in the backup should be 5.
	backupMs, err = backupMigrator.List(ctx)
	require.NoError(t, err)
	require.Equal(t, 5, len(backupMs))
}

func newTestBoltStoreWithoutMigrations(t *testing.T) (*bolt.KVStore, func(), error) {
	f, err := os.CreateTemp("", "influxdata-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	path := f.Name()
	s := bolt.NewKVStore(zaptest.NewLogger(t), path, bolt.WithNoSync)
	if err := s.Open(context.Background()); err != nil {
		return nil, nil, err
	}

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close, nil
}
