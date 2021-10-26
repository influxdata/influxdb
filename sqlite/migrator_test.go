package sqlite

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/errors"
	"github.com/influxdata/influxdb/v2/sqlite/test_migrations"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

type tableInfo struct {
	Cid        int         `db:"cid"`
	Name       string      `db:"name"`
	Db_type    string      `db:"type"`
	Notnull    int         `db:"notnull"`
	Dflt_value interface{} `db:"dflt_value"`
	Pk         int         `db:"pk"`
}

func TestUp(t *testing.T) {
	t.Parallel()

	store, clean := NewTestStore(t)
	defer clean(t)
	ctx := context.Background()

	// a new database should have a user_version of 0
	v, err := store.userVersion()
	require.NoError(t, err)
	require.Equal(t, 0, v)

	migrator := NewMigrator(store, zaptest.NewLogger(t))
	migrator.Up(ctx, test_migrations.All)

	// user_version should now be 3 after applying the migrations
	v, err = store.userVersion()
	require.NoError(t, err)
	require.Equal(t, 3, v)

	// make sure that test_table_1 had the "id" column renamed to "org_id"
	table1Info := []*tableInfo{}
	err = store.DB.Select(&table1Info, "PRAGMA table_info(test_table_1)")
	require.NoError(t, err)
	require.Len(t, table1Info, 3)
	require.Equal(t, "org_id", table1Info[0].Name)

	// make sure that test_table_2 was created correctly
	table2Info := []*tableInfo{}
	err = store.DB.Select(&table2Info, "PRAGMA table_info(test_table_2)")
	require.NoError(t, err)
	require.Len(t, table2Info, 3)
	require.Equal(t, "user_id", table2Info[0].Name)
}

func TestUpWithBackups(t *testing.T) {
	t.Parallel()

	store, clean := NewTestStore(t)
	defer clean(t)
	ctx := context.Background()

	logger := zaptest.NewLogger(t)
	migrator := NewMigrator(store, logger)
	backupPath := fmt.Sprintf("%s.bak", store.path)
	migrator.SetBackupPath(backupPath)

	// Run the first migration.
	require.NoError(t, migrator.Up(ctx, test_migrations.First))

	// user_version should now be 1.
	v, err := store.userVersion()
	require.NoError(t, err)
	require.Equal(t, 1, v)

	// Backup file should exist.
	backup1Fi, err := os.Stat(backupPath)
	require.NoError(t, err)

	// Run the remaining migrations.
	require.NoError(t, migrator.Up(ctx, test_migrations.Rest))

	// user_version should now be 3.
	v, err = store.userVersion()
	require.NoError(t, err)
	require.Equal(t, 3, v)

	// Backup file should have been overwritten.
	backup2Fi, err := os.Stat(backupPath)
	require.NoError(t, err)
	require.Greater(t, backup2Fi.Size(), backup1Fi.Size())
	require.True(t, backup2Fi.ModTime().After(backup1Fi.ModTime()))

	// Open a 2nd store using the backup file.
	backupStore, err := NewSqlStore(backupPath, zap.NewNop())
	require.NoError(t, err)
	defer backupStore.Close()

	// user_version should be 1 in the backup.
	v, err = backupStore.userVersion()
	require.NoError(t, err)
	require.Equal(t, 1, v)

	// Run the remaining migrations on the backup.
	backupMigrator := NewMigrator(backupStore, logger)
	require.NoError(t, backupMigrator.Up(ctx, test_migrations.Rest))

	// user_version should now be 3 in the backup.
	v, err = backupStore.userVersion()
	require.NoError(t, err)
	require.Equal(t, 3, v)
}

func TestScriptVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filename string
		want     int
		wantErr  error
	}{
		{
			"single digit number",
			"0001_some_file_name.sql",
			1,
			nil,
		},
		{
			"larger number",
			"0921_another_file.sql",
			921,
			nil,
		},
		{
			"bad name",
			"not_numbered_correctly.sql",
			0,
			&errors.Error{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := scriptVersion(tt.filename)
			require.Equal(t, tt.want, got)
			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
