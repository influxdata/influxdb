package sqlite

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/errors"
	"github.com/influxdata/influxdb/v2/kit/migration"
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

	store := NewTestStore(t)

	upsOnlyAll, err := test_migrations.AllUp.ReadDir(".")
	require.NoError(t, err)

	upsOnlyFirst, err := test_migrations.FirstUp.ReadDir(".")
	require.NoError(t, err)

	migrator := NewMigrator(store, zaptest.NewLogger(t))

	// empty db contains no migrations
	names, err := store.allMigrationNames()
	require.NoError(t, err)
	require.Equal(t, []string(nil), names)

	// run the first migrations
	migrateUpAndCheck(t, migrator, store, test_migrations.FirstUp, upsOnlyFirst)

	// run the rest of the migrations
	migrateUpAndCheck(t, migrator, store, test_migrations.AllUp, upsOnlyAll)

	// test_table_1 had the "id" column renamed to "org_id"
	var table1Info []*tableInfo
	err = store.DB.Select(&table1Info, "PRAGMA table_info(test_table_1)")
	require.NoError(t, err)
	require.Len(t, table1Info, 3)
	require.Equal(t, "org_id", table1Info[0].Name)

	// test_table_2 was created correctly
	var table2Info []*tableInfo
	err = store.DB.Select(&table2Info, "PRAGMA table_info(test_table_2)")
	require.NoError(t, err)
	require.Len(t, table2Info, 3)
	require.Equal(t, "user_id", table2Info[0].Name)
}

func TestUpErrors(t *testing.T) {
	t.Parallel()

	t.Run("only unknown migration exists", func(t *testing.T) {
		store := NewTestStore(t)
		ctx := context.Background()

		migrator := NewMigrator(store, zaptest.NewLogger(t))
		require.NoError(t, migrator.Up(ctx, test_migrations.MigrationTable))
		require.NoError(t, store.execTrans(ctx, `INSERT INTO migrations (name) VALUES ("0010_some_bad_migration")`))
		require.Equal(t, migration.ErrInvalidMigration("0010_some_bad_migration"), migrator.Up(ctx, test_migrations.AllUp))
	})

	t.Run("known + unknown migrations exist", func(t *testing.T) {
		store := NewTestStore(t)
		ctx := context.Background()

		migrator := NewMigrator(store, zaptest.NewLogger(t))
		require.NoError(t, migrator.Up(ctx, test_migrations.FirstUp))
		require.NoError(t, store.execTrans(ctx, `INSERT INTO migrations (name) VALUES ("0010_some_bad_migration")`))
		require.Equal(t, migration.ErrInvalidMigration("0010_some_bad_migration"), migrator.Up(ctx, test_migrations.AllUp))
	})
}

func TestUpWithBackups(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)

	logger := zaptest.NewLogger(t)
	migrator := NewMigrator(store, logger)
	backupPath := fmt.Sprintf("%s.bak", store.path)
	migrator.SetBackupPath(backupPath)

	upsOnlyAll, err := test_migrations.AllUp.ReadDir(".")
	require.NoError(t, err)

	upsOnlyFirst, err := test_migrations.FirstUp.ReadDir(".")
	require.NoError(t, err)

	// Run the first migrations.
	migrateUpAndCheck(t, migrator, store, test_migrations.FirstUp, upsOnlyFirst)

	// Backup file shouldn't exist, because there was nothing to back up.
	_, err = os.Stat(backupPath)
	require.True(t, os.IsNotExist(err))

	// Run the remaining migrations.
	migrateUpAndCheck(t, migrator, store, test_migrations.AllUp, upsOnlyAll)

	// Backup file should now exist.
	_, err = os.Stat(backupPath)
	require.NoError(t, err)

	// Open a 2nd store using the backup file.
	backupStore, err := NewSqlStore(backupPath, zap.NewNop())
	require.NoError(t, err)
	defer backupStore.Close()

	// Backup store contains the first migrations records.
	backupNames, err := backupStore.allMigrationNames()
	require.NoError(t, err)
	migrationNamesMatch(t, backupNames, upsOnlyFirst)

	// Run the remaining migrations on the backup and verify that it now contains the rest of the migration records.
	backupMigrator := NewMigrator(backupStore, logger)
	migrateUpAndCheck(t, backupMigrator, store, test_migrations.AllUp, upsOnlyAll)
}

func TestDown(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)

	upsOnlyAll, err := test_migrations.AllUp.ReadDir(".")
	require.NoError(t, err)

	upsOnlyFirst, err := test_migrations.FirstUp.ReadDir(".")
	require.NoError(t, err)

	migrator := NewMigrator(store, zaptest.NewLogger(t))

	// no up migrations, then some down migrations
	migrateDownAndCheck(t, migrator, store, test_migrations.FirstDown, []fs.DirEntry{}, 0)

	// all up migrations, then all down migrations
	migrateUpAndCheck(t, migrator, store, test_migrations.AllUp, upsOnlyAll)
	migrateDownAndCheck(t, migrator, store, test_migrations.AllDown, []fs.DirEntry{}, 0)

	// first of the up migrations, then first of the down migrations
	migrateUpAndCheck(t, migrator, store, test_migrations.FirstUp, upsOnlyFirst)
	migrateDownAndCheck(t, migrator, store, test_migrations.FirstDown, []fs.DirEntry{}, 0)

	// first of the up migrations, then all of the down migrations
	migrateUpAndCheck(t, migrator, store, test_migrations.FirstUp, upsOnlyFirst)
	migrateDownAndCheck(t, migrator, store, test_migrations.AllDown, []fs.DirEntry{}, 0)

	// all up migrations, then some of the down migrations (using untilMigration)
	migrateUpAndCheck(t, migrator, store, test_migrations.AllUp, upsOnlyAll)
	migrateDownAndCheck(t, migrator, store, test_migrations.AllDown, upsOnlyFirst, 2)
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
		tt := tt // capture range variable
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

func TestDropExtension(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			input: "0001_some_migration",
			want:  "0001_some_migration",
		},
		{
			input: "0001_some_migration.sql",
			want:  "0001_some_migration",
		},
		{
			input: "0001_some_migration.down.sql",
			want:  "0001_some_migration",
		},
		{
			input: "0001_some_migration.something.anything.else",
			want:  "0001_some_migration",
		},
	}

	for _, tt := range tests {
		got := dropExtension(tt.input)
		require.Equal(t, tt.want, got)
	}
}

func migrateUpAndCheck(t *testing.T, m *Migrator, s *SqlStore, source embed.FS, expected []fs.DirEntry) {
	t.Helper()

	require.NoError(t, m.Up(context.Background(), source))
	names, err := s.allMigrationNames()
	require.NoError(t, err)
	migrationNamesMatch(t, names, expected)
}

func migrateDownAndCheck(t *testing.T, m *Migrator, s *SqlStore, source embed.FS, expected []fs.DirEntry, untilMigration int) {
	t.Helper()

	require.NoError(t, m.Down(context.Background(), untilMigration, source))
	names, err := s.allMigrationNames()
	require.NoError(t, err)
	migrationNamesMatch(t, names, expected)
}

func migrationNamesMatch(t *testing.T, names []string, files []fs.DirEntry) {
	t.Helper()

	require.Equal(t, len(names), len(files))

	for idx := range files {
		require.Equal(t, dropExtension(files[idx].Name()), names[idx])
	}
}
