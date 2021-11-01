package sqlite

import (
	"context"
	"embed"
	"fmt"
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

	store, clean := NewTestStore(t)
	defer clean(t)
	ctx := context.Background()

	// empty db contains no migrations
	names, err := store.allMigrationNames()
	require.NoError(t, err)
	require.Equal(t, []string(nil), names)

	// run the first migrations
	migrator := NewMigrator(store, zaptest.NewLogger(t))
	require.NoError(t, migrator.Up(ctx, test_migrations.First))
	names, err = store.allMigrationNames()
	require.NoError(t, err)
	migrationNamesMatch(t, names, test_migrations.First)

	// run the rest of the migrations
	err = migrator.Up(ctx, test_migrations.All)
	require.NoError(t, err)
	names, err = store.allMigrationNames()
	require.NoError(t, err)
	migrationNamesMatch(t, names, test_migrations.All)

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
		store, clean := NewTestStore(t)
		defer clean(t)
		ctx := context.Background()

		migrator := NewMigrator(store, zaptest.NewLogger(t))
		require.NoError(t, migrator.Up(ctx, test_migrations.MigrationTable))
		require.NoError(t, store.execTrans(ctx, `INSERT INTO migrations (name) VALUES ("0010_some_bad_migration")`))
		require.Equal(t, migration.ErrInvalidMigration("0010_some_bad_migration"), migrator.Up(ctx, test_migrations.All))
	})

	t.Run("known + unknown migrations exist", func(t *testing.T) {
		store, clean := NewTestStore(t)
		defer clean(t)
		ctx := context.Background()

		migrator := NewMigrator(store, zaptest.NewLogger(t))
		require.NoError(t, migrator.Up(ctx, test_migrations.First))
		require.NoError(t, store.execTrans(ctx, `INSERT INTO migrations (name) VALUES ("0010_some_bad_migration")`))
		require.Equal(t, migration.ErrInvalidMigration("0010_some_bad_migration"), migrator.Up(ctx, test_migrations.All))
	})
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

	// Run the first migrations.
	require.NoError(t, migrator.Up(ctx, test_migrations.First))
	names, err := store.allMigrationNames()
	require.NoError(t, err)
	migrationNamesMatch(t, names, test_migrations.First)

	// Backup file shouldn't exist, because there was nothing to back up.
	_, err = os.Stat(backupPath)
	require.True(t, os.IsNotExist(err))

	// Run the remaining migrations.
	require.NoError(t, migrator.Up(ctx, test_migrations.All))
	names, err = store.allMigrationNames()
	require.NoError(t, err)
	migrationNamesMatch(t, names, test_migrations.All)

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
	migrationNamesMatch(t, backupNames, test_migrations.First)

	// Run the remaining migrations on the backup.
	backupMigrator := NewMigrator(backupStore, logger)
	require.NoError(t, backupMigrator.Up(ctx, test_migrations.All))

	// Backup store now contains the rest of the migration records.
	backupNames, err = backupStore.allMigrationNames()
	require.NoError(t, err)
	migrationNamesMatch(t, backupNames, test_migrations.All)
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

func migrationNamesMatch(t *testing.T, names []string, files embed.FS) {
	t.Helper()
	storedMigrations, err := files.ReadDir(".")
	require.NoError(t, err)
	require.Equal(t, len(storedMigrations), len(names))

	for idx := range storedMigrations {
		require.Equal(t, dropExtension(storedMigrations[idx].Name()), names[idx])
	}
}
