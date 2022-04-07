package sqlite

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)

	err := store.execTrans(ctx, `CREATE TABLE test_table_1 (id TEXT NOT NULL PRIMARY KEY)`)
	require.NoError(t, err)

	err = store.execTrans(ctx, `INSERT INTO test_table_1 (id) VALUES ("one"), ("two"), ("three")`)
	require.NoError(t, err)

	vals, err := store.queryToStrings(`SELECT * FROM test_table_1`)
	require.NoError(t, err)
	require.Equal(t, 3, len(vals))

	store.Flush(context.Background())

	vals, err = store.queryToStrings(`SELECT * FROM test_table_1`)
	require.NoError(t, err)
	require.Equal(t, 0, len(vals))
}

func TestFlushMigrationsTable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := NewTestStore(t)

	require.NoError(t, store.execTrans(ctx, fmt.Sprintf(`CREATE TABLE %s (id TEXT NOT NULL PRIMARY KEY)`, migrationsTableName)))
	require.NoError(t, store.execTrans(ctx, fmt.Sprintf(`INSERT INTO %s (id) VALUES ("one"), ("two"), ("three")`, migrationsTableName)))
	store.Flush(context.Background())

	got, err := store.queryToStrings(fmt.Sprintf(`SELECT * FROM %s`, migrationsTableName))
	require.NoError(t, err)
	want := []string{"one", "two", "three"}
	require.Equal(t, want, got)
}

func TestBackupSqlStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// this temporary dir/file is is used as the source db path for testing a bacup
	// from a non-memory database. each individual test also creates a separate temporary dir/file
	// to backup into.
	td := t.TempDir()
	tf := fmt.Sprintf("%s/%s", td, DefaultFilename)

	tests := []struct {
		name   string
		dbPath string
	}{
		{
			"in-memory db",
			":memory:",
		},
		{
			"file-based db",
			tf,
		},
	}

	for _, tt := range tests {
		store, err := NewSqlStore(tt.dbPath, zap.NewNop())
		require.NoError(t, err)
		defer store.Close()

		_, err = store.DB.Exec(`CREATE TABLE test_table_1 (id TEXT NOT NULL PRIMARY KEY)`)
		require.NoError(t, err)
		_, err = store.DB.Exec(`INSERT INTO test_table_1 (id) VALUES ("one"), ("two"), ("three")`)
		require.NoError(t, err)

		_, err = store.DB.Exec(`CREATE TABLE test_table_2 (id TEXT NOT NULL PRIMARY KEY)`)
		require.NoError(t, err)
		_, err = store.DB.Exec(`INSERT INTO test_table_2 (id) VALUES ("four"), ("five"), ("six")`)
		require.NoError(t, err)

		// create a file to write the backup to.
		tempDir := t.TempDir()

		// open the file to use as a writer for BackupSqlStore
		backupPath := tempDir + "/db.sqlite"
		dest, err := os.Create(backupPath)
		require.NoError(t, err)
		defer dest.Close()

		// run the backup
		err = store.BackupSqlStore(ctx, dest)
		require.NoError(t, err)

		// open the backup file as a database
		backup, err := NewSqlStore(backupPath, zap.NewNop())
		require.NoError(t, err)
		defer backup.Close()

		// perform a query to verify that the database has been backed up properly
		var res1, res2 []string
		err = backup.DB.Select(&res1, `SELECT * FROM test_table_1`)
		require.NoError(t, err)
		err = backup.DB.Select(&res2, `SELECT * FROM test_table_2`)
		require.NoError(t, err)

		require.Equal(t, []string{"one", "two", "three"}, res1)
		require.Equal(t, []string{"four", "five", "six"}, res2)
	}
}

func TestRestoreSqlStore(t *testing.T) {
	t.Parallel()

	// this temporary dir/file is is used as the destination db path for testing a restore
	// into a non-memory database. each individual test also creates a separate temporary dir/file
	// to hold a test db to restore from.
	td := t.TempDir()
	tf := fmt.Sprintf("%s/%s", td, DefaultFilename)

	tests := []struct {
		name   string
		dbPath string
	}{
		{
			"in-memory db",
			":memory:",
		},
		{
			"file-based db",
			tf,
		},
	}

	for _, tt := range tests {
		ctx := context.Background()

		// create the test db to restore from
		tempDir := t.TempDir()
		tempFileName := fmt.Sprintf("%s/%s", tempDir, DefaultFilename)

		restoreDB, err := NewSqlStore(tempFileName, zap.NewNop())
		require.NoError(t, err)
		t.Cleanup(func() { restoreDB.Close() })

		// add some data to the test db
		_, err = restoreDB.DB.Exec(`CREATE TABLE test_table_1 (id TEXT NOT NULL PRIMARY KEY)`)
		require.NoError(t, err)
		_, err = restoreDB.DB.Exec(`INSERT INTO test_table_1 (id) VALUES ("one"), ("two"), ("three")`)
		require.NoError(t, err)

		_, err = restoreDB.DB.Exec(`CREATE TABLE test_table_2 (id TEXT NOT NULL PRIMARY KEY)`)
		require.NoError(t, err)
		_, err = restoreDB.DB.Exec(`INSERT INTO test_table_2 (id) VALUES ("four"), ("five"), ("six")`)
		require.NoError(t, err)

		// we're done using the restore db as a database, so close it now
		err = restoreDB.Close()
		require.NoError(t, err)

		// open the test "restore-from" db file as a reader
		f, err := os.Open(tempFileName)
		require.NoError(t, err)
		t.Cleanup(func() { f.Close() })

		// open a db to restore into. it will be empty to begin with.
		restore, err := NewSqlStore(tt.dbPath, zap.NewNop())
		require.NoError(t, err)
		t.Cleanup(func() { restore.Close() })

		// run the restore
		err = restore.RestoreSqlStore(ctx, f)
		require.NoError(t, err)

		// perform a query to verify that the database has been restored up properly
		var res1, res2 []string
		err = restore.DB.Select(&res1, `SELECT * FROM test_table_1`)
		require.NoError(t, err)
		err = restore.DB.Select(&res2, `SELECT * FROM test_table_2`)
		require.NoError(t, err)

		require.Equal(t, []string{"one", "two", "three"}, res1)
		require.Equal(t, []string{"four", "five", "six"}, res2)

		require.NoError(t, f.Close())
	}
}

func TestTableNames(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	err := store.execTrans(ctx, `CREATE TABLE test_table_1 (id TEXT NOT NULL PRIMARY KEY);
	CREATE TABLE test_table_3 (id TEXT NOT NULL PRIMARY KEY);
	CREATE TABLE test_table_2 (id TEXT NOT NULL PRIMARY KEY);`)
	require.NoError(t, err)

	got, err := store.tableNames()
	require.NoError(t, err)
	require.Equal(t, []string{"test_table_1", "test_table_3", "test_table_2"}, got)
}

func TestAllMigrationNames(t *testing.T) {
	t.Parallel()

	store := NewTestStore(t)
	ctx := context.Background()

	// Empty db, returns nil slice and no error
	got, err := store.allMigrationNames()
	require.NoError(t, err)
	require.Equal(t, []string(nil), got)

	// DB contains migrations table but no migrations
	err = store.execTrans(ctx, `CREATE TABLE migrations (
			id TEXT NOT NULL PRIMARY KEY,
			name TEXT NOT NULL)`)
	require.NoError(t, err)
	got, err = store.allMigrationNames()
	require.NoError(t, err)
	require.Equal(t, []string(nil), got)

	// DB contains one migration
	err = store.execTrans(ctx, `INSERT INTO migrations (id, name) VALUES ("1", "0000_create_migrations_table.sql")`)
	require.NoError(t, err)
	got, err = store.allMigrationNames()
	require.NoError(t, err)
	require.Equal(t, []string{"0000_create_migrations_table.sql"}, got)

	// DB contains multiple migrations - they are returned sorted by name
	err = store.execTrans(ctx, `INSERT INTO migrations (id, name) VALUES ("3", "0001_first_migration.sql")`)
	require.NoError(t, err)
	err = store.execTrans(ctx, `INSERT INTO migrations (id, name) VALUES ("2", "0002_second_migration.sql")`)
	require.NoError(t, err)
	got, err = store.allMigrationNames()
	require.NoError(t, err)
	require.Equal(t, []string{"0000_create_migrations_table.sql", "0001_first_migration.sql", "0002_second_migration.sql"}, got)
}
