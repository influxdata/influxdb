package sqlite

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFlush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, clean := NewTestStore(t)
	defer clean(t)

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

func TestBackupSqlStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// create a new in-memory database and add some data to it. this will be the "source" of the backup.
	// we're using an in-memory database to verify that the sqlite API is able to back up an in-memory database
	// to a file.
	store, err := NewSqlStore(":memory:", zap.NewNop())
	require.NoError(t, err)
	defer store.DB.Close()

	_, err = store.DB.Exec(`CREATE TABLE test_table_1 (id TEXT NOT NULL PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = store.DB.Exec(`INSERT INTO test_table_1 (id) VALUES ("one"), ("two"), ("three")`)
	require.NoError(t, err)

	_, err = store.DB.Exec(`CREATE TABLE test_table_2 (id TEXT NOT NULL PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = store.DB.Exec(`INSERT INTO test_table_2 (id) VALUES ("four"), ("five"), ("six")`)
	require.NoError(t, err)

	// create a file to write the backup to.
	tempDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// open the file to use as a writer for BackupSqlStore
	backupPath := tempDir + "/db.sqlite"
	dest, err := os.Create(backupPath)
	require.NoError(t, err)

	// run the backup
	err = store.BackupSqlStore(ctx, dest)
	require.NoError(t, err)

	// open the backup file as a database
	backup, err := NewSqlStore(backupPath, zap.NewNop())
	require.NoError(t, err)
	defer backup.DB.Close()

	// perform a query to verify that the database has been backed up properly
	var res1, res2 []string
	err = backup.DB.Select(&res1, `SELECT * FROM test_table_1`)
	require.NoError(t, err)
	err = backup.DB.Select(&res2, `SELECT * FROM test_table_2`)
	require.NoError(t, err)

	require.Equal(t, []string{"one", "two", "three"}, res1)
	require.Equal(t, []string{"four", "five", "six"}, res2)
}

func TestUserVersion(t *testing.T) {
	t.Parallel()

	store, clean := NewTestStore(t)
	defer clean(t)
	ctx := context.Background()

	err := store.execTrans(ctx, `PRAGMA user_version=12`)
	require.NoError(t, err)

	got, err := store.userVersion()
	require.NoError(t, err)
	require.Equal(t, 12, got)
}

func TestTableNames(t *testing.T) {
	t.Parallel()

	store, clean := NewTestStore(t)
	defer clean(t)
	ctx := context.Background()

	err := store.execTrans(ctx, `CREATE TABLE test_table_1 (id TEXT NOT NULL PRIMARY KEY);
	CREATE TABLE test_table_3 (id TEXT NOT NULL PRIMARY KEY);
	CREATE TABLE test_table_2 (id TEXT NOT NULL PRIMARY KEY);`)
	require.NoError(t, err)

	got, err := store.tableNames()
	require.NoError(t, err)
	require.Equal(t, []string{"test_table_1", "test_table_3", "test_table_2"}, got)
}
