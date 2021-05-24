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

	store, clean := newTestStore(t)
	defer clean(t)

	err := store.execTrans(`CREATE TABLE test_table_1 (id TEXT NOT NULL PRIMARY KEY)`)
	require.NoError(t, err)

	err = store.execTrans(`INSERT INTO test_table_1 (id) VALUES ("one"), ("two"), ("three")`)
	require.NoError(t, err)

	vals, err := store.queryToStrings(`SELECT * FROM test_table_1`)
	require.NoError(t, err)
	require.Equal(t, 3, len(vals))

	store.Flush(context.Background())

	vals, err = store.queryToStrings(`SELECT * FROM test_table_1`)
	require.NoError(t, err)
	require.Equal(t, 0, len(vals))
}

func TestUserVersion(t *testing.T) {
	t.Parallel()

	store, clean := newTestStore(t)
	defer clean(t)

	err := store.execTrans(`PRAGMA user_version=12`)
	require.NoError(t, err)

	got, err := store.userVersion()
	require.NoError(t, err)
	require.Equal(t, 12, got)
}

func TestTableNames(t *testing.T) {
	t.Parallel()

	store, clean := newTestStore(t)
	defer clean(t)

	err := store.execTrans(`CREATE TABLE test_table_1 (id TEXT NOT NULL PRIMARY KEY);
	CREATE TABLE test_table_3 (id TEXT NOT NULL PRIMARY KEY);
	CREATE TABLE test_table_2 (id TEXT NOT NULL PRIMARY KEY);`)
	require.NoError(t, err)

	got, err := store.tableNames()
	require.NoError(t, err)
	require.Equal(t, []string{"test_table_1", "test_table_3", "test_table_2"}, got)
}

func newTestStore(t *testing.T) (*SqlStore, func(t *testing.T)) {
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("unable to create temporary test directory %v", err)
	}

	cleanUpFn := func(t *testing.T) {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Fatalf("unable to delete temporary test directory %s: %v", tempDir, err)
		}
	}

	s, err := NewSqlStore(tempDir+"/"+DefaultFilename, zap.NewNop())
	if err != nil {
		t.Fatal("unable to open testing database")
	}

	return s, cleanUpFn
}
