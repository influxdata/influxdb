package sqlite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
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
