package sqlite

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/errors"
	"github.com/influxdata/influxdb/v2/sqlite/test_migrations"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestUp(t *testing.T) {
	t.Parallel()

	store, clean := newTestStore(t)
	defer clean(t)
	ctx := context.Background()

	// a new database should have a user_version of 0
	v, err := store.userVersion()
	require.NoError(t, err)
	require.Equal(t, 0, v)

	migrator := NewMigrator(store, zaptest.NewLogger(t))
	migrator.Up(ctx, &test_migrations.All{})

	// user_version should now be 3 after applying the migrations
	v, err = store.userVersion()
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
