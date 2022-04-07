package sqlite

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func NewTestStore(t *testing.T) *SqlStore {
	tempDir := t.TempDir()

	s, err := NewSqlStore(tempDir+"/"+DefaultFilename, zap.NewNop())
	require.NoError(t, err, "unable to open testing database")

	t.Cleanup(func() {
		require.NoError(t, s.Close(), "failed to close testing database")
	})

	return s
}
