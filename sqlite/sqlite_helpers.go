package sqlite

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func NewTestStore(t *testing.T) (*SqlStore, func(t *testing.T)) {
	tempDir, err := os.MkdirTemp("", "")
	require.NoError(t, err, "unable to create temporary test directory")

	s, err := NewSqlStore(tempDir+"/"+DefaultFilename, zap.NewNop())
	require.NoError(t, err, "unable to open testing database")

	cleanUpFn := func(t *testing.T) {
		require.NoError(t, s.Close(), "failed to close testing database")
		require.NoErrorf(t, os.RemoveAll(tempDir), "unable to delete temporary test directory %s", tempDir)
	}

	return s, cleanUpFn
}
