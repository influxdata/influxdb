package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSqlStore_CheckName(t *testing.T) {
	s := NewTestStore(t)
	require.Equal(t, HealthCheckName, s.CheckName())
}

func TestSqlStore_CheckPasses(t *testing.T) {
	s := NewTestStore(t)
	resp := s.Check(context.Background())
	require.Equal(t, check.StatusPass, resp.Status)
}

func TestSqlStore_CheckFailsAfterClose(t *testing.T) {
	// Intentionally not using NewTestStore: it registers a Cleanup that
	// would re-close the already-closed DB and fail the test.
	s, err := NewSqlStore(InmemPath, zap.NewNop())
	require.NoError(t, err)
	require.NoError(t, s.Close())

	resp := s.Check(context.Background())
	require.Equal(t, check.StatusFail, resp.Status)
	require.NotEmpty(t, resp.Message)
}

func TestSqlStore_CheckCompletesPromptlyOnCanceledContext(t *testing.T) {
	s := NewTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Wait well above check.DefaultProbeTimeout so a single scheduling
	// hiccup does not flake the test while still catching a real hang.
	const deadline = 4 * check.DefaultProbeTimeout
	done := make(chan struct{})
	go func() {
		s.Check(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(deadline):
		t.Fatalf("Check did not complete within %s on canceled context", deadline)
	}
}
