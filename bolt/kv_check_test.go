package bolt_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/stretchr/testify/require"
)

func TestKVStore_CheckName(t *testing.T) {
	s, closeFn, err := NewTestKVStore(t)
	require.NoError(t, err)
	defer closeFn()

	require.Equal(t, bolt.HealthCheckName, s.CheckName())
}

func TestKVStore_CheckPasses(t *testing.T) {
	s, closeFn, err := NewTestKVStore(t)
	require.NoError(t, err)
	defer closeFn()

	resp := s.Check(context.Background())
	require.Equal(t, check.StatusPass, resp.Status)
}

func TestKVStore_CheckFailsAfterClose(t *testing.T) {
	s, closeFn, err := NewTestKVStore(t)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	defer closeFn()

	resp := s.Check(context.Background())
	require.Equal(t, check.StatusFail, resp.Status)
	require.NotEmpty(t, resp.Message)
}

func TestKVStore_CheckCompletesPromptlyOnCanceledContext(t *testing.T) {
	s, closeFn, err := NewTestKVStore(t)
	require.NoError(t, err)
	defer closeFn()

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
