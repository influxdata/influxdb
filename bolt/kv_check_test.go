package bolt_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/stretchr/testify/require"
)

func TestKVStore_CheckPasses(t *testing.T) {
	s, closeFn, err := NewTestKVStore(t)
	require.NoError(t, err)
	defer closeFn()

	resp := s.Check(context.Background())
	require.Equal(t, check.StatusPass, resp.Status())
}

func TestKVStore_CheckFailsAfterClose(t *testing.T) {
	// Short staleness so the prober's silence after Close ages out
	// quickly enough to satisfy the test's Eventually deadline.
	const staleness = 50 * time.Millisecond
	s, closeFn, err := NewTestKVStore(t, bolt.WithStaleness(staleness))
	require.NoError(t, err)
	require.NoError(t, s.Close())
	defer closeFn()

	// Close stops the prober from calling Update on the freshness
	// wrapper; the wrapper's last snapshot ages past `staleness` and
	// Check flips to fail with a "stale" message.
	require.Eventually(t, func() bool {
		resp := s.Check(context.Background())
		return resp.Status() == check.StatusFail && resp.Message() != ""
	}, time.Second, 10*time.Millisecond)
}

func TestKVStore_CheckCompletesPromptlyOnCanceledContext(t *testing.T) {
	s, closeFn, err := NewTestKVStore(t)
	require.NoError(t, err)
	defer closeFn()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Check is a non-blocking cached read; a canceled context must not
	// cause it to hang. The deadline is generous so a scheduling hiccup
	// does not flake the test.
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
