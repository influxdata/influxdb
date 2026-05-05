package bolt

import (
	"context"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// newProbeTestStore mirrors bolt_test.NewTestKVStore but stays in the bolt
// package so a test can reach unexported probe state.
func newProbeTestStore(t *testing.T) *KVStore {
	f, err := os.CreateTemp(t.TempDir(), "bolt-probe-")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	s := NewKVStore(zaptest.NewLogger(t), f.Name(), WithNoSync)
	require.NoError(t, s.Open(context.Background()))
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

// TestKVStore_StartProberPopulatesCache verifies that Open's invocation
// of startProberOnce runs a synchronous initial probe — probeState must
// be non-nil and reporting Pass by the time Open returns, so the first
// /health hit doesn't see "probe pending".
func TestKVStore_StartProberPopulatesCache(t *testing.T) {
	s := newProbeTestStore(t)

	got := s.probeState.Load()
	require.NotNil(t, got)
	require.Equal(t, check.StatusPass, got.Status)
}

// TestKVStore_CheckReturnsCachedState verifies that Check is a pure read
// of probeState — overwriting the cache directly is reflected on the
// next Check, with no probe in between.
func TestKVStore_CheckReturnsCachedState(t *testing.T) {
	s := newProbeTestStore(t)

	stub := check.Response{Status: check.StatusFail, Message: "stubbed"}
	s.probeState.Store(&stub)

	got := s.Check(context.Background())
	require.Equal(t, stub, got)
}
