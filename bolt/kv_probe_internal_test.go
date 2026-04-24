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
// package so a test can reach the unexported probeMu field.
func newProbeTestStore(t *testing.T) *KVStore {
	f, err := os.CreateTemp(t.TempDir(), "bolt-probe-")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	s := NewKVStore(zaptest.NewLogger(t), f.Name(), WithNoSync)
	require.NoError(t, s.Open(context.Background()))
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

// TestKVStore_CheckReportsInFlight verifies that when a prior probe
// goroutine is stranded (simulated by holding probeMu), a subsequent
// Check returns ProbeInFlightMsg immediately instead of spawning a
// second goroutine.
func TestKVStore_CheckReportsInFlight(t *testing.T) {
	s := newProbeTestStore(t)

	// Sanity-check the happy path first — confirms the guard doesn't
	// break normal probes.
	require.Equal(t, check.StatusPass, s.Check(context.Background()).Status)

	s.probeMu.Lock()
	defer s.probeMu.Unlock()

	resp := s.Check(context.Background())
	require.Equal(t, check.StatusFail, resp.Status)
	require.Equal(t, ProbeInFlightMsg, resp.Message)
}
