package bolt

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/stretchr/testify/require"
	bbolt "go.etcd.io/bbolt"
	"go.uber.org/zap/zaptest"
)

// newProbeTestStore mirrors bolt_test.NewTestKVStore but stays in the bolt
// package so a test can reach unexported probe state.
func newProbeTestStore(t *testing.T, opts ...KVOption) *KVStore {
	f, err := os.CreateTemp(t.TempDir(), "bolt-probe-")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	opts = append([]KVOption{WithNoSync}, opts...)
	s := NewKVStore(zaptest.NewLogger(t), f.Name(), opts...)
	require.NoError(t, s.Open(context.Background()))
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

// TestKVStore_StartProberPopulatesCache verifies that Open's invocation
// of startProberOnce runs a synchronous probe before returning, so
// Check reports StatusPass by the time Open is done. The first /health
// hit must not see "no probe yet".
func TestKVStore_StartProberPopulatesCache(t *testing.T) {
	s := newProbeTestStore(t)

	got := s.Check(context.Background())
	require.Equal(t, check.StatusPass, got.Status())
}

// TestKVStore_CheckReturnsCachedState verifies that Check is a pure read
// of probeState — pushing a stub via Update is reflected on the next
// Check, with no probe in between.
func TestKVStore_CheckReturnsCachedState(t *testing.T) {
	s := newProbeTestStore(t)

	s.probeState.Update(check.Fail("stubbed"))
	got := s.Check(context.Background())
	require.Equal(t, check.StatusFail, got.Status())
	require.Equal(t, "stubbed", got.Message())
}

// TestKVStore_StopProberLeavesDBOpen exercises the launcher's pattern:
// the *bolt.DB is owned externally (bolt.Client) and the KVStore gets
// it via WithDB. StopProber must signal the prober to exit without
// touching the DB so the owner can close it. Regression test for a
// goroutine leak introduced by the launcher closing the bolt.Client
// directly without first stopping the KVStore prober.
func TestKVStore_StopProberLeavesDBOpen(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "bolt-stopprober-")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	db, err := bbolt.Open(f.Name(), 0600, &bbolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	// Use a short staleness so the freshness wrapper can age past it
	// well within the test's Eventually deadline.
	const staleness = 50 * time.Millisecond
	s := NewKVStore(zaptest.NewLogger(t), f.Name(), WithStaleness(staleness))
	s.WithDB(db)

	s.StopProber()

	// After StopProber the prober stops calling Update; the wrapper
	// ages past its staleness budget and Check flips to fail.
	require.Eventually(t, func() bool {
		return s.Check(context.Background()).Status() == check.StatusFail
	}, time.Second, 10*time.Millisecond)

	select {
	case <-s.probeStop:
	default:
		t.Fatal("probeStop should be closed after StopProber")
	}

	// DB is still usable — StopProber must not close it.
	require.NoError(t, db.View(func(*bbolt.Tx) error { return nil }))

	require.NotPanics(t, func() { s.StopProber() }, "StopProber must be idempotent")
}
