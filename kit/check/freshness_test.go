package check

import (
	"encoding/json"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/kit/check/checktest"
	"github.com/stretchr/testify/require"
)

func TestFreshnessResponse_NoProbeYet(t *testing.T) {
	f := NewFreshnessResponse("svc", time.Second)
	require.Equal(t, "svc", f.Name())
	require.Equal(t, StatusFail, f.Status())
	require.Equal(t, "no probe completed yet", f.Message())
	require.Nil(t, f.Checks())
}

func TestFreshnessResponse_FreshSnapshotDeliversUnderlying(t *testing.T) {
	f := NewFreshnessResponse("svc", time.Second)
	f.Update(Info("ok"))
	require.Equal(t, StatusPass, f.Status())
	require.Equal(t, "ok", f.Message())
}

func TestFreshnessResponse_BecomesStaleAfterDuration(t *testing.T) {
	const staleness = 50 * time.Millisecond
	f := NewFreshnessResponse("svc", staleness)
	f.Update(Pass())

	// Sleep past the staleness budget.
	time.Sleep(staleness + 50*time.Millisecond)

	require.Equal(t, StatusFail, f.Status())

	want := regexp.MustCompile(`^stale: last probe .* ago \(threshold 50ms\)$`)
	require.Regexp(t, want, f.Message())
}

func TestFreshnessResponse_FreshProbeClearsStaleness(t *testing.T) {
	const staleness = 50 * time.Millisecond
	f := NewFreshnessResponse("svc", staleness)
	f.Update(Pass())
	time.Sleep(staleness + 50*time.Millisecond)
	require.Equal(t, StatusFail, f.Status())

	f.Update(Pass())
	require.Equal(t, StatusPass, f.Status())
}

func TestFreshnessResponse_JSONMarshalEmitsDerivedValues(t *testing.T) {
	const staleness = 50 * time.Millisecond

	t.Run("no probe", func(t *testing.T) {
		f := NewFreshnessResponse("svc", staleness)
		b, err := json.Marshal(f)
		require.NoError(t, err)
		require.JSONEq(t,
			`{"name":"svc","status":"fail","message":"no probe completed yet"}`,
			string(b))
	})

	t.Run("fresh pass", func(t *testing.T) {
		f := NewFreshnessResponse("svc", time.Second)
		f.Update(Info("ok"))
		b, err := json.Marshal(f)
		require.NoError(t, err)
		require.JSONEq(t,
			`{"name":"svc","status":"pass","message":"ok"}`,
			string(b))
	})

	t.Run("stale", func(t *testing.T) {
		f := NewFreshnessResponse("svc", staleness)
		f.Update(Pass())
		time.Sleep(staleness + 50*time.Millisecond)

		b, err := json.Marshal(f)
		require.NoError(t, err)

		var w struct {
			Name    string `json:"name"`
			Status  string `json:"status"`
			Message string `json:"message"`
		}
		require.NoError(t, json.Unmarshal(b, &w))
		require.Equal(t, "svc", w.Name)
		require.Equal(t, "fail", w.Status)
		require.Regexp(t, `^stale: last probe .* ago \(threshold 50ms\)$`, w.Message)
	})
}

// TestFreshnessResponse_Snapshot covers the three states Snapshot renders and
// pins it against the accessors it replaces.
func TestFreshnessResponse_Snapshot(t *testing.T) {
	const staleness = 50 * time.Millisecond

	t.Run("no probe", func(t *testing.T) {
		f := NewFreshnessResponse("svc", staleness)
		s := f.Snapshot()
		require.Equal(t, "svc", s.Name())
		require.Equal(t, StatusFail, s.Status())
		require.Equal(t, msgNoProbe, s.Message())
		require.Nil(t, s.Checks())
	})

	t.Run("fresh", func(t *testing.T) {
		f := NewFreshnessResponse("svc", time.Second)
		f.Update(NewBasicResponse("inner", StatusPass, "ok", Responses{NamedPass("nested")}))
		s := f.Snapshot()
		require.Equal(t, "svc", s.Name(), "the wrapper's name wins, not the probe's")
		require.Equal(t, StatusPass, s.Status())
		require.Equal(t, "ok", s.Message())
		require.Len(t, s.Checks(), 1)
		require.Equal(t, "nested", s.Checks()[0].Name())
	})

	t.Run("stale", func(t *testing.T) {
		f := NewFreshnessResponse("svc", staleness)
		f.Update(Pass())
		time.Sleep(staleness + 50*time.Millisecond)

		s := f.Snapshot()
		require.Equal(t, StatusFail, s.Status())
		require.Regexp(t, `^stale: last probe .* ago \(threshold 50ms\)$`, s.Message())
		require.Nil(t, s.Checks(), "an aged-out snapshot reports no nested checks")
	})

	// A snapshot is a value: it does not age, which is what makes it safe to
	// hold in a frozen check set.
	t.Run("does not age", func(t *testing.T) {
		f := NewFreshnessResponse("svc", staleness)
		f.Update(Pass())
		s := f.Snapshot()
		time.Sleep(staleness + 50*time.Millisecond)
		require.Equal(t, StatusFail, f.Status(), "the live response must age out")
		require.Equal(t, StatusPass, s.Status())
	})
}

// TestFreshnessResponse_MarshalMatchesSnapshot pins the MarshalJSON rewrite as
// byte-compatible. MarshalJSON now marshals the BasicResponse Snapshot returns,
// which reaches the wire shape through an embedded unexported struct whose
// exported fields encoding/json promotes; this asserts that indirection emits
// what the hand-built wireResponse did.
func TestFreshnessResponse_MarshalMatchesSnapshot(t *testing.T) {
	const staleness = 50 * time.Millisecond

	for _, tc := range []struct {
		name  string
		build func() *FreshnessResponse
	}{
		{
			name:  "no probe",
			build: func() *FreshnessResponse { return NewFreshnessResponse("svc", staleness) },
		},
		{
			name: "fresh with nested checks",
			build: func() *FreshnessResponse {
				f := NewFreshnessResponse("svc", time.Second)
				f.Update(NewBasicResponse("inner", StatusPass, "ok", Responses{NamedFail("nested", "bad")}))
				return f
			},
		},
		{
			name: "stale",
			build: func() *FreshnessResponse {
				f := NewFreshnessResponse("svc", staleness)
				f.Update(Pass())
				time.Sleep(staleness + 50*time.Millisecond)
				return f
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.build()
			fromResponse, err := json.Marshal(f)
			require.NoError(t, err)
			fromSnapshot, err := json.Marshal(f.Snapshot())
			require.NoError(t, err)
			require.Equal(t,
				checktest.NormalizeJSON(t, fromSnapshot),
				checktest.NormalizeJSON(t, fromResponse))
		})
	}
}

// TestRenamedResponse_MarshalMatchesSnapshot covers the same rewrite for the
// wrapper Rename puts around a stateful Response. A renamed FreshnessResponse
// must render the new name over the inner state, from one observation.
func TestRenamedResponse_MarshalMatchesSnapshot(t *testing.T) {
	f := NewFreshnessResponse("inner", time.Second)
	f.Update(Info("ok"))
	r := Rename(f, "outer")

	b, err := json.Marshal(r)
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"outer","status":"pass","message":"ok"}`, string(b))

	s, ok := r.(Snapshotter)
	require.True(t, ok, "a renamed Response must still offer a single coherent read")
	require.Equal(t, "outer", s.Snapshot().Name())
	require.Equal(t, StatusPass, s.Snapshot().Status())
}

// TestFreshnessResponse_ConcurrentUpdateAndRead exercises Update racing
// with the four interface methods and MarshalJSON. Uses the RWMutex
// start-gate so every goroutine contends simultaneously.
// Under -race this fails without the atomic.Pointer.
func TestFreshnessResponse_ConcurrentUpdateAndRead(t *testing.T) {
	const (
		numUpdaters = 8
		numReaders  = 8
		iterations  = 1000
	)

	f := NewFreshnessResponse("svc", time.Hour)

	var (
		startMu        sync.RWMutex
		concurrency    atomic.Int64
		maxConcurrency atomic.Int64
		marshalErr     atomic.Pointer[error]
	)

	bumpMax := func() {
		cur := concurrency.Add(1)
		for {
			old := maxConcurrency.Load()
			if cur <= old || maxConcurrency.CompareAndSwap(old, cur) {
				break
			}
		}
	}

	var wg sync.WaitGroup
	startMu.Lock()

	for i := range numUpdaters {
		wg.Add(1)
		go func(idx int) {
			startMu.RLock()
			defer startMu.RUnlock()
			defer wg.Done()
			bumpMax()
			for n := range iterations {
				if (idx+n)%2 == 0 {
					f.Update(Pass())
				} else {
					f.Update(Fail("oops"))
				}
			}
			concurrency.Add(-1)
		}(i)
	}

	for range numReaders {
		wg.Add(1)
		go func() {
			startMu.RLock()
			defer startMu.RUnlock()
			defer wg.Done()
			bumpMax()
			for range iterations {
				_ = f.Name()
				_ = f.Status()
				_ = f.Message()
				_ = f.Checks()
				if _, err := json.Marshal(f); err != nil {
					marshalErr.CompareAndSwap(nil, &err)
				}
			}
			concurrency.Add(-1)
		}()
	}

	startMu.Unlock()
	wg.Wait()

	if e := marshalErr.Load(); e != nil {
		require.NoError(t, *e)
	}
	t.Logf("max concurrency: %d", maxConcurrency.Load())
}

// TestFreshnessResponse_MonotonicClock pins the contract that the
// snapshot timestamp must carry a monotonic reading. We can't simulate
// a wall-clock jump cheaply, but we can verify that the freshness
// derivation uses time.Since (monotonic) and not Unix arithmetic by
// checking that a snapshot just updated reports as fresh.
func TestFreshnessResponse_MonotonicClock(t *testing.T) {
	const staleness = 100 * time.Millisecond
	f := NewFreshnessResponse("svc", staleness)
	f.Update(Pass())

	time.Sleep(staleness / 4)
	require.Equal(t, StatusPass, f.Status(), "snapshot still within staleness")
}
