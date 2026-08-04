package check

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadyGate_CheckName(t *testing.T) {
	g := NewReadyGate("engine")
	require.Equal(t, "engine", g.CheckName())
}

func TestReadyGate_Check(t *testing.T) {
	g := NewReadyGate("engine")

	// Initially fails with "not ready".
	resp := g.Check(context.Background())
	require.Equal(t, StatusFail, resp.Status())
	require.Equal(t, "not ready", resp.Message())

	// After Ready() it passes.
	g.Ready()
	resp = g.Check(context.Background())
	require.Equal(t, StatusPass, resp.Status())
	assert.Empty(t, resp.Message())
}

func TestReadyGate_UnreadyRoundTrip(t *testing.T) {
	g := NewReadyGate("engine")

	g.Ready()
	resp := g.Check(context.Background())
	require.Equal(t, StatusPass, resp.Status())

	g.Unready()
	resp = g.Check(context.Background())
	require.Equal(t, StatusFail, resp.Status())
	require.Equal(t, "not ready", resp.Message())
	require.Equal(t, "engine", resp.Name())
}

// nilErr is a typed nil stored in an error interface: err == nil is false,
// but calling Error() dereferences nothing and panics. ReadyGate.Fail is
// reached from startup paths that may hand it a wrapped error built from a
// concrete type, and it is read from an HTTP handler, so this case must be
// ignored rather than latched.
type nilErr struct{ msg string }

func (e *nilErr) Error() string { return e.msg }

func TestReadyGate_Fail(t *testing.T) {
	const failMsg = "engine open failed: no such file"

	t.Run("fail before ready outranks ready", func(t *testing.T) {
		g := NewReadyGate("engine")
		g.Fail(errors.New(failMsg))

		resp := g.Check(context.Background())
		require.Equal(t, StatusFail, resp.Status())
		require.Equal(t, failMsg, resp.Message())
		require.Equal(t, "engine", resp.Name())

		// Fail is terminal: Ready cannot clear it.
		g.Ready()
		resp = g.Check(context.Background())
		require.Equal(t, StatusFail, resp.Status())
		require.Equal(t, failMsg, resp.Message())
	})

	t.Run("fail after ready outranks ready", func(t *testing.T) {
		g := NewReadyGate("engine")
		g.Ready()
		require.Equal(t, StatusPass, g.Check(context.Background()).Status())

		g.Fail(errors.New(failMsg))
		resp := g.Check(context.Background())
		require.Equal(t, StatusFail, resp.Status())
		require.Equal(t, failMsg, resp.Message())
	})

	t.Run("fail after unready keeps the reason", func(t *testing.T) {
		g := NewReadyGate("engine")
		g.Ready()
		g.Unready()
		g.Fail(errors.New(failMsg))

		resp := g.Check(context.Background())
		require.Equal(t, StatusFail, resp.Status())
		require.Equal(t, failMsg, resp.Message())
	})

	t.Run("unready after fail keeps the reason", func(t *testing.T) {
		g := NewReadyGate("engine")
		g.Fail(errors.New(failMsg))
		g.Unready()

		require.Equal(t, failMsg, g.Check(context.Background()).Message())
	})

	t.Run("first error wins", func(t *testing.T) {
		g := NewReadyGate("engine")
		g.Fail(errors.New(failMsg))
		g.Fail(errors.New("a later, less useful error"))

		require.Equal(t, failMsg, g.Check(context.Background()).Message())
	})

	t.Run("nil error ignored", func(t *testing.T) {
		g := NewReadyGate("engine")
		g.Fail(nil)

		resp := g.Check(context.Background())
		require.Equal(t, StatusFail, resp.Status())
		require.Equal(t, MsgNotReady, resp.Message(), "a nil Fail must leave the gate untouched")

		g.Ready()
		require.Equal(t, StatusPass, g.Check(context.Background()).Status())
	})

	t.Run("typed nil in interface ignored", func(t *testing.T) {
		var typedNil *nilErr
		var err error = typedNil
		require.Panics(t, func() { _ = err.Error() },
			"test setup: Error() on this value must panic, or the case is not the one under test")

		g := NewReadyGate("engine")
		require.NotPanics(t, func() { g.Fail(err) })
		require.Equal(t, MsgNotReady, g.Check(context.Background()).Message())
	})

	t.Run("non-nil typed pointer latches", func(t *testing.T) {
		g := NewReadyGate("engine")
		g.Fail(&nilErr{msg: failMsg})

		require.Equal(t, failMsg, g.Check(context.Background()).Message())
	})
}

// TestReadyGate_Fail_Concurrent holds every goroutine at a barrier so Fail,
// Ready, Unready and Check race against each other as hard as the scheduler
// allows. Run under -race. Exactly one Fail message must win, and Check must
// never observe a torn or empty one.
//
// A barrier rather than a start gate, because the assertion on the overlap has
// to be sound. A gate releases the goroutines together but does not keep them
// alive together: nothing in this body blocks, so at GOMAXPROCS=1 each one runs
// to completion before the next is scheduled, max concurrency is 1, and the
// assertion fails on a gate that is working exactly as intended. With the
// barrier no goroutine proceeds until all of them have arrived, so the overlap
// is structural and the count is exact.
func TestReadyGate_Fail_Concurrent(t *testing.T) {
	const (
		goroutines = 64
		failers    = goroutines / 2
		readiers   = goroutines / 4
	)

	g := NewReadyGate("engine")

	// Build the errors up front so no allocation happens inside the window.
	errMsgs := make([]string, failers)
	errs := make([]error, failers)
	for i := range errs {
		errMsgs[i] = fmt.Sprintf("failure from goroutine %d", i)
		errs[i] = errors.New(errMsgs[i])
	}

	var (
		concurrency    atomic.Int64
		maxConcurrency atomic.Int64
		observed       sync.Map // failing message -> struct{}
		wg             sync.WaitGroup
		arrived        sync.WaitGroup
	)

	arrived.Add(goroutines)
	proceed := make(chan struct{})
	go func() {
		arrived.Wait()
		close(proceed)
	}()

	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			c := concurrency.Add(1)
			for {
				old := maxConcurrency.Load()
				if c <= old || maxConcurrency.CompareAndSwap(old, c) {
					break
				}
			}

			// Counted in above, so no goroutine can decrement below until every
			// one of them has incremented -- which is what makes the high-water
			// mark exactly `goroutines` rather than whatever the scheduler felt
			// like overlapping.
			arrived.Done()
			<-proceed

			switch {
			case idx < failers:
				g.Fail(errs[idx])
			case idx < failers+readiers:
				g.Ready()
			default:
				g.Unready()
			}
			if resp := g.Check(context.Background()); resp.Status() == StatusFail {
				observed.Store(resp.Message(), struct{}{})
			}

			concurrency.Add(-1)
		}(i)
	}
	wg.Wait()

	require.Equal(t, int64(goroutines), maxConcurrency.Load(),
		"every goroutine must be live at once, or this test proves nothing about concurrent access")

	final := g.Check(context.Background())
	require.Equal(t, StatusFail, final.Status(), "a latched gate can never report pass")
	require.Contains(t, errMsgs, final.Message(),
		"the winning message must be one of the errors actually passed to Fail")

	// Every failing message seen along the way was either the pre-latch
	// MsgNotReady or the single winner: no goroutine can observe a failure
	// message that later changes.
	observed.Range(func(k, _ any) bool {
		if msg := k.(string); msg != MsgNotReady {
			require.Equalf(t, final.Message(), msg,
				"observed failure message %q differs from the latched one", msg)
		}
		return true
	})
}

func TestBoundDeadline(t *testing.T) {
	t.Run("no deadline on parent", func(t *testing.T) {
		const max = 50 * time.Millisecond
		before := time.Now()
		ctx, cancel := BoundDeadline(context.Background(), max)
		defer cancel()

		dl, ok := ctx.Deadline()
		require.True(t, ok, "expected a deadline on returned ctx")
		// Deadline should be ~now+max. Allow a generous tolerance so the
		// test is robust on slow CI runners.
		require.WithinDuration(t, before.Add(max), dl, 50*time.Millisecond)
	})

	t.Run("parent deadline inside max returns parent unchanged", func(t *testing.T) {
		parent, parentCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer parentCancel()

		ctx, cancel := BoundDeadline(parent, 1*time.Second)
		// BoundDeadline returns the same ctx when the parent deadline is
		// already inside max.
		require.Equal(t, parent, ctx)

		// The returned cancel should be a no-op: calling it must not
		// cancel the parent.
		cancel()
		require.NoError(t, parent.Err())
	})

	t.Run("parent deadline outside max produces a tighter child", func(t *testing.T) {
		parent, parentCancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer parentCancel()

		parentDL, _ := parent.Deadline()
		ctx, cancel := BoundDeadline(parent, 20*time.Millisecond)
		defer cancel()

		childDL, ok := ctx.Deadline()
		require.True(t, ok)
		require.True(t, childDL.Before(parentDL), "child deadline should be earlier than parent")

		select {
		case <-ctx.Done():
		case <-time.After(500 * time.Millisecond):
			t.Fatal("child ctx did not fire within 500ms")
		}
		require.NoError(t, parent.Err(), "parent should not be cancelled by child timeout")
	})
}

func TestPass(t *testing.T) {
	resp := Pass()
	require.Equal(t, StatusPass, resp.Status())
	assert.Empty(t, resp.Name())
	assert.Empty(t, resp.Message())
}

func TestInfo(t *testing.T) {
	resp := Info("hello %s", "world")
	require.Equal(t, StatusPass, resp.Status())
	require.Equal(t, "hello world", resp.Message())
}

func TestError(t *testing.T) {
	resp := Error(errors.New("boom"))
	require.Equal(t, StatusFail, resp.Status())
	require.Equal(t, "boom", resp.Message())
}

func TestReadyGate_IntegrationWithCheck_NamedWrapping(t *testing.T) {
	// Verify that when a *ReadyGate is added to *Check, the resulting
	// response uses the gate's configured name.
	const gateName = "metastores"
	c := NewCheck()
	gate := NewReadyGate(gateName)
	c.AddNamedReadyCheck(gate)

	resp := c.CheckReady(context.Background())
	require.Equal(t, StatusFail, resp.Status())
	require.Len(t, resp.Checks(), 1)
	require.Equal(t, gateName, resp.Checks()[0].Name())

	gate.Ready()
	resp = c.CheckReady(context.Background())
	require.Equal(t, StatusPass, resp.Status())
	require.Len(t, resp.Checks(), 1)
	require.Equal(t, gateName, resp.Checks()[0].Name())
}
