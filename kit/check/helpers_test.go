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

// Fixtures shared by the ReadyGate tests. Declared once so a typo cannot make
// an assertion agree with a value the gate never produced.
const (
	gateName = "engine"

	// latchedMsg is the terminal error latched by Fail; firstMsg and secondMsg
	// exercise first-error-wins.
	latchedMsg = "boom"
	firstMsg   = "root cause"
	secondMsg  = "later generic error"
)

func TestReadyGate_CheckName(t *testing.T) {
	g := NewReadyGate(gateName)
	require.Equal(t, gateName, g.CheckName())
}

func TestReadyGate_Check(t *testing.T) {
	g := NewReadyGate(gateName)

	// Initially fails with "not ready".
	resp := g.Check(context.Background())
	require.Equal(t, StatusFail, resp.Status())
	require.Equal(t, MsgNotReady, resp.Message())

	// After Ready() it passes.
	g.Ready()
	resp = g.Check(context.Background())
	require.Equal(t, StatusPass, resp.Status())
	assert.Empty(t, resp.Message())
}

func TestReadyGate_UnreadyRoundTrip(t *testing.T) {
	g := NewReadyGate(gateName)

	g.Ready()
	resp := g.Check(context.Background())
	require.Equal(t, StatusPass, resp.Status())

	g.Unready()
	resp = g.Check(context.Background())
	require.Equal(t, StatusFail, resp.Status())
	require.Equal(t, MsgNotReady, resp.Message())
	require.Equal(t, gateName, resp.Name())
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
	const metastoresGate = "metastores"
	c := NewCheck()
	gate := NewReadyGate(metastoresGate)
	c.AddNamedReadyCheck(gate)

	resp := c.CheckReady(context.Background())
	require.Equal(t, StatusFail, resp.Status())
	require.Len(t, resp.Checks(), 1)
	require.Equal(t, metastoresGate, resp.Checks()[0].Name())

	gate.Ready()
	resp = c.CheckReady(context.Background())
	require.Equal(t, StatusPass, resp.Status())
	require.Len(t, resp.Checks(), 1)
	require.Equal(t, metastoresGate, resp.Checks()[0].Name())

	// A latched failure keeps the gate's name and replaces the message.
	gate.Fail(errors.New(latchedMsg))
	resp = c.CheckReady(context.Background())
	require.Equal(t, StatusFail, resp.Status())
	require.Len(t, resp.Checks(), 1)
	require.Equal(t, metastoresGate, resp.Checks()[0].Name())
	require.Equal(t, latchedMsg, resp.Checks()[0].Message())
}

func TestReadyGate_Fail(t *testing.T) {
	// Every ordering of Ready/Unready around Fail must land on the same
	// terminal response: a failed gate never reports ready again, which is
	// what makes Fail startup-only.
	orderings := []struct {
		name string
		ops  func(*ReadyGate)
	}{
		{"beats unready", func(g *ReadyGate) { g.Fail(errors.New(latchedMsg)) }},
		{"beats ready", func(g *ReadyGate) { g.Ready(); g.Fail(errors.New(latchedMsg)) }},
		{"terminal: Ready cannot clear it", func(g *ReadyGate) { g.Fail(errors.New(latchedMsg)); g.Ready() }},
		{"terminal: Unready keeps the message", func(g *ReadyGate) { g.Fail(errors.New(latchedMsg)); g.Unready() }},
	}
	for _, tt := range orderings {
		t.Run(tt.name, func(t *testing.T) {
			g := NewReadyGate(gateName)
			tt.ops(g)

			resp := g.Check(context.Background())
			require.Equal(t, StatusFail, resp.Status())
			require.Equal(t, latchedMsg, resp.Message(),
				"Ready/Unready must not replace the cause with the default message")
			require.Equal(t, gateName, resp.Name())
		})
	}

	t.Run("first error wins", func(t *testing.T) {
		g := NewReadyGate(gateName)
		g.Fail(errors.New(firstMsg))
		g.Fail(errors.New(secondMsg))

		resp := g.Check(context.Background())
		require.Equal(t, firstMsg, resp.Message())
	})

	t.Run("nil is ignored", func(t *testing.T) {
		g := NewReadyGate(gateName)
		g.Ready()
		g.Fail(nil)

		resp := g.Check(context.Background())
		require.Equal(t, StatusPass, resp.Status())
	})
}

// TestReadyGate_Fail_Concurrent hammers Fail from many goroutines against
// concurrent Check calls and asserts exactly one message survives. Uses the
// RWMutex-synchronized start so every goroutine is released at once, which
// maximizes contention on the CompareAndSwap.
func TestReadyGate_Fail_Concurrent(t *testing.T) {
	const goroutines = 64

	g := NewReadyGate(gateName)

	// Build the errors up front so goroutines do no allocation before racing.
	errs := make([]error, goroutines)
	for i := range errs {
		errs[i] = fmt.Errorf("failure %d", i)
	}

	var mu sync.RWMutex
	var concurrency, maxConcurrency atomic.Int64

	var wg sync.WaitGroup
	mu.Lock()
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			mu.RLock()
			defer mu.RUnlock()
			defer wg.Done()

			c := concurrency.Add(1)
			if old := maxConcurrency.Load(); c > old {
				maxConcurrency.CompareAndSwap(old, c)
			}

			g.Fail(errs[idx])
			// Read concurrently with the writes; the race detector covers
			// the latchedMsg pointer, and the response must always be coherent.
			resp := g.Check(context.Background())
			assert.Equal(t, StatusFail, resp.Status())
			assert.NotEmpty(t, resp.Message())

			concurrency.Add(-1)
		}(i)
	}
	mu.Unlock() // Release to start all goroutines simultaneously.
	wg.Wait()
	t.Logf("max concurrency: %d", maxConcurrency.Load())

	// Exactly one of the messages survived, and it never changes afterwards.
	final := g.Check(context.Background()).Message()
	require.Regexp(t, `^failure \d+$`, final)
	require.Equal(t, final, g.Check(context.Background()).Message())
}
