package check

import (
	"context"
	"errors"
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
	require.Equal(t, StatusFail, resp.Status)
	require.Equal(t, "not ready", resp.Message)

	// After Ready() it passes.
	g.Ready()
	resp = g.Check(context.Background())
	require.Equal(t, StatusPass, resp.Status)
	assert.Empty(t, resp.Message)
}

func TestReadyGate_UnreadyRoundTrip(t *testing.T) {
	g := NewReadyGate("engine")

	g.Ready()
	resp := g.Check(context.Background())
	require.Equal(t, StatusPass, resp.Status)

	g.Unready()
	resp = g.Check(context.Background())
	require.Equal(t, StatusFail, resp.Status)
	require.Equal(t, "not ready", resp.Message)
	require.Equal(t, "engine", resp.Name)
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
	require.Equal(t, StatusPass, resp.Status)
	assert.Empty(t, resp.Name)
	assert.Empty(t, resp.Message)
}

func TestInfo(t *testing.T) {
	resp := Info("hello %s", "world")
	require.Equal(t, StatusPass, resp.Status)
	require.Equal(t, "hello world", resp.Message)
}

func TestError(t *testing.T) {
	resp := Error(errors.New("boom"))
	require.Equal(t, StatusFail, resp.Status)
	require.Equal(t, "boom", resp.Message)
}

func TestReadyGate_IntegrationWithCheck_NamedWrapping(t *testing.T) {
	// Verify that when a *ReadyGate is added to *Check, the resulting
	// response uses the gate's configured name.
	c := NewCheck()
	gate := NewReadyGate("metastores")
	c.AddReadyCheck(gate)

	resp := c.CheckReady(context.Background())
	require.Equal(t, StatusFail, resp.Status)
	require.Len(t, resp.Checks, 1)
	require.Equal(t, "metastores", resp.Checks[0].Name)

	gate.Ready()
	resp = c.CheckReady(context.Background())
	require.Equal(t, StatusPass, resp.Status)
	require.Len(t, resp.Checks, 1)
	require.Equal(t, "metastores", resp.Checks[0].Name)
}
