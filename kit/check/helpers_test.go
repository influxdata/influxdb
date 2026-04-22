package check

import (
	"context"
	"testing"

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
