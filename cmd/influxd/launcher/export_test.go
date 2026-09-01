package launcher

import (
	"context"
	"testing"
	"time"
)

// Test-only exports for the external launcher_test package. Both packages
// compile into the same test binary, so an identifier defined here is visible
// there while staying out of the production build. Nothing in this file may be
// referenced by non-test code.

// HoldForStartupError exposes holdForStartupError, the wait a failed startup
// takes before the process exits. It blocks for up to d; a test that needs it
// to end sooner calls CancelRun, which is what a SIGINT does in production.
func (tl *TestLauncher) HoldForStartupError(ctx context.Context, d time.Duration) {
	tl.Launcher.holdForStartupError(ctx, d)
}

// CancelRun cancels the context run was given, closing Done and so ending any
// hold in progress. Safe only after Run has been called: run is what installs
// the cancel func.
func (tl *TestLauncher) CancelRun() {
	tl.Launcher.cancel()
}

// RequireReturnsWithin exposes requireReturnsWithin, so the internal and
// external test packages share one bounded-wait helper rather than a copy
// each. See there for why a blocking call must never be made directly.
func RequireReturnsWithin(t *testing.T, d time.Duration, fn func()) {
	t.Helper()
	requireReturnsWithin(t, d, fn)
}
