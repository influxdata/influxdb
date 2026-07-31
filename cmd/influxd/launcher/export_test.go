package launcher

import (
	"context"
	"time"
)

// HoldForStartupError exposes holdForStartupError to the external
// launcher_test package, which exercises the teardown split around the hold
// against a fully constructed launcher. cmdRunE itself is not directly
// testable in-process: it calls fluxinit.FluxInit, which panics on a second
// call, and this package already finalizes the Flux runtime via the
// fluxinit/static import in launcher_test.go.
func (m *Launcher) HoldForStartupError(ctx context.Context, d time.Duration) {
	m.holdForStartupError(ctx, d)
}
