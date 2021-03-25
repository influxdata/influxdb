// Package builtin ensures all packages related to Flux built-ins are imported and initialized.
// This should only be imported from main or test packages.
// It is a mistake to import it from any other package.
package builtin

import (
	"sync"

	"github.com/influxdata/flux/runtime"
	_ "github.com/influxdata/flux/stdlib"
	_ "github.com/influxdata/influxdb/flux/stdlib"
)

var once sync.Once

// Initialize ensures all Flux builtins are configured and should be called
// prior to using the Flux runtime. Initialize is safe to call concurrently
// and is idempotent.
func Initialize() {
	once.Do(func() {
		// NOTE: We don't use runtime.FinalizeBuiltins() here because the flux test harness
		// also calls that method, and calling it twice results in a panic.
		//
		// Intercepting the double-call error here is messy, but has the least impact to
		// existing code.
		if err := runtime.Default.Finalize(); err == nil || err.Error() == "already finalized" {
			return
		} else {
			panic(err)
		}
	})
}
