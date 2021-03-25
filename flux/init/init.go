// Package init ensures all packages related to Flux built-ins are imported and initialized.
// This should only be imported from main or test packages.
// It is a mistake to import it from any other package.
//
// NOTE: This is a superset-wrapper of Flux's built-in initialization logic.
// It also ensures V1-specific flux builtins are initialized.
package init

import (
	"github.com/influxdata/flux/runtime"
	_ "github.com/influxdata/flux/stdlib"
	_ "github.com/influxdata/influxdb/flux/stdlib"
)

// Initialize ensures all Flux builtins are configured and should be called
// prior to using the Flux runtime. Initialize is safe to call concurrently
// and is idempotent.
func Initialize() {
	runtime.FinalizeBuiltIns()
}
