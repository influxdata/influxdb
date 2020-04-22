// Package builtin ensures all packages related to Flux built-ins are imported and initialized.
// This should only be imported from main or test packages.
// It is a mistake to import it from any other package.
package builtin

import (
	"sync"

	"github.com/influxdata/flux"
	_ "github.com/influxdata/flux/stdlib"
	_ "github.com/influxdata/influxdb/v2/v1/flux/stdlib"
)

var once sync.Once

// Initialize ensures all Flux builtins are configured and should be called
// prior to using the Flux runtime. Initialize is safe to call concurrently
// and is idempotent.
func Initialize() {
	once.Do(func() {
		flux.FinalizeBuiltIns()
	})
}
