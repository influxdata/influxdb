package builtinlazy

import (
	"sync"

	"github.com/influxdata/flux"
	_ "github.com/influxdata/flux/stdlib"              // Import the stdlib
	_ "github.com/influxdata/influxdb/v2/query/stdlib" // Import the stdlib
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
