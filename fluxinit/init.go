// This package imports all the influxdb-specific query builtins. From influxdb
// we must use this package and not the init package provided by flux.
//
// This package is used for initializing with a function call. As a
// convenience, the fluxinit/static package can be imported for use cases where
// static initialization is okay, such as tests.
package fluxinit

import (
	"github.com/influxdata/flux/runtime"
	_ "github.com/influxdata/flux/stdlib"

	// Import the stdlib
	_ "github.com/influxdata/influxdb/v2/query/stdlib"
)

// The FluxInit() function prepares the runtime for compilation and execution
// of Flux. This is a costly step and should only be performed if the intention
// is to compile and execute flux code.
func FluxInit() {
	runtime.FinalizeBuiltIns()
}
