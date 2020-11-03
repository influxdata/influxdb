package fluxinit

import (
	"github.com/influxdata/flux/runtime"

	_ "github.com/influxdata/flux/stdlib"
	_ "github.com/influxdata/influxdb/v2/query/stdlib" // Import the stdlib
)

// FluxInit() prepares the runtime for compilation and execution of flux. This
// is a costly step and should only be performed if the intention is to compile
// and execute flux code.
//
// Importing this package and calling FluxInit is equivalent to importing the
// "builtin" package. It draws in the standard library functions, which
// register themselves in init() functions, then performs the final steps
// necessary to prepare for executing flux.
func FluxInit() {
	runtime.FinalizeBuiltIns()
}
