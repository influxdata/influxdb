// Package builtin ensures all packages related to Flux built-ins are imported and initialized.
// This should only be imported from main or test packages.
// It is a mistake to import it from any other package.
package builtin

import (
	"github.com/influxdata/flux"

	_ "github.com/influxdata/flux/functions/inputs"          // Import the built-in input functions
	_ "github.com/influxdata/flux/functions/outputs"         // Import the built-in output functions
	_ "github.com/influxdata/flux/functions/transformations" // Import the built-in transformations
	_ "github.com/influxdata/flux/options"                   // Import the built-in options
	_ "github.com/influxdata/influxdb/flux/functions/inputs" // Import the built-in functions
)

func init() {
	flux.FinalizeBuiltIns()
}
