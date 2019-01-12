// Package builtin ensures all packages related to Flux built-ins are imported and initialized.
// This should only be imported from main or test packages.
// It is a mistake to import it from any other package.
package builtin

import (
	"github.com/influxdata/flux"

	_ "github.com/influxdata/flux/stdlib"           // Import the stdlib
	_ "github.com/influxdata/influxdb/query/stdlib" // Import the stdlib
)

func init() {
	flux.FinalizeBuiltIns()
}
