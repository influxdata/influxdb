// Package builtin ensures all packages related to Flux built-ins are imported and initialized.
// This should only be imported from main or test packages.
// It is a mistake to import it from any other package.
package builtin

import (
	"github.com/influxdata/platform/query"
	_ "github.com/influxdata/platform/query/functions" // Import the built-in functions
	_ "github.com/influxdata/platform/query/options"   // Import the built-in options
)

func init() {
	query.FinalizeBuiltIns()
}
