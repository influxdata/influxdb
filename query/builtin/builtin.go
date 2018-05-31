// This package ensures all packages related to built-ins are imported and initialized.
// It provides helper functions for constructing various types that depend on the built-ins.
package query

import (
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/complete"
	_ "github.com/influxdata/platform/query/functions" // Import the built-in functions
	"github.com/influxdata/platform/query/interpreter"
)

func init() {
	query.FinalizeBuiltIns()
}

// DefaultCompleter creates a completer with builtin scope and declarations
func DefaultCompleter() complete.Completer {
	scope, declarations := query.BuiltIns()
	interpScope := interpreter.NewScopeWithValues(scope)
	return complete.NewCompleter(interpScope, declarations)
}
