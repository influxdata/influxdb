// The init/static package can be imported in test cases and other uses
// cases where it is okay to always initialize flux.
package static

import fluxinit "github.com/influxdata/influxdb/flux/init"

func init() {
	fluxinit.Initialize()
}
