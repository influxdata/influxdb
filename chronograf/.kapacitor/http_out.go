package kapacitor

import (
	"fmt"

	"github.com/influxdata/influxdb/chronograf"
)

// HTTPEndpoint is the default location of the tickscript output
const HTTPEndpoint = "output"

// HTTPOut adds a kapacitor httpOutput to a tickscript
func HTTPOut(rule chronograf.AlertRule) (string, error) {
	return fmt.Sprintf(`trigger|httpOut('%s')`, HTTPEndpoint), nil
}
