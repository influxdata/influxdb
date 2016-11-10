package kapacitor

import (
	"fmt"

	"github.com/influxdata/chronograf"
)

const HTTPEndpoint = "output"

func HTTPOut(rule chronograf.AlertRule) (string, error) {
	return fmt.Sprintf(`trigger|httpOut('%s')`, HTTPEndpoint), nil
}
