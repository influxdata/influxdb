package kapacitor

import (
	"fmt"

	"github.com/influxdata/chronograf"
)

// InfluxOut creates a kapacitor influxDBOut node to write alert data to Database, RP, Measurement.
func InfluxOut(rule chronograf.AlertRule) (string, error) {
	// For some of the alert, the data needs to be renamed (normalized)
	// before being sent to influxdb.

	rename := ""
	if rule.Trigger == "deadman" {
		fld, err := field(rule.Query)
		if err != nil {
			return "", err
		}
		rename = fmt.Sprintf(`|eval(lambda: '%s').as('value')`, fld)
	}
	return fmt.Sprintf(`
			trigger
			%s
			|influxDBOut()
            	.create()
            	.database(output_db)
            	.retentionPolicy(output_rp)
            	.measurement(output_mt)
				.tag('alertName', name)
				.tag('triggerType', triggerType)
			`, rename), nil
}
