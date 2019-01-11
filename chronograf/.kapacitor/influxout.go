package kapacitor

import (
	"fmt"

	"github.com/influxdata/influxdb/chronograf"
)

// InfluxOut creates a kapacitor influxDBOut node to write alert data to Database, RP, Measurement.
func InfluxOut(rule chronograf.AlertRule) (string, error) {
	// For some of the alert, the data needs to be renamed (normalized)
	// before being sent to influxdb.

	rename := ""
	if rule.Trigger == "deadman" {
		rename = `|eval(lambda: "emitted")
        	.as('value')
       		.keep('value', messageField, durationField)`
	}
	return fmt.Sprintf(`
			trigger
			%s
			|eval(lambda: float("value"))
				.as('value')
				.keep()
			|influxDBOut()
				.create()
				.database(outputDB)
				.retentionPolicy(outputRP)
				.measurement(outputMeasurement)
				.tag('alertName', name)
				.tag('triggerType', triggerType)
			`, rename), nil
}
