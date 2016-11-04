package kapacitor

import (
	"fmt"

	"github.com/influxdata/chronograf"
)

// InfluxOut creates a kapacitor influxDBOut node to write alert data to Database, RP, Measurement.
func InfluxOut(rule chronograf.AlertRule) string {
	// For some of the alert, the data needs to be renamed (normalized)
	// before being sent to influxdb.
	rename := ""
	if rule.Trigger == "deadman" {
		rename = `|eval(lambda: field).as('value')`
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
			`, rename)
}
