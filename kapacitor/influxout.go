package kapacitor

import "github.com/influxdata/chronograf"

// InfluxOut creates a kapacitor influxDBOut node to write alert data to Database, RP, Measurement.
func InfluxOut(rule chronograf.AlertRule) string {
	return `
			trigger
			|influxDBOut()
            	.create()
            	.database(output_db)
            	.retentionPolicy(output_rp)
            	.measurement(output_mt)
				.tag('alertName', name)
				.tag('triggerType', triggerType)
			`
}
