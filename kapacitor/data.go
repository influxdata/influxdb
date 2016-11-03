package kapacitor

import (
	"fmt"

	"github.com/influxdata/chronograf"
)

// Data returns the tickscript data section for querying
func Data(rule chronograf.AlertRule) (string, error) {
	if rule.Query.RawText != "" {
		batch := `
     var data = batch
     |query('''
        %s
      ''')
        .period(period)
        .every(every)
        .align()`
		batch = fmt.Sprintf(batch, rule.Query.RawText)
		if rule.Query.GroupBy.Time != "" {
			batch = batch + fmt.Sprintf(".groupBy(%s)", rule.Query.GroupBy.Time)
		}
		return batch, nil
	}
	stream := `var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
  `

	stream = fmt.Sprintf("%s\n.groupBy(groupby)\n", stream)
	stream = stream + ".where(where_filter)\n"
	// Only need aggregate functions for threshold and relative
	if rule.Type != "deadman" {
		for _, field := range rule.Query.Fields {
			for _, fnc := range field.Funcs {
				// Only need a window if we have an aggregate function
				stream = stream + "|window().period(period).every(every).align()\n"
				stream = stream + fmt.Sprintf(`|%s(field).as(metric)`, fnc)
				break // only support a single field
			}
			break // only support a single field
		}
	}

	return stream, nil
}
