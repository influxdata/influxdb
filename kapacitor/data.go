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

	if rule.Trigger != "deadman" {
		value := ""
		for _, field := range rule.Query.Fields {
			for _, fnc := range field.Funcs {
				// Only need a window if we have an aggregate function
				value = value + "|window().period(period).every(every).align()\n"
				value = value + fmt.Sprintf(`|%s(field).as('value')`, fnc)
				break // only support a single field
			}
			break // only support a single field
		}
		if value == "" {
			value = `|eval(lambda: field).as('value')`
		}
		stream = stream + value
	}
	return stream, nil
}
