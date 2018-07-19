package kapacitor

import (
	"fmt"

	"github.com/influxdata/platform/chronograf"
)

// Data returns the tickscript data section for querying
func Data(rule chronograf.AlertRule) (string, error) {
	if rule.Query.RawText != nil && *rule.Query.RawText != "" {
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

	stream = fmt.Sprintf("%s\n.groupBy(groupBy)\n", stream)
	stream = stream + ".where(whereFilter)\n"
	// Only need aggregate functions for threshold and relative

	if rule.Trigger != "deadman" {
		fld, err := field(rule.Query)
		if err != nil {
			return "", err
		}
		value := ""
		for _, field := range rule.Query.Fields {
			if field.Type == "func" && len(field.Args) > 0 && field.Args[0].Type == "field" {
				// Only need a window if we have an aggregate function
				value = value + "|window().period(period).every(every).align()\n"
				value = value + fmt.Sprintf(`|%s('%s').as('value')`, field.Value, field.Args[0].Value)
				break // only support a single field
			}
			if value != "" {
				break // only support a single field
			}
			if field.Type == "field" {
				value = fmt.Sprintf(`|eval(lambda: "%s").as('value')`, field.Value)
			}
		}
		if value == "" {
			value = fmt.Sprintf(`|eval(lambda: "%s").as('value')`, fld)
		}
		stream = stream + value
	}
	return stream, nil
}
