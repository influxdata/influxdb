package kapacitor

import (
	"fmt"

	"github.com/influxdata/chronograf"
)

// Data returns the tickscript data section for querying
// TODO: Someone else needs to build the var period and var every
func Data(q chronograf.QueryConfig) (string, error) {
	if q.RawText != "" {
		batch := `
     var data = batch
     |query('''
        %s
      ''')
        .period(period)
        .every(every)
        .align()`
		batch = fmt.Sprintf(batch, q.RawText)
		if q.GroupBy.Time != "" {
			batch = batch + fmt.Sprintf(".groupBy(%s)", q.GroupBy.Time)
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
	stream = stream + "|window().period(period).every(every).align()\n"

	for _, field := range q.Fields {
		for _, fnc := range field.Funcs {
			stream = stream + fmt.Sprintf(`|%s(field).as(metric)`, fnc)
			break // only support a single field
		}
		break // only support a single field
	}

	stream = stream + "|where(where_filter)\n"
	return stream, nil
}
