package server

import (
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/influx"
)

// ToQueryConfig converts InfluxQL into queryconfigs
// If influxql cannot be represented by a full query config, then, the
// query config's raw text is set to the query.
func ToQueryConfig(query string) chronograf.QueryConfig {
	qc, err := influx.Convert(query)
	if err == nil {
		return qc
	}
	return chronograf.QueryConfig{
		RawText: &query,
		Fields:  []chronograf.Field{},
		GroupBy: chronograf.GroupBy{
			Tags: []string{},
		},
		Tags: make(map[string][]string, 0),
	}
}
