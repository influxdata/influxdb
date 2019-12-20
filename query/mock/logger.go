package mock

import "github.com/influxdata/influxdb/v2/query"

var _ query.Logger = (*QueryLogger)(nil)

type QueryLogger struct {
	LogFn func(query.Log) error
}

func (l *QueryLogger) Log(log query.Log) error {
	return l.LogFn(log)
}
