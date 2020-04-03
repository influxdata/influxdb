package noop

import "github.com/influxdata/influxdb/v2/resource"

type ResourceLogger struct{}

func (ResourceLogger) Log(resource.Change) error {
	return nil
}
