package noop

import "github.com/influxdata/influxdb/resource"

type ResourceLogger struct{}

func (ResourceLogger) Log(resource.Change) error {
	return nil
}
