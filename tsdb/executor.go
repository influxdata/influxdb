package tsdb

import "github.com/influxdata/influxdb/models"

// Executor is an interface for a query executor.
type Executor interface {
	Execute(closing <-chan struct{}) <-chan *models.Row
}
