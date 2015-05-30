package cluster

import (
	"github.com/influxdb/influxdb/tsdb"
)

// QueryCoordinator provides an interface for translating queries to the
// appropriate metastore or data node function calls.
type QueryCoordinator struct {
	MetaStore interface {
		//...
	}
	indexes map[string]*tsdb.DatabaseIndex
}
