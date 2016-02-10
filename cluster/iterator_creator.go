package cluster

import (
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
)

// IteratorCreator is responsible for creating iterators for queries.
// Iterators can be created for the local node or can be retrieved remotely.
type IteratorCreator struct {
	MetaStore interface {
		NodeID() uint64
		Node(id uint64) (ni *meta.NodeInfo, err error)
	}

	TSDBStore influxql.IteratorCreator

	// Duration before idle remote iterators are disconnected.
	Timeout time.Duration

	// Treats all shards as remote. Useful for testing.
	ForceRemoteMapping bool

	pool *clientPool
}

// NewIteratorCreator returns a new instance of IteratorCreator.
func NewIteratorCreator() *IteratorCreator {
	return &IteratorCreator{
		pool: newClientPool(),
	}
}

// CreateIterator creates an iterator from local and remote shards.
func (ic *IteratorCreator) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	// FIXME(benbjohnson): Integrate remote execution.
	return ic.TSDBStore.CreateIterator(opt)
}

// FieldDimensions returns the unique fields and dimensions across a list of sources.
func (ic *IteratorCreator) FieldDimensions(sources influxql.Sources) (fields, dimensions map[string]struct{}, err error) {
	// FIXME(benbjohnson): Integrate remote execution.
	return ic.TSDBStore.FieldDimensions(sources)
}
