package predicate

import (
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/tsm1"
)

// Node is a predicate node.
type Node interface {
	ToDataType() (*datatypes.Node, error)
}

// New predicate from a node
func New(n Node) (influxdb.Predicate, error) {
	if n == nil {
		return nil, nil
	}
	dt, err := n.ToDataType()
	if err != nil {
		return nil, err
	}
	pred, err := tsm1.NewProtobufPredicate(&datatypes.Predicate{
		Root: dt,
	})
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	return pred, nil
}
