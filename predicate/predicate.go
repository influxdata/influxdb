package predicate

import (
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
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
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Err:  err,
		}
	}
	return pred, nil
}
