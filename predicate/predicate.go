package predicate

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/tsm1"
)

// Node is a predicate node.
type Node interface {
	json.Marshaler
	NodeType() string
	ToDataType() (*datatypes.Node, error)
}

// node types
const (
	logicalNodeType = "logical"
	tagRuleNodeType = "tagRule"
)

var typeToNode = map[string]func() Node{
	logicalNodeType: func() Node { return &LogicalNode{} },
	tagRuleNodeType: func() Node { return &TagRuleNode{} },
}

type rawNodeJSON struct {
	Typ string `json:"nodeType"`
}

// UnmarshalJSON will convert
func UnmarshalJSON(b []byte) (Node, error) {
	if len(b) == 0 ||
		string(b) == "{}" ||
		string(b) == "null" {
		return nil, nil
	}
	var raw rawNodeJSON
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "unable to detect the node type from json",
		}
	}
	convertedFunc, ok := typeToNode[raw.Typ]
	if !ok {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("invalid node type %s", raw.Typ),
		}
	}
	converted := convertedFunc()
	err := json.Unmarshal(b, converted)
	return converted, err
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
