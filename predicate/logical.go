package predicate

import (
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
)

// LogicalOperator is a string type of logical operator.
type LogicalOperator int

// LogicalOperators
var (
	LogicalAnd LogicalOperator = 1
)

// Value returns the node logical type.
func (op LogicalOperator) Value() (datatypes.Node_Logical, error) {
	switch op {
	case LogicalAnd:
		return datatypes.Node_LogicalAnd, nil
	default:
		return 0, &errors.Error{
			Code: errors.EInvalid,
			Msg:  fmt.Sprintf("the logical operator %q is invalid", op),
		}
	}
}

// LogicalNode is a node type includes a logical expression with other nodes.
type LogicalNode struct {
	Operator LogicalOperator `json:"operator"`
	Children [2]Node         `json:"children"`
}

// ToDataType convert a LogicalNode to datatypes.Node.
func (n LogicalNode) ToDataType() (*datatypes.Node, error) {
	logicalOp, err := n.Operator.Value()
	if err != nil {
		return nil, err
	}
	children := make([]*datatypes.Node, len(n.Children))
	for k, node := range n.Children {
		children[k], err = node.ToDataType()
		if err != nil {
			return nil, &errors.Error{
				Code: errors.EInvalid,
				Msg:  fmt.Sprintf("Err in Child %d, err: %s", k, err.Error()),
			}
		}
	}
	return &datatypes.Node{
		NodeType: datatypes.Node_TypeLogicalExpression,
		Value: &datatypes.Node_Logical_{
			Logical: logicalOp,
		},
		Children: children,
	}, nil
}
