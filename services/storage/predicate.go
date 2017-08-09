package storage

import "strconv"

// NodeVisitor can be called by Walk to traverse the Node hierarchy.
// The Visit() function is called once per node.
type NodeVisitor interface {
	Visit(*Node) NodeVisitor
}

func (x Node_Type) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(x.String())), nil
}

func (x Node_Logical) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(x.String())), nil
}

func (x Node_Comparison) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(x.String())), nil
}

func WalkNode(v NodeVisitor, node *Node) {
	if v = v.Visit(node); v == nil {
		return
	}

	for _, n := range node.Children {
		WalkNode(v, n)
	}
}
