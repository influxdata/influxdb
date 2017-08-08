package storage

import "strconv"

// Visitor can be called by Walk to traverse an AST hierarchy.
// The Visit() function is called once per node.
type PredicateVisitor interface {
	Visit(*Node) PredicateVisitor
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

func WalkNode(v PredicateVisitor, node *Node) {
	if v = v.Visit(node); v == nil {
		return
	}

	for _, n := range node.Children {
		WalkNode(v, n)
	}
}
