package query

// orderNode configures a Node's ordering so it produces an ordered output.
// By default, data is assumed to be unordered. If the Node is not capable of
// determining ordering, it is ignored and the parent nodes are considered for
// ordering. If a Node has no inputs and it cannot be set to be ordered, then
// an error is returned.
func orderNode(n Node) (bool, error) {
	switch n := n.(type) {
	case *IteratorCreator:
		n.Ordered = true
		return false, nil
	case *Merge:
		n.Ordered = true
		return true, nil
	case *FunctionCall:
		n.Ordered = n.Interval.IsZero()
		return false, nil
	case *Interval:
		return true, nil
	case *TopBottomSelector:
		n.Ordered = true
		return false, nil
	}
	return true, nil
}
