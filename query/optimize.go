package query

func optimize(n Node) error {
	if err := optimizeMerges(n); err != nil {
		return err
	}
	if err := optimizeFunctionCalls(n); err != nil {
		return err
	}
	return nil
}

func optimizeMerges(n Node) error {
	return Walk(n, VisitorFunc(func(n Node) (bool, error) {
		if m, ok := n.(*Merge); ok && len(m.InputNodes) == 1 {
			RemoveNode(m.InputNodes[0], m.Output)
		}
		return true, nil
	}))
}

func optimizeFunctionCalls(n Node) error {
	return Walk(n, VisitorFunc(func(n Node) (bool, error) {
		fc, ok := n.(*FunctionCall)
		if !ok {
			return true, nil
		}

		// If our output node is a function, check if it is one of the ones we can
		// do as a partial aggregate.
		switch fc.Name {
		case "min", "max", "sum", "first", "last", "mean", "count":
			// Pass through.
		default:
			return true, nil
		}

		// Optimize if the parent node is a merge node or
		switch parent := fc.Input.Input.Node.(type) {
		case *IteratorCreator:
			// Check if the expression used by the iterator creator is a variable reference.
			// Ensure this expression doesn't already have a function call.
			if parent.FunctionCall != "" {
				return false, nil
			}
			// Mark this iterator creator as being a function call.
			parent.FunctionCall = fc.Name
			parent.Location = fc.Location
			// Remove the function call node.
			RemoveNode(fc.Input, fc.Output)
			return false, nil
		case *Merge:
			// Create a new function call and insert it at the end of every
			// input to the merge node.
			for _, input := range parent.InputNodes {
				call := *fc
				call.Input, call.Output = input.Insert(&call)
			}
			// If the function call was count(), modify it so it is now sum().
			if fc.Name == "count" {
				fc.Name = "sum"
			}
		}
		return true, nil
	}))
}
