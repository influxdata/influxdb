package storage

import (
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxql"
)

var measurementRemap = map[string]string{
	"_measurement":           "_name",
	models.MeasurementTagKey: "_name",
	models.FieldKeyTagKey:    "_field",
}

func RewriteExprRemoveFieldKeyAndValue(expr influxql.Expr) influxql.Expr {
	return influxql.RewriteExpr(expr, func(expr influxql.Expr) influxql.Expr {
		if be, ok := expr.(*influxql.BinaryExpr); ok {
			if ref, ok := be.LHS.(*influxql.VarRef); ok {
				if ref.Val == "_field" || ref.Val == "$" {
					return &influxql.BooleanLiteral{Val: true}
				}
			}
		}

		return expr
	})
}

type measEval struct {
	measNodeHeads map[*influxql.BinaryExpr][]string
	invalidHeads  map[*influxql.BinaryExpr]struct{}
	onlyAnds      bool
	head          *influxql.BinaryExpr
}

// Visit is called on every node to evaluate the tree for a possible measurement
// optimization. A measurement optimization is possible if the tree contains a
// single group of one or many measurements (in the form of _measurement =
// measName, equality operator only) grouped together by OR operators, with the
// subtree containing the OR'd measurements accessible from root of the tree
// either directly (tree contains nothing but OR'd measurements) or by
// traversing AND binary expression nodes.
func (v measEval) Visit(node influxql.Node) influxql.Visitor {
	switch n := node.(type) {
	case *influxql.BinaryExpr:
		if n.Op != influxql.AND {
			// If this is the first time encountering a non-AND node, mark this as the
			// head for the subtree.
			if v.onlyAnds {
				v.head = n
			}
			v.onlyAnds = false
		}

		switch n.Op {
		case influxql.AND:
			// If only AND ops have been encountered so far, move the head of this
			// subtree along and continue.
			if v.onlyAnds {
				v.head = n
				return v
			}

			// Otherwise, encountering this AND after something other than an AND
			// makes this subtree not a viable canditate for the optimization.
			v.invalidHeads[v.head] = struct{}{}
			return v
		case influxql.OR:
			// OR ops do not invalidate the subtree.
			return v
		case influxql.EQ:
			// For EQ ops, the only valid configuration is the form of
			// "_measurement = measurementName", so check for that and add the
			// resulting name to the list of names for this subtree.
			if meas, ok := measNameFromEqual(n); ok {
				v.measNodeHeads[v.head] = append(v.measNodeHeads[v.head], meas)
				return v
			}

			// If it wasn't a measurement, this tree is invalid.
			v.invalidHeads[v.head] = struct{}{}
			return v
		default:
			// Any other operation means this subtree is invalid.
			v.invalidHeads[v.head] = struct{}{}

			// If a measurement name is being operated on with a non-equality
			// operator, add the measurement name to the subtree. This must be done to
			// indicate that there is a measurement in an invalid subtree.
			if ref, ok := n.LHS.(*influxql.VarRef); ok {
				if ref.Val == measurementRemap[measurementKey] {
					v.measNodeHeads[v.head] = append(v.measNodeHeads[v.head], ref.Val)

				}
			}

			return v
		}
	case *influxql.VarRef, *influxql.StringLiteral, *influxql.ParenExpr:
		// VarRefs and StringLiterals will have been evaluated in the EQ case.
		// Parens also do not invalidate the subtree and can be traversed through.
		return v
	}

	// If the function did not return in the type switch, that means that this is
	// some other kind of node, which invalidates the tree
	v.onlyAnds = false
	v.invalidHeads[v.head] = struct{}{}

	return v
}

func MeasurementOptimization(expr influxql.Expr) ([]string, bool) {
	measNodeHeads := make(map[*influxql.BinaryExpr][]string)
	invalidHeads := make(map[*influxql.BinaryExpr]struct{})
	dummyHead := &influxql.BinaryExpr{}

	v := measEval{
		measNodeHeads: measNodeHeads,
		invalidHeads:  invalidHeads,
		onlyAnds:      true,
		head:          dummyHead,
	}

	influxql.Walk(v, expr)

	// There must be measurements in exactly one subtree for this optimization to
	// work. Note: It may be possible to have measurements in multiple subtrees,
	// as long as there are no measurements in invalid subtrees, by computing a
	// union of the measurement names across all valid subtrees - this is not
	// currently implemented.
	if len(measNodeHeads) != 1 {
		return nil, false
	}

	names := []string{}
	for h, ns := range measNodeHeads {
		// The subtree containing the measurements must not have been invalidated.
		if _, ok := invalidHeads[h]; ok {
			return nil, false
		}

		names = ns
	}

	return names, true
}

func measNameFromEqual(be *influxql.BinaryExpr) (string, bool) {
	lhs, ok := be.LHS.(*influxql.VarRef)
	if !ok {
		return "", false
	} else if lhs.Val != measurementRemap[measurementKey] {
		return "", false
	}

	rhs, ok := be.RHS.(*influxql.StringLiteral)
	if !ok {
		return "", false
	}

	return rhs.Val, true
}

type hasRefs struct {
	refs  []string
	found []bool
}

func (v *hasRefs) allFound() bool {
	for _, val := range v.found {
		if !val {
			return false
		}
	}
	return true
}

func (v *hasRefs) Visit(node influxql.Node) influxql.Visitor {
	if v.allFound() {
		return nil
	}

	if n, ok := node.(*influxql.VarRef); ok {
		for i, r := range v.refs {
			if !v.found[i] && r == n.Val {
				v.found[i] = true
				if v.allFound() {
					return nil
				}
			}
		}
	}
	return v
}

func HasFieldKeyOrValue(expr influxql.Expr) (bool, bool) {
	refs := hasRefs{refs: []string{fieldKey, "$"}, found: make([]bool, 2)}
	influxql.Walk(&refs, expr)
	return refs.found[0], refs.found[1]
}

type hasAnyTagKeys struct {
	found bool
}

func (v *hasAnyTagKeys) Visit(node influxql.Node) influxql.Visitor {
	if v.found {
		return nil
	}

	if n, ok := node.(*influxql.VarRef); ok {
		// The influxql expression will have had references to "_measurement"
		// remapped to "_name" at this point by reads.NodeToExpr, so be sure to
		// check for the appropriate value here using the measurementRemap map.
		if n.Val != fieldKey && n.Val != measurementRemap[measurementKey] && n.Val != "$" {
			v.found = true
			return nil
		}
	}
	return v
}

func hasTagKey(expr influxql.Expr) bool {
	v := &hasAnyTagKeys{}
	influxql.Walk(v, expr)
	return v.found
}
