package storage

import (
	"encoding/json"
	fmt "fmt"

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

func MeasurementOptimization(expr influxql.Expr) ([]string, bool) {
	names := []string{}
	foundMeasBranch := false

	// clone the expr - it may be mutated by the walk func if a valid measurement
	// branch is found
	exprCopy := influxql.CloneExpr(expr)
	var walk func(influxql.Expr)
	walk = func(node influxql.Expr) {
		if node == nil || foundMeasBranch {
			return
		}

		switch n := node.(type) {
		case *influxql.BinaryExpr:
			switch n.Op {
			case influxql.AND:
				lhsNames, ok := IsMeasBranch(n.LHS)
				if ok {
					names = append(names, lhsNames...)
					n.LHS = nil
					foundMeasBranch = true
				} else {
					rhsNames, ok := IsMeasBranch(n.RHS)
					if ok {
						names = append(names, rhsNames...)
						n.RHS = nil
						foundMeasBranch = true
					}
				}
				walk(n.LHS)
				walk(n.RHS)
			case influxql.EQ:
				ns, ok := IsMeasBranch(n)
				if ok {
					names = append(names, ns...)
					n.LHS = nil
					n.RHS = nil
					foundMeasBranch = true
				}
				walk(n.LHS)
				walk(n.RHS)
			}
		case *influxql.ParenExpr:
			ns, ok := IsMeasBranch(n)
			if ok {
				names = append(names, ns...)
				n.Expr = nil
				foundMeasBranch = true
			}
			walk(n.Expr)
		default:
			return
		}
	}

	walk(exprCopy)

	fmt.Println("*** BEFORE ***")
	ex, _ := json.MarshalIndent(expr, "", "\t")
	fmt.Println(string(ex))

	fmt.Println("*** AFTER ***")
	ex, _ = json.MarshalIndent(exprCopy, "", "\t")
	fmt.Println(string(ex))

	// If a measurement branch was found, but the remaining expression has a
	// measurement anywhere else, the optimization cannot be applied, so return a
	// negative response
	if exprHasMeas(exprCopy) {
		return []string{}, false
	}

	return names, foundMeasBranch
}

// IsMeasBranch determines if the branch with the head at expr represents a
// group of OR'd measurements from a tree of binary expressions or paren
// expressions.
func IsMeasBranch(expr influxql.Expr) ([]string, bool) {
	valid := true
	names := []string{}

	var walk func(influxql.Expr)
	walk = func(node influxql.Expr) {
		// don't need to continue if the tree has already been determined invalid
		if !valid {
			return
		}

		switch n := node.(type) {
		// most of the time these will be binary expressions
		case *influxql.BinaryExpr:
			switch n.Op {
			case influxql.EQ:
				name, ok := measNameFromEqual(n)
				if ok {
					names = append(names, name)
				} else {
					valid = false
				}
			case influxql.OR:
				walk(n.LHS)
				walk(n.RHS)
			default:
				// anything else is not valid
				valid = false
			}
		case *influxql.ParenExpr:
			// paren expressions are essentially wrappers for other expressions, so
			// continue
			walk(n.Expr)
		default:
			// any other type of node is not valid
			valid = false
		}
	}

	walk(expr)

	if !valid {
		names = []string{}
	}

	return names, valid
}

func exprHasMeas(expr influxql.Expr) bool {
	found := false

	influxql.WalkFunc(expr, func(node influxql.Node) {
		if ref, ok := node.(*influxql.VarRef); ok {
			if ref.Val == measurementRemap[measurementKey] {
				found = true
			}
		}
	})

	return found
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

// func MeasurementOptimization(expr influxql.Expr) ([]string, bool) {
// 	be, ok := expr.(*influxql.BinaryExpr)

// 	if !ok || be.Op != influxql.AND {
// 		return nil, false
// 	}

// 	LHSValid, LHSAnyMeasurements, LHSNames := evalMeasBranch(be.LHS)
// 	RHSValid, RHSAnyMeasurements, RHSNames := evalMeasBranch(be.RHS)

// 	if LHSValid == RHSValid {
// 		return nil, false
// 	}

// 	if LHSValid && RHSAnyMeasurements || RHSValid && LHSAnyMeasurements {
// 		return nil, false
// 	}

// 	var names []string
// 	if LHSValid {
// 		names = LHSNames
// 	} else {
// 		names = RHSNames
// 	}

// 	return names, true
// }

// func evalMeasBranch(expr influxql.Expr) (valid, anyMeasurements bool, names []string) {
// 	valid = true
// 	anyMeasurements = false
// 	names = []string{}

// 	influxql.WalkFunc(expr, func(node influxql.Node) {
// 		if _, ok := node.(*influxql.ParenExpr); ok {
// 			return
// 		}

// 		if nt, ok := node.(*influxql.BinaryExpr); ok {
// 			if nt.Op == influxql.OR {
// 				_, lhsIsBinary := nt.LHS.(*influxql.BinaryExpr)
// 				_, rhsIsBinary := nt.RHS.(*influxql.BinaryExpr)
// 				_, lhsIsParen := nt.LHS.(*influxql.ParenExpr)
// 				_, rhsIsParen := nt.RHS.(*influxql.ParenExpr)

// 				if (!lhsIsBinary && !lhsIsParen) || (!rhsIsBinary && !rhsIsParen) {
// 					valid = false
// 				}

// 				return
// 			}

// 			if nt.Op == influxql.EQ {
// 				if _, ok := nt.LHS.(*influxql.VarRef); !ok {
// 					valid = false
// 				}

// 				if _, ok := nt.RHS.(*influxql.StringLiteral); !ok {
// 					valid = false
// 				}

// 				return
// 			}
// 		}

// 		if ref, ok := node.(*influxql.VarRef); ok {
// 			if ref.Val != measurementRemap[measurementKey] {
// 				valid = false
// 				return
// 			}

// 			anyMeasurements = true
// 			return
// 		}

// 		if sl, ok := node.(*influxql.StringLiteral); ok {
// 			names = append(names, sl.Val)
// 			return
// 		}

// 		valid = false
// 	})

// 	return valid, anyMeasurements, names
// }

// HasSingleMeasurementNoOR determines if an index optimisation is available.
//
// Typically the read service will use the query engine to retrieve all field
// keys for all measurements that match the expression, which can be very
// inefficient if it can be proved that only one measurement matches the expression.
//
// This condition is determined when the following is true:
//
//		* there is only one occurrence of the tag key `_measurement`.
//		* there are no OR operators in the expression tree.
//		* the operator for the `_measurement` binary expression is ==.
//
func HasSingleMeasurementNoOR(expr influxql.Expr) (string, bool) {
	var lastMeasurement string
	foundOnce := true
	var invalidOP bool

	influxql.WalkFunc(expr, func(node influxql.Node) {
		if !foundOnce || invalidOP {
			return
		}

		if be, ok := node.(*influxql.BinaryExpr); ok {
			if be.Op == influxql.OR {
				invalidOP = true
				return
			}

			if ref, ok := be.LHS.(*influxql.VarRef); ok {
				if ref.Val == measurementRemap[measurementKey] {
					if be.Op != influxql.EQ {
						invalidOP = true
						return
					}

					if lastMeasurement != "" {
						foundOnce = false
					}

					// Check that RHS is a literal string
					if ref, ok := be.RHS.(*influxql.StringLiteral); ok {
						lastMeasurement = ref.Val
					}
				}
			}
		}
	})
	return lastMeasurement, len(lastMeasurement) > 0 && foundOnce && !invalidOP
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
