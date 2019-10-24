package storage

import (
	"github.com/influxdata/influxdb/models"
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
