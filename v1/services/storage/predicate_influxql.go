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
