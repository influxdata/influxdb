package reads

import (
	"github.com/influxdata/influxql"
)

// TODO(sgc): build expression evaluator that does not use influxql AST

type expression interface {
	EvalBool(v Valuer) bool
}

type astExpr struct {
	expr influxql.Expr
}

func (e *astExpr) EvalBool(v Valuer) bool {
	return EvalExprBool(e.expr, v)
}

// Valuer is the interface that wraps the Value() method.
type Valuer interface {
	// Value returns the value and existence flag for a given key.
	Value(key string) (interface{}, bool)
}
