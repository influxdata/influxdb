package influxql

import "github.com/influxdata/influxql"

// isMathFunction returns true if the call is a math function.
func isMathFunction(expr *influxql.Call) bool {
	switch expr.Name {
	case "abs", "sin", "cos", "tan", "asin", "acos", "atan", "atan2", "exp", "log", "ln", "log2", "log10", "sqrt", "pow", "floor", "ceil", "round":
		return true
	}
	return false
}
