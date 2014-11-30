package influxdb

import (
	"strings"

	"github.com/influxdb/influxdb/influxql"
)

// Planner creates an execution plan for an InfluxQL statement.
type Planner struct {
	NewIteratorFunc func(name string, tags map[string]string) []Iterator
}

// Plan generates an executable plan for a SELECT statement.
func (p *Planner) Plan(stmt *influxql.SelectStatement) Executor {
	// Create a new executor.
	e := &executor{
		stmt:            stmt,
		newIteratorFunc: p.NewIteratorFunc,
	}

	// Generate mappers and reducers for each field.
	e.mappers = make([]mapper, len(stmt.Fields))
	e.reducers = make([]reducer, len(stmt.Fields))

	for i, f := range stmt.Fields {
		switch expr := f.Expr.(type) {
		case *influxql.Call:
			switch strings.ToLower(expr.Name) {
			case "count":
				e.mappers[i] = &countMapper{}
				e.reducers[i] = &sumReducer{}
			case "sum":
				e.mappers[i] = &evalMapper{expr: expr.Args[0]}
				e.reducers[i] = &sumReducer{}
			default:
				panic("pending: non-count calls")
			}
		default:
			e.mappers[i] = &evalMapper{expr: f.Expr}
		}
	}

	return e
}

// Executor represents an execution plan that can be run.
type Executor interface {
	Execute() (<-chan []interface{}, error)
}

type executor struct {
	stmt            *influxql.SelectStatement
	mappers         []mapper
	reducers        []reducer
	newIteratorFunc func(name string, tags map[string]string) []Iterator
}

func (e *executor) Execute() (<-chan []interface{}, error) {
	// Retrieve a list of iterators.
	// TODO: Support multiple sources.
	iterators := e.newIteratorFunc(e.stmt.Source.(*influxql.Series).Name, nil)

	// Reduce intermediate data to our final dataset.
	result := make(chan []interface{}, 0)
	go func() {
		// Execute the mappers for every element in the iterator.
		intermediate := e.executeMappers(iterators)

		// TODO: Sort, if specified.

		// If the statement is aggregated then execute reducers.
		// Otherwise stream raw data rows as-is.
		if e.stmt.Aggregated() {
			e.executeReducers(intermediate, result)
		} else {
			for _, values := range intermediate {
				for _, row := range values {
					result <- row
				}
			}
		}

		// Close the result channel to notify the caller of the end.
		close(result)
	}()

	return result, nil
}

// executes mappers to generate data for each field.
func (e *executor) executeMappers(iterators []Iterator) map[string][][]interface{} {
	intermediate := make(map[string][][]interface{})
	for _, itr := range iterators {
		for p := itr.Next(); p != nil; p = itr.Next() {
			// Generate an intermediate row with the mappers.
			value := make([]interface{}, len(e.mappers))
			for i, m := range e.mappers {
				value[i] = m.Map(p)
			}

			// Append row to the key in the intermediate data.
			var key = "" //  TODO: Generate key.
			intermediate[key] = append(intermediate[key], value)
		}
	}
	return intermediate
}

// executes reducers to combine data by key and then sends the data to the result channel.
func (e *executor) executeReducers(intermediate map[string][][]interface{}, result chan []interface{}) {
	for _, values := range intermediate {
		row := make([]interface{}, len(e.reducers))
		for i, mr := range e.reducers {
			row[i] = mr.Reduce(values, i)
		}
		result <- row
	}
}

// Iterator represents an object that can iterate over raw points.
type Iterator interface {
	Next() Point
}

// Point represents a timeseries data point with a timestamp and values.
type Point interface {
	Timestamp() int64
	Value(name string) interface{}
}

type mapper interface {
	Map(Point) interface{}
}

type countMapper struct{}

func (m *countMapper) Map(_ Point) interface{} { return 1 }

type evalMapper struct {
	expr influxql.Expr
}

func (m *evalMapper) Map(p Point) interface{} { return eval(p, m.expr) }

type reducer interface {
	Reduce(values [][]interface{}, index int) interface{}
}

type sumReducer struct{}

func (r *sumReducer) Reduce(values [][]interface{}, index int) interface{} {
	var n int
	for _, value := range values {
		v, _ := value[index].(int)
		n += v
	}
	return n
}

// eval computes the value of an expression for a given point.
func eval(p Point, expr influxql.Expr) interface{} {
	switch expr := expr.(type) {
	case *influxql.VarRef:
		return p.Value(expr.Val)
	case *influxql.Call:
		panic("not implemented: eval: call")
	case *influxql.NumberLiteral:
		return expr.Val
	case *influxql.StringLiteral:
		return expr.Val
	case *influxql.BooleanLiteral:
		return expr.Val
	case *influxql.TimeLiteral:
		return expr.Val
	case *influxql.DurationLiteral:
		return expr.Val
	case *influxql.BinaryExpr:
		return evalBinaryExpr(p, expr)
	case *influxql.ParenExpr:
		return eval(p, expr.Expr)
	}
	panic("unsupported expression type")
}

func evalBinaryExpr(p Point, expr *influxql.BinaryExpr) interface{} {
	// Compute the left and right hand side values.
	lhs := eval(p, expr.LHS)
	rhs := eval(p, expr.RHS)

	// Execute them with the appropriate types.
	switch expr.Op {
	case influxql.ADD:
		return lhs.(float64) + rhs.(float64)
	case influxql.SUB:
		return lhs.(float64) - rhs.(float64)
	case influxql.MUL:
		return lhs.(float64) * rhs.(float64)
	case influxql.DIV:
		if rhs == 0 {
			return float64(0)
		}
		return lhs.(float64) / rhs.(float64)

	case influxql.AND:
		return lhs.(bool) && rhs.(bool)
	case influxql.OR:
		return lhs.(bool) || rhs.(bool)

	case influxql.EQ:
		return lhs == rhs
	case influxql.NEQ:
		return lhs != rhs
	case influxql.LT:
		return lhs.(float64) < rhs.(float64)
	case influxql.LTE:
		return lhs.(float64) <= rhs.(float64)
	case influxql.GT:
		return lhs.(float64) > rhs.(float64)
	case influxql.GTE:
		return lhs.(float64) >= rhs.(float64)

	default:
		panic("invalid binary expr operator:" + expr.Op.String())
	}
}

// EXAMPLE: SELECT COUNT(value) FROM some_series GROUP BY TIME(5m) HAVING COUNT(value) > 23
// EXAMPLE: SELECT * FROM cpu GROUP BY TIME(1h), host HAVING TOP(value, 10) WHERE time > NOW()
// EXAMPLE: SELECT MAX(value) AS max_value, host FROM cpu GROUP BY TIME(1h), host HAVING TOP(max_value, 13)
