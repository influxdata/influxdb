package influxql

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/ifql/ast"
	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/semantic"
	"github.com/influxdata/influxql"
)

// Transpiler converts InfluxQL queries into a query spec.
type Transpiler struct{}

func NewTranspiler() *Transpiler {
	return new(Transpiler)
}

func (t *Transpiler) Transpile(ctx context.Context, txt string) (*query.Spec, error) {
	// Parse the text of the query.
	q, err := influxql.ParseQuery(txt)
	if err != nil {
		return nil, err
	}

	if len(q.Statements) != 1 {
		// TODO(jsternberg): Handle queries with multiple statements.
		return nil, errors.New("unimplemented: only one statement is allowed")
	}

	s, ok := q.Statements[0].(*influxql.SelectStatement)
	if !ok {
		// TODO(jsternberg): Support meta queries.
		return nil, errors.New("only supports select statements")
	}

	transpiler := newTranspilerState(s)
	return transpiler.Transpile(ctx)
}

type transpilerState struct {
	stmt    *influxql.SelectStatement
	symbols []symbol
	spec    *query.Spec
	nextID  map[string]int
	now     time.Time

	// calls maintains an index to each function call within the select statement
	// for easy access.
	calls map[*influxql.Call]struct{}
}

func newTranspilerState(stmt *influxql.SelectStatement) *transpilerState {
	state := &transpilerState{
		stmt:   stmt.Clone(),
		spec:   &query.Spec{},
		nextID: make(map[string]int),
		now:    time.Now(),
	}
	// Omit the time from the cloned statement so it doesn't show up in
	// the list of column names.
	state.stmt.OmitTime = true
	return state
}

type selectInfo struct {
	names []string
	exprs map[string]influxql.Expr
	calls map[*influxql.Call]struct{}
	refs  map[*influxql.VarRef]struct{}
}

func newSelectInfo(n influxql.Node) *selectInfo {
	info := &selectInfo{
		exprs: make(map[string]influxql.Expr),
		calls: make(map[*influxql.Call]struct{}),
		refs:  make(map[*influxql.VarRef]struct{}),
	}
	influxql.Walk(info, n)
	return info
}

func (s *selectInfo) ProcessExpressions(fn func(expr influxql.Expr) error) error {
	for _, name := range s.names {
		if err := fn(s.exprs[name]); err != nil {
			return err
		}
	}
	return nil
}

func (s *selectInfo) Visit(n influxql.Node) influxql.Visitor {
	switch n.(type) {
	case *influxql.Call, *influxql.VarRef:
		// TODO(jsternberg): Identify if this is a math function and skip over
		// it if it is.
		sym := n.String()
		if _, ok := s.exprs[sym]; ok {
			return nil
		}
		s.names = append(s.names, sym)
		s.exprs[sym] = n.(influxql.Expr)

		switch expr := n.(type) {
		case *influxql.Call:
			s.calls[expr] = struct{}{}
		case *influxql.VarRef:
			s.refs[expr] = struct{}{}
		}
		return nil
	}
	return s
}

func (t *transpilerState) Transpile(ctx context.Context) (*query.Spec, error) {
	info := newSelectInfo(t.stmt.Fields)
	if len(info.exprs) == 0 {
		// TODO(jsternberg): Find the correct error message for this.
		// This is supposed to be handled in the compiler, but we haven't added
		// that yet.
		return nil, errors.New("no fields")
	}
	t.calls = info.calls

	// Use the list of expressions to create a specification for each of the
	// values that access data and fill in the operation id in the symbol table.
	if err := info.ProcessExpressions(func(expr influxql.Expr) (err error) {
		sym := symbol{id: expr.String()}
		sym.op, err = t.createIteratorSpec(expr)
		if err != nil {
			return err
		}
		t.symbols = append(t.symbols, sym)
		return nil
	}); err != nil {
		return nil, err
	}

	var (
		mapIn       query.OperationID
		symbolTable map[string]string
	)
	if len(t.symbols) > 1 {
		// We will need a join table for this.
		// Join the symbols together. This returns the join's operation id
		// and the mapping of expressions to the symbol in the join table.
		mapIn, symbolTable = t.join(t.symbols)
	} else {
		sym := t.symbols[0]
		symbolTable = map[string]string{sym.id: "_value"}
		mapIn = sym.op
	}

	// Map each of the fields in the symbol table to their appropriate column.
	if _, err := t.mapFields(mapIn, symbolTable); err != nil {
		return nil, err
	}
	return t.spec, nil
}

func (t *transpilerState) createIteratorSpec(expr influxql.Expr) (query.OperationID, error) {
	switch expr := expr.(type) {
	case *influxql.VarRef:
		return t.processVarRef(expr)
	case *influxql.Call:
		ref, err := t.processVarRef(expr.Args[0].(*influxql.VarRef))
		if err != nil {
			return "", err
		}

		// TODO(jsternberg): Handle group by tags and the windowing function.
		group := t.op("group", &functions.GroupOpSpec{
			By: []string{"_measurement"},
		}, ref)

		switch expr.Name {
		case "mean":
			return t.op("mean", &functions.MeanOpSpec{
				AggregateConfig: execute.AggregateConfig{
					TimeSrc: execute.DefaultStartColLabel,
				},
			}, group), nil
		case "max":
			//TODO add map after selector to handle src time
			//src := execute.DefaultStartColLabel
			//if len(t.calls) == 1 {
			//	src = execute.DefaultTimeColLabel
			//}
			return t.op("max", &functions.MaxOpSpec{
				SelectorConfig: execute.SelectorConfig{},
			}, group), nil
		}
	}
	return "", errors.New("unimplemented")
}

func (t *transpilerState) processVarRef(ref *influxql.VarRef) (query.OperationID, error) {
	if len(t.stmt.Sources) != 1 {
		// TODO(jsternberg): Support multiple sources.
		return "", errors.New("unimplemented: only one source is allowed")
	}

	// Only support a direct measurement. Subqueries are not supported yet.
	mm, ok := t.stmt.Sources[0].(*influxql.Measurement)
	if !ok {
		return "", errors.New("unimplemented: source must be a measurement")
	}

	// TODO(jsternberg): Verify the retention policy is the default one so we avoid
	// unexpected behavior.

	// Create the from spec and add it to the list of operations.
	// TODO(jsternberg): Autogenerate these IDs and track the resulting operation
	// so we can reference them from other locations.
	from := t.op("from", &functions.FromOpSpec{
		Database: mm.Database,
	})

	valuer := influxql.NowValuer{Now: t.now}
	cond, tr, err := influxql.ConditionExpr(t.stmt.Condition, &valuer)
	if err != nil {
		return "", err
	} else if cond != nil {
		// TODO(jsternberg): Handle conditions.
		return "", errors.New("unimplemented: conditions have not been implemented yet")
	}

	range_ := t.op("range", &functions.RangeOpSpec{
		Start: query.Time{Absolute: tr.MinTime()},
		Stop:  query.Time{Absolute: tr.MaxTime()},
	}, from)

	return t.op("filter", &functions.FilterOpSpec{
		Fn: &semantic.FunctionExpression{
			Params: []*semantic.FunctionParam{
				{Key: &semantic.Identifier{Name: "r"}},
			},
			Body: &semantic.LogicalExpression{
				Operator: ast.AndOperator,
				Left: &semantic.BinaryExpression{
					Operator: ast.EqualOperator,
					Left: &semantic.MemberExpression{
						Object:   &semantic.IdentifierExpression{Name: "r"},
						Property: "_measurement",
					},
					Right: &semantic.StringLiteral{Value: mm.Name},
				},
				Right: &semantic.BinaryExpression{
					Operator: ast.EqualOperator,
					Left: &semantic.MemberExpression{
						Object:   &semantic.IdentifierExpression{Name: "r"},
						Property: "_field",
					},
					Right: &semantic.StringLiteral{Value: ref.Val},
				},
			},
		},
	}, range_), nil
}

func (t *transpilerState) op(name string, spec query.OperationSpec, parents ...query.OperationID) query.OperationID {
	op := query.Operation{
		ID:   query.OperationID(fmt.Sprintf("%s%d", name, t.nextID[name])),
		Spec: spec,
	}
	t.spec.Operations = append(t.spec.Operations, &op)
	for _, pid := range parents {
		t.spec.Edges = append(t.spec.Edges, query.Edge{
			Parent: pid,
			Child:  op.ID,
		})
	}
	t.nextID[name]++
	return op.ID
}

type symbol struct {
	id string
	op query.OperationID
}

// join takes a mapping of expressions to operation ids and generates a join spec
// that merges all of them into a single table with an output of a table that
// contains mappings of each of the expressions to arbitrary symbol names.
func (t *transpilerState) join(symbols []symbol) (query.OperationID, map[string]string) {
	op := &functions.JoinOpSpec{
		// TODO(jsternberg): This is wrong and the join needs to be done on
		// what is basically a wildcard for everything except field.
		// This also needs to be an outer join, which is not currently supported
		// and is not capable of being expressed in the spec.
		On:         []string{"_measurement"},
		TableNames: make(map[query.OperationID]string),
	}

	parents := make([]query.OperationID, 0, len(symbols))
	joinTable := make(map[string]string, len(symbols))
	properties := make([]*semantic.Property, 0, len(symbols))
	for _, sym := range symbols {
		name := fmt.Sprintf("val%d", len(joinTable))
		joinTable[sym.id] = name
		op.TableNames[sym.op] = name
		parents = append(parents, sym.op)
		properties = append(properties, &semantic.Property{
			Key: &semantic.Identifier{Name: name},
			Value: &semantic.MemberExpression{
				Object: &semantic.IdentifierExpression{
					Name: "tables",
				},
				Property: name,
			},
		})
	}

	// Generate a function that takes the tables as an input and maps
	// each of the properties to itself.
	op.Fn = &semantic.FunctionExpression{
		Params: []*semantic.FunctionParam{{
			Key: &semantic.Identifier{Name: "tables"},
		}},
		Body: &semantic.ObjectExpression{
			Properties: properties,
		},
	}
	return t.op("join", op, parents...), joinTable
}

// columns to always pass through in the map operation
var passThrough = []string{"_time", "_measurement", "_field"}
var PassThroughProperties []*semantic.Property

func init() {
	PassThroughProperties = make([]*semantic.Property, len(passThrough))
	for i, name := range passThrough {
		PassThroughProperties[i] = &semantic.Property{
			Key: &semantic.Identifier{Name: name},
			Value: &semantic.MemberExpression{
				Object: &semantic.IdentifierExpression{
					Name: "r",
				},
				Property: name,
			},
		}
	}
}

// mapFields will take the list of symbols and maps each of the operations
// using the column names.
func (t *transpilerState) mapFields(in query.OperationID, symbols map[string]string) (query.OperationID, error) {
	columns := t.stmt.ColumnNames()
	if len(columns) != len(t.stmt.Fields) {
		// TODO(jsternberg): This scenario should not be possible. Replace the use of ColumnNames with a more
		// statically verifiable list of columns when we process the fields from the select statement instead
		// of doing this in the future.
		panic("number of columns does not match the number of fields")
	}

	properties := make([]*semantic.Property, len(t.stmt.Fields)+len(passThrough))
	copy(properties, PassThroughProperties)
	for i, f := range t.stmt.Fields {
		value, err := t.mapField(f.Expr, symbols)
		if err != nil {
			return "", err
		}
		properties[i+len(passThrough)] = &semantic.Property{
			Key:   &semantic.Identifier{Name: columns[i]},
			Value: value,
		}
	}
	return t.op("map", &functions.MapOpSpec{Fn: &semantic.FunctionExpression{
		Params: []*semantic.FunctionParam{{
			Key: &semantic.Identifier{Name: "r"},
		}},
		Body: &semantic.ObjectExpression{
			Properties: properties,
		},
	}}, in), nil
}

func (t *transpilerState) mapField(expr influxql.Expr, symbols map[string]string) (semantic.Expression, error) {
	switch expr := expr.(type) {
	case *influxql.Call, *influxql.VarRef:
		sym, ok := symbols[expr.String()]
		if !ok {
			return nil, fmt.Errorf("missing symbol for %s", expr)
		}
		return &semantic.MemberExpression{
			Object: &semantic.IdentifierExpression{
				Name: "r",
			},
			Property: sym,
		}, nil
	case *influxql.BinaryExpr:
		return t.evalBinaryExpr(expr, symbols)
	case *influxql.ParenExpr:
		return t.mapField(expr.Expr, symbols)
	case *influxql.StringLiteral:
		if ts, err := expr.ToTimeLiteral(time.UTC); err == nil {
			return &semantic.DateTimeLiteral{Value: ts.Val}, nil
		}
		return &semantic.StringLiteral{Value: expr.Val}, nil
	case *influxql.NumberLiteral:
		return &semantic.FloatLiteral{Value: expr.Val}, nil
	case *influxql.IntegerLiteral:
		return &semantic.IntegerLiteral{Value: expr.Val}, nil
	case *influxql.BooleanLiteral:
		return &semantic.BooleanLiteral{Value: expr.Val}, nil
	case *influxql.DurationLiteral:
		return &semantic.DurationLiteral{Value: expr.Val}, nil
	case *influxql.TimeLiteral:
		return &semantic.DateTimeLiteral{Value: expr.Val}, nil
	default:
		// TODO(jsternberg): Handle the other expressions by turning them into
		// an equivalent expression.
		return nil, fmt.Errorf("unimplemented: %s", expr)
	}
}

func (t *transpilerState) evalBinaryExpr(expr *influxql.BinaryExpr, symbols map[string]string) (semantic.Expression, error) {
	fn := func() func(left, right semantic.Expression) semantic.Expression {
		b := evalBuilder{}
		switch expr.Op {
		case influxql.EQ:
			return b.eval(ast.EqualOperator)
		case influxql.NEQ:
			return b.eval(ast.NotEqualOperator)
		case influxql.GT:
			return b.eval(ast.GreaterThanOperator)
		case influxql.GTE:
			return b.eval(ast.GreaterThanEqualOperator)
		case influxql.LT:
			return b.eval(ast.LessThanOperator)
		case influxql.LTE:
			return b.eval(ast.LessThanEqualOperator)
		case influxql.ADD:
			return b.eval(ast.AdditionOperator)
		case influxql.SUB:
			return b.eval(ast.SubtractionOperator)
		case influxql.AND:
			return b.logical(ast.AndOperator)
		case influxql.OR:
			return b.logical(ast.OrOperator)
		default:
			return nil
		}
	}()
	if fn == nil {
		return nil, fmt.Errorf("unimplemented binary expression: %s", expr)
	}

	lhs, err := t.mapField(expr.LHS, symbols)
	if err != nil {
		return nil, err
	}
	rhs, err := t.mapField(expr.RHS, symbols)
	if err != nil {
		return nil, err
	}
	return fn(lhs, rhs), nil
}

// evalBuilder is used for namespacing the logical and eval wrapping functions.
type evalBuilder struct{}

func (evalBuilder) logical(op ast.LogicalOperatorKind) func(left, right semantic.Expression) semantic.Expression {
	return func(left, right semantic.Expression) semantic.Expression {
		return &semantic.LogicalExpression{
			Operator: op,
			Left:     left,
			Right:    right,
		}
	}
}

func (evalBuilder) eval(op ast.OperatorKind) func(left, right semantic.Expression) semantic.Expression {
	return func(left, right semantic.Expression) semantic.Expression {
		return &semantic.BinaryExpression{
			Operator: op,
			Left:     left,
			Right:    right,
		}
	}
}
