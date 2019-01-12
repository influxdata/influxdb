// Package influxql implements the transpiler for executing influxql queries in the 2.0 query engine.
package influxql

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/flux/stdlib/universe"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/query/stdlib/influxdata/influxdb/v1"
	"github.com/influxdata/influxql"
)

// Transpiler converts InfluxQL queries into a query spec.
type Transpiler struct {
	Config         *Config
	dbrpMappingSvc platform.DBRPMappingService
}

func NewTranspiler(dbrpMappingSvc platform.DBRPMappingService) *Transpiler {
	return NewTranspilerWithConfig(dbrpMappingSvc, Config{})
}

func NewTranspilerWithConfig(dbrpMappingSvc platform.DBRPMappingService, cfg Config) *Transpiler {
	return &Transpiler{
		Config:         &cfg,
		dbrpMappingSvc: dbrpMappingSvc,
	}
}

func (t *Transpiler) Transpile(ctx context.Context, txt string) (*flux.Spec, error) {
	// Parse the text of the query.
	q, err := influxql.ParseQuery(txt)
	if err != nil {
		return nil, err
	}

	transpiler := newTranspilerState(t.dbrpMappingSvc, t.Config)
	for i, s := range q.Statements {
		if err := transpiler.Transpile(ctx, i, s); err != nil {
			return nil, err
		}
	}
	return transpiler.spec, nil
}

type transpilerState struct {
	stmt           *influxql.SelectStatement
	config         Config
	spec           *flux.Spec
	nextID         map[string]int
	dbrpMappingSvc platform.DBRPMappingService
}

func newTranspilerState(dbrpMappingSvc platform.DBRPMappingService, config *Config) *transpilerState {
	state := &transpilerState{
		spec:           &flux.Spec{},
		nextID:         make(map[string]int),
		dbrpMappingSvc: dbrpMappingSvc,
	}
	if config != nil {
		state.config = *config
	}
	if state.config.NowFn == nil {
		state.config.NowFn = time.Now
	}

	// Stamp the current time using the now function from the config or the default.
	state.spec.Now = state.config.NowFn()
	return state
}

func (t *transpilerState) Transpile(ctx context.Context, id int, s influxql.Statement) error {
	op, err := t.transpile(ctx, s)
	if err != nil {
		return err
	}
	t.op("yield", &universe.YieldOpSpec{Name: strconv.Itoa(id)}, op)
	return nil
}

func (t *transpilerState) transpile(ctx context.Context, s influxql.Statement) (flux.OperationID, error) {
	switch stmt := s.(type) {
	case *influxql.SelectStatement:
		return t.transpileSelect(ctx, stmt)
	case *influxql.ShowTagValuesStatement:
		return t.transpileShowTagValues(ctx, stmt)
	case *influxql.ShowDatabasesStatement:
		return t.transpileShowDatabases(ctx, stmt)
	case *influxql.ShowRetentionPoliciesStatement:
		return t.transpileShowRetentionPolicies(ctx, stmt)
	default:
		return "", fmt.Errorf("unknown statement type %T", s)
	}
}

func (t *transpilerState) transpileShowTagValues(ctx context.Context, stmt *influxql.ShowTagValuesStatement) (flux.OperationID, error) {
	// While the ShowTagValuesStatement contains a sources section and those sources are measurements, they do
	// not actually contain the database and we do not factor in retention policies. So we are always going to use
	// the default retention policy when evaluating which bucket we are querying and we do not have to consult
	// the sources in the statement.
	if stmt.Database == "" {
		if t.config.DefaultDatabase == "" {
			return "", errDatabaseNameRequired
		}
		stmt.Database = t.config.DefaultDatabase
	}

	op, err := t.from(&influxql.Measurement{Database: stmt.Database})
	if err != nil {
		return "", err
	}

	// TODO(jsternberg): Read the range from the condition expression. 1.x doesn't actually do this so it isn't
	// urgent to implement this functionality so we can use the default range.
	op = t.op("range", &universe.RangeOpSpec{
		Start: flux.Time{
			Relative:   -time.Hour,
			IsRelative: true,
		},
		Stop: flux.Now,
	}, op)

	// If we have a list of sources, look through it and add each of the measurement names.
	measurementNames := make([]string, 0, len(stmt.Sources))
	for _, source := range stmt.Sources {
		mm := source.(*influxql.Measurement)
		measurementNames = append(measurementNames, mm.Name)
	}

	if len(measurementNames) > 0 {
		var expr semantic.Expression = &semantic.BinaryExpression{
			Operator: ast.EqualOperator,
			Left: &semantic.MemberExpression{
				Object:   &semantic.IdentifierExpression{Name: "r"},
				Property: "_measurement",
			},
			Right: &semantic.StringLiteral{Value: measurementNames[len(measurementNames)-1]},
		}
		for i := len(measurementNames) - 2; i >= 0; i-- {
			expr = &semantic.LogicalExpression{
				Operator: ast.OrOperator,
				Left: &semantic.BinaryExpression{
					Operator: ast.EqualOperator,
					Left: &semantic.MemberExpression{
						Object:   &semantic.IdentifierExpression{Name: "r"},
						Property: "_measurement",
					},
					Right: &semantic.StringLiteral{Value: measurementNames[i]},
				},
				Right: expr,
			}
		}
		op = t.op("filter", &universe.FilterOpSpec{
			Fn: &semantic.FunctionExpression{
				Block: &semantic.FunctionBlock{
					Parameters: &semantic.FunctionParameters{
						List: []*semantic.FunctionParameter{
							{Key: &semantic.Identifier{Name: "r"}},
						},
					},
					Body: expr,
				},
			},
		}, op)
	}

	// TODO(jsternberg): Add the condition filter for the where clause.

	// Create the key values op spec from the
	var keyValues universe.KeyValuesOpSpec
	switch expr := stmt.TagKeyExpr.(type) {
	case *influxql.ListLiteral:
		keyValues.KeyColumns = expr.Vals
	case *influxql.StringLiteral:
		switch stmt.Op {
		case influxql.EQ:
			keyValues.KeyColumns = []string{expr.Val}
		case influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
			return "", fmt.Errorf("unimplemented: tag key operand: %s", stmt.Op)
		default:
			return "", fmt.Errorf("unsupported operand: %s", stmt.Op)
		}
	default:
		return "", fmt.Errorf("unsupported literal type: %T", expr)
	}
	op = t.op("keyValues", &keyValues, op)

	// Group by the measurement and key, find distinct values, then group by the measurement
	// to join all of the different keys together. Finish by renaming the columns. This is static.
	return t.op("rename", &universe.RenameOpSpec{
		Columns: map[string]string{
			"_key":   "key",
			"_value": "value",
		},
	}, t.op("group", &universe.GroupOpSpec{
		Columns: []string{"_measurement"},
		Mode:    "by",
	}, t.op("distinct", &universe.DistinctOpSpec{
		Column: execute.DefaultValueColLabel,
	}, t.op("group", &universe.GroupOpSpec{
		Columns: []string{"_measurement", "_key"},
		Mode:    "by",
	}, op)))), nil
}

func (t *transpilerState) transpileShowDatabases(ctx context.Context, stmt *influxql.ShowDatabasesStatement) (flux.OperationID, error) {
	// While the ShowTagValuesStatement contains a sources section and those sources are measurements, they do
	// not actually contain the database and we do not factor in retention policies. So we are always going to use
	// the default retention policy when evaluating which bucket we are querying and we do not have to consult
	// the sources in the statement.

	spec := &v1.DatabasesOpSpec{}
	op := t.op("databases", spec)

	// SHOW DATABASES has one column, name
	return t.op("extractcol", &universe.KeepOpSpec{
		Columns: []string{
			"name",
		},
	}, t.op("rename", &universe.RenameOpSpec{
		Columns: map[string]string{
			"databaseName": "name",
		},
	}, op)), nil
}

func (t *transpilerState) transpileShowRetentionPolicies(ctx context.Context, stmt *influxql.ShowRetentionPoliciesStatement) (flux.OperationID, error) {
	// While the ShowTagValuesStatement contains a sources section and those sources are measurements, they do
	// not actually contain the database and we do not factor in retention policies. So we are always going to use
	// the default retention policy when evaluating which bucket we are querying and we do not have to consult
	// the sources in the statement.

	spec := &v1.DatabasesOpSpec{}
	op := t.op("databases", spec)
	var expr semantic.Expression = &semantic.BinaryExpression{
		Operator: ast.EqualOperator,
		Left: &semantic.MemberExpression{
			Object:   &semantic.IdentifierExpression{Name: "r"},
			Property: "databaseName",
		},
		Right: &semantic.StringLiteral{Value: stmt.Database},
	}

	op = t.op("filter", &universe.FilterOpSpec{
		Fn: &semantic.FunctionExpression{
			Block: &semantic.FunctionBlock{
				Parameters: &semantic.FunctionParameters{
					List: []*semantic.FunctionParameter{
						{Key: &semantic.Identifier{Name: "r"}},
					},
				},
				Body: expr,
			},
		},
	}, op)

	return t.op("keep",
		&universe.KeepOpSpec{
			Columns: []string{
				"name",
				"duration",
				"shardGroupDuration",
				"replicaN",
				"default",
			},
		},
		t.op("set",
			&universe.SetOpSpec{
				Key:   "replicaN",
				Value: "2",
			},
			t.op("set",
				&universe.SetOpSpec{
					Key:   "shardGroupDuration",
					Value: "0",
				},
				t.op("rename",
					&universe.RenameOpSpec{
						Columns: map[string]string{
							"retentionPolicy": "name",
							"retentionPeriod": "duration",
						},
					},
					op)))), nil
}

func (t *transpilerState) transpileSelect(ctx context.Context, stmt *influxql.SelectStatement) (flux.OperationID, error) {
	// Clone the select statement and omit the time from the list of column names.
	t.stmt = stmt.Clone()
	t.stmt.OmitTime = true

	groups, err := identifyGroups(t.stmt)
	if err != nil {
		return "", err
	} else if len(groups) == 0 {
		return "", errors.New("at least 1 non-time field must be queried")
	}

	cursors := make([]cursor, 0, len(groups))
	for _, gr := range groups {
		cur, err := gr.createCursor(t)
		if err != nil {
			return "", err
		}
		cursors = append(cursors, cur)
	}

	// Join the cursors together on the measurement name.
	// TODO(jsternberg): This needs to join on all remaining group keys.
	cur := Join(t, cursors, []string{"_time", "_measurement"})

	// Map each of the fields into another cursor. This evaluates any lingering expressions.
	cur, err = t.mapFields(cur)
	if err != nil {
		return "", err
	}
	return cur.ID(), nil
}

func (t *transpilerState) mapType(ref *influxql.VarRef) influxql.DataType {
	// TODO(jsternberg): Actually evaluate the type against the schema.
	return influxql.Tag
}

func (t *transpilerState) from(m *influxql.Measurement) (flux.OperationID, error) {
	db, rp := m.Database, m.RetentionPolicy
	if db == "" {
		if t.config.DefaultDatabase == "" {
			return "", errors.New("database is required")
		}
		db = t.config.DefaultDatabase
	}
	if rp == "" {
		if t.config.DefaultRetentionPolicy != "" {
			rp = t.config.DefaultRetentionPolicy
		}
	}

	var filter platform.DBRPMappingFilter
	filter.Cluster = &t.config.Cluster
	if db != "" {
		filter.Database = &db
	}
	if rp != "" {
		filter.RetentionPolicy = &rp
	}
	defaultRP := rp == ""
	filter.Default = &defaultRP
	mapping, err := t.dbrpMappingSvc.Find(context.TODO(), filter)
	if err != nil {
		return "", err
	}

	spec := &influxdb.FromOpSpec{
		BucketID: mapping.BucketID.String(),
	}
	return t.op("from", spec), nil
}

func (t *transpilerState) op(name string, spec flux.OperationSpec, parents ...flux.OperationID) flux.OperationID {
	op := flux.Operation{
		ID:   flux.OperationID(fmt.Sprintf("%s%d", name, t.nextID[name])),
		Spec: spec,
	}
	t.spec.Operations = append(t.spec.Operations, &op)
	for _, pid := range parents {
		t.spec.Edges = append(t.spec.Edges, flux.Edge{
			Parent: pid,
			Child:  op.ID,
		})
	}
	t.nextID[name]++
	return op.ID
}
