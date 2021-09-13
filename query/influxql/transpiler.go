// Package influxql implements the transpiler for executing influxql queries in the 2.0 query engine.
package influxql

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxql"
)

// Transpiler converts InfluxQL queries into a query spec.
type Transpiler struct {
	Config         *Config
	dbrpMappingSvc influxdb.DBRPMappingService
}

func NewTranspiler(dbrpMappingSvc influxdb.DBRPMappingService) *Transpiler {
	return NewTranspilerWithConfig(dbrpMappingSvc, Config{})
}

func NewTranspilerWithConfig(dbrpMappingSvc influxdb.DBRPMappingService, cfg Config) *Transpiler {
	return &Transpiler{
		Config:         &cfg,
		dbrpMappingSvc: dbrpMappingSvc,
	}
}

func (t *Transpiler) Transpile(ctx context.Context, txt string) (*ast.Package, error) {
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
	return &ast.Package{
		Package: "main",
		Files: []*ast.File{
			transpiler.file,
		},
	}, nil
}

type transpilerState struct {
	stmt           *influxql.SelectStatement
	config         Config
	file           *ast.File
	assignments    map[string]ast.Expression
	dbrpMappingSvc influxdb.DBRPMappingService
}

func newTranspilerState(dbrpMappingSvc influxdb.DBRPMappingService, config *Config) *transpilerState {
	state := &transpilerState{
		file: &ast.File{
			Package: &ast.PackageClause{
				Name: &ast.Identifier{
					Name: "main",
				},
			},
		},
		assignments:    make(map[string]ast.Expression),
		dbrpMappingSvc: dbrpMappingSvc,
	}
	if config != nil {
		state.config = *config
	}
	if state.config.Now.IsZero() {
		// Stamp the current time using the now time.
		state.config.Now = time.Now()
	}
	return state
}

func (t *transpilerState) Transpile(ctx context.Context, id int, s influxql.Statement) error {
	expr, err := t.transpile(ctx, s)
	if err != nil {
		return err
	}
	t.file.Body = append(t.file.Body, &ast.ExpressionStatement{
		Expression: &ast.PipeExpression{
			Argument: expr,
			Call: &ast.CallExpression{
				Callee: &ast.Identifier{Name: "yield"},
				Arguments: []ast.Expression{
					&ast.ObjectExpression{
						Properties: []*ast.Property{
							{
								Key: &ast.Identifier{Name: "name"},
								Value: &ast.StringLiteral{
									Value: strconv.Itoa(id),
								},
							},
						},
					},
				},
			},
		},
	})
	return nil
}

func (t *transpilerState) transpile(ctx context.Context, s influxql.Statement) (ast.Expression, error) {
	switch stmt := s.(type) {
	case *influxql.SelectStatement:
		cur, err := t.transpileSelect(ctx, stmt)
		if err != nil {
			return nil, err
		}
		return cur.Expr(), nil
	case *influxql.ShowTagValuesStatement:
		return t.transpileShowTagValues(ctx, stmt)
	case *influxql.ShowDatabasesStatement:
		return t.transpileShowDatabases(ctx, stmt)
	case *influxql.ShowRetentionPoliciesStatement:
		return t.transpileShowRetentionPolicies(ctx, stmt)
	default:
		return nil, fmt.Errorf("unknown statement type %T", s)
	}
}

func (t *transpilerState) transpileShowTagValues(ctx context.Context, stmt *influxql.ShowTagValuesStatement) (ast.Expression, error) {
	// While the ShowTagValuesStatement contains a sources section and those sources are measurements, they do
	// not actually contain the database and we do not factor in retention policies. So we are always going to use
	// the default retention policy when evaluating which bucket we are querying and we do not have to consult
	// the sources in the statement.
	if stmt.Database == "" {
		if t.config.DefaultDatabase == "" {
			return nil, errDatabaseNameRequired
		}
		stmt.Database = t.config.DefaultDatabase
	}

	expr, err := t.from(&influxql.Measurement{Database: stmt.Database})
	if err != nil {
		return nil, err
	}

	// TODO(jsternberg): Read the range from the condition expression. 1.x doesn't actually do this so it isn't
	// urgent to implement this functionality so we can use the default range.
	expr = &ast.PipeExpression{
		Argument: expr,
		Call: &ast.CallExpression{
			Callee: &ast.Identifier{
				Name: "range",
			},
			Arguments: []ast.Expression{
				&ast.ObjectExpression{
					Properties: []*ast.Property{
						{
							Key: &ast.Identifier{
								Name: "start",
							},
							Value: &ast.DurationLiteral{
								Values: []ast.Duration{{
									Magnitude: -1,
									Unit:      "h",
								}},
							},
						},
					},
				},
			},
		},
	}

	// If we have a list of sources, look through it and add each of the measurement names.
	measurementNames := make([]string, 0, len(stmt.Sources))
	for _, source := range stmt.Sources {
		mm := source.(*influxql.Measurement)
		measurementNames = append(measurementNames, mm.Name)
	}

	if len(measurementNames) > 0 {
		var filterExpr ast.Expression = &ast.BinaryExpression{
			Operator: ast.EqualOperator,
			Left: &ast.MemberExpression{
				Object:   &ast.Identifier{Name: "r"},
				Property: &ast.Identifier{Name: "_measurement"},
			},
			Right: &ast.StringLiteral{
				Value: measurementNames[len(measurementNames)-1],
			},
		}
		for i := len(measurementNames) - 2; i >= 0; i-- {
			filterExpr = &ast.LogicalExpression{
				Operator: ast.OrOperator,
				Left: &ast.BinaryExpression{
					Operator: ast.EqualOperator,
					Left: &ast.MemberExpression{
						Object:   &ast.Identifier{Name: "r"},
						Property: &ast.Identifier{Name: "_measurement"},
					},
					Right: &ast.StringLiteral{
						Value: measurementNames[i],
					},
				},
				Right: filterExpr,
			}
		}
		expr = &ast.PipeExpression{
			Argument: expr,
			Call: &ast.CallExpression{
				Callee: &ast.Identifier{
					Name: "filter",
				},
				Arguments: []ast.Expression{
					&ast.ObjectExpression{
						Properties: []*ast.Property{
							{
								Key: &ast.Identifier{Name: "fn"},
								Value: &ast.FunctionExpression{
									Params: []*ast.Property{
										{
											Key: &ast.Identifier{Name: "r"},
										},
									},
									Body: filterExpr,
								},
							},
						},
					},
				},
			},
		}
	}

	// TODO(jsternberg): Add the condition filter for the where clause.

	// Create the key values op spec from the
	var keyColumns []ast.Expression
	switch expr := stmt.TagKeyExpr.(type) {
	case *influxql.ListLiteral:
		keyColumns = make([]ast.Expression, 0, len(expr.Vals))
		for _, name := range expr.Vals {
			keyColumns = append(keyColumns, &ast.StringLiteral{
				Value: name,
			})
		}
	case *influxql.StringLiteral:
		switch stmt.Op {
		case influxql.EQ:
			keyColumns = []ast.Expression{
				&ast.StringLiteral{
					Value: expr.Val,
				},
			}
		case influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
			return nil, fmt.Errorf("unimplemented: tag key operand: %s", stmt.Op)
		default:
			return nil, fmt.Errorf("unsupported operand: %s", stmt.Op)
		}
	default:
		return nil, fmt.Errorf("unsupported literal type: %T", expr)
	}
	expr = &ast.PipeExpression{
		Argument: expr,
		Call: &ast.CallExpression{
			Callee: &ast.Identifier{
				Name: "keyValues",
			},
			Arguments: []ast.Expression{
				&ast.ObjectExpression{
					Properties: []*ast.Property{
						{
							Key: &ast.Identifier{
								Name: "keyColumns",
							},
							Value: &ast.ArrayExpression{
								Elements: keyColumns,
							},
						},
					},
				},
			},
		},
	}

	// Group by the measurement and key, find distinct values, then group by the measurement
	// to join all of the different keys together. Finish by renaming the columns. This is static.
	return &ast.PipeExpression{
		Argument: &ast.PipeExpression{
			Argument: &ast.PipeExpression{
				Argument: &ast.PipeExpression{
					Argument: expr,
					Call: &ast.CallExpression{
						Callee: &ast.Identifier{Name: "group"},
						Arguments: []ast.Expression{
							&ast.ObjectExpression{
								Properties: []*ast.Property{
									{
										Key: &ast.Identifier{
											Name: "columns",
										},
										Value: &ast.ArrayExpression{
											Elements: []ast.Expression{
												&ast.StringLiteral{Value: "_measurement"},
												&ast.StringLiteral{Value: "_key"},
											},
										},
									},
									{
										Key: &ast.Identifier{
											Name: "mode",
										},
										Value: &ast.StringLiteral{
											Value: "by",
										},
									},
								},
							},
						},
					},
				},
				Call: &ast.CallExpression{
					Callee: &ast.Identifier{Name: "distinct"},
				},
			},
			Call: &ast.CallExpression{
				Callee: &ast.Identifier{Name: "group"},
				Arguments: []ast.Expression{
					&ast.ObjectExpression{
						Properties: []*ast.Property{
							{
								Key: &ast.Identifier{
									Name: "columns",
								},
								Value: &ast.ArrayExpression{
									Elements: []ast.Expression{
										&ast.StringLiteral{Value: "_measurement"},
									},
								},
							},
							{
								Key: &ast.Identifier{
									Name: "mode",
								},
								Value: &ast.StringLiteral{
									Value: "by",
								},
							},
						},
					},
				},
			},
		},
		Call: &ast.CallExpression{
			Callee: &ast.Identifier{Name: "rename"},
			Arguments: []ast.Expression{
				&ast.ObjectExpression{
					Properties: []*ast.Property{
						{
							Key: &ast.Identifier{
								Name: "columns",
							},
							Value: &ast.ObjectExpression{
								Properties: []*ast.Property{
									{
										Key: &ast.Identifier{
											Name: "_key",
										},
										Value: &ast.StringLiteral{
											Value: "key",
										},
									},
									{
										Key: &ast.Identifier{
											Name: "_value",
										},
										Value: &ast.StringLiteral{
											Value: "value",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}, nil
}

func (t *transpilerState) transpileShowDatabases(ctx context.Context, stmt *influxql.ShowDatabasesStatement) (ast.Expression, error) {
	v1 := t.requireImport("influxdata/influxdb/v1")
	return &ast.PipeExpression{
		Argument: &ast.PipeExpression{
			Argument: &ast.CallExpression{
				Callee: &ast.MemberExpression{
					Object: v1,
					Property: &ast.Identifier{
						Name: "databases",
					},
				},
			},
			Call: &ast.CallExpression{
				Callee: &ast.Identifier{
					Name: "rename",
				},
				Arguments: []ast.Expression{
					&ast.ObjectExpression{
						Properties: []*ast.Property{
							{
								Key: &ast.Identifier{
									Name: "columns",
								},
								Value: &ast.ObjectExpression{
									Properties: []*ast.Property{
										{
											Key: &ast.Identifier{Name: "databaseName"},
											Value: &ast.StringLiteral{
												Value: "name",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		Call: &ast.CallExpression{
			Callee: &ast.Identifier{
				Name: "keep",
			},
			Arguments: []ast.Expression{
				&ast.ObjectExpression{
					Properties: []*ast.Property{
						{
							Key: &ast.Identifier{
								Name: "columns",
							},
							Value: &ast.ArrayExpression{
								Elements: []ast.Expression{
									&ast.StringLiteral{
										Value: "name",
									},
								},
							},
						},
					},
				},
			},
		},
	}, nil
}

func (t *transpilerState) transpileShowRetentionPolicies(ctx context.Context, stmt *influxql.ShowRetentionPoliciesStatement) (ast.Expression, error) {
	v1 := t.requireImport("influxdata/influxdb/v1")
	return &ast.PipeExpression{
		Argument: &ast.PipeExpression{
			Argument: &ast.PipeExpression{
				Argument: &ast.PipeExpression{
					Argument: &ast.PipeExpression{
						Argument: &ast.CallExpression{
							Callee: &ast.MemberExpression{
								Object: v1,
								Property: &ast.Identifier{
									Name: "databases",
								},
							},
						},
						Call: &ast.CallExpression{
							Callee: &ast.Identifier{
								Name: "filter",
							},
							Arguments: []ast.Expression{
								&ast.ObjectExpression{
									Properties: []*ast.Property{
										{
											Key: &ast.Identifier{
												Name: "fn",
											},
											Value: &ast.FunctionExpression{
												Params: []*ast.Property{
													{
														Key: &ast.Identifier{
															Name: "r",
														},
													},
												},
												Body: &ast.BinaryExpression{
													Operator: ast.EqualOperator,
													Left: &ast.MemberExpression{
														Object: &ast.Identifier{
															Name: "r",
														},
														Property: &ast.Identifier{
															Name: "databaseName",
														},
													},
													Right: &ast.StringLiteral{
														Value: stmt.Database,
													},
												},
											},
										},
									},
								},
							},
						},
					},
					Call: &ast.CallExpression{
						Callee: &ast.Identifier{
							Name: "rename",
						},
						Arguments: []ast.Expression{
							&ast.ObjectExpression{
								Properties: []*ast.Property{
									{
										Key: &ast.Identifier{Name: "columns"},
										Value: &ast.ObjectExpression{
											Properties: []*ast.Property{
												{
													Key:   &ast.Identifier{Name: "retentionPolicy"},
													Value: &ast.StringLiteral{Value: "name"},
												},
												{
													Key:   &ast.Identifier{Name: "retentionPeriod"},
													Value: &ast.StringLiteral{Value: "duration"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				Call: &ast.CallExpression{
					Callee: &ast.Identifier{
						Name: "set",
					},
					Arguments: []ast.Expression{
						&ast.ObjectExpression{
							Properties: []*ast.Property{
								{
									Key: &ast.Identifier{Name: "key"},
									Value: &ast.StringLiteral{
										Value: "shardGroupDuration",
									},
								},
								{
									Key: &ast.Identifier{Name: "value"},
									Value: &ast.StringLiteral{
										Value: "0",
									},
								},
							},
						},
					},
				},
			},
			Call: &ast.CallExpression{
				Callee: &ast.Identifier{
					Name: "set",
				},
				Arguments: []ast.Expression{
					&ast.ObjectExpression{
						Properties: []*ast.Property{
							{
								Key: &ast.Identifier{Name: "key"},
								Value: &ast.StringLiteral{
									Value: "replicaN",
								},
							},
							{
								Key: &ast.Identifier{Name: "value"},
								Value: &ast.StringLiteral{
									Value: "2",
								},
							},
						},
					},
				},
			},
		},
		Call: &ast.CallExpression{
			Callee: &ast.Identifier{
				Name: "keep",
			},
			Arguments: []ast.Expression{
				&ast.ObjectExpression{
					Properties: []*ast.Property{
						{
							Key: &ast.Identifier{
								Name: "columns",
							},
							Value: &ast.ArrayExpression{
								Elements: []ast.Expression{
									&ast.StringLiteral{Value: "name"},
									&ast.StringLiteral{Value: "duration"},
									&ast.StringLiteral{Value: "shardGroupDuration"},
									&ast.StringLiteral{Value: "replicaN"},
									&ast.StringLiteral{Value: "default"},
								},
							},
						},
					},
				},
			},
		},
	}, nil
}

func (t *transpilerState) transpileSelect(ctx context.Context, stmt *influxql.SelectStatement) (cursor, error) {
	// Clone the select statement and omit the time from the list of column names.
	t.stmt = stmt.Clone()
	t.stmt.OmitTime = true

	groups, err := identifyGroups(t.stmt)
	if err != nil {
		return nil, err
	} else if len(groups) == 0 {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "unable to transpile: at least one non-time field must be queried",
		}
	}

	cursors := make([]cursor, 0, len(groups))
	for _, gr := range groups {
		cur, err := gr.createCursor(t)
		if err != nil {
			return nil, err
		}
		cursors = append(cursors, cur)
	}

	// Join the cursors together on the measurement name.
	// TODO(jsternberg): This needs to join on all remaining group keys.
	cur := Join(t, cursors, []string{"_time", "_measurement"})

	// Map each of the fields into another cursor. This evaluates any lingering expressions.
	cur, err = t.mapFields(cur)
	if err != nil {
		return nil, err
	}
	return cur, nil
}

func (t *transpilerState) mapType(ref *influxql.VarRef) influxql.DataType {
	// TODO(jsternberg): Actually evaluate the type against the schema.
	return influxql.Tag
}

func (t *transpilerState) from(m *influxql.Measurement) (ast.Expression, error) {
	var args []ast.Expression
	// Use the bucket inteasd of dbrp mapping if it exists.
	if t.config.Bucket != "" {
		args = []ast.Expression{
			&ast.ObjectExpression{
				Properties: []*ast.Property{
					{
						Key: &ast.Identifier{
							Name: "bucket",
						},
						Value: &ast.StringLiteral{
							Value: t.config.Bucket,
						},
					},
				},
			},
		}
	} else {
		if t.dbrpMappingSvc == nil {
			return nil, &errors.Error{
				Code: errors.EInternal,
				Msg:  "unable to transpile: db and rp mappings need to be created by some way",
			}
		}
		db, rp := m.Database, m.RetentionPolicy
		if db == "" {
			if t.config.DefaultDatabase == "" {
				return nil, &errors.Error{
					Code: errors.EInvalid,
					Msg:  "unable to transpile: database is required",
				}
			}
			db = t.config.DefaultDatabase
		}
		if rp == "" {
			if t.config.DefaultRetentionPolicy != "" {
				rp = t.config.DefaultRetentionPolicy
			}
		}

		var filter influxdb.DBRPMappingFilter
		if db != "" {
			filter.Database = &db
		}
		if rp != "" {
			filter.RetentionPolicy = &rp
		}
		defaultRP := rp == ""
		filter.Default = &defaultRP
		mappings, _, err := t.dbrpMappingSvc.FindMany(context.TODO(), filter)
		if err != nil || len(mappings) == 0 {
			if !t.config.FallbackToDBRP {
				return nil, err
			}
			// use `db/rp` naming convention
			args = []ast.Expression{
				&ast.ObjectExpression{
					Properties: []*ast.Property{
						{
							Key: &ast.Identifier{
								Name: "bucket",
							},
							Value: &ast.StringLiteral{
								Value: fmt.Sprintf("%s/%s", db, rp),
							},
						},
					},
				},
			}
		} else {
			// use mapping bucket id
			args = []ast.Expression{
				&ast.ObjectExpression{
					Properties: []*ast.Property{
						{
							Key: &ast.Identifier{
								Name: "bucketID",
							},
							Value: &ast.StringLiteral{
								Value: mappings[0].BucketID.String(),
							},
						},
					},
				},
			}
		}
	}

	return &ast.CallExpression{
		Callee: &ast.Identifier{
			Name: "from",
		},
		Arguments: args,
	}, nil
}

func (t *transpilerState) assignment(expr ast.Expression) *ast.Identifier {
	for i := 0; ; i++ {
		key := fmt.Sprintf("t%d", i)
		if _, ok := t.assignments[key]; !ok {
			ident := &ast.Identifier{Name: key}
			t.assignments[key] = expr
			t.file.Body = append(t.file.Body, &ast.VariableAssignment{
				ID:   ident,
				Init: expr,
			})
			return ident
		}
	}
}

// requireImport will ensure the import is included in the file and return
// the variable used to access the package.
func (t *transpilerState) requireImport(pkgpath string) *ast.Identifier {
	// Check to see if the import already exists.
	for _, decl := range t.file.Imports {
		if decl.Path.Value == pkgpath {
			return decl.As
		}
	}

	// Append the import to the file.
	as := &ast.Identifier{
		Name: filepath.Base(pkgpath),
	}
	t.file.Imports = append(t.file.Imports, &ast.ImportDeclaration{
		Path: &ast.StringLiteral{
			Value: pkgpath,
		},
		As: as,
	})
	return as
}
