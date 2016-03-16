package influxql

import "errors"

// RewriteStatement rewrites stmt into a new statement, if applicable.
func RewriteStatement(stmt Statement) (Statement, error) {
	switch stmt := stmt.(type) {
	case *ShowFieldKeysStatement:
		return rewriteShowFieldKeysStatement(stmt)
	case *ShowMeasurementsStatement:
		return rewriteShowMeasurementsStatement(stmt)
	case *ShowSeriesStatement:
		return rewriteShowSeriesStatement(stmt)
	case *ShowTagKeysStatement:
		return rewriteShowTagKeysStatement(stmt)
	case *ShowTagValuesStatement:
		return rewriteShowTagValuesStatement(stmt)
	default:
		return stmt, nil
	}
}

func rewriteShowFieldKeysStatement(stmt *ShowFieldKeysStatement) (Statement, error) {
	return &SelectStatement{
		Fields: Fields([]*Field{
			{Expr: &VarRef{Val: "fieldKey"}},
		}),
		Sources: Sources([]Source{
			&Measurement{Name: "_fieldKeys"},
		}),
		Condition:  rewriteSourcesCondition(stmt.Sources, nil),
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		SortFields: stmt.SortFields,
		OmitTime:   true,
		Dedupe:     true,
	}, nil
}

func rewriteShowMeasurementsStatement(stmt *ShowMeasurementsStatement) (Statement, error) {
	// Check for time in WHERE clause (not supported).
	if HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW MEASUREMENTS doesn't support time in WHERE clause")
	}

	condition := stmt.Condition
	if stmt.Source != nil {
		condition = rewriteSourcesCondition(Sources([]Source{stmt.Source}), stmt.Condition)
	}

	return &SelectStatement{
		Fields: Fields([]*Field{
			{Expr: &VarRef{Val: "_name"}, Alias: "name"},
		}),
		Sources: Sources([]Source{
			&Measurement{Name: "_measurements"},
		}),
		Condition:  condition,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		SortFields: stmt.SortFields,
		OmitTime:   true,
		Dedupe:     true,
	}, nil
}

func rewriteShowSeriesStatement(stmt *ShowSeriesStatement) (Statement, error) {
	// Check for time in WHERE clause (not supported).
	if HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW SERIES doesn't support time in WHERE clause")
	}

	return &SelectStatement{
		Fields: []*Field{
			{Expr: &VarRef{Val: "key"}},
		},
		Sources: []Source{
			&Measurement{Name: "_series"},
		},
		Condition:  rewriteSourcesCondition(stmt.Sources, stmt.Condition),
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		SortFields: stmt.SortFields,
		OmitTime:   true,
		Dedupe:     true,
	}, nil
}

func rewriteShowTagValuesStatement(stmt *ShowTagValuesStatement) (Statement, error) {
	// Check for time in WHERE clause (not supported).
	if HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW TAG VALUES doesn't support time in WHERE clause")
	}

	condition := stmt.Condition
	if len(stmt.TagKeys) > 0 {
		var expr Expr
		for _, tagKey := range stmt.TagKeys {
			tagExpr := &BinaryExpr{
				Op:  EQ,
				LHS: &VarRef{Val: "_tagKey"},
				RHS: &StringLiteral{Val: tagKey},
			}

			if expr != nil {
				expr = &BinaryExpr{
					Op:  OR,
					LHS: expr,
					RHS: tagExpr,
				}
			} else {
				expr = tagExpr
			}
		}

		// Set condition or "AND" together.
		if condition == nil {
			condition = expr
		} else {
			condition = &BinaryExpr{
				Op:  AND,
				LHS: &ParenExpr{Expr: condition},
				RHS: &ParenExpr{Expr: expr},
			}
		}
	}
	condition = rewriteSourcesCondition(stmt.Sources, condition)

	return &SelectStatement{
		Fields: []*Field{
			{Expr: &VarRef{Val: "_tagKey"}, Alias: "key"},
			{Expr: &VarRef{Val: "value"}},
		},
		Sources: []Source{
			&Measurement{Name: "_tags"},
		},
		Condition:  condition,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		SortFields: stmt.SortFields,
		OmitTime:   true,
		Dedupe:     true,
	}, nil
}

func rewriteShowTagKeysStatement(stmt *ShowTagKeysStatement) (Statement, error) {
	// Check for time in WHERE clause (not supported).
	if HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW TAG KEYS doesn't support time in WHERE clause")
	}

	return &SelectStatement{
		Fields: []*Field{
			{Expr: &VarRef{Val: "tagKey"}},
		},
		Sources: []Source{
			&Measurement{Name: "_tagKeys"},
		},
		Condition:  rewriteSourcesCondition(stmt.Sources, stmt.Condition),
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		SortFields: stmt.SortFields,
		OmitTime:   true,
		Dedupe:     true,
	}, nil
}

// rewriteSourcesCondition rewrites sources into `name` expressions.
// Merges with cond and returns a new condition.
func rewriteSourcesCondition(sources Sources, cond Expr) Expr {
	if len(sources) == 0 {
		return cond
	}

	// Generate an OR'd set of filters on source name.
	var scond Expr
	for _, source := range sources {
		mm := source.(*Measurement)

		// Generate a filtering expression on the measurement name.
		var expr Expr
		if mm.Regex != nil {
			expr = &BinaryExpr{
				Op:  EQREGEX,
				LHS: &VarRef{Val: "_name"},
				RHS: &RegexLiteral{Val: mm.Regex.Val},
			}
		} else if mm.Name != "" {
			expr = &BinaryExpr{
				Op:  EQ,
				LHS: &VarRef{Val: "_name"},
				RHS: &StringLiteral{Val: mm.Name},
			}
		}

		if scond == nil {
			scond = expr
		} else {
			scond = &BinaryExpr{
				Op:  OR,
				LHS: scond,
				RHS: expr,
			}
		}
	}

	if cond != nil {
		return &BinaryExpr{
			Op:  AND,
			LHS: &ParenExpr{Expr: scond},
			RHS: &ParenExpr{Expr: cond},
		}
	}
	return scond
}
