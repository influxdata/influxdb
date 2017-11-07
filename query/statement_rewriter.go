package query

import (
	"errors"
	"regexp"

	"github.com/influxdata/influxql"
)

// RewriteStatement rewrites stmt into a new statement, if applicable.
func RewriteStatement(stmt influxql.Statement) (influxql.Statement, error) {
	switch stmt := stmt.(type) {
	case *influxql.ShowFieldKeysStatement:
		return rewriteShowFieldKeysStatement(stmt)
	case *influxql.ShowFieldKeyCardinalityStatement:
		return rewriteShowFieldKeyCardinalityStatement(stmt)
	case *influxql.ShowMeasurementsStatement:
		return rewriteShowMeasurementsStatement(stmt)
	case *influxql.ShowMeasurementCardinalityStatement:
		return rewriteShowMeasurementCardinalityStatement(stmt)
	case *influxql.ShowSeriesStatement:
		return rewriteShowSeriesStatement(stmt)
	case *influxql.ShowSeriesCardinalityStatement:
		return rewriteShowSeriesCardinalityStatement(stmt)
	case *influxql.ShowTagKeysStatement:
		return rewriteShowTagKeysStatement(stmt)
	case *influxql.ShowTagKeyCardinalityStatement:
		return rewriteShowTagKeyCardinalityStatement(stmt)
	case *influxql.ShowTagValuesStatement:
		return rewriteShowTagValuesStatement(stmt)
	case *influxql.ShowTagValuesCardinalityStatement:
		return rewriteShowTagValuesCardinalityStatement(stmt)
	default:
		return stmt, nil
	}
}

func rewriteShowFieldKeysStatement(stmt *influxql.ShowFieldKeysStatement) (influxql.Statement, error) {
	return &influxql.SelectStatement{
		Fields: influxql.Fields([]*influxql.Field{
			{Expr: &influxql.VarRef{Val: "fieldKey"}},
			{Expr: &influxql.VarRef{Val: "fieldType"}},
		}),
		Sources:    rewriteSources(stmt.Sources, "_fieldKeys", stmt.Database),
		Condition:  rewriteSourcesCondition(stmt.Sources, nil),
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		SortFields: stmt.SortFields,
		OmitTime:   true,
		Dedupe:     true,
		IsRawQuery: true,
	}, nil
}

func rewriteShowFieldKeyCardinalityStatement(stmt *influxql.ShowFieldKeyCardinalityStatement) (influxql.Statement, error) {
	// Check for time in WHERE clause (not supported).
	if influxql.HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW FIELD KEY CARDINALITY doesn't support time in WHERE clause")
	}

	// Use all field keys, if zero.
	if len(stmt.Sources) == 0 {
		stmt.Sources = influxql.Sources{
			&influxql.Measurement{Regex: &influxql.RegexLiteral{Val: regexp.MustCompile(`.+`)}},
		}
	}

	return &influxql.SelectStatement{
		Fields: []*influxql.Field{
			{
				Expr: &influxql.Call{
					Name: "count",
					Args: []influxql.Expr{
						&influxql.Call{
							Name: "distinct",
							Args: []influxql.Expr{&influxql.VarRef{Val: "_fieldKey"}},
						},
					},
				},
				Alias: "count",
			},
		},
		Sources:    rewriteSources2(stmt.Sources, stmt.Database),
		Condition:  stmt.Condition,
		Dimensions: stmt.Dimensions,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		OmitTime:   true,
	}, nil
}

func rewriteShowMeasurementsStatement(stmt *influxql.ShowMeasurementsStatement) (influxql.Statement, error) {
	var sources influxql.Sources
	if stmt.Source != nil {
		sources = influxql.Sources{stmt.Source}
	}

	// Currently time based SHOW MEASUREMENT queries can't be supported because
	// it's not possible to appropriate set operations such as a negated regex
	// using the query engine.
	if influxql.HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW MEASUREMENTS doesn't support time in WHERE clause")
	}

	// rewrite condition to push a source measurement into a "_name" tag.
	stmt.Condition = rewriteSourcesCondition(sources, stmt.Condition)
	return stmt, nil
}

func rewriteShowMeasurementCardinalityStatement(stmt *influxql.ShowMeasurementCardinalityStatement) (influxql.Statement, error) {
	// TODO(edd): currently we only support cardinality estimation for certain
	// types of query. As the estimation coverage is expanded, this condition
	// will become less strict.
	if !stmt.Exact && stmt.Sources == nil && stmt.Condition == nil && stmt.Dimensions == nil && stmt.Limit == 0 && stmt.Offset == 0 {
		return stmt, nil
	}

	// Check for time in WHERE clause (not supported).
	if influxql.HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW MEASUREMENT EXACT CARDINALITY doesn't support time in WHERE clause")
	}

	// Use all measurements, if zero.
	if len(stmt.Sources) == 0 {
		stmt.Sources = influxql.Sources{
			&influxql.Measurement{Regex: &influxql.RegexLiteral{Val: regexp.MustCompile(`.+`)}},
		}
	}

	return &influxql.SelectStatement{
		Fields: []*influxql.Field{
			{
				Expr: &influxql.Call{
					Name: "count",
					Args: []influxql.Expr{
						&influxql.Call{
							Name: "distinct",
							Args: []influxql.Expr{&influxql.VarRef{Val: "_name"}},
						},
					},
				},
				Alias: "count",
			},
		},
		Sources:    rewriteSources2(stmt.Sources, stmt.Database),
		Condition:  stmt.Condition,
		Dimensions: stmt.Dimensions,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		OmitTime:   true,
		StripName:  true,
	}, nil
}

func rewriteShowSeriesStatement(stmt *influxql.ShowSeriesStatement) (influxql.Statement, error) {
	s := &influxql.SelectStatement{
		Condition:  stmt.Condition,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		SortFields: stmt.SortFields,
		OmitTime:   true,
		StripName:  true,
		Dedupe:     true,
		IsRawQuery: true,
	}
	// Check if we can exclusively use the index.
	if !influxql.HasTimeExpr(stmt.Condition) {
		s.Fields = []*influxql.Field{{Expr: &influxql.VarRef{Val: "key"}}}
		s.Sources = rewriteSources(stmt.Sources, "_series", stmt.Database)
		s.Condition = rewriteSourcesCondition(s.Sources, s.Condition)
		return s, nil
	}

	// The query is bounded by time then it will have to query TSM data rather
	// than utilising the index via system iterators.
	s.Fields = []*influxql.Field{
		{Expr: &influxql.VarRef{Val: "_seriesKey"}, Alias: "key"},
	}
	s.Sources = rewriteSources2(stmt.Sources, stmt.Database)
	return s, nil
}

func rewriteShowSeriesCardinalityStatement(stmt *influxql.ShowSeriesCardinalityStatement) (influxql.Statement, error) {
	// TODO(edd): currently we only support cardinality estimation for certain
	// types of query. As the estimation coverage is expanded, this condition
	// will become less strict.
	if !stmt.Exact && stmt.Sources == nil && stmt.Condition == nil && stmt.Dimensions == nil && stmt.Limit == 0 && stmt.Offset == 0 {
		return stmt, nil
	}

	// Check for time in WHERE clause (not supported).
	if influxql.HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW SERIES EXACT CARDINALITY doesn't support time in WHERE clause")
	}

	// Use all measurements, if zero.
	if len(stmt.Sources) == 0 {
		stmt.Sources = influxql.Sources{
			&influxql.Measurement{Regex: &influxql.RegexLiteral{Val: regexp.MustCompile(`.+`)}},
		}
	}

	return &influxql.SelectStatement{
		Fields: []*influxql.Field{
			{Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "_seriesKey"}}}, Alias: "count"},
		},
		Sources:    rewriteSources2(stmt.Sources, stmt.Database),
		Condition:  stmt.Condition,
		Dimensions: stmt.Dimensions,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		OmitTime:   true,
	}, nil
}

func rewriteShowTagValuesStatement(stmt *influxql.ShowTagValuesStatement) (influxql.Statement, error) {
	var expr influxql.Expr
	if list, ok := stmt.TagKeyExpr.(*influxql.ListLiteral); ok {
		for _, tagKey := range list.Vals {
			tagExpr := &influxql.BinaryExpr{
				Op:  influxql.EQ,
				LHS: &influxql.VarRef{Val: "_tagKey"},
				RHS: &influxql.StringLiteral{Val: tagKey},
			}

			if expr != nil {
				expr = &influxql.BinaryExpr{
					Op:  influxql.OR,
					LHS: expr,
					RHS: tagExpr,
				}
			} else {
				expr = tagExpr
			}
		}
	} else {
		expr = &influxql.BinaryExpr{
			Op:  stmt.Op,
			LHS: &influxql.VarRef{Val: "_tagKey"},
			RHS: stmt.TagKeyExpr,
		}
	}

	// Set condition or "AND" together.
	condition := stmt.Condition
	if condition == nil {
		condition = expr
	} else {
		condition = &influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: &influxql.ParenExpr{Expr: condition},
			RHS: &influxql.ParenExpr{Expr: expr},
		}
	}
	condition = rewriteSourcesCondition(stmt.Sources, condition)

	return &influxql.ShowTagValuesStatement{
		Database:   stmt.Database,
		Op:         stmt.Op,
		TagKeyExpr: stmt.TagKeyExpr,
		Condition:  condition,
		SortFields: stmt.SortFields,
		Limit:      stmt.Limit,
		Offset:     stmt.Offset,
	}, nil
}

func rewriteShowTagValuesCardinalityStatement(stmt *influxql.ShowTagValuesCardinalityStatement) (influxql.Statement, error) {
	// Use all measurements, if zero.
	if len(stmt.Sources) == 0 {
		stmt.Sources = influxql.Sources{
			&influxql.Measurement{Regex: &influxql.RegexLiteral{Val: regexp.MustCompile(`.+`)}},
		}
	}

	var expr influxql.Expr
	if list, ok := stmt.TagKeyExpr.(*influxql.ListLiteral); ok {
		for _, tagKey := range list.Vals {
			tagExpr := &influxql.BinaryExpr{
				Op:  influxql.EQ,
				LHS: &influxql.VarRef{Val: "_tagKey"},
				RHS: &influxql.StringLiteral{Val: tagKey},
			}

			if expr != nil {
				expr = &influxql.BinaryExpr{
					Op:  influxql.OR,
					LHS: expr,
					RHS: tagExpr,
				}
			} else {
				expr = tagExpr
			}
		}
	} else {
		expr = &influxql.BinaryExpr{
			Op:  stmt.Op,
			LHS: &influxql.VarRef{Val: "_tagKey"},
			RHS: stmt.TagKeyExpr,
		}
	}

	// Set condition or "AND" together.
	condition := stmt.Condition
	if condition == nil {
		condition = expr
	} else {
		condition = &influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: &influxql.ParenExpr{Expr: condition},
			RHS: &influxql.ParenExpr{Expr: expr},
		}
	}

	return &influxql.SelectStatement{
		Fields: []*influxql.Field{
			{
				Expr: &influxql.Call{
					Name: "count",
					Args: []influxql.Expr{
						&influxql.Call{
							Name: "distinct",
							Args: []influxql.Expr{&influxql.VarRef{Val: "_tagValue"}},
						},
					},
				},
				Alias: "count",
			},
		},
		Sources:    rewriteSources2(stmt.Sources, stmt.Database),
		Condition:  condition,
		Dimensions: stmt.Dimensions,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		OmitTime:   true,
	}, nil
}

func rewriteShowTagKeysStatement(stmt *influxql.ShowTagKeysStatement) (influxql.Statement, error) {
	return &influxql.ShowTagKeysStatement{
		Database:   stmt.Database,
		Condition:  rewriteSourcesCondition(stmt.Sources, stmt.Condition),
		SortFields: stmt.SortFields,
		Limit:      stmt.Limit,
		Offset:     stmt.Offset,
		SLimit:     stmt.SLimit,
		SOffset:    stmt.SOffset,
	}, nil
}

func rewriteShowTagKeyCardinalityStatement(stmt *influxql.ShowTagKeyCardinalityStatement) (influxql.Statement, error) {
	// Check for time in WHERE clause (not supported).
	if influxql.HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW TAG KEY EXACT CARDINALITY doesn't support time in WHERE clause")
	}

	// Use all measurements, if zero.
	if len(stmt.Sources) == 0 {
		stmt.Sources = influxql.Sources{
			&influxql.Measurement{Regex: &influxql.RegexLiteral{Val: regexp.MustCompile(`.+`)}},
		}
	}

	return &influxql.SelectStatement{
		Fields: []*influxql.Field{
			{
				Expr: &influxql.Call{
					Name: "count",
					Args: []influxql.Expr{
						&influxql.Call{
							Name: "distinct",
							Args: []influxql.Expr{&influxql.VarRef{Val: "_tagKey"}},
						},
					},
				},
				Alias: "count",
			},
		},
		Sources:    rewriteSources2(stmt.Sources, stmt.Database),
		Condition:  stmt.Condition,
		Dimensions: stmt.Dimensions,
		Offset:     stmt.Offset,
		Limit:      stmt.Limit,
		OmitTime:   true,
	}, nil
}

// rewriteSources rewrites sources to include the provided system iterator.
//
// rewriteSources also sets the default database where necessary.
func rewriteSources(sources influxql.Sources, systemIterator, defaultDatabase string) influxql.Sources {
	newSources := influxql.Sources{}
	for _, src := range sources {
		if src == nil {
			continue
		}
		mm := src.(*influxql.Measurement)
		database := mm.Database
		if database == "" {
			database = defaultDatabase
		}

		newM := mm.Clone()
		newM.SystemIterator, newM.Database = systemIterator, database
		newSources = append(newSources, newM)
	}

	if len(newSources) <= 0 {
		return append(newSources, &influxql.Measurement{
			Database:       defaultDatabase,
			SystemIterator: systemIterator,
		})
	}
	return newSources
}

// rewriteSourcesCondition rewrites sources into `name` expressions.
// Merges with cond and returns a new condition.
func rewriteSourcesCondition(sources influxql.Sources, cond influxql.Expr) influxql.Expr {
	if len(sources) == 0 {
		return cond
	}

	// Generate an OR'd set of filters on source name.
	var scond influxql.Expr
	for _, source := range sources {
		mm := source.(*influxql.Measurement)

		// Generate a filtering expression on the measurement name.
		var expr influxql.Expr
		if mm.Regex != nil {
			expr = &influxql.BinaryExpr{
				Op:  influxql.EQREGEX,
				LHS: &influxql.VarRef{Val: "_name"},
				RHS: &influxql.RegexLiteral{Val: mm.Regex.Val},
			}
		} else if mm.Name != "" {
			expr = &influxql.BinaryExpr{
				Op:  influxql.EQ,
				LHS: &influxql.VarRef{Val: "_name"},
				RHS: &influxql.StringLiteral{Val: mm.Name},
			}
		}

		if scond == nil {
			scond = expr
		} else {
			scond = &influxql.BinaryExpr{
				Op:  influxql.OR,
				LHS: scond,
				RHS: expr,
			}
		}
	}

	// This is the case where the original query has a WHERE on a tag, and also
	// is requesting from a specific source.
	if cond != nil && scond != nil {
		return &influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: &influxql.ParenExpr{Expr: scond},
			RHS: &influxql.ParenExpr{Expr: cond},
		}
	} else if cond != nil {
		// This is the case where the original query has a WHERE on a tag but
		// is not requesting from a specific source.
		return cond
	}
	return scond
}

func rewriteSources2(sources influxql.Sources, database string) influxql.Sources {
	if len(sources) == 0 {
		sources = influxql.Sources{&influxql.Measurement{Regex: &influxql.RegexLiteral{Val: matchAllRegex.Copy()}}}
	}
	for _, source := range sources {
		switch source := source.(type) {
		case *influxql.Measurement:
			if source.Database == "" {
				source.Database = database
			}
		}
	}
	return sources
}

var matchAllRegex = regexp.MustCompile(`.+`)
