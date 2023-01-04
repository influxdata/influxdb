#![allow(dead_code)]

use crate::plan::influxql::expr_type_evaluator::evaluate_type;
use crate::plan::influxql::field::field_name;
use crate::plan::influxql::field_mapper::{FieldMapper, FieldTypeMap, SchemaFieldMapper, TagSet};
use datafusion::catalog::schema::SchemaProvider;
use datafusion::common::{DataFusionError, Result};
use influxdb_influxql_parser::common::{MeasurementName, QualifiedMeasurementName};
use influxdb_influxql_parser::expression::{Expr, VarRefDataType, WildcardType};
use influxdb_influxql_parser::identifier::Identifier;
use influxdb_influxql_parser::literal::Literal;
use influxdb_influxql_parser::select::{
    Dimension, Field, FieldList, FromMeasurementClause, GroupByClause, MeasurementSelection,
    SelectStatement,
};
use influxdb_influxql_parser::string::Regex;
use influxdb_influxql_parser::visit::{Recursion, Visitable, Visitor, VisitorResult};
use itertools::Itertools;
use query_functions::clean_non_meta_escapes;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;

fn parse_regex(re: &Regex) -> Result<regex::Regex> {
    let pattern = clean_non_meta_escapes(re.as_str());
    regex::Regex::new(&pattern).map_err(|e| {
        DataFusionError::External(format!("invalid regular expression '{}': {}", re, e).into())
    })
}

/// Recursively expand the `from` clause of `stmt` and any subqueries.
fn rewrite_from(stmt: &mut SelectStatement, schema: Arc<dyn SchemaProvider>) -> Result<()> {
    let mut new_from = Vec::new();
    let schema = &Arc::clone(&schema);
    for ms in stmt.from.iter() {
        match ms {
            MeasurementSelection::Name(qmn) => match qmn {
                QualifiedMeasurementName {
                    name: MeasurementName::Name(name),
                    ..
                } => {
                    if schema.table(name).is_some() {
                        new_from.push(ms.clone())
                    }
                }
                QualifiedMeasurementName {
                    name: MeasurementName::Regex(re),
                    ..
                } => {
                    let re = parse_regex(re)?;
                    schema
                        .table_names()
                        .into_iter()
                        .filter(|table| re.is_match(table.as_str()))
                        .for_each(|table| {
                            new_from.push(MeasurementSelection::Name(QualifiedMeasurementName {
                                database: None,
                                retention_policy: None,
                                name: MeasurementName::Name(table.into()),
                            }))
                        });
                }
            },
            MeasurementSelection::Subquery(q) => {
                let mut q = *q.clone();
                rewrite_from(&mut q, Arc::clone(schema))?;
                new_from.push(MeasurementSelection::Subquery(Box::new(q)))
            }
        }
    }
    stmt.from = FromMeasurementClause::new(new_from);
    Ok(())
}

/// Determine the merged fields and tags of the `FROM` clause.
fn from_field_and_dimensions(
    from: &FromMeasurementClause,
    schema: Arc<dyn SchemaProvider>,
) -> Result<(FieldTypeMap, TagSet)> {
    let mut fs = FieldTypeMap::new();
    let mut ts = TagSet::new();
    let fm = &SchemaFieldMapper::new(schema) as &dyn FieldMapper;

    for ms in from.deref() {
        match ms {
            MeasurementSelection::Name(QualifiedMeasurementName {
                name: MeasurementName::Name(name),
                ..
            }) => {
                let (field_set, tag_set) = match fm.field_and_dimensions(name.as_str())? {
                    Some(res) => res,
                    None => continue,
                };

                // Merge field_set with existing
                for (name, ft) in &field_set {
                    match fs.get(name) {
                        Some(existing_type) => {
                            if ft < existing_type {
                                fs.insert(name.to_string(), *ft);
                            }
                        }
                        None => {
                            fs.insert(name.to_string(), *ft);
                        }
                    };
                }

                ts.extend(tag_set);
            }
            MeasurementSelection::Subquery(select) => {
                for f in select.fields.iter() {
                    let dt = match evaluate_type(&f.expr, &select.from, fm)? {
                        Some(dt) => dt,
                        None => continue,
                    };

                    let name = field_name(f);

                    match fs.get(name.as_str()) {
                        Some(existing_type) => {
                            if dt < *existing_type {
                                fs.insert(name, dt);
                            }
                        }
                        None => {
                            fs.insert(name, dt);
                        }
                    }
                }

                if let Some(group_by) = &select.group_by {
                    // Merge the dimensions from the subquery
                    ts.extend(group_by.iter().filter_map(|d| match d {
                        Dimension::Tag(ident) => Some(ident.to_string()),
                        _ => None,
                    }));
                }
            }
            _ => {
                // Unreachable, as the from clause should be normalised at this point.
                return Err(DataFusionError::Internal(
                    "Unexpected MeasurementSelection in from".to_string(),
                ));
            }
        }
    }
    Ok((fs, ts))
}

/// Returns a tuple indicating whether the specifies `SELECT` statement
/// has any wildcards or regular expressions in the projection list
/// and `GROUP BY` clause respectively.
fn has_wildcards(stmt: &SelectStatement) -> (bool, bool) {
    struct HasWildcardsVisitor(bool, bool);

    impl Visitor for HasWildcardsVisitor {
        fn pre_visit_expr(self, n: &Expr) -> VisitorResult<Recursion<Self>> {
            Ok(
                if matches!(n, Expr::Wildcard(_) | Expr::Literal(Literal::Regex(_))) {
                    Recursion::Stop(Self(true, self.1))
                } else {
                    Recursion::Continue(self)
                },
            )
        }

        fn pre_visit_select_from_clause(
            self,
            _n: &FromMeasurementClause,
        ) -> VisitorResult<Recursion<Self>> {
            // Don't traverse FROM and potential subqueries
            Ok(Recursion::Stop(self))
        }

        fn pre_visit_select_dimension(self, n: &Dimension) -> VisitorResult<Recursion<Self>> {
            Ok(if matches!(n, Dimension::Wildcard | Dimension::Regex(_)) {
                Recursion::Stop(Self(self.0, true))
            } else {
                Recursion::Continue(self)
            })
        }
    }

    let res = Visitable::accept(stmt, HasWildcardsVisitor(false, false)).unwrap();
    (res.0, res.1)
}

/// Perform a depth-first traversal of the expression tree.
fn walk_expr_mut(expr: &mut Expr, visit: &mut impl FnMut(&mut Expr) -> Result<()>) -> Result<()> {
    match expr {
        Expr::Binary { lhs, rhs, .. } => {
            walk_expr_mut(lhs, visit)?;
            walk_expr_mut(rhs, visit)?;
        }
        Expr::UnaryOp(_, expr) => walk_expr_mut(expr, visit)?,
        Expr::Nested(expr) => walk_expr_mut(expr, visit)?,
        Expr::Call { args, .. } => {
            args.iter_mut().try_for_each(|n| walk_expr_mut(n, visit))?;
        }
        Expr::VarRef { .. }
        | Expr::BindParameter(_)
        | Expr::Literal(_)
        | Expr::Wildcard(_)
        | Expr::Distinct(_) => {}
    }

    visit(expr)
}

/// Perform a depth-first traversal of the expression tree.
pub(crate) fn walk_expr(expr: &Expr, visit: &mut impl FnMut(&Expr) -> Result<()>) -> Result<()> {
    match expr {
        Expr::Binary { lhs, rhs, .. } => {
            walk_expr(lhs, visit)?;
            walk_expr(rhs, visit)?;
        }
        Expr::UnaryOp(_, expr) => walk_expr(expr, visit)?,
        Expr::Nested(expr) => walk_expr(expr, visit)?,
        Expr::Call { args, .. } => {
            args.iter().try_for_each(|n| walk_expr(n, visit))?;
        }
        Expr::VarRef { .. }
        | Expr::BindParameter(_)
        | Expr::Literal(_)
        | Expr::Wildcard(_)
        | Expr::Distinct(_) => {}
    }

    visit(expr)
}

/// Rewrite the projection list and GROUP BY of the specified `SELECT` statement.
///
/// Wildcards and regular expressions in the `SELECT` projection list and `GROUP BY` are expanded.
/// Any fields with no type specifier are rewritten with the appropriate type, if they exist in the
/// underlying schema.
///
/// Derived from [Go implementation](https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L1185).
fn rewrite_field_list(stmt: &mut SelectStatement, schema: Arc<dyn SchemaProvider>) -> Result<()> {
    // Iterate through the `FROM` clause and rewrite any subqueries first.
    for ms in stmt.from.iter_mut() {
        if let MeasurementSelection::Subquery(subquery) = ms {
            rewrite_field_list(subquery, Arc::clone(&schema))?;
        }
    }

    // Attempt to rewrite all variable references in the fields with their types, if one
    // hasn't been specified.
    let fm = &SchemaFieldMapper::new(Arc::clone(&schema)) as &dyn FieldMapper;
    stmt.fields.iter_mut().try_for_each(|f| {
        walk_expr_mut(&mut f.expr, &mut |e| {
            if matches!(e, Expr::VarRef { .. }) {
                let new_type = evaluate_type(e.borrow(), &stmt.from, fm)?;

                if let Expr::VarRef { data_type, .. } = e {
                    *data_type = new_type;
                }
            }
            Ok(())
        })
    })?;

    let (has_field_wildcard, has_group_by_wildcard) = has_wildcards(stmt);
    if (has_field_wildcard, has_group_by_wildcard) == (false, false) {
        return Ok(());
    }

    let (field_set, mut tag_set) = from_field_and_dimensions(&stmt.from, Arc::clone(&schema))?;

    if !has_group_by_wildcard {
        if let Some(group_by) = &stmt.group_by {
            // Remove any explicitly listed tags in the GROUP BY clause, so they are not expanded
            // in the wildcard specified in the SELECT projection list
            group_by.iter().for_each(|dim| {
                if let Dimension::Tag(ident) = dim {
                    tag_set.remove(ident.as_str());
                }
            });
        }
    }

    #[derive(PartialEq, PartialOrd, Eq, Ord)]
    struct VarRef {
        name: String,
        data_type: VarRefDataType,
    }

    let fields = if !field_set.is_empty() {
        let fields_iter = field_set.iter().map(|(k, v)| VarRef {
            name: k.clone(),
            data_type: *v,
        });

        if !has_group_by_wildcard {
            fields_iter
                .chain(tag_set.iter().map(|tag| VarRef {
                    name: tag.clone(),
                    data_type: VarRefDataType::Tag,
                }))
                .sorted()
                .collect::<Vec<_>>()
        } else {
            fields_iter.sorted().collect::<Vec<_>>()
        }
    } else {
        vec![]
    };

    if has_field_wildcard {
        let mut new_fields = Vec::new();

        for f in stmt.fields.iter() {
            let add_field = |f: &VarRef| {
                new_fields.push(Field {
                    expr: Expr::VarRef {
                        name: f.name.clone().into(),
                        data_type: Some(f.data_type),
                    },
                    alias: None,
                })
            };

            match &f.expr {
                Expr::Wildcard(wct) => {
                    let filter: fn(&&VarRef) -> bool = match wct {
                        None => |_| true,
                        Some(WildcardType::Tag) => |v| v.data_type.is_tag_type(),
                        Some(WildcardType::Field) => |v| v.data_type.is_field_type(),
                    };

                    fields.iter().filter(filter).for_each(add_field);
                }

                Expr::Literal(Literal::Regex(re)) => {
                    let re = parse_regex(re)?;
                    fields
                        .iter()
                        .filter(|v| re.is_match(v.name.as_str()))
                        .for_each(add_field);
                }

                Expr::Call { name, args } => {
                    let mut name = name;
                    let mut args = args;

                    // Search for the call with a wildcard by continuously descending until
                    // we no longer have a call.
                    while let Some(Expr::Call {
                        name: inner_name,
                        args: inner_args,
                    }) = args.first()
                    {
                        name = inner_name;
                        args = inner_args;
                    }

                    let mut supported_types = HashSet::from([
                        VarRefDataType::Float,
                        VarRefDataType::Integer,
                        VarRefDataType::Unsigned,
                    ]);

                    // Add additional types for certain functions.
                    match name.to_lowercase().as_str() {
                        "count" | "first" | "last" | "distinct" | "elapsed" | "mode" | "sample" => {
                            supported_types
                                .extend([VarRefDataType::String, VarRefDataType::Boolean]);
                        }
                        "min" | "max" => {
                            supported_types.insert(VarRefDataType::Boolean);
                        }
                        "holt_winters" | "holt_winters_with_fit" => {
                            supported_types.remove(&VarRefDataType::Unsigned);
                        }
                        _ => {}
                    }

                    let add_field = |v: &VarRef| {
                        let mut args = args.clone();
                        args[0] = Expr::VarRef {
                            name: v.name.clone().into(),
                            data_type: Some(v.data_type),
                        };
                        new_fields.push(Field {
                            expr: Expr::Call {
                                name: name.clone(),
                                args,
                            },
                            alias: Some(format!("{}_{}", field_name(f), v.name).into()),
                        })
                    };

                    match args.first() {
                        Some(Expr::Wildcard(Some(WildcardType::Tag))) => {
                            return Err(DataFusionError::External(
                                format!("unable to use tag as wildcard in {}()", name).into(),
                            ))
                        }
                        Some(Expr::Wildcard(_)) => {
                            fields
                                .iter()
                                .filter(|v| supported_types.contains(&v.data_type))
                                .for_each(add_field);
                        }
                        Some(Expr::Literal(Literal::Regex(re))) => {
                            let re = parse_regex(re)?;
                            fields
                                .iter()
                                .filter(|v| {
                                    supported_types.contains(&v.data_type)
                                        && re.is_match(v.name.as_str())
                                })
                                .for_each(add_field);
                        }
                        _ => {
                            new_fields.push(f.clone());
                            continue;
                        }
                    }
                }

                Expr::Binary { .. } => {
                    let mut has_wildcard = false;

                    walk_expr(&f.expr, &mut |e| {
                        match e {
                            Expr::Wildcard(_) | Expr::Literal(Literal::Regex(_)) => {
                                has_wildcard = true
                            }
                            _ => {}
                        }
                        Ok(())
                    })?;

                    if has_wildcard {
                        return Err(DataFusionError::External(
                            "unsupported expression: contains a wildcard or regular expression"
                                .into(),
                        ));
                    }

                    new_fields.push(f.clone());
                }

                _ => new_fields.push(f.clone()),
            }
        }

        stmt.fields = FieldList::new(new_fields);
    }

    if has_group_by_wildcard {
        let group_by_tags = if has_group_by_wildcard {
            tag_set.into_iter().sorted().collect::<Vec<_>>()
        } else {
            vec![]
        };

        if let Some(group_by) = &stmt.group_by {
            let mut new_dimensions = Vec::new();

            for dim in group_by.iter() {
                let add_dim = |dim: &String| {
                    new_dimensions.push(Dimension::Tag(Identifier::new(dim.clone())))
                };

                match dim {
                    Dimension::Wildcard => {
                        group_by_tags.iter().for_each(add_dim);
                    }
                    Dimension::Regex(re) => {
                        let re = parse_regex(re)?;

                        group_by_tags
                            .iter()
                            .filter(|dim| re.is_match(dim.as_str()))
                            .for_each(add_dim);
                    }
                    _ => new_dimensions.push(dim.clone()),
                }
            }
            stmt.group_by = Some(GroupByClause::new(new_dimensions));
        }
    }

    Ok(())
}

/// Resolve the outer-most `SELECT` projection list column names in accordance with the
/// [original implementation]. The names are assigned to the `alias` field of the [`Field`] struct.
///
/// [original implementation]: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L1651
fn rewrite_field_list_aliases(field_list: &mut FieldList) -> Result<()> {
    let names = field_list.iter().map(field_name).collect::<Vec<_>>();
    let mut column_aliases = HashMap::<&str, _>::from_iter(names.iter().map(|f| (f.as_str(), 0)));
    names
        .iter()
        .zip(field_list.iter_mut())
        .for_each(|(name, field)| {
            // Generate a new name if there is an existing alias
            field.alias = Some(match column_aliases.get(name.as_str()) {
                Some(0) => {
                    column_aliases.insert(name, 1);
                    name.as_str().into()
                }
                Some(count) => {
                    let mut count = *count;
                    loop {
                        let resolved_name = format!("{}_{}", name, count);
                        if column_aliases.contains_key(resolved_name.as_str()) {
                            count += 1;
                        } else {
                            column_aliases.insert(name, count + 1);
                            break resolved_name.as_str().into();
                        }
                    }
                }
                None => unreachable!(),
            })
        });

    Ok(())
}

/// Recursively rewrite the specified [`SelectStatement`], expanding any wildcards or regular expressions
/// found in the projection list, `FROM` clause or `GROUP BY` clause.
pub(crate) fn rewrite_statement(
    q: &SelectStatement,
    schema: Arc<dyn SchemaProvider>,
) -> Result<SelectStatement> {
    let mut stmt = q.clone();
    rewrite_from(&mut stmt, Arc::clone(&schema))?;
    rewrite_field_list(&mut stmt, schema)?;
    rewrite_field_list_aliases(&mut stmt.fields)?;

    Ok(stmt)
}

#[cfg(test)]
mod test {
    use crate::plan::influxql::rewriter::{has_wildcards, rewrite_statement, walk_expr_mut};
    use crate::plan::influxql::test_utils::{get_first_field, MockSchemaProvider};
    use influxdb_influxql_parser::expression::Expr;
    use influxdb_influxql_parser::literal::Literal;
    use influxdb_influxql_parser::parse_statements;
    use influxdb_influxql_parser::select::SelectStatement;
    use influxdb_influxql_parser::statement::Statement;
    use test_helpers::assert_contains;

    fn parse_select(s: &str) -> SelectStatement {
        let statements = parse_statements(s).unwrap();
        match statements.first() {
            Some(Statement::Select(sel)) => *sel.clone(),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_rewrite_statement() {
        // Exact, match
        let stmt = parse_select("SELECT usage_user FROM cpu");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_user::float AS usage_user FROM cpu"
        );

        // Duplicate columns do not have conflicting aliases
        let stmt = parse_select("SELECT usage_user, usage_user FROM cpu");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_user::float AS usage_user, usage_user::float AS usage_user_1 FROM cpu"
        );

        // Multiple aliases with no conflicts
        let stmt = parse_select("SELECT usage_user as usage_user_1, usage_user FROM cpu");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_user::float AS usage_user_1, usage_user::float AS usage_user FROM cpu"
        );

        // Multiple aliases with conflicts
        let stmt =
            parse_select("SELECT usage_user as usage_user_1, usage_user, usage_user, usage_user as usage_user_2, usage_user, usage_user_2 FROM cpu");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(stmt.to_string(), "SELECT usage_user::float AS usage_user_1, usage_user::float AS usage_user, usage_user::float AS usage_user_3, usage_user::float AS usage_user_2, usage_user::float AS usage_user_4, usage_user_2 AS usage_user_2_1 FROM cpu");

        // Rewriting FROM clause

        // Regex, match
        let stmt = parse_select("SELECT bytes_free FROM /d/");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT bytes_free::integer AS bytes_free FROM disk, diskio"
        );

        // Exact, no match
        let stmt = parse_select("SELECT usage_idle FROM foo");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert!(stmt.from.is_empty());

        // Regex, no match
        let stmt = parse_select("SELECT bytes_free FROM /^d$/");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert!(stmt.from.is_empty());

        // Rewriting projection list

        // Single wildcard, single measurement
        let stmt = parse_select("SELECT * FROM cpu");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT host::tag AS host, region::tag AS region, usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu"
        );

        let stmt = parse_select("SELECT * FROM cpu, disk");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT bytes_free::integer AS bytes_free, bytes_used::integer AS bytes_used, host::tag AS host, region::tag AS region, usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu, disk"
        );

        // Regular expression selects fields from multiple measurements
        let stmt = parse_select("SELECT /usage|bytes/ FROM cpu, disk");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT bytes_free::integer AS bytes_free, bytes_used::integer AS bytes_used, usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu, disk"
        );

        // Selective wildcard for tags
        let stmt = parse_select("SELECT *::tag FROM cpu");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT host::tag AS host, region::tag AS region FROM cpu"
        );

        // Selective wildcard for fields
        let stmt = parse_select("SELECT *::field FROM cpu");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu"
        );

        // Mixed fields and wildcards
        let stmt = parse_select("SELECT usage_idle, *::tag FROM cpu");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_idle::float AS usage_idle, host::tag AS host, region::tag AS region FROM cpu"
        );

        // GROUP BY expansion

        let stmt = parse_select("SELECT usage_idle FROM cpu GROUP BY host");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_idle::float AS usage_idle FROM cpu GROUP BY host"
        );

        let stmt = parse_select("SELECT usage_idle FROM cpu GROUP BY *");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_idle::float AS usage_idle FROM cpu GROUP BY host, region"
        );

        // Fallible

        // Invalid regex
        let stmt = parse_select("SELECT usage_idle FROM /(not/");
        let err = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap_err();
        assert_contains!(err.to_string(), "invalid regular expression");

        // Subqueries

        // Subquery, exact, match
        let stmt = parse_select("SELECT usage_idle FROM (SELECT usage_idle FROM cpu)");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_idle::float AS usage_idle FROM (SELECT usage_idle::float FROM cpu)"
        );

        // Subquery, regex, match
        let stmt = parse_select("SELECT bytes_free FROM (SELECT bytes_free FROM /d/)");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT bytes_free::integer AS bytes_free FROM (SELECT bytes_free::integer FROM disk, diskio)"
        );

        // Subquery, exact, no match
        let stmt = parse_select("SELECT usage_idle FROM (SELECT usage_idle FROM foo)");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_idle AS usage_idle FROM (SELECT usage_idle )"
        );

        // Subquery, regex, no match
        let stmt = parse_select("SELECT bytes_free FROM (SELECT bytes_free FROM /^d$/)");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT bytes_free AS bytes_free FROM (SELECT bytes_free )"
        );

        // Binary expression
        let stmt = parse_select("SELECT bytes_free+bytes_used FROM disk");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT bytes_free::integer + bytes_used::integer AS bytes_free_bytes_used FROM disk"
        );

        // Unary expressions
        let stmt = parse_select("SELECT -bytes_free FROM disk");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT -bytes_free::integer AS bytes_free FROM disk"
        );

        // Call expressions

        let stmt = parse_select("SELECT COUNT(field_i64) FROM temp_01");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT COUNT(field_i64::integer) AS COUNT FROM temp_01"
        );

        // Duplicate aggregate columns
        let stmt = parse_select("SELECT COUNT(field_i64), COUNT(field_i64) FROM temp_01");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT COUNT(field_i64::integer) AS COUNT, COUNT(field_i64::integer) AS COUNT_1 FROM temp_01"
        );

        let stmt = parse_select("SELECT COUNT(field_f64) FROM temp_01");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT COUNT(field_f64::float) AS COUNT FROM temp_01"
        );

        // Expands all fields
        let stmt = parse_select("SELECT COUNT(*) FROM temp_01");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT COUNT(field_f64::float) AS COUNT_field_f64, COUNT(field_i64::integer) AS COUNT_field_i64, COUNT(field_str::string) AS COUNT_field_str, COUNT(shared_field0::float) AS COUNT_shared_field0 FROM temp_01"
        );

        // Expands matching fields
        let stmt = parse_select("SELECT COUNT(/64$/) FROM temp_01");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT COUNT(field_f64::float) AS COUNT_field_f64, COUNT(field_i64::integer) AS COUNT_field_i64 FROM temp_01"
        );

        // Expands only numeric fields
        let stmt = parse_select("SELECT SUM(*) FROM temp_01");
        let stmt = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT SUM(field_f64::float) AS SUM_field_f64, SUM(field_i64::integer) AS SUM_field_i64, SUM(shared_field0::float) AS SUM_shared_field0 FROM temp_01"
        );

        // Fallible cases

        let stmt = parse_select("SELECT *::field + *::tag FROM cpu");
        let err = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap_err();
        assert_eq!(
            err.to_string(),
            "External error: unsupported expression: contains a wildcard or regular expression"
        );

        let stmt = parse_select("SELECT COUNT(*::tag) FROM cpu");
        let err = rewrite_statement(&stmt, MockSchemaProvider::new_schema_provider()).unwrap_err();
        assert_eq!(
            err.to_string(),
            "External error: unable to use tag as wildcard in COUNT()"
        );
    }

    #[test]
    fn test_has_wildcards() {
        // no GROUP BY
        let sel = parse_select("select a from b");
        let res = has_wildcards(&sel);
        assert!(!res.0);
        assert!(!res.1);

        let sel = parse_select("select a from b group by c");
        let res = has_wildcards(&sel);
        assert!(!res.0);
        assert!(!res.1);

        let sel = parse_select("select * from b group by c");
        let res = has_wildcards(&sel);
        assert!(res.0);
        assert!(!res.1);

        let sel = parse_select("select /a/ from b group by c");
        let res = has_wildcards(&sel);
        assert!(res.0);
        assert!(!res.1);

        let sel = parse_select("select a from b group by *");
        let res = has_wildcards(&sel);
        assert!(!res.0);
        assert!(res.1);

        let sel = parse_select("select a from b group by /a/");
        let res = has_wildcards(&sel);
        assert!(!res.0);
        assert!(res.1);

        let sel = parse_select("select * from b group by *");
        let res = has_wildcards(&sel);
        assert!(res.0);
        assert!(res.1);

        let sel = parse_select("select /a/ from b group by /b/");
        let res = has_wildcards(&sel);
        assert!(res.0);
        assert!(res.1);

        // finds wildcard in nested expressions
        let sel = parse_select("select COUNT(*) from b group by *");
        let res = has_wildcards(&sel);
        assert!(res.0);
        assert!(res.1);

        // does not traverse subqueries
        let sel = parse_select("select a from (select * from c group by *) group by c");
        let res = has_wildcards(&sel);
        assert!(!res.0);
        assert!(!res.1);
    }

    #[test]
    fn test_walk_expr() {
        fn walk_expr(s: &str) -> String {
            let expr = get_first_field(format!("SELECT {} FROM f", s).as_str()).expr;
            let mut calls = Vec::new();
            let mut call_no = 0;
            super::walk_expr(&expr, &mut |n| {
                calls.push(format!("{}: {}", call_no, n));
                call_no += 1;
                Ok(())
            })
            .unwrap();
            calls.join("\n")
        }

        insta::assert_display_snapshot!(walk_expr("5 + 6"));
        insta::assert_display_snapshot!(walk_expr("count(5, foo + 7)"));
        insta::assert_display_snapshot!(walk_expr("count(5, foo + 7) + sum(bar)"));
    }

    #[test]
    fn test_walk_expr_mut() {
        fn walk_expr_mut(s: &str) -> String {
            let mut expr = get_first_field(format!("SELECT {} FROM f", s).as_str()).expr;
            let mut calls = Vec::new();
            let mut call_no = 0;
            super::walk_expr_mut(&mut expr, &mut |n| {
                calls.push(format!("{}: {}", call_no, n));
                call_no += 1;
                Ok(())
            })
            .unwrap();
            calls.join("\n")
        }

        insta::assert_display_snapshot!(walk_expr_mut("5 + 6"));
        insta::assert_display_snapshot!(walk_expr_mut("count(5, foo + 7)"));
        insta::assert_display_snapshot!(walk_expr_mut("count(5, foo + 7) + sum(bar)"));
    }

    #[test]
    fn test_walk_expr_mut_modify() {
        let mut expr = get_first_field("SELECT foo + bar + 5 FROM f").expr;
        walk_expr_mut(&mut expr, &mut |e| {
            match e {
                Expr::VarRef { name, .. } => *name = format!("c_{}", name).into(),
                Expr::Literal(Literal::Unsigned(v)) => *v *= 10,
                _ => {}
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(format!("{}", expr), "c_foo + c_bar + 50")
    }
}
