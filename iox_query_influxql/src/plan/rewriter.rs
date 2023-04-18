#![allow(dead_code)]

use crate::plan::expr_type_evaluator::evaluate_type;
use crate::plan::field::field_name;
use crate::plan::field_mapper::{field_and_dimensions, FieldTypeMap, TagSet};
use crate::plan::planner::is_scalar_math_function;
use crate::plan::{error, util, SchemaProvider};
use datafusion::common::{DataFusionError, Result};
use influxdb_influxql_parser::common::{MeasurementName, QualifiedMeasurementName};
use influxdb_influxql_parser::expression::walk::{walk_expr, walk_expr_mut};
use influxdb_influxql_parser::expression::{Call, Expr, VarRef, VarRefDataType, WildcardType};
use influxdb_influxql_parser::identifier::Identifier;
use influxdb_influxql_parser::literal::Literal;
use influxdb_influxql_parser::select::{
    Dimension, Field, FieldList, FromMeasurementClause, GroupByClause, MeasurementSelection,
    SelectStatement,
};
use itertools::Itertools;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::ops::{ControlFlow, Deref};

/// Recursively expand the `from` clause of `stmt` and any subqueries.
fn rewrite_from(s: &dyn SchemaProvider, stmt: &mut SelectStatement) -> Result<()> {
    let mut new_from = Vec::new();
    for ms in stmt.from.iter() {
        match ms {
            MeasurementSelection::Name(qmn) => match qmn {
                QualifiedMeasurementName {
                    name: MeasurementName::Name(name),
                    ..
                } => {
                    if s.table_exists(name) {
                        new_from.push(ms.clone())
                    }
                }
                QualifiedMeasurementName {
                    name: MeasurementName::Regex(re),
                    ..
                } => {
                    let re = util::parse_regex(re)?;
                    s.table_names()
                        .into_iter()
                        .filter(|table| re.is_match(table))
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
                rewrite_from(s, &mut q)?;
                new_from.push(MeasurementSelection::Subquery(Box::new(q)))
            }
        }
    }
    stmt.from = FromMeasurementClause::new(new_from);
    Ok(())
}

/// Determine the merged fields and tags of the `FROM` clause.
fn from_field_and_dimensions(
    s: &dyn SchemaProvider,
    from: &FromMeasurementClause,
) -> Result<(FieldTypeMap, TagSet)> {
    let mut fs = FieldTypeMap::new();
    let mut ts = TagSet::new();

    for ms in from.deref() {
        match ms {
            MeasurementSelection::Name(QualifiedMeasurementName {
                name: MeasurementName::Name(name),
                ..
            }) => {
                let (field_set, tag_set) = match field_and_dimensions(s, name.as_str())? {
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
                    let dt = match evaluate_type(s, &f.expr, &select.from)? {
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
                return error::internal("Unexpected MeasurementSelection in from");
            }
        }
    }
    Ok((fs, ts))
}

/// Returns a tuple indicating whether the specifies `SELECT` statement
/// has any wildcards or regular expressions in the projection list
/// and `GROUP BY` clause respectively.
fn has_wildcards(stmt: &SelectStatement) -> (bool, bool) {
    use influxdb_influxql_parser::visit::{Recursion, Visitable, Visitor};

    struct HasWildcardsVisitor(bool, bool);

    impl Visitor for HasWildcardsVisitor {
        type Error = DataFusionError;

        fn pre_visit_expr(self, n: &Expr) -> Result<Recursion<Self>> {
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
        ) -> Result<Recursion<Self>> {
            // Don't traverse FROM and potential subqueries
            Ok(Recursion::Stop(self))
        }

        fn pre_visit_select_dimension(self, n: &Dimension) -> Result<Recursion<Self>> {
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

/// Rewrite the projection list and GROUP BY of the specified `SELECT` statement.
///
/// The following transformations are performed:
///
/// * Wildcards and regular expressions in the `SELECT` projection list and `GROUP BY` are expanded.
/// * Any fields with no type specifier are rewritten with the appropriate type, if they exist in the
///   underlying schema.
///
/// Derived from [Go implementation](https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L1185).
fn rewrite_field_list(s: &dyn SchemaProvider, stmt: &mut SelectStatement) -> Result<()> {
    // Iterate through the `FROM` clause and rewrite any subqueries first.
    for ms in stmt.from.iter_mut() {
        if let MeasurementSelection::Subquery(subquery) = ms {
            rewrite_field_list(s, subquery)?;
        }
    }

    // Rewrite all `DISTINCT <identifier>` expressions to `DISTINCT(<var ref>)`
    if let ControlFlow::Break(e) = stmt.fields.iter_mut().try_for_each(|f| {
        walk_expr_mut::<DataFusionError>(&mut f.expr, &mut |e| {
            if let Expr::Distinct(ident) = e {
                *e = Expr::Call(Call {
                    name: "distinct".to_owned(),
                    args: vec![Expr::VarRef(VarRef {
                        name: ident.take().into(),
                        data_type: None,
                    })],
                });
            }
            ControlFlow::Continue(())
        })
    }) {
        return Err(e);
    }

    // Attempt to rewrite all variable references in the fields with their types, if one
    // hasn't been specified.
    if let ControlFlow::Break(e) = stmt.fields.iter_mut().try_for_each(|f| {
        walk_expr_mut::<DataFusionError>(&mut f.expr, &mut |e| {
            if matches!(e, Expr::VarRef(_)) {
                let new_type = match evaluate_type(s, e.borrow(), &stmt.from) {
                    Err(e) => ControlFlow::Break(e)?,
                    Ok(v) => v,
                };

                if let Expr::VarRef(v) = e {
                    v.data_type = new_type;
                }
            }
            ControlFlow::Continue(())
        })
    }) {
        return Err(e);
    }

    let (has_field_wildcard, has_group_by_wildcard) = has_wildcards(stmt);
    if (has_field_wildcard, has_group_by_wildcard) == (false, false) {
        return Ok(());
    }

    let (field_set, mut tag_set) = from_field_and_dimensions(s, &stmt.from)?;

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

    let fields = if !field_set.is_empty() {
        let fields_iter = field_set.iter().map(|(k, v)| VarRef {
            name: k.clone().into(),
            data_type: Some(*v),
        });

        if !has_group_by_wildcard {
            fields_iter
                .chain(tag_set.iter().map(|tag| VarRef {
                    name: tag.clone().into(),
                    data_type: Some(VarRefDataType::Tag),
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
                    expr: Expr::VarRef(f.clone()),
                    alias: None,
                })
            };

            match &f.expr {
                Expr::Wildcard(wct) => {
                    let filter: fn(&&VarRef) -> bool = match wct {
                        None => |_| true,
                        Some(WildcardType::Tag) => {
                            |v| v.data_type.map_or(false, |dt| dt.is_tag_type())
                        }
                        Some(WildcardType::Field) => {
                            |v| v.data_type.map_or(false, |dt| dt.is_field_type())
                        }
                    };

                    fields.iter().filter(filter).for_each(add_field);
                }

                Expr::Literal(Literal::Regex(re)) => {
                    let re = util::parse_regex(re)?;
                    fields
                        .iter()
                        .filter(|v| re.is_match(v.name.as_str()))
                        .for_each(add_field);
                }

                Expr::Call(Call { name, args }) => {
                    let mut name = name;
                    let mut args = args;

                    // Search for the call with a wildcard by continuously descending until
                    // we no longer have a call.
                    while let Some(Expr::Call(Call {
                        name: inner_name,
                        args: inner_args,
                    })) = args.first()
                    {
                        name = inner_name;
                        args = inner_args;
                    }

                    let mut supported_types = HashSet::from([
                        Some(VarRefDataType::Float),
                        Some(VarRefDataType::Integer),
                        Some(VarRefDataType::Unsigned),
                    ]);

                    // Add additional types for certain functions.
                    match name.to_lowercase().as_str() {
                        "count" | "first" | "last" | "distinct" | "elapsed" | "mode" | "sample" => {
                            supported_types.extend([
                                Some(VarRefDataType::String),
                                Some(VarRefDataType::Boolean),
                            ]);
                        }
                        "min" | "max" => {
                            supported_types.insert(Some(VarRefDataType::Boolean));
                        }
                        "holt_winters" | "holt_winters_with_fit" => {
                            supported_types.remove(&Some(VarRefDataType::Unsigned));
                        }
                        _ => {}
                    }

                    let add_field = |v: &VarRef| {
                        let mut args = args.clone();
                        args[0] = Expr::VarRef(v.clone());
                        new_fields.push(Field {
                            expr: Expr::Call(Call {
                                name: name.clone(),
                                args,
                            }),
                            alias: Some(format!("{}_{}", field_name(f), v.name).into()),
                        })
                    };

                    match args.first() {
                        Some(Expr::Wildcard(Some(WildcardType::Tag))) => {
                            return error::query(format!(
                                "unable to use tag as wildcard in {name}()"
                            ));
                        }
                        Some(Expr::Wildcard(_)) => {
                            fields
                                .iter()
                                .filter(|v| supported_types.contains(&v.data_type))
                                .for_each(add_field);
                        }
                        Some(Expr::Literal(Literal::Regex(re))) => {
                            let re = util::parse_regex(re)?;
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
                    let has_wildcard = walk_expr(&f.expr, &mut |e| {
                        match e {
                            Expr::Wildcard(_) | Expr::Literal(Literal::Regex(_)) => {
                                return ControlFlow::Break(())
                            }
                            _ => {}
                        }
                        ControlFlow::Continue(())
                    })
                    .is_break();

                    if has_wildcard {
                        return error::query(
                            "unsupported expression: contains a wildcard or regular expression",
                        );
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
                        let re = util::parse_regex(re)?;

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
                        let resolved_name = format!("{name}_{count}");
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
    s: &dyn SchemaProvider,
    q: &SelectStatement,
) -> Result<SelectStatement> {
    let mut stmt = q.clone();
    rewrite_from(s, &mut stmt)?;
    rewrite_field_list(s, &mut stmt)?;
    rewrite_field_list_aliases(&mut stmt.fields)?;

    Ok(stmt)
}

/// Check the length of the arguments slice is within
/// the expected bounds.
macro_rules! check_exp_args {
    ($NAME:expr, $EXP:expr, $ARGS:expr) => {
        let args_len = $ARGS.len();
        if args_len != $EXP {
            return error::query(format!(
                "invalid number of arguments for {}, expected {}, got {args_len}",
                $NAME, $EXP
            ));
        }
    };
    ($NAME:expr, $LO:literal, $HI:literal, $ARGS:expr) => {
        let args_len = $ARGS.len();
        if !($LO..=$HI).contains(&args_len) {
            return error::query(format!(
                "invalid number of arguments for {}, expected at least {} but no more than {}, got {args_len}",
                $NAME, $LO, $HI
            ));
        }
    };
}

/// Verify the argument at a specific position is a [`Literal::Integer`].
macro_rules! lit_integer {
    ($NAME:expr, $ARGS:expr, $POS:literal) => {
        match &$ARGS[$POS] {
            Expr::Literal(Literal::Integer(v)) => *v,
            _ => return error::query(format!("expected integer argument in {}()", $NAME)),
        }
    };

    ($NAME:expr, $ARGS:expr, $POS:literal?) => {
        if $POS < $ARGS.len() {
            Some(lit_integer!($NAME, $ARGS, $POS))
        } else {
            None
        }
    };
}

/// Verify the argument at a specific position is a [`Literal::String`].
macro_rules! lit_string {
    ($NAME:expr, $ARGS:expr, $POS:literal) => {
        match &$ARGS[$POS] {
            Expr::Literal(Literal::String(s)) => s.as_str(),
            _ => return error::query(format!("expected string argument in {}()", $NAME)),
        }
    };

    ($NAME:expr, $ARGS:expr, $POS:literal?) => {
        if $POS < $ARGS.len() {
            Some(lit_string!($NAME, $ARGS, $POS))
        } else {
            None
        }
    };
}

/// Checks a number of expectations for the fields of a [`SelectStatement`].
#[derive(Default)]
struct FieldChecker {
    /// `true` if the statement contains a `GROUP BY TIME` clause.
    has_group_by_time: bool,

    /// `true` if the interval was inherited by a parent.
    /// If this is set, then an interval that was inherited will not cause
    /// a query that shouldn't have an interval to fail.
    inherited_group_by_time: bool,

    /// `true` if the projection contains an invocation of the `TOP` or `BOTTOM` function.
    has_top_bottom: bool,

    /// `true` when one or more projections do not contain an aggregate expression.
    has_non_aggregate_fields: bool,

    /// `true` when the projection contains a `DISTINCT` function or unary `DISTINCT` operator.
    has_distinct: bool,

    /// Accumulator for the number of aggregate or window expressions for the statement.
    aggregate_count: usize,

    /// Accumulator for the number of selector expressions for the statement.
    selector_count: usize,
}

impl FieldChecker {
    fn check_fields(&mut self, q: &SelectStatement) -> Result<ProjectionType> {
        q.fields.iter().try_for_each(|f| self.check_expr(&f.expr))?;

        match self.function_count() {
            0 => {
                // If there are no aggregate functions, the FILL clause does not make sense
                //
                // NOTE
                // This is a deviation from InfluxQL, which allowed `FILL(previous)`, `FILL(<number>)`,
                // and `FILL(null)` for queries that did not have a `GROUP BY time`. This is
                // undocumented behaviour, and the `FILL` clause is documented as part of the
                // `GROUP BY` clause, per https://docs.influxdata.com/influxdb/v1.8/query_language/spec/#clauses
                //
                // Normally, `FILL` is associated with gap-filling, and is applied to aggregate
                // projections only, however, without the `GROUP BY` time clause, and no aggregate
                // functions, it is applied to all columns, including tags.
                //
                //
                // * `FILL(previous)` carries the previous non-null value forward
                // * `FILL(<number>)` defaults `NULL` values to `<number>`, including tag columns
                // * `FILL(null)` is the default behaviour.
                //
                // See: https://github.com/influxdata/influxdb/blob/98361e207349a3643bcc332d54b009818fe7585f/query/compile.go#L1002-L1012
                if let Some(fill) = q.fill {
                    return error::query(format!("{fill} must be used with an aggregate function"));
                }

                if self.has_group_by_time && !self.inherited_group_by_time {
                    return error::query("GROUP BY requires at least one aggregate function");
                }
            }
            2.. if self.has_top_bottom => {
                return error::query(
                    "selector functions top and bottom cannot be combined with other functions",
                )
            }
            _ => {}
        }

        // If a distinct() call is present, ensure there is exactly one aggregate function.
        //
        // See: https://github.com/influxdata/influxdb/blob/98361e207349a3643bcc332d54b009818fe7585f/query/compile.go#L1013-L1016
        if self.has_distinct && (self.function_count() != 1 || self.has_non_aggregate_fields) {
            return error::query(
                "aggregate function distinct() cannot be combined with other functions or fields",
            );
        }

        // Validate we are using a selector or raw query if non-aggregate fields are projected.
        if self.has_non_aggregate_fields {
            if self.aggregate_count > 0 {
                return error::query("mixing aggregate and non-aggregate columns is not supported");
            } else if self.selector_count > 1 {
                return error::query(
                    "mixing multiple selector functions with tags or fields is not supported",
                );
            }
        }

        // By this point the statement is valid, so lets
        // determine the projection type

        if self.has_top_bottom {
            Ok(ProjectionType::TopBottomSelector)
        } else if self.has_group_by_time {
            Ok(ProjectionType::Aggregate)
        } else if self.has_distinct {
            Ok(ProjectionType::RawDistinct)
        } else if self.selector_count == 1 && self.aggregate_count == 0 {
            Ok(ProjectionType::Selector {
                has_fields: self.has_non_aggregate_fields,
            })
        } else if self.selector_count > 1 || self.aggregate_count > 0 {
            Ok(ProjectionType::Aggregate)
        } else {
            Ok(ProjectionType::Raw)
        }
    }

    /// The total number of functions observed.
    fn function_count(&self) -> usize {
        self.aggregate_count + self.selector_count
    }
}

impl FieldChecker {
    fn check_expr(&mut self, e: &Expr) -> Result<()> {
        match e {
            Expr::VarRef(_) => {
                self.has_non_aggregate_fields = true;
                Ok(())
            }
            Expr::Call(c) if is_scalar_math_function(&c.name) => self.check_math_function(c),
            Expr::Call(c) => self.check_aggregate_function(c),
            Expr::Binary(b) => match (&*b.lhs, &*b.rhs) {
                (Expr::Literal(_), Expr::Literal(_)) => {
                    error::query("cannot perform a binary expression on two literals")
                }
                (Expr::Literal(_), other) | (other, Expr::Literal(_)) => self.check_expr(other),
                (lhs, rhs) => {
                    self.check_expr(lhs)?;
                    self.check_expr(rhs)
                }
            },
            Expr::Nested(e) => self.check_expr(e),
            // BindParameter should be substituted prior to validating fields.
            Expr::BindParameter(_) => error::internal("unexpected bind parameter"),
            Expr::Wildcard(_) => error::internal("unexpected wildcard"),
            Expr::Literal(Literal::Regex(_)) => error::internal("unexpected regex"),
            Expr::Distinct(_) => error::internal("unexpected distinct clause"),
            // See: https://github.com/influxdata/influxdb/blob/98361e207349a3643bcc332d54b009818fe7585f/query/compile.go#L347
            Expr::Literal(_) => error::query("field must contain at least one variable"),
        }
    }

    fn check_math_function(&mut self, c: &Call) -> Result<()> {
        let name = c.name.as_str();
        check_exp_args!(
            name,
            match name {
                "atan2" | "pow" | "log" => 2,
                _ => 1,
            },
            c.args
        );

        // Check each argument that is not a literal number.
        //
        // NOTE
        // This is a slight deviation from OSS, where we only skip
        // numeric literals, which are the only literal argument types supported by the mathematical
        // functions in InfluxQL.
        //
        // See: https://github.com/influxdata/influxdb/blob/98361e207349a3643bcc332d54b009818fe7585f/query/compile.go#L910-L911
        c.args.iter().try_for_each(|e| {
            if matches!(e, Expr::Literal(Literal::Integer(_) | Literal::Float(_))) {
                Ok(())
            } else {
                self.check_expr(e)
            }
        })
    }

    /// Validate `c` is an aggregate, window aggregate or selector function.
    fn check_aggregate_function(&mut self, c: &Call) -> Result<()> {
        let name = c.name.as_str();

        match name {
            "percentile" => self.check_percentile(&c.args),
            "sample" => self.check_sample(&c.args),
            "distinct" => self.check_distinct(&c.args, false),
            "top" | "bottom" if self.has_top_bottom => error::query(format!(
                "selector function {name}() cannot be combined with other functions"
            )),
            "top" | "bottom" => self.check_top_bottom(name, &c.args),
            "derivative" | "non_negative_derivative" => self.check_derivative(name, &c.args),
            "difference" | "non_negative_difference" => self.check_difference(name, &c.args),
            "cumulative_sum" => self.check_cumulative_sum(&c.args),
            "moving_average" => self.check_moving_average(&c.args),
            "exponential_moving_average"
            | "double_exponential_moving_average"
            | "triple_exponential_moving_average"
            | "relative_strength_index"
            | "triple_exponential_derivative" => {
                self.check_exponential_moving_average(name, &c.args)
            }
            "kaufmans_efficiency_ratio" | "kaufmans_adaptive_moving_average" => {
                self.check_kaufmans(name, &c.args)
            }
            "chande_momentum_oscillator" => self.check_chande_momentum_oscillator(name, &c.args),
            "elapsed" => self.check_elapsed(name, &c.args),
            "integral" => self.check_integral(name, &c.args),
            "count_hll" => self.check_count_hll(&c.args),
            "holt_winters" | "holt_winters_with_fit" => self.check_holt_winters(name, &c.args),
            "max" | "min" | "first" | "last" => {
                self.inc_selector_count();
                check_exp_args!(name, 1, c.args);
                self.check_symbol(name, &c.args[0])
            }
            "count" | "sum" | "mean" | "median" | "mode" | "stddev" | "spread" | "sum_hll" => {
                self.inc_aggregate_count();
                check_exp_args!(name, 1, c.args);

                // If this is a call to count(), allow distinct() to be used as the function argument.
                if name == "count" {
                    match &c.args[0] {
                        Expr::Call(c) if c.name == "distinct" => {
                            return self.check_distinct(&c.args, true);
                        }
                        Expr::Distinct(_) => {
                            return error::internal("unexpected distinct clause in count");
                        }
                        _ => {}
                    }
                }
                self.check_symbol(name, &c.args[0])
            }
            _ => error::query(format!("unsupported function {name}()")),
        }
    }

    fn check_percentile(&mut self, args: &[Expr]) -> Result<()> {
        self.inc_selector_count();

        check_exp_args!("percentile", 2, args);
        if !matches!(
            &args[1],
            Expr::Literal(Literal::Integer(_)) | Expr::Literal(Literal::Float(_))
        ) {
            return error::query(format!(
                "expected number for percentile(), got {:?}",
                &args[1]
            ));
        }
        self.check_symbol("percentile", &args[0])
    }

    fn check_sample(&mut self, args: &[Expr]) -> Result<()> {
        self.inc_selector_count();

        check_exp_args!("sample", 2, args);
        let v = lit_integer!("sample", args, 1);
        // NOTE: this is a deviation from InfluxQL, which incorrectly performs the check for <= 0
        //
        // See: https://github.com/influxdata/influxdb/blob/98361e207349a3643bcc332d54b009818fe7585f/query/compile.go#L441-L443
        if v <= 1 {
            return error::query(format!("sample window must be greater than 1, got {v}"));
        }

        self.check_symbol("sample", &args[0])
    }

    /// Validate the arguments for the `distinct` function call.
    fn check_distinct(&mut self, args: &[Expr], nested: bool) -> Result<()> {
        self.inc_aggregate_count();

        check_exp_args!("distinct", 1, args);
        if !matches!(&args[0], Expr::VarRef(_)) {
            return error::query("expected field argument in distinct()");
        }

        if !nested {
            self.has_distinct = true;
        }

        Ok(())
    }

    fn check_top_bottom(&mut self, name: &str, args: &[Expr]) -> Result<()> {
        assert!(!self.has_top_bottom, "should not be called if true");

        self.inc_selector_count();
        self.has_top_bottom = true;

        if args.len() < 2 {
            return error::query(format!(
                "invalid number of arguments for {name}, expected at least 2, got {}",
                args.len()
            ));
        }

        let (last, args) = args.split_last().expect("length >= 2");

        match last {
            Expr::Literal(Literal::Integer(limit)) => {
                if *limit <= 0 {
                    return error::query(format!(
                        "limit ({limit}) for {name} must be greater than 0"
                    ));
                }
            }
            got => {
                return error::query(format!(
                    "expected integer as last argument for {name}, got {got:?}"
                ))
            }
        }

        let (first, rest) = args.split_first().expect("length >= 1");

        if !matches!(first, Expr::VarRef(_)) {
            return error::query(format!("expected first argument to be a field for {name}"));
        }

        for expr in rest {
            if !matches!(expr, Expr::VarRef(_)) {
                return error::query(format!(
                    "only fields or tags are allow for {name}(), got {expr:?}"
                ));
            }
        }

        if !rest.is_empty() {
            // projecting additional fields and tags, such as <tag> or <field> in `TOP(usage_idle, <tag>, <field>, 5)`
            self.has_non_aggregate_fields = true
        }

        Ok(())
    }

    fn check_derivative(&mut self, name: &str, args: &[Expr]) -> Result<()> {
        self.inc_aggregate_count();

        check_exp_args!(name, 1, 2, args);
        match args.get(1) {
            Some(Expr::Literal(Literal::Duration(d))) if **d <= 0 => {
                return error::query(format!("duration argument must be positive, got {d}"))
            }
            None | Some(Expr::Literal(Literal::Duration(_))) => {}
            Some(got) => {
                return error::query(format!(
                    "second argument to {name} must be a duration, got {got:?}"
                ))
            }
        }

        self.check_nested_symbol(name, &args[0])
    }

    fn check_elapsed(&mut self, name: &str, args: &[Expr]) -> Result<()> {
        self.inc_aggregate_count();
        check_exp_args!(name, 1, 2, args);

        match args.get(1) {
            Some(Expr::Literal(Literal::Duration(d))) if **d <= 0 => {
                return error::query(format!("duration argument must be positive, got {d}"))
            }
            None | Some(Expr::Literal(Literal::Duration(_))) => {}
            Some(got) => {
                return error::query(format!(
                    "second argument to {name} must be a duration, got {got:?}"
                ))
            }
        }

        self.check_nested_symbol(name, &args[0])
    }

    fn check_difference(&mut self, name: &str, args: &[Expr]) -> Result<()> {
        self.inc_aggregate_count();
        check_exp_args!(name, 1, args);

        self.check_nested_symbol(name, &args[0])
    }

    fn check_cumulative_sum(&mut self, args: &[Expr]) -> Result<()> {
        self.inc_aggregate_count();
        check_exp_args!("cumulative_sum", 1, args);

        self.check_nested_symbol("cumulative_sum", &args[0])
    }

    fn check_moving_average(&mut self, args: &[Expr]) -> Result<()> {
        self.inc_aggregate_count();
        check_exp_args!("moving_average", 2, args);

        let v = lit_integer!("moving_average", args, 1);
        if v <= 1 {
            return error::query(format!(
                "moving_average window must be greater than 1, got {v}"
            ));
        }

        self.check_nested_symbol("moving_average", &args[0])
    }

    fn check_exponential_moving_average(&mut self, name: &str, args: &[Expr]) -> Result<()> {
        self.inc_aggregate_count();
        check_exp_args!(name, 2, 4, args);

        let v = lit_integer!(name, args, 1);
        if v < 1 {
            return error::query(format!("{name} period must be greater than 1, got {v}"));
        }

        if let Some(v) = lit_integer!(name, args, 2?) {
            match (v, name) {
                (v, "triple_exponential_derivative") if v < 1 && v != -1 => {
                    return error::query(format!(
                        "{name} hold period must be greater than or equal to 1"
                    ))
                }
                (v, _) if v < 0 && v != -1 => {
                    return error::query(format!(
                        "{name} hold period must be greater than or equal to 0"
                    ))
                }
                _ => {}
            }
        }

        match lit_string!(name, args, 3?) {
            Some("exponential" | "simple") => {}
            Some(warmup) => {
                return error::query(format!(
                    "{name} warmup type must be one of: 'exponential', 'simple', got {warmup}"
                ))
            }
            None => {}
        }

        self.check_nested_symbol(name, &args[0])
    }

    fn check_kaufmans(&mut self, name: &str, args: &[Expr]) -> Result<()> {
        self.inc_aggregate_count();
        check_exp_args!(name, 2, 3, args);

        let v = lit_integer!(name, args, 1);
        if v < 1 {
            return error::query(format!("{name} period must be greater than 1, got {v}"));
        }

        if let Some(v) = lit_integer!(name, args, 2?) {
            if v < 0 && v != -1 {
                return error::query(format!(
                    "{name} hold period must be greater than or equal to 0"
                ));
            }
        }

        self.check_nested_symbol(name, &args[0])
    }

    fn check_chande_momentum_oscillator(&mut self, name: &str, args: &[Expr]) -> Result<()> {
        self.inc_aggregate_count();
        check_exp_args!(name, 2, 4, args);

        let v = lit_integer!(name, args, 1);
        if v < 1 {
            return error::query(format!("{name} period must be greater than 1, got {v}"));
        }

        if let Some(v) = lit_integer!(name, args, 2?) {
            if v < 0 && v != -1 {
                return error::query(format!(
                    "{name} hold period must be greater than or equal to 0"
                ));
            }
        }

        match lit_string!(name, args, 3?) {
            Some("none" | "exponential" | "simple") => {}
            Some(warmup) => {
                return error::query(format!(
                "{name} warmup type must be one of: 'none', 'exponential' or 'simple', got {warmup}"
            ))
            }
            None => {}
        }

        self.check_nested_symbol(name, &args[0])
    }

    fn check_integral(&mut self, name: &str, args: &[Expr]) -> Result<()> {
        self.inc_aggregate_count();
        check_exp_args!(name, 1, 2, args);

        match args.get(1) {
            Some(Expr::Literal(Literal::Duration(d))) if **d <= 0 => {
                return error::query(format!("duration argument must be positive, got {d}"))
            }
            None | Some(Expr::Literal(Literal::Duration(_))) => {}
            Some(got) => {
                return error::query(format!(
                    "second argument to {name} must be a duration, got {got:?}"
                ))
            }
        }

        self.check_symbol(name, &args[0])
    }

    fn check_count_hll(&mut self, _args: &[Expr]) -> Result<()> {
        self.inc_aggregate_count();
        // The count hyperloglog function is not documented for versions 1.8 or the latest 2.7.
        // If anyone is using it, we'd like to know, so we'll explicitly return a not implemented
        // message.
        //
        // See: https://docs.influxdata.com/influxdb/v2.7/query-data/influxql/functions/
        // See: https://docs.influxdata.com/influxdb/v1.8/query_language/functions
        error::not_implemented("count_hll")
    }

    fn check_holt_winters(&mut self, name: &str, args: &[Expr]) -> Result<()> {
        self.inc_aggregate_count();
        check_exp_args!(name, 3, args);

        let v = lit_integer!(name, args, 1);
        if v < 1 {
            return error::query(format!("{name} N argument must be greater than 0, got {v}"));
        }

        let v = lit_integer!(name, args, 2);
        if v < 0 {
            return error::query(format!("{name} S argument cannot be negative, got {v}"));
        }

        match &args[0] {
            Expr::Call(_) if !self.has_group_by_time => {
                error::query(format!("{name} aggregate requires a GROUP BY interval"))
            }
            expr @ Expr::Call(_) => self.check_nested_expr(expr),
            _ => error::query(format!("must use aggregate function with {name}")),
        }
    }

    /// Increments the function call count
    fn inc_aggregate_count(&mut self) {
        self.aggregate_count += 1
    }

    fn inc_selector_count(&mut self) {
        self.selector_count += 1
    }

    fn check_nested_expr(&mut self, expr: &Expr) -> Result<()> {
        match expr {
            Expr::Call(c) if c.name == "distinct" => self.check_distinct(&c.args, true),
            _ => self.check_expr(expr),
        }
    }

    fn check_nested_symbol(&mut self, name: &str, expr: &Expr) -> Result<()> {
        match expr {
            Expr::Call(_) if !self.has_group_by_time => {
                error::query(format!("{name} aggregate requires a GROUP BY interval"))
            }
            Expr::Call(_) => self.check_nested_expr(expr),
            _ if self.has_group_by_time && !self.inherited_group_by_time => error::query(format!(
                "aggregate function required inside the call to {name}"
            )),
            _ => self.check_symbol(name, expr),
        }
    }

    /// Validate that `expr` is either a [`Expr::VarRef`] or a [`Expr::Wildcard`] or
    /// [`Literal::Regex`] under specific conditions.
    fn check_symbol(&mut self, name: &str, expr: &Expr) -> Result<()> {
        match expr {
            Expr::VarRef(_) => Ok(()),
            Expr::Wildcard(_) | Expr::Literal(Literal::Regex(_)) => {
                error::internal("unexpected wildcard or regex")
            }
            expr => error::query(format!("expected field argument in {name}(), got {expr:?}")),
        }
    }
}

#[derive(Default, Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum ProjectionType {
    /// A query that projects no aggregate or selector functions.
    #[default]
    Raw,
    /// A query that projects a single DISTINCT(field)
    RawDistinct,
    /// A query that projects one or more aggregate functions or
    /// two or more selector functions.
    Aggregate,
    /// A query that projects a single selector function,
    /// such as `last` or `first`.
    Selector {
        /// When `true`, the projection contains additional tags or fields.
        has_fields: bool,
    },
    /// A query that projects the `top` or `bottom` selector function.
    TopBottomSelector,
}

/// Holds high-level information as the result of analysing
/// a `SELECT` query.
#[derive(Default, Debug, Copy, Clone)]
pub(crate) struct SelectStatementInfo {
    /// Identifies the projection type for the `SELECT` query.
    pub projection_type: ProjectionType,
}

/// Gather information about the semantics of a [`SelectStatement`] and verify
/// the `SELECT` projection clause is semantically correct.
///
/// Upon success the fields list is guaranteed to adhere to a number of conditions.
///
/// Generally:
///
/// * All aggregate, selector and window-like functions, such as `sum`, `last` or `difference`,
///   specify a field expression as their first argument
/// * All projected columns must refer to a field or tag ensuring there are no literal
///   projections such as `SELECT 1`
/// * Argument types and values are valid
///
/// When `GROUP BY TIME` is present, the `SelectStatement` is an aggregate query and the
/// following additional rules apply:
///
/// * All projected fields are aggregate or selector expressions
/// * All window-like functions, such as `difference` or `integral` specify an aggregate
///   expression, such as `SUM(foo)`, as their first argument
///
/// For selector queries, which are those that use selector functions like `last` or `max`:
///
/// * Projecting **multiple** selector functions, such as `last` or `first` will not be
/// combined with non-aggregate columns
/// * Projecting a **single** selector function, such as `last` or `first` may be combined
/// with non-aggregate columns
///
/// Finally, the `top` and `bottom` function have the following additional restrictions:
///
/// * Are not combined with other aggregate, selector or window-like functions and may
///   only project additional fields
pub(crate) fn select_statement_info(q: &SelectStatement) -> Result<SelectStatementInfo> {
    let has_group_by_time = q
        .group_by
        .as_ref()
        .and_then(|gb| gb.time_dimension())
        .is_some();

    let mut fc = FieldChecker {
        has_group_by_time,
        ..Default::default()
    };

    let projection_type = fc.check_fields(q)?;

    Ok(SelectStatementInfo { projection_type })
}

#[cfg(test)]
mod test {
    use crate::plan::rewriter::{
        has_wildcards, rewrite_statement, select_statement_info, ProjectionType,
    };
    use crate::plan::test_utils::{parse_select, MockSchemaProvider};
    use assert_matches::assert_matches;
    use datafusion::error::DataFusionError;
    use test_helpers::{assert_contains, assert_error};

    #[test]
    fn test_select_statement_info() {
        let info = select_statement_info(&parse_select("SELECT foo, bar FROM cpu")).unwrap();
        assert_matches!(info.projection_type, ProjectionType::Raw);

        let info = select_statement_info(&parse_select("SELECT distinct(foo) FROM cpu")).unwrap();
        assert_matches!(info.projection_type, ProjectionType::RawDistinct);

        let info = select_statement_info(&parse_select("SELECT last(foo) FROM cpu")).unwrap();
        assert_matches!(
            info.projection_type,
            ProjectionType::Selector { has_fields: false }
        );

        let info = select_statement_info(&parse_select("SELECT last(foo), bar FROM cpu")).unwrap();
        assert_matches!(
            info.projection_type,
            ProjectionType::Selector { has_fields: true }
        );

        let info = select_statement_info(&parse_select(
            "SELECT last(foo) FROM cpu GROUP BY TIME(10s)",
        ))
        .unwrap();
        assert_matches!(info.projection_type, ProjectionType::Aggregate);

        let info =
            select_statement_info(&parse_select("SELECT last(foo), first(foo) FROM cpu")).unwrap();
        assert_matches!(info.projection_type, ProjectionType::Aggregate);

        let info = select_statement_info(&parse_select("SELECT count(foo) FROM cpu")).unwrap();
        assert_matches!(info.projection_type, ProjectionType::Aggregate);

        let info = select_statement_info(&parse_select("SELECT top(foo, 3) FROM cpu")).unwrap();
        assert_matches!(info.projection_type, ProjectionType::TopBottomSelector);
    }

    /// Verify all the aggregate, window-like and selector functions are handled
    /// by `select_statement_info`.
    #[test]
    fn test_select_statement_info_functions() {
        // percentile
        let sel = parse_select("SELECT percentile(foo, 2) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT percentile(foo) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "invalid number of arguments for percentile, expected 2, got 1");
        let sel = parse_select("SELECT percentile('foo', /a/) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "expected number for percentile(), got Literal(Regex(Regex(\"a\")))");

        // sample
        let sel = parse_select("SELECT sample(foo, 2) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT sample(foo) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "invalid number of arguments for sample, expected 2, got 1");
        let sel = parse_select("SELECT sample(foo, -2) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "sample window must be greater than 1, got -2");

        // distinct
        let sel = parse_select("SELECT distinct(foo) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT distinct(foo, 1) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "invalid number of arguments for distinct, expected 1, got 2");
        let sel = parse_select("SELECT distinct(sum(foo)) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "expected field argument in distinct()");
        let sel = parse_select("SELECT distinct(foo), distinct(bar) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "aggregate function distinct() cannot be combined with other functions or fields");

        // top / bottom
        let sel = parse_select("SELECT top(foo, 3) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT bottom(foo, 3) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT top(foo, 3), bar FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT top(foo, bar, 3) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT top(foo) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "invalid number of arguments for top, expected at least 2, got 1");
        let sel = parse_select("SELECT bottom(foo) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "invalid number of arguments for bottom, expected at least 2, got 1");
        let sel = parse_select("SELECT top(foo, -2) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "limit (-2) for top must be greater than 0");
        let sel = parse_select("SELECT top(foo, bar) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "expected integer as last argument for top, got VarRef(VarRef { name: Identifier(\"bar\"), data_type: None })");
        let sel = parse_select("SELECT top('foo', 3) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "expected first argument to be a field for top");
        let sel = parse_select("SELECT top(foo, 2, 3) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "only fields or tags are allow for top(), got Literal(Integer(2))");
        let sel = parse_select("SELECT top(foo, 2), mean(bar) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "selector functions top and bottom cannot be combined with other functions");

        // derivative
        let sel = parse_select("SELECT derivative(foo) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT derivative(foo, 2s) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT derivative(mean(foo)) FROM cpu GROUP BY TIME(30s)");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT derivative(foo, 2) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "second argument to derivative must be a duration, got Literal(Integer(2))");
        let sel = parse_select("SELECT derivative(foo, -2s) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "duration argument must be positive, got -2s");
        let sel = parse_select("SELECT derivative(foo, 2s, 1) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "invalid number of arguments for derivative, expected at least 1 but no more than 2, got 3");
        let sel = parse_select("SELECT derivative(foo) FROM cpu GROUP BY TIME(30s)");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "aggregate function required inside the call to derivative");

        // elapsed
        let sel = parse_select("SELECT elapsed(foo) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT elapsed(foo, 5s) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT elapsed(foo, 2) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "second argument to elapsed must be a duration, got Literal(Integer(2))");
        let sel = parse_select("SELECT elapsed(foo, -2s) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "duration argument must be positive, got -2s");

        // difference / non_negative_difference
        let sel = parse_select("SELECT difference(foo) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT non_negative_difference(foo) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT difference(foo, 2) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "invalid number of arguments for difference, expected 1, got 2");

        // cumulative_sum
        let sel = parse_select("SELECT cumulative_sum(foo) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT cumulative_sum(foo, 2) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "invalid number of arguments for cumulative_sum, expected 1, got 2");

        // moving_average
        let sel = parse_select("SELECT moving_average(foo, 2) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT moving_average(foo, bar, 3) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "invalid number of arguments for moving_average, expected 2, got 3");
        let sel = parse_select("SELECT moving_average(foo, 1) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "moving_average window must be greater than 1, got 1");

        // exponential_moving_average, double_exponential_moving_average
        // triple_exponential_moving_average, relative_strength_index and triple_exponential_derivative
        let sel = parse_select("SELECT exponential_moving_average(foo, 2) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT exponential_moving_average(foo, 2, 3) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT exponential_moving_average(foo, 2, -1) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel =
            parse_select("SELECT exponential_moving_average(foo, 2, 3, 'exponential') FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT exponential_moving_average(foo, 2, 3, 'simple') FROM cpu");
        select_statement_info(&sel).unwrap();
        // check variants
        let sel = parse_select("SELECT double_exponential_moving_average(foo, 2) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT triple_exponential_moving_average(foo, 2) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT relative_strength_index(foo, 2) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT triple_exponential_derivative(foo, 2) FROM cpu");
        select_statement_info(&sel).unwrap();

        let sel = parse_select("SELECT exponential_moving_average(foo) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "invalid number of arguments for exponential_moving_average, expected at least 2 but no more than 4, got 1");
        let sel = parse_select("SELECT exponential_moving_average(foo, 2, 3, 'bad') FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "exponential_moving_average warmup type must be one of: 'exponential', 'simple', got bad");
        let sel = parse_select("SELECT exponential_moving_average(foo, 2, 3, 4) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "expected string argument in exponential_moving_average()");
        let sel = parse_select("SELECT exponential_moving_average(foo, 2, -2) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "exponential_moving_average hold period must be greater than or equal to 0");
        let sel = parse_select("SELECT triple_exponential_derivative(foo, 2, 0) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "triple_exponential_derivative hold period must be greater than or equal to 1");

        // kaufmans_efficiency_ratio, kaufmans_adaptive_moving_average
        let sel = parse_select("SELECT kaufmans_efficiency_ratio(foo, 2) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT kaufmans_adaptive_moving_average(foo, 2) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT kaufmans_efficiency_ratio(foo) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "invalid number of arguments for kaufmans_efficiency_ratio, expected at least 2 but no more than 3, got 1");
        let sel = parse_select("SELECT kaufmans_efficiency_ratio(foo, 2, -2) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "kaufmans_efficiency_ratio hold period must be greater than or equal to 0");

        // chande_momentum_oscillator
        let sel = parse_select("SELECT chande_momentum_oscillator(foo, 2) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT chande_momentum_oscillator(foo, 2, 3) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT chande_momentum_oscillator(foo, 2, 3, 'none') FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel =
            parse_select("SELECT chande_momentum_oscillator(foo, 2, 3, 'exponential') FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT chande_momentum_oscillator(foo, 2, 3, 'simple') FROM cpu");
        select_statement_info(&sel).unwrap();

        let sel = parse_select("SELECT chande_momentum_oscillator(foo) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "invalid number of arguments for chande_momentum_oscillator, expected at least 2 but no more than 4, got 1");
        let sel = parse_select("SELECT chande_momentum_oscillator(foo, 2, 3, 'bad') FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "chande_momentum_oscillator warmup type must be one of: 'none', 'exponential' or 'simple', got bad");

        // integral
        let sel = parse_select("SELECT integral(foo) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT integral(foo, 2s) FROM cpu");
        select_statement_info(&sel).unwrap();

        let sel = parse_select("SELECT integral(foo, -2s) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "duration argument must be positive, got -2s");
        let sel = parse_select("SELECT integral(foo, 2) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "second argument to integral must be a duration, got Literal(Integer(2))");

        // count_hll
        let sel = parse_select("SELECT count_hll(foo) FROM cpu");
        assert_error!(
            select_statement_info(&sel),
            DataFusionError::NotImplemented(_)
        );

        // holt_winters, holt_winters_with_fit
        let sel = parse_select("SELECT holt_winters(mean(foo), 2, 3) FROM cpu GROUP BY time(30s)");
        select_statement_info(&sel).unwrap();
        let sel = parse_select(
            "SELECT holt_winters_with_fit(sum(foo), 2, 3) FROM cpu GROUP BY time(30s)",
        );
        select_statement_info(&sel).unwrap();

        let sel = parse_select("SELECT holt_winters(sum(foo), 2, 3) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "holt_winters aggregate requires a GROUP BY interval");
        let sel = parse_select("SELECT holt_winters(foo, 2, 3) FROM cpu GROUP BY time(30s)");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "must use aggregate function with holt_winters");
        let sel = parse_select("SELECT holt_winters(sum(foo), 2) FROM cpu GROUP BY time(30s)");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "invalid number of arguments for holt_winters, expected 3, got 2");
        let sel = parse_select("SELECT holt_winters(foo, 0, 3) FROM cpu GROUP BY time(30s)");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "holt_winters N argument must be greater than 0, got 0");
        let sel = parse_select("SELECT holt_winters(foo, 1, -3) FROM cpu GROUP BY time(30s)");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "holt_winters S argument cannot be negative, got -3");

        // max, min, first, last
        for name in [
            "max", "min", "first", "last", "count", "sum", "mean", "median", "mode", "stddev",
            "spread", "sum_hll",
        ] {
            let sel = parse_select(&format!("SELECT {name}(foo) FROM cpu"));
            select_statement_info(&sel).unwrap();
            let sel = parse_select(&format!("SELECT {name}(foo, 2) FROM cpu"));
            let exp = format!("invalid number of arguments for {name}, expected 1, got 2");
            assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == &exp);
        }

        // count(distinct)
        let sel = parse_select("SELECT count(distinct(foo)) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT count(distinct('foo')) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "expected field argument in distinct()");
        let sel = parse_select("SELECT count(distinct foo) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::External(ref s) if s.to_string() == "internal: unexpected distinct clause in count");

        // Test rules for math functions
        let sel = parse_select("SELECT abs(usage_idle) FROM cpu");
        select_statement_info(&sel).unwrap();
        let sel = parse_select("SELECT abs(*) + ceil(foo) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::External(ref s) if s.to_string() == "internal: unexpected wildcard");
        let sel = parse_select("SELECT abs(/f/) + ceil(foo) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::External(ref s) if s.to_string() == "internal: unexpected regex");

        // Fallible

        // abs expects 1 argument
        let sel = parse_select("SELECT abs(foo, 2) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "invalid number of arguments for abs, expected 1, got 2");
        // pow expects 2 arguments
        let sel = parse_select("SELECT pow(foo, 2, 3) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "invalid number of arguments for pow, expected 2, got 3");

        // Cannot perform binary operations on literals
        // See: https://github.com/influxdata/influxdb/blob/98361e207349a3643bcc332d54b009818fe7585f/query/compile.go#L329
        let sel = parse_select("SELECT 1 + 1 FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "cannot perform a binary expression on two literals");

        // can't project literals
        let sel = parse_select("SELECT foo, 1 FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "field must contain at least one variable");

        // wildcard expansion is not supported in binary expressions for aggregates
        let sel = parse_select("SELECT count(*) + count(foo) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::External(ref s) if s.to_string() == "internal: unexpected wildcard or regex");

        // regex expansion is not supported in binary expressions
        let sel = parse_select("SELECT sum(/foo/) + count(foo) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::External(ref s) if s.to_string() == "internal: unexpected wildcard or regex");

        // aggregate functions require a field reference
        let sel = parse_select("SELECT sum(1) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "expected field argument in sum(), got Literal(Integer(1))");
    }

    #[test]
    fn test_rewrite_statement() {
        let namespace = MockSchemaProvider::default();
        // Exact, match
        let stmt = parse_select("SELECT usage_user FROM cpu");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_user::float AS usage_user FROM cpu"
        );

        // Duplicate columns do not have conflicting aliases
        let stmt = parse_select("SELECT usage_user, usage_user FROM cpu");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_user::float AS usage_user, usage_user::float AS usage_user_1 FROM cpu"
        );

        // Multiple aliases with no conflicts
        let stmt = parse_select("SELECT usage_user as usage_user_1, usage_user FROM cpu");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_user::float AS usage_user_1, usage_user::float AS usage_user FROM cpu"
        );

        // Multiple aliases with conflicts
        let stmt =
            parse_select("SELECT usage_user as usage_user_1, usage_user, usage_user, usage_user as usage_user_2, usage_user, usage_user_2 FROM cpu");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(stmt.to_string(), "SELECT usage_user::float AS usage_user_1, usage_user::float AS usage_user, usage_user::float AS usage_user_3, usage_user::float AS usage_user_2, usage_user::float AS usage_user_4, usage_user_2 AS usage_user_2_1 FROM cpu");

        // Rewriting FROM clause

        // Regex, match
        let stmt = parse_select("SELECT bytes_free FROM /d/");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT bytes_free::integer AS bytes_free FROM disk, diskio"
        );

        // Exact, no match
        let stmt = parse_select("SELECT usage_idle FROM foo");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert!(stmt.from.is_empty());

        // Regex, no match
        let stmt = parse_select("SELECT bytes_free FROM /^d$/");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert!(stmt.from.is_empty());

        // Rewriting projection list

        // Single wildcard, single measurement
        let stmt = parse_select("SELECT * FROM cpu");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT cpu::tag AS cpu, host::tag AS host, region::tag AS region, usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu"
        );

        let stmt = parse_select("SELECT * FROM cpu, disk");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT bytes_free::integer AS bytes_free, bytes_used::integer AS bytes_used, cpu::tag AS cpu, device::tag AS device, host::tag AS host, region::tag AS region, usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu, disk"
        );

        // Regular expression selects fields from multiple measurements
        let stmt = parse_select("SELECT /usage|bytes/ FROM cpu, disk");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT bytes_free::integer AS bytes_free, bytes_used::integer AS bytes_used, usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu, disk"
        );

        // Selective wildcard for tags
        let stmt = parse_select("SELECT *::tag FROM cpu");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT cpu::tag AS cpu, host::tag AS host, region::tag AS region FROM cpu"
        );

        // Selective wildcard for fields
        let stmt = parse_select("SELECT *::field FROM cpu");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu"
        );

        // Mixed fields and wildcards
        let stmt = parse_select("SELECT usage_idle, *::tag FROM cpu");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_idle::float AS usage_idle, cpu::tag AS cpu, host::tag AS host, region::tag AS region FROM cpu"
        );

        // GROUP BY expansion

        let stmt = parse_select("SELECT usage_idle FROM cpu GROUP BY host");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_idle::float AS usage_idle FROM cpu GROUP BY host"
        );

        let stmt = parse_select("SELECT usage_idle FROM cpu GROUP BY *");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_idle::float AS usage_idle FROM cpu GROUP BY cpu, host, region"
        );

        // Does not include tags in projection when expanded in GROUP BY
        let stmt = parse_select("SELECT * FROM cpu GROUP BY *");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu GROUP BY cpu, host, region"
        );

        // Does include explicitly listed tags in projection
        let stmt = parse_select("SELECT host, * FROM cpu GROUP BY *");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT host::tag AS host, usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu GROUP BY cpu, host, region"
        );

        // Fallible

        // Invalid regex
        let stmt = parse_select("SELECT usage_idle FROM /(not/");
        let err = rewrite_statement(&namespace, &stmt).unwrap_err();
        assert_contains!(err.to_string(), "invalid regular expression");

        // Subqueries

        // Subquery, exact, match
        let stmt = parse_select("SELECT usage_idle FROM (SELECT usage_idle FROM cpu)");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_idle::float AS usage_idle FROM (SELECT usage_idle::float FROM cpu)"
        );

        // Subquery, regex, match
        let stmt = parse_select("SELECT bytes_free FROM (SELECT bytes_free FROM /d/)");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT bytes_free::integer AS bytes_free FROM (SELECT bytes_free::integer FROM disk, diskio)"
        );

        // Subquery, exact, no match
        let stmt = parse_select("SELECT usage_idle FROM (SELECT usage_idle FROM foo)");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT usage_idle AS usage_idle FROM (SELECT usage_idle )"
        );

        // Subquery, regex, no match
        let stmt = parse_select("SELECT bytes_free FROM (SELECT bytes_free FROM /^d$/)");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT bytes_free AS bytes_free FROM (SELECT bytes_free )"
        );

        // Binary expression
        let stmt = parse_select("SELECT bytes_free+bytes_used FROM disk");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT bytes_free::integer + bytes_used::integer AS bytes_free_bytes_used FROM disk"
        );

        // Unary expressions
        let stmt = parse_select("SELECT -bytes_free FROM disk");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT -1 * bytes_free::integer AS bytes_free FROM disk"
        );

        // DISTINCT clause

        // COUNT(DISTINCT)
        let stmt = parse_select("SELECT COUNT(DISTINCT bytes_free) FROM disk");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT count(distinct(bytes_free::integer)) AS count FROM disk"
        );

        let stmt = parse_select("SELECT DISTINCT bytes_free FROM disk");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT distinct(bytes_free::integer) AS \"distinct\" FROM disk"
        );

        // Call expressions

        let stmt = parse_select("SELECT COUNT(field_i64) FROM temp_01");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT count(field_i64::integer) AS count FROM temp_01"
        );

        // Duplicate aggregate columns
        let stmt = parse_select("SELECT COUNT(field_i64), COUNT(field_i64) FROM temp_01");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT count(field_i64::integer) AS count, count(field_i64::integer) AS count_1 FROM temp_01"
        );

        let stmt = parse_select("SELECT COUNT(field_f64) FROM temp_01");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT count(field_f64::float) AS count FROM temp_01"
        );

        // Expands all fields
        let stmt = parse_select("SELECT COUNT(*) FROM temp_01");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT count(field_f64::float) AS count_field_f64, count(field_i64::integer) AS count_field_i64, count(field_str::string) AS count_field_str, count(field_u64::unsigned) AS count_field_u64, count(shared_field0::float) AS count_shared_field0 FROM temp_01"
        );

        // Expands matching fields
        let stmt = parse_select("SELECT COUNT(/64$/) FROM temp_01");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT count(field_f64::float) AS count_field_f64, count(field_i64::integer) AS count_field_i64, count(field_u64::unsigned) AS count_field_u64 FROM temp_01"
        );

        // Expands only numeric fields
        let stmt = parse_select("SELECT SUM(*) FROM temp_01");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT sum(field_f64::float) AS sum_field_f64, sum(field_i64::integer) AS sum_field_i64, sum(field_u64::unsigned) AS sum_field_u64, sum(shared_field0::float) AS sum_shared_field0 FROM temp_01"
        );

        let stmt = parse_select("SELECT * FROM merge_00, merge_01");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT col0::float AS col0, col0::tag AS col0_1, col1::float AS col1, col1::tag AS col1_1, col2::string AS col2, col3::string AS col3 FROM merge_00, merge_01"
        );

        let stmt = parse_select("SELECT /col0/ FROM merge_00, merge_01");
        let stmt = rewrite_statement(&namespace, &stmt).unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT col0::float AS col0, col0::tag AS col0_1 FROM merge_00, merge_01"
        );

        // Fallible cases

        let stmt = parse_select("SELECT *::field + *::tag FROM cpu");
        let err = rewrite_statement(&namespace, &stmt).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: unsupported expression: contains a wildcard or regular expression"
        );

        let stmt = parse_select("SELECT COUNT(*::tag) FROM cpu");
        let err = rewrite_statement(&namespace, &stmt).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: unable to use tag as wildcard in count()"
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
}
