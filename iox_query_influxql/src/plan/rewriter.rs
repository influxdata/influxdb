use crate::error;
use crate::plan::expr_type_evaluator::TypeEvaluator;
use crate::plan::field::{field_by_name, field_name};
use crate::plan::field_mapper::{field_and_dimensions, FieldTypeMap};
use crate::plan::ir::{DataSource, Field, Interval, Select, SelectQuery, TagSet};
use crate::plan::var_ref::{influx_type_to_var_ref_data_type, var_ref_data_type_to_influx_type};
use crate::plan::{util, SchemaProvider};
use datafusion::common::{DataFusionError, Result};
use influxdb_influxql_parser::common::{MeasurementName, QualifiedMeasurementName, WhereClause};
use influxdb_influxql_parser::expression::walk::{
    walk_expr, walk_expr_mut, walk_expression_mut, ExpressionMut,
};
use influxdb_influxql_parser::expression::{
    AsVarRefExpr, Call, Expr, VarRef, VarRefDataType, WildcardType,
};
use influxdb_influxql_parser::functions::is_scalar_math_function;
use influxdb_influxql_parser::identifier::Identifier;
use influxdb_influxql_parser::literal::Literal;
use influxdb_influxql_parser::select::{
    Dimension, FillClause, FromMeasurementClause, GroupByClause, MeasurementSelection,
    SelectStatement,
};
use influxdb_influxql_parser::time_range::{
    duration_expr_to_nanoseconds, split_cond, ReduceContext, TimeRange,
};
use influxdb_influxql_parser::timestamp::Timestamp;
use itertools::Itertools;
use schema::InfluxColumnType;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::ops::{ControlFlow, Deref, DerefMut};

/// Recursively rewrite the specified [`SelectStatement`] by performing a series of passes
/// to validate and normalize the statement.
pub(super) fn rewrite_statement(
    s: &dyn SchemaProvider,
    q: &SelectStatement,
) -> Result<SelectQuery> {
    let mut select = rewrite_select(s, q)?;
    from_drop_empty(s, &mut select);
    field_list_normalize_time(&mut select);

    Ok(SelectQuery { select })
}

/// Find the unique list of tables used by `s`, recursively following all `FROM` clauses and
/// return the results in lexicographically in ascending order.
pub(super) fn find_table_names(s: &Select) -> BTreeSet<&str> {
    let mut data_sources = vec![s.from.as_slice()];
    let mut tables = BTreeSet::new();
    while let Some(from) = data_sources.pop() {
        for ds in from {
            match ds {
                DataSource::Table(name) => {
                    tables.insert(name.as_str());
                }
                DataSource::Subquery(q) => data_sources.push(q.from.as_slice()),
            }
        }
    }
    tables
}

/// Transform a `SelectStatement` to a `Select`, which is an intermediate representation used by
/// the InfluxQL planner. Transformations include expanding wildcards.
fn rewrite_select(s: &dyn SchemaProvider, stmt: &SelectStatement) -> Result<Select> {
    let rw = RewriteSelect::default();
    rw.rewrite(s, stmt)
}

/// Asserts that the `SELECT` statement does not use any unimplemented features.
///
/// The list of unimplemented or unsupported features are listed below.
///
/// # `SLIMIT` and `SOFFSET`
///
/// * `SLIMIT` and `SOFFSET` don't work as expected per issue [#7571]
/// * This issue [is noted](https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#the-slimit-clause) in our official documentation
///
/// [#7571]: https://github.com/influxdata/influxdb/issues/7571
fn check_features(stmt: &SelectStatement) -> Result<()> {
    if stmt.series_limit.is_some() || stmt.series_offset.is_some() {
        return error::not_implemented("SLIMIT or SOFFSET");
    }

    Ok(())
}

#[derive(Default)]
struct RewriteSelect {
    /// The depth of the `SELECT` statement currently processed by the rewriter.
    depth: u32,
}

impl RewriteSelect {
    /// Transform a `SelectStatement` to a `Select`, which is an intermediate representation used by
    /// the InfluxQL planner. Transformations include expanding wildcards.
    fn rewrite(&self, s: &dyn SchemaProvider, stmt: &SelectStatement) -> Result<Select> {
        check_features(stmt)?;

        let from = self.expand_from(s, stmt)?;
        let tag_set = from_tag_set(s, &from);
        let (fields, group_by) = self.expand_projection(s, stmt, &from, &tag_set)?;
        let condition = self.condition_resolve_types(s, stmt, &from)?;

        let now = Timestamp::from(s.execution_props().query_execution_start_time);
        let rc = ReduceContext {
            now: Some(now),
            tz: stmt.timezone.map(|tz| *tz),
        };

        let interval = self.find_interval_offset(&rc, group_by.as_ref())?;

        let (condition, time_range) = match condition {
            Some(where_clause) => split_cond(&rc, &where_clause).map_err(error::map::expr_error)?,
            None => (None, TimeRange::default()),
        };

        // If the interval is non-zero and there is no upper bound, default to `now`
        // for compatibility with InfluxQL OG.
        //
        // See: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/compile.go#L172-L179
        let time_range = match (interval, time_range.upper) {
            (Some(interval), None) if interval.duration > 0 => TimeRange {
                lower: time_range.lower,
                upper: Some(now.timestamp_nanos()),
            },
            _ => time_range,
        };

        let SelectStatementInfo {
            projection_type,
            extra_intervals,
        } = select_statement_info(&fields, &group_by, stmt.fill)?;

        // Following InfluxQL OG behaviour, if this is a subquery, and the fill strategy equates
        // to `FILL(null)`, switch to `FILL(none)`.
        //
        // See: https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/iterator.go#L757-L765
        let fill = if projection_type != ProjectionType::Raw
            && self.is_subquery()
            && matches!(stmt.fill, Some(FillClause::Null) | None)
        {
            Some(FillClause::None)
        } else {
            stmt.fill
        };

        Ok(Select {
            projection_type,
            interval,
            extra_intervals,
            fields,
            from,
            condition,
            time_range,
            group_by,
            tag_set,
            fill,
            order_by: stmt.order_by,
            limit: stmt.limit,
            offset: stmt.offset,
            timezone: stmt.timezone.map(|v| *v),
        })
    }

    /// Returns true if the receiver is processing a subquery.
    #[inline]
    fn is_subquery(&self) -> bool {
        self.depth > 0
    }

    /// Rewrite the `SELECT` statement by applying specific rules for subqueries.
    fn rewrite_subquery(&self, s: &dyn SchemaProvider, stmt: &SelectStatement) -> Result<Select> {
        let rw = Self {
            depth: self.depth + 1,
        };

        rw.rewrite(s, stmt)
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
    fn expand_projection(
        &self,
        s: &dyn SchemaProvider,
        stmt: &SelectStatement,
        from: &[DataSource],
        from_tag_set: &TagSet,
    ) -> Result<(Vec<Field>, Option<GroupByClause>)> {
        let tv = TypeEvaluator::new(s, from);
        let fields = stmt
            .fields
            .iter()
            .map(|f| (f.expr.clone(), f.alias.clone()))
            .map(|(mut expr, alias)| {
                match walk_expr_mut::<_>(&mut expr, &mut |e| match e {
                    // Rewrite all `DISTINCT <identifier>` expressions to `DISTINCT(VarRef)`
                    Expr::Distinct(ident) => {
                        let mut v = VarRef {
                            name: ident.take().into(),
                            data_type: None,
                        };
                        v.data_type = match tv.eval_var_ref(&v) {
                            Ok(v) => v,
                            Err(e) => ControlFlow::Break(e)?,
                        };

                        *e = Expr::Call(Call {
                            name: "distinct".to_owned(),
                            args: vec![Expr::VarRef(v)],
                        });
                        ControlFlow::Continue(())
                    }

                    // Attempt to rewrite all variable (column) references with their concrete types,
                    // if one hasn't been specified.
                    Expr::VarRef(ref mut v) => {
                        v.data_type = match tv.eval_var_ref(v) {
                            Ok(v) => v,
                            Err(e) => ControlFlow::Break(e)?,
                        };
                        ControlFlow::Continue(())
                    }

                    _ => ControlFlow::Continue(()),
                }) {
                    ControlFlow::Break(err) => Err(err),
                    ControlFlow::Continue(()) => {
                        Ok(influxdb_influxql_parser::select::Field { expr, alias })
                    }
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let (has_field_wildcard, has_group_by_wildcard) = has_wildcards(stmt);

        let (fields, mut group_by) = if has_field_wildcard || has_group_by_wildcard {
            let (field_set, mut tag_set) = from_field_and_dimensions(s, from)?;

            if !has_group_by_wildcard {
                if let Some(group_by) = &stmt.group_by {
                    // Remove any explicitly listed tags in the GROUP BY clause, so they are not
                    // expanded by any wildcards specified in the SELECT projection list
                    group_by.tag_names().for_each(|ident| {
                        tag_set.remove(ident.as_str());
                    });
                }
            }

            let fields = if has_field_wildcard {
                let var_refs = if field_set.is_empty() {
                    vec![]
                } else {
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
                };

                fields_expand_wildcards(fields, var_refs)?
            } else {
                fields
            };

            let group_by = match (&stmt.group_by, has_group_by_wildcard) {
                // GROUP BY with a wildcard
                (Some(group_by), true) => {
                    let group_by_tags = tag_set.into_iter().sorted().collect::<Vec<_>>();
                    let mut new_dimensions = Vec::new();

                    for dim in group_by.iter() {
                        let add_dim = |dim: &String| {
                            new_dimensions.push(Dimension::VarRef(VarRef {
                                name: Identifier::new(dim.clone()),
                                data_type: Some(VarRefDataType::Tag),
                            }))
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
                    Some(GroupByClause::new(new_dimensions))
                }
                // GROUP BY no wildcard
                (Some(group_by), false) => Some(group_by.clone()),
                // No GROUP BY
                (None, _) => None,
            };

            (fields, group_by)
        } else {
            (fields, stmt.group_by.clone())
        };

        // resolve possible tag references in group_by
        if let Some(group_by) = group_by.as_mut() {
            for dim in group_by.iter_mut() {
                let Dimension::VarRef(var_ref) = dim else {
                    continue;
                };
                if from_tag_set.contains(var_ref.name.as_str()) {
                    var_ref.data_type = Some(VarRefDataType::Tag);
                }
            }
        }

        Ok((fields_resolve_aliases_and_types(s, fields, from)?, group_by))
    }

    /// Recursively expand the `from` clause of `stmt` and any subqueries.
    fn expand_from(
        &self,
        s: &dyn SchemaProvider,
        stmt: &SelectStatement,
    ) -> Result<Vec<DataSource>> {
        let mut new_from = Vec::new();
        for ms in &*stmt.from {
            match ms {
                MeasurementSelection::Name(qmn) => match qmn {
                    QualifiedMeasurementName {
                        name: MeasurementName::Name(name),
                        ..
                    } => {
                        if s.table_exists(name) {
                            new_from.push(DataSource::Table(name.deref().to_owned()))
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
                            .for_each(|table| new_from.push(DataSource::Table(table.to_owned())));
                    }
                },
                MeasurementSelection::Subquery(q) => {
                    new_from.push(DataSource::Subquery(Box::new(self.rewrite_subquery(s, q)?)))
                }
            }
        }
        Ok(new_from)
    }

    /// Resolve the data types of any [`VarRef`] expressions in the `WHERE` condition.
    fn condition_resolve_types(
        &self,
        s: &dyn SchemaProvider,
        stmt: &SelectStatement,
        from: &[DataSource],
    ) -> Result<Option<WhereClause>> {
        let Some(mut where_clause) = stmt.condition.clone() else {
            return Ok(None);
        };

        let tv = TypeEvaluator::new(s, from);

        if let ControlFlow::Break(err) = walk_expression_mut(where_clause.deref_mut(), &mut |e| {
            match e {
                ExpressionMut::Arithmetic(e) => walk_expr_mut(e, &mut |e| match e {
                    // Attempt to rewrite all variable (column) references with their concrete types,
                    // if one hasn't been specified.
                    Expr::VarRef(ref mut v) => {
                        v.data_type = match tv.eval_var_ref(v) {
                            Ok(v) => v,
                            Err(e) => ControlFlow::Break(e)?,
                        };
                        ControlFlow::Continue(())
                    }
                    _ => ControlFlow::Continue(()),
                }),
                ExpressionMut::Conditional(_) => ControlFlow::<DataFusionError>::Continue(()),
            }
        }) {
            Err(err)
        } else {
            Ok(Some(where_clause))
        }
    }

    /// Return the interval value of the `GROUP BY` clause if it specifies a `TIME`.
    fn find_interval_offset(
        &self,
        ctx: &ReduceContext,
        group_by: Option<&GroupByClause>,
    ) -> Result<Option<Interval>> {
        Ok(
            if let Some(td) = group_by.and_then(|v| v.time_dimension()) {
                let duration = duration_expr_to_nanoseconds(ctx, &td.interval)
                    .map_err(error::map::expr_error)?;
                let offset = td
                    .offset
                    .as_ref()
                    .map(|o| duration_expr_to_nanoseconds(ctx, o))
                    .transpose()
                    .map_err(error::map::expr_error)?;
                Some(Interval { duration, offset })
            } else {
                None
            },
        )
    }
}

/// Ensures the `time` column is presented consistently across all `SELECT` queries.
///
/// The following transformations may occur
///
/// * Ensure the `time` field is added to all projections;
/// * move the `time` field to the first position; and
/// * remove column alias for `time` in subqueries.
fn field_list_normalize_time(stmt: &mut Select) {
    fn normalize_time(stmt: &mut Select, is_subquery: bool) {
        if let Some(f) = match stmt
            .fields
            .iter()
            .find_position(
                |c| matches!(&c.expr, Expr::VarRef(VarRef { name, .. }) if name.deref() == "time"),
            )
            .map(|(i, _)| i)
        {
            Some(0) => None,
            Some(idx) => Some(stmt.fields.remove(idx)),
            None => Some(Field {
                expr: "time".to_var_ref_expr(),
                name: "time".to_owned(),
                data_type: None,
            }),
        } {
            stmt.fields.insert(0, f)
        }

        let c = &mut stmt.fields[0];
        c.data_type = Some(InfluxColumnType::Timestamp);

        // time aliases in subqueries is ignored
        if is_subquery {
            c.name = "time".to_owned()
        }

        if let Expr::VarRef(VarRef {
            ref mut data_type, ..
        }) = c.expr
        {
            *data_type = Some(VarRefDataType::Timestamp);
        }
    }

    normalize_time(stmt, false);

    // traverse all the subqueries
    let mut data_sources = vec![stmt.from.as_mut_slice()];
    while let Some(from) = data_sources.pop() {
        for sel in from.iter_mut().filter_map(|ds| match ds {
            DataSource::Subquery(q) => Some(q),
            _ => None,
        }) {
            normalize_time(sel, true);
            data_sources.push(&mut sel.from);
        }
    }
}

/// Recursively drop any measurements of the `from` clause of `stmt` that do not project
/// any fields.
fn from_drop_empty(s: &dyn SchemaProvider, stmt: &mut Select) {
    stmt.from.retain_mut(|tr| {
        match tr {
            DataSource::Table(name) => {
                // drop any measurements that have no matching fields in the
                // projection

                if let Some(table) = s.table_schema(name.as_str()) {
                    stmt.fields.iter().any(|f| {
                        walk_expr(&f.expr, &mut |e| {
                            if matches!(e, Expr::VarRef(VarRef { name, ..}) if matches!(table.field_type_by_name(name.deref()), Some(InfluxColumnType::Field(_)))) {
                                ControlFlow::Break(())
                            } else {
                                ControlFlow::Continue(())
                            }
                        }).is_break()
                    })
                } else {
                    false
                }
            }
            DataSource::Subquery(q) => {
                from_drop_empty(s, q);
                if q.from.is_empty() {
                    return false;
                }

                stmt.fields.iter().any(|f| {
                    walk_expr(&f.expr, &mut |e| {
                        if matches!(e, Expr::VarRef(VarRef{ name, ..}) if field_by_name(&q.fields, name.as_str()).is_some()) {
                            ControlFlow::Break(())
                        } else {
                            ControlFlow::Continue(())
                        }
                    }).is_break()
                })
            }
        }
    });
}

/// Determine the combined tag set for the specified `from`.
fn from_tag_set(s: &dyn SchemaProvider, from: &[DataSource]) -> TagSet {
    let mut tag_set = TagSet::new();

    for ds in from {
        match ds {
            DataSource::Table(table_name) => {
                if let Some(table) = s.table_schema(table_name) {
                    tag_set.extend(table.tags_iter().map(|f| f.name().to_owned()))
                }
            }
            DataSource::Subquery(q) => tag_set.extend(q.tag_set.clone()),
        }
    }

    tag_set
}

/// Determine the merged fields and tags of the `FROM` clause.
fn from_field_and_dimensions(
    s: &dyn SchemaProvider,
    from: &[DataSource],
) -> Result<(FieldTypeMap, TagSet)> {
    let mut fs = FieldTypeMap::new();
    let mut ts = TagSet::new();

    for tr in from {
        match tr {
            DataSource::Table(name) => {
                let Some((field_set, tag_set)) = field_and_dimensions(s, name.as_str()) else {
                    continue;
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
            DataSource::Subquery(select) => {
                for f in &select.fields {
                    let Field {
                        name, data_type, ..
                    } = f;
                    let Some(dt) = influx_type_to_var_ref_data_type(*data_type) else {
                        continue;
                    };

                    match fs.get(name.as_str()) {
                        Some(existing_type) => {
                            if dt < *existing_type {
                                fs.insert(name.to_owned(), dt);
                            }
                        }
                        None => {
                            fs.insert(name.to_owned(), dt);
                        }
                    }
                }

                if let Some(group_by) = &select.group_by {
                    // Merge the dimensions from the subquery
                    ts.extend(group_by.tag_names().map(|i| i.deref().to_string()));
                }
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

/// Traverse expressions of all `fields` and expand wildcard or regular expressions
/// either at the top-level or as the first argument of function calls, such as `SUM(*)`.
///
/// `var_refs` contains the list of field and tags that should be expanded by wildcards.
fn fields_expand_wildcards(
    fields: Vec<influxdb_influxql_parser::select::Field>,
    var_refs: Vec<VarRef>,
) -> Result<Vec<influxdb_influxql_parser::select::Field>> {
    let mut new_fields = Vec::new();

    for f in fields {
        let add_field = |f: &VarRef| {
            new_fields.push(influxdb_influxql_parser::select::Field {
                expr: Expr::VarRef(f.clone()),
                alias: None,
            })
        };

        match &f.expr {
            Expr::Wildcard(wct) => {
                let filter: fn(&&VarRef) -> bool = match wct {
                    None => |_| true,
                    Some(WildcardType::Tag) => |v| v.data_type.map_or(false, |dt| dt.is_tag_type()),
                    Some(WildcardType::Field) => {
                        |v| v.data_type.map_or(false, |dt| dt.is_field_type())
                    }
                };

                var_refs.iter().filter(filter).for_each(add_field);
            }

            Expr::Literal(Literal::Regex(re)) => {
                let re = util::parse_regex(re)?;
                var_refs
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

                // a list of supported types that may be selected from the var_refs
                // vector when expanding wildcards in functions.
                let mut supported_types = HashSet::from([
                    Some(VarRefDataType::Float),
                    Some(VarRefDataType::Integer),
                    Some(VarRefDataType::Unsigned),
                ]);

                // Modify the supported types for certain functions.
                match name.as_str() {
                    "count" | "first" | "last" | "distinct" | "elapsed" | "mode" | "sample" => {
                        supported_types
                            .extend([Some(VarRefDataType::String), Some(VarRefDataType::Boolean)]);
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
                    new_fields.push(influxdb_influxql_parser::select::Field {
                        expr: Expr::Call(Call {
                            name: name.clone(),
                            args,
                        }),
                        alias: Some(format!("{}_{}", field_name(&f), v.name).into()),
                    })
                };

                match args.first() {
                    Some(Expr::Wildcard(Some(WildcardType::Tag))) => {
                        return error::query(format!("unable to use tag as wildcard in {name}()"));
                    }
                    Some(Expr::Wildcard(_)) => {
                        var_refs
                            .iter()
                            .filter(|v| supported_types.contains(&v.data_type))
                            .for_each(add_field);
                    }
                    Some(Expr::Literal(Literal::Regex(re))) => {
                        let re = util::parse_regex(re)?;
                        var_refs
                            .iter()
                            .filter(|v| {
                                supported_types.contains(&v.data_type)
                                    && re.is_match(v.name.as_str())
                            })
                            .for_each(add_field);
                    }
                    _ => {
                        new_fields.push(f);
                        continue;
                    }
                }
            }

            Expr::Binary { .. } => {
                let has_wildcard = walk_expr(&f.expr, &mut |e| {
                    if matches!(e, Expr::Wildcard(_) | Expr::Literal(Literal::Regex(_))) {
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                })
                .is_break();

                if has_wildcard {
                    return error::query(
                        "unsupported binary expression: contains a wildcard or regular expression",
                    );
                }

                new_fields.push(f);
            }

            _ => new_fields.push(f),
        }
    }

    Ok(new_fields)
}

/// Resolve the projection list column names and data types.
/// Column names are resolved in accordance with the [original implementation].
///
/// [original implementation]: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L1651
fn fields_resolve_aliases_and_types(
    s: &dyn SchemaProvider,
    fields: Vec<influxdb_influxql_parser::select::Field>,
    from: &[DataSource],
) -> Result<Vec<Field>> {
    let names = fields.iter().map(field_name).collect::<Vec<_>>();
    let mut column_aliases = HashMap::<&str, _>::from_iter(names.iter().map(|f| (f.as_str(), 0)));

    let tv = TypeEvaluator::new(s, from);

    names
        .iter()
        .zip(fields)
        .map(|(name, field)| {
            let expr = field.expr;
            let data_type = tv.eval_type(&expr)?;
            let name = match column_aliases.get(name.as_str()) {
                Some(0) => {
                    column_aliases.insert(name, 1);
                    name.to_owned()
                }
                Some(count) => {
                    let mut count = *count;
                    let mut resolved_name = name.to_owned();
                    resolved_name.push('_');
                    let orig_len = resolved_name.len();

                    loop {
                        resolved_name.push_str(count.to_string().as_str());
                        if column_aliases.contains_key(resolved_name.as_str()) {
                            count += 1;
                            resolved_name.truncate(orig_len)
                        } else {
                            column_aliases.insert(name, count + 1);
                            break resolved_name;
                        }
                    }
                }
                None => unreachable!(),
            };

            Ok(Field {
                expr,
                name,
                data_type: var_ref_data_type_to_influx_type(data_type),
            })
        })
        .collect::<Result<Vec<_>>>()
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

/// Set the `extra_intervals` field of [`FieldChecker`] if it is
/// less than then proposed new value.
macro_rules! set_extra_intervals {
    ($SELF:expr, $NEW:expr) => {
        if $SELF.extra_intervals < $NEW as usize {
            $SELF.extra_intervals = $NEW as usize
        }
    };
}

/// Checks a number of expectations for the fields of a [`SelectStatement`].
#[derive(Default)]
struct FieldChecker {
    /// `true` if the statement contains a `GROUP BY TIME` clause.
    has_group_by_time: bool,

    /// The number of additional intervals that must be read
    /// for queries that group by time and use window functions such as
    /// `DIFFERENCE` or `DERIVATIVE`. This ensures data for the first
    /// window is available.
    ///
    /// See: <https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/compile.go#L50>
    extra_intervals: usize,

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

    /// Accumulator for the number of window expressions for the statement.
    window_count: usize,

    /// Accumulator for the number of selector expressions for the statement.
    selector_count: usize,
    // Set to `true` if any window or aggregate functions are expected to
    // only produce non-null results.
    //
    // This replicates the
    // filter_null_rows: bool,
}

impl FieldChecker {
    fn check_fields(
        &mut self,
        fields: &[Field],
        fill: Option<FillClause>,
    ) -> Result<SelectStatementInfo> {
        fields.iter().try_for_each(|f| self.check_expr(&f.expr))?;

        match self.function_count() {
            0 => {
                // FILL(PREVIOUS) and FILL(<value>) are both supported for non-aggregate queries
                //
                // See: https://github.com/influxdata/influxdb/blob/98361e207349a3643bcc332d54b009818fe7585f/query/compile.go#L1002-L1012
                match fill {
                    Some(FillClause::None) => {
                        return error::query(
                            "FILL(none) must be used with an aggregate or selector function",
                        )
                    }
                    Some(FillClause::Linear) => {
                        return error::query(
                            "FILL(linear) must be used with an aggregate or selector function",
                        )
                    }
                    _ => {}
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
            if self.window_aggregate_count() > 0 {
                return error::query("mixing aggregate and non-aggregate columns is not supported");
            } else if self.selector_count > 1 {
                return error::query(
                    "mixing multiple selector functions with tags or fields is not supported",
                );
            }
        }

        // At this point the statement is valid, and numerous preconditions
        // have been met. The final state of the `FieldChecker` is inspected
        // to determine the type of projection. The ProjectionType dictates
        // how the query will be planned and other cases, such as how NULL
        // values are handled, to ensure compatibility with InfluxQL OG.

        let projection_type = if self.has_top_bottom {
            ProjectionType::TopBottomSelector
        } else if self.has_group_by_time {
            if self.window_count > 0 {
                if self.window_count == self.aggregate_count + self.selector_count {
                    ProjectionType::WindowAggregate
                } else {
                    ProjectionType::WindowAggregateMixed
                }
            } else {
                ProjectionType::Aggregate
            }
        } else if self.has_distinct {
            ProjectionType::RawDistinct
        } else if self.selector_count == 1 && self.aggregate_count == 0 {
            ProjectionType::Selector {
                has_fields: self.has_non_aggregate_fields,
            }
        } else if self.selector_count > 1 || self.aggregate_count > 0 {
            ProjectionType::Aggregate
        } else if self.window_count > 0 {
            ProjectionType::Window
        } else {
            ProjectionType::Raw
        };

        Ok(SelectStatementInfo {
            projection_type,
            extra_intervals: self.extra_intervals,
        })
    }

    /// The total number of functions observed.
    fn function_count(&self) -> usize {
        self.window_aggregate_count() + self.selector_count
    }

    /// The total number of window and aggregate functions observed.
    fn window_aggregate_count(&self) -> usize {
        self.aggregate_count + self.window_count
    }
}

impl FieldChecker {
    fn check_expr(&mut self, e: &Expr) -> Result<()> {
        match e {
            // The `time` column is ignored
            Expr::VarRef(VarRef { name, .. }) if name.deref() == "time" => Ok(()),
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
        self.inc_window_count();

        check_exp_args!(name, 1, 2, args);

        set_extra_intervals!(self, 1);

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
        self.inc_window_count();
        check_exp_args!(name, 1, 2, args);

        set_extra_intervals!(self, 1);

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
        self.inc_window_count();
        check_exp_args!(name, 1, args);

        set_extra_intervals!(self, 1);

        self.check_nested_symbol(name, &args[0])
    }

    fn check_cumulative_sum(&mut self, args: &[Expr]) -> Result<()> {
        self.inc_window_count();
        check_exp_args!("cumulative_sum", 1, args);
        self.check_nested_symbol("cumulative_sum", &args[0])
    }

    fn check_moving_average(&mut self, args: &[Expr]) -> Result<()> {
        self.inc_window_count();
        check_exp_args!("moving_average", 2, args);

        let v = lit_integer!("moving_average", args, 1);
        if v <= 1 {
            return error::query(format!(
                "moving_average window must be greater than 1, got {v}"
            ));
        }

        set_extra_intervals!(self, v);

        self.check_nested_symbol("moving_average", &args[0])
    }

    fn check_exponential_moving_average(&mut self, name: &str, args: &[Expr]) -> Result<()> {
        self.inc_window_count();
        check_exp_args!(name, 2, 4, args);

        let v = lit_integer!(name, args, 1);
        if v < 1 {
            return error::query(format!("{name} period must be greater than 1, got {v}"));
        }

        set_extra_intervals!(self, v);

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
        self.inc_window_count();
        check_exp_args!(name, 2, 3, args);

        let v = lit_integer!(name, args, 1);
        if v < 1 {
            return error::query(format!("{name} period must be greater than 1, got {v}"));
        }

        set_extra_intervals!(self, v);

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
        self.inc_window_count();
        check_exp_args!(name, 2, 4, args);

        let v = lit_integer!(name, args, 1);
        if v < 1 {
            return error::query(format!("{name} period must be greater than 1, got {v}"));
        }

        set_extra_intervals!(self, v);

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

    /// Increments the aggregate function call count
    fn inc_aggregate_count(&mut self) {
        self.aggregate_count += 1
    }

    /// Increments the window function call count
    fn inc_window_count(&mut self) {
        self.window_count += 1
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
    /// A query that projects one or more window functions.
    Window,
    /// A query that projects a combination of window and nested aggregate functions.
    WindowAggregate,
    /// A query that projects a combination of window and nested aggregate functions, including
    /// separate projections that are just aggregates. This requires special handling of
    /// windows that produce `NULL` results.
    WindowAggregateMixed,
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
struct SelectStatementInfo {
    /// Identifies the projection type for the `SELECT` query.
    projection_type: ProjectionType,
    /// Copied from [extra_intervals](FieldChecker::extra_intervals)
    ///
    /// [See also](Select::extra_intervals).
    extra_intervals: usize,
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
fn select_statement_info(
    fields: &[Field],
    group_by: &Option<GroupByClause>,
    fill: Option<FillClause>,
) -> Result<SelectStatementInfo> {
    let has_group_by_time = group_by
        .as_ref()
        .and_then(|gb| gb.time_dimension())
        .is_some();

    let mut fc = FieldChecker {
        has_group_by_time,
        ..Default::default()
    };

    fc.check_fields(fields, fill)
}

#[cfg(test)]
mod test {
    use super::Result;
    use crate::plan::ir::{Field, Select};
    use crate::plan::rewriter::{
        find_table_names, has_wildcards, rewrite_select, rewrite_statement, ProjectionType,
        SelectStatementInfo,
    };
    use crate::plan::test_utils::{parse_select, MockSchemaProvider};
    use assert_matches::assert_matches;
    use datafusion::error::DataFusionError;
    use influxdb_influxql_parser::select::SelectStatement;
    use test_helpers::{assert_contains, assert_error};

    #[test]
    fn test_find_table_names() {
        let namespace = MockSchemaProvider::default();
        let parse_select = |s: &str| -> Select {
            let select = parse_select(s);
            rewrite_select(&namespace, &select).unwrap()
        };

        /// Return `find_table_names` as a `Vec` for tests.
        fn find_table_names_vec(s: &Select) -> Vec<&str> {
            find_table_names(s).into_iter().collect()
        }

        let s = parse_select("SELECT usage_idle FROM cpu");
        assert_eq!(find_table_names_vec(&s), &["cpu"]);

        let s = parse_select("SELECT usage_idle FROM cpu, disk");
        assert_eq!(find_table_names_vec(&s), &["cpu", "disk"]);

        let s = parse_select("SELECT usage_idle FROM disk, cpu, disk");
        assert_eq!(find_table_names_vec(&s), &["cpu", "disk"]);

        // subqueries

        let s = parse_select("SELECT usage_idle FROM (select * from cpu, disk)");
        assert_eq!(find_table_names_vec(&s), &["cpu", "disk"]);

        let s = parse_select("SELECT usage_idle FROM cpu, (select * from cpu, disk)");
        assert_eq!(find_table_names_vec(&s), &["cpu", "disk"]);
    }

    #[test]
    fn test_select_statement_info() {
        let namespace = MockSchemaProvider::default();
        let parse_select = |s: &str| -> Select {
            let select = parse_select(s);
            rewrite_select(&namespace, &select).unwrap()
        };

        fn select_statement_info(q: &Select) -> Result<SelectStatementInfo> {
            super::select_statement_info(&q.fields, &q.group_by, q.fill)
        }

        let info = select_statement_info(&parse_select("SELECT foo, bar FROM cpu")).unwrap();
        assert_matches!(info.projection_type, ProjectionType::Raw);

        let info = select_statement_info(&parse_select("SELECT distinct(foo) FROM cpu")).unwrap();
        assert_matches!(info.projection_type, ProjectionType::RawDistinct);

        let info = select_statement_info(&parse_select("SELECT last(foo) FROM cpu")).unwrap();
        assert_matches!(
            info.projection_type,
            ProjectionType::Selector { has_fields: false }
        );

        // updates extra_intervals
        let info = select_statement_info(&parse_select("SELECT difference(foo) FROM cpu")).unwrap();
        assert_matches!(info.projection_type, ProjectionType::Window);
        assert_matches!(info.extra_intervals, 1);
        // derives extra intervals from the window function
        let info =
            select_statement_info(&parse_select("SELECT moving_average(foo, 5) FROM cpu")).unwrap();
        assert_matches!(info.projection_type, ProjectionType::Window);
        assert_matches!(info.extra_intervals, 5);
        // uses the maximum extra intervals
        let info = select_statement_info(&parse_select(
            "SELECT difference(foo), moving_average(foo, 4) FROM cpu",
        ))
        .unwrap();
        assert_matches!(info.extra_intervals, 4);

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

        let info = select_statement_info(&parse_select(
            "SELECT difference(count(foo)) FROM cpu GROUP BY TIME(10s)",
        ))
        .unwrap();
        assert_matches!(info.projection_type, ProjectionType::WindowAggregate);

        let info = select_statement_info(&parse_select(
            "SELECT difference(count(foo)), mean(foo) FROM cpu GROUP BY TIME(10s)",
        ))
        .unwrap();
        assert_matches!(info.projection_type, ProjectionType::WindowAggregateMixed);

        let info = select_statement_info(&parse_select("SELECT top(foo, 3) FROM cpu")).unwrap();
        assert_matches!(info.projection_type, ProjectionType::TopBottomSelector);
    }

    /// Verify all the aggregate, window-like and selector functions are handled
    /// by `select_statement_info`.
    #[test]
    fn test_select_statement_info_functions() {
        fn select_statement_info(q: &SelectStatement) -> Result<SelectStatementInfo> {
            let columns = q
                .fields
                .iter()
                .map(|f| Field {
                    expr: f.expr.clone(),
                    name: "".to_owned(),
                    data_type: None,
                })
                .collect::<Vec<_>>();
            super::select_statement_info(&columns, &q.group_by, q.fill)
        }

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

        // Test rules for math functions
        let sel = parse_select("SELECT abs(usage_idle) FROM cpu");
        select_statement_info(&sel).unwrap();

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

        // aggregate functions require a field reference
        let sel = parse_select("SELECT sum(1) FROM cpu");
        assert_error!(select_statement_info(&sel), DataFusionError::Plan(ref s) if s == "expected field argument in sum(), got Literal(Integer(1))");
    }

    mod rewrite_statement {
        use super::*;
        use crate::plan::ir::TagSet;
        use datafusion::common::Result;
        use influxdb_influxql_parser::select::SelectStatement;
        use schema::{InfluxColumnType, InfluxFieldType, SchemaBuilder};

        /// Test implementation that converts `Select` to `SelectStatement` so that it can be
        /// converted back to a string.
        fn rewrite_select_statement(
            s: &MockSchemaProvider,
            q: &SelectStatement,
        ) -> Result<SelectStatement> {
            let stmt = rewrite_statement(s, q)?;
            Ok(stmt.select.into())
        }

        /// Validate the data types of the fields of a [`Select`].
        #[test]
        fn projection_schema() {
            let namespace = MockSchemaProvider::default();

            let stmt = parse_select("SELECT usage_idle, usage_idle + usage_system, cpu FROM cpu");
            let q = rewrite_statement(&namespace, &stmt).unwrap();
            // first field is always the time column and thus a Timestamp
            assert_matches!(
                q.select.fields[0].data_type,
                Some(InfluxColumnType::Timestamp)
            );
            // usage_idle is a Float
            assert_matches!(
                q.select.fields[1].data_type,
                Some(InfluxColumnType::Field(InfluxFieldType::Float))
            );
            // The expression usage_idle + usage_system is a Float
            assert_matches!(
                q.select.fields[2].data_type,
                Some(InfluxColumnType::Field(InfluxFieldType::Float))
            );
            // cpu is a Tag
            assert_matches!(q.select.fields[3].data_type, Some(InfluxColumnType::Tag));

            let stmt = parse_select("SELECT field_i64 + field_f64, field_i64 / field_i64, field_u64 / field_i64 FROM all_types");
            let q = rewrite_statement(&namespace, &stmt).unwrap();
            // first field is always the time column and thus a Timestamp
            assert_matches!(
                q.select.fields[0].data_type,
                Some(InfluxColumnType::Timestamp)
            );
            // Expression is promoted to a Float
            assert_matches!(
                q.select.fields[1].data_type,
                Some(InfluxColumnType::Field(InfluxFieldType::Float))
            );
            // Integer division is promoted to a Float
            assert_matches!(
                q.select.fields[2].data_type,
                Some(InfluxColumnType::Field(InfluxFieldType::Float))
            );
            // Unsigned division is still Unsigned
            assert_matches!(
                q.select.fields[3].data_type,
                Some(InfluxColumnType::Field(InfluxFieldType::UInteger))
            );
        }

        /// Validate the tag_set field of a [`Select]`
        #[test]
        fn tag_set_schema() {
            let namespace = MockSchemaProvider::default();

            macro_rules! assert_tag_set {
                ($Q:ident, $($TAG:literal),*) => {
                    assert_eq!($Q.select.tag_set, TagSet::from([$($TAG.to_owned(),)*]))
                };
            }

            let stmt = parse_select("SELECT usage_system FROM cpu");
            let q = rewrite_statement(&namespace, &stmt).unwrap();
            assert_tag_set!(q, "cpu", "host", "region");

            let stmt = parse_select("SELECT usage_system FROM cpu, disk");
            let q = rewrite_statement(&namespace, &stmt).unwrap();
            assert_tag_set!(q, "cpu", "host", "region", "device");

            let stmt =
                parse_select("SELECT usage_system FROM (select * from cpu), (select * from disk)");
            let q = rewrite_statement(&namespace, &stmt).unwrap();
            assert_tag_set!(q, "cpu", "host", "region", "device");
        }

        /// Validating types for simple projections
        #[test]
        fn projection_simple() {
            let namespace = MockSchemaProvider::default();

            // Exact, match
            let stmt = parse_select("SELECT usage_user FROM cpu");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_user::float AS usage_user FROM cpu"
            );

            // Duplicate columns do not have conflicting aliases
            let stmt = parse_select("SELECT usage_user, usage_user FROM cpu");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_user::float AS usage_user, usage_user::float AS usage_user_1 FROM cpu"
            );

            // Multiple aliases with no conflicts
            let stmt = parse_select("SELECT usage_user as usage_user_1, usage_user FROM cpu");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_user::float AS usage_user_1, usage_user::float AS usage_user FROM cpu"
            );

            // Multiple aliases with conflicts
            let stmt =
                parse_select("SELECT usage_user as usage_user_1, usage_user, usage_user, usage_user as usage_user_2, usage_user, usage_user_2 FROM cpu");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(stmt.to_string(), "SELECT time::timestamp AS time, usage_user::float AS usage_user_1, usage_user::float AS usage_user, usage_user::float AS usage_user_3, usage_user::float AS usage_user_2, usage_user::float AS usage_user_4, usage_user_2 AS usage_user_2_1 FROM cpu");

            // Only include measurements with at least one field projection
            let stmt = parse_select("SELECT usage_idle FROM cpu, disk");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle FROM cpu"
            );

            // Field does not exist in single measurement
            let stmt = parse_select("SELECT usage_idle, bytes_free FROM cpu");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle, bytes_free AS bytes_free FROM cpu"
            );

            // Field exists in each measurement
            let stmt = parse_select("SELECT usage_idle, bytes_free FROM cpu, disk");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle, bytes_free::integer AS bytes_free FROM cpu, disk"
            );
        }

        /// Validate the expansion of the `FROM` clause using regular expressions
        #[test]
        fn from_expand_wildcards() {
            let namespace = MockSchemaProvider::default();

            // Regex, match, fields from multiple measurements
            let stmt = parse_select("SELECT bytes_free, bytes_read FROM /d/");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, bytes_free::integer AS bytes_free, bytes_read::integer AS bytes_read FROM disk, diskio"
            );

            // Regex matches multiple measurement, but only one has a matching field
            let stmt = parse_select("SELECT bytes_free FROM /d/");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, bytes_free::integer AS bytes_free FROM disk"
            );

            // Exact, no match
            let stmt = parse_select("SELECT usage_idle FROM foo");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert!(stmt.from.is_empty());

            // Regex, no match
            let stmt = parse_select("SELECT bytes_free FROM /^d$/");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert!(stmt.from.is_empty());
        }

        /// Expanding the projection using wildcards
        #[test]
        fn projection_expand_wildcards() {
            let namespace = MockSchemaProvider::default();

            // Single wildcard, single measurement
            let stmt = parse_select("SELECT * FROM cpu");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, cpu::tag AS cpu, host::tag AS host, region::tag AS region, usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu"
            );

            let stmt = parse_select("SELECT * FROM cpu, disk");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, bytes_free::integer AS bytes_free, bytes_used::integer AS bytes_used, cpu::tag AS cpu, device::tag AS device, host::tag AS host, region::tag AS region, usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu, disk"
            );

            // Regular expression selects fields from multiple measurements
            let stmt = parse_select("SELECT /usage|bytes/ FROM cpu, disk");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, bytes_free::integer AS bytes_free, bytes_used::integer AS bytes_used, usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu, disk"
            );

            // Selective wildcard for tags
            let stmt = parse_select("SELECT *::tag, usage_idle FROM cpu");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, cpu::tag AS cpu, host::tag AS host, region::tag AS region, usage_idle::float AS usage_idle FROM cpu"
            );

            // Selective wildcard for tags only should not select any measurements
            let stmt = parse_select("SELECT *::tag FROM cpu");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert!(stmt.from.is_empty());

            // Selective wildcard for fields
            let stmt = parse_select("SELECT *::field FROM cpu");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu"
            );

            // Mixed fields and wildcards
            let stmt = parse_select("SELECT usage_idle, *::tag FROM cpu");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle, cpu::tag AS cpu, host::tag AS host, region::tag AS region FROM cpu"
            );

            let stmt = parse_select("SELECT * FROM merge_00, merge_01");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, col0::float AS col0, col0::tag AS col0_1, col1::float AS col1, col1::tag AS col1_1, col2::string AS col2, col3::string AS col3 FROM merge_00, merge_01"
            );

            // This should only select merge_01, as col0 is a tag in merge_00
            let stmt = parse_select("SELECT /col0/ FROM merge_00, merge_01");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, col0::float AS col0, col0::tag AS col0_1 FROM merge_01"
            );
        }

        /// Validate type resolution of [`VarRef`] nodes in the `WHERE` clause.
        #[test]
        fn condition() {
            let namespace = MockSchemaProvider::default();

            // resolves float field
            let stmt = parse_select("SELECT usage_idle FROM cpu WHERE usage_user > 0");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle FROM cpu WHERE usage_user::float > 0"
            );

            // resolves tag field
            let stmt = parse_select("SELECT usage_idle FROM cpu WHERE cpu =~ /foo/");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle FROM cpu WHERE cpu::tag =~ /foo/"
            );

            // Does not resolve an unknown field
            let stmt = parse_select("SELECT usage_idle FROM cpu WHERE non_existent = 'bar'");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle FROM cpu WHERE non_existent = 'bar'"
            );

            // Handles multiple measurements; `bytes_free` is from the `disk` measurement
            let stmt =
                parse_select("SELECT usage_idle, bytes_free FROM cpu, disk WHERE bytes_free = 3");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle, bytes_free::integer AS bytes_free FROM cpu, disk WHERE bytes_free::integer = 3"
            );

            // Resolves recursively through subqueries and aliases
            let stmt = parse_select("SELECT bytes FROM (SELECT bytes_free AS bytes FROM disk WHERE bytes_free = 3) WHERE bytes > 0");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, bytes::integer AS bytes FROM (SELECT time::timestamp AS time, bytes_free::integer AS bytes FROM disk WHERE bytes_free::integer = 3) WHERE bytes::integer > 0"
            );
        }

        #[test]
        fn group_by() {
            let namespace = MockSchemaProvider::default();

            let stmt = parse_select("SELECT usage_idle FROM cpu GROUP BY host");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle FROM cpu GROUP BY host::tag"
            );

            // resolves tag types from multiple measurements
            let stmt =
                parse_select("SELECT usage_idle, bytes_free FROM cpu, disk GROUP BY host, device");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle, bytes_free::integer AS bytes_free FROM cpu, disk GROUP BY host::tag, device::tag"
            );

            // does not resolve non-existent tag
            let stmt = parse_select("SELECT usage_idle FROM cpu GROUP BY host, non_existent");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle FROM cpu GROUP BY host::tag, non_existent"
            );

            let stmt = parse_select("SELECT usage_idle FROM cpu GROUP BY *");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle FROM cpu GROUP BY cpu::tag, host::tag, region::tag"
            );

            // Does not include tags in projection when expanded in GROUP BY
            let stmt = parse_select("SELECT * FROM cpu GROUP BY *");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu GROUP BY cpu::tag, host::tag, region::tag"
            );

            // Does include explicitly listed tags in projection
            let stmt = parse_select("SELECT host, * FROM cpu GROUP BY *");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, host::tag AS host, usage_idle::float AS usage_idle, usage_system::float AS usage_system, usage_user::float AS usage_user FROM cpu GROUP BY cpu::tag, host::tag, region::tag"
            );

            //
            // TIME
            //

            // Explicitly adds an upper bound for the time-range for aggregate queries
            let stmt = parse_select("SELECT mean(usage_idle) FROM cpu WHERE time >= '2022-04-09T12:13:14Z' GROUP BY TIME(30s)");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, mean(usage_idle::float) AS mean FROM cpu WHERE time >= 1649506394000000000 AND time <= 1672531200000000000 GROUP BY TIME(30s)"
            );

            // Does not add an upper bound time range if already specified
            let stmt = parse_select("SELECT mean(usage_idle) FROM cpu WHERE time >= '2022-04-09T12:13:14Z' AND time < '2022-04-10T12:00:00Z' GROUP BY TIME(30s)");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, mean(usage_idle::float) AS mean FROM cpu WHERE time >= 1649506394000000000 AND time <= 1649591999999999999 GROUP BY TIME(30s)"
            );
        }

        /// Uncategorized fallible cases
        #[test]
        fn fallible() {
            let namespace = MockSchemaProvider::default();

            // invalid expression, combining float and string fields
            let stmt = parse_select("SELECT field_f64 + field_str FROM all_types");
            let err = rewrite_select_statement(&namespace, &stmt).unwrap_err();
            assert_contains!(
                err.to_string(),
                "Error during planning: incompatible operands for operator +: float and string"
            );

            // invalid expression, combining string and string fields, which is compatible with InfluxQL
            let stmt = parse_select("SELECT field_str + field_str FROM all_types");
            let err = rewrite_select_statement(&namespace, &stmt).unwrap_err();
            assert_contains!(
                err.to_string(),
                "Error during planning: incompatible operands for operator +: string and string"
            );

            // Invalid regex
            let stmt = parse_select("SELECT usage_idle FROM /(not/");
            let err = rewrite_select_statement(&namespace, &stmt).unwrap_err();
            assert_contains!(err.to_string(), "invalid regular expression");

            let stmt = parse_select("SELECT *::field + *::tag FROM cpu");
            let err = rewrite_select_statement(&namespace, &stmt).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Error during planning: unsupported binary expression: contains a wildcard or regular expression"
            );

            let stmt = parse_select("SELECT COUNT(*) + SUM(usage_idle) FROM cpu");
            let err = rewrite_select_statement(&namespace, &stmt).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Error during planning: unsupported binary expression: contains a wildcard or regular expression"
            );

            let stmt = parse_select("SELECT COUNT(*::tag) FROM cpu");
            let err = rewrite_select_statement(&namespace, &stmt).unwrap_err();
            assert_eq!(
                err.to_string(),
                "Error during planning: unable to use tag as wildcard in count()"
            );

            let stmt = parse_select("SELECT usage_idle FROM cpu SLIMIT 1");
            let err = rewrite_select_statement(&namespace, &stmt).unwrap_err();
            assert_eq!(
                err.to_string(),
                "This feature is not implemented: SLIMIT or SOFFSET"
            );

            let stmt = parse_select("SELECT usage_idle FROM cpu SOFFSET 1");
            let err = rewrite_select_statement(&namespace, &stmt).unwrap_err();
            assert_eq!(
                err.to_string(),
                "This feature is not implemented: SLIMIT or SOFFSET"
            );
        }

        /// Verify subqueries
        #[test]
        fn subqueries() {
            let namespace = MockSchemaProvider::default();

            // Subquery, exact, match
            let stmt = parse_select("SELECT usage_idle FROM (SELECT usage_idle FROM cpu)");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle FROM (SELECT time::timestamp AS time, usage_idle::float AS usage_idle FROM cpu)"
            );

            // Subquery, regex, match
            let stmt =
                parse_select("SELECT bytes_free FROM (SELECT bytes_free, bytes_read FROM /d/)");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, bytes_free::integer AS bytes_free FROM (SELECT time::timestamp AS time, bytes_free::integer AS bytes_free, bytes_read::integer AS bytes_read FROM disk, diskio)"
            );

            // Subquery, exact, no match
            let stmt = parse_select("SELECT usage_idle FROM (SELECT usage_idle FROM foo)");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert!(stmt.from.is_empty());

            // Subquery, regex, no match
            let stmt = parse_select("SELECT bytes_free FROM (SELECT bytes_free FROM /^d$/)");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert!(stmt.from.is_empty());

            // Correct data type is resolved from subquery
            let stmt =
                parse_select("SELECT *::field FROM (SELECT usage_system + usage_idle FROM cpu)");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_system_usage_idle::float AS usage_system_usage_idle FROM (SELECT time::timestamp AS time, usage_system::float + usage_idle::float AS usage_system_usage_idle FROM cpu)"
            );

            // Subquery, no fields projected should be dropped
            let stmt = parse_select("SELECT usage_idle FROM cpu, (SELECT usage_system FROM cpu)");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, usage_idle::float AS usage_idle FROM cpu"
            );

            // Outer query are permitted to project tags only, as long as there are other fields
            // in the subquery
            let stmt = parse_select("SELECT cpu FROM (SELECT cpu, usage_system FROM cpu)");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, cpu::tag AS cpu FROM (SELECT time::timestamp AS time, cpu::tag AS cpu, usage_system::float AS usage_system FROM cpu)"
            );

            // Outer FROM should be empty, as the subquery does not project any fields
            let stmt = parse_select("SELECT cpu FROM (SELECT cpu FROM cpu)");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert!(stmt.from.is_empty());

            // GROUP BY clauses

            // Projects cpu tag in outer query, as it was specified in the GROUP BY of the subquery
            let stmt = parse_select("SELECT * FROM (SELECT usage_system FROM cpu GROUP BY cpu)");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, cpu::tag AS cpu, usage_system::float AS usage_system FROM (SELECT time::timestamp AS time, usage_system::float AS usage_system FROM cpu GROUP BY cpu::tag)"
            );

            // Specifically project cpu tag from GROUP BY
            let stmt = parse_select(
                "SELECT cpu, usage_system FROM (SELECT usage_system FROM cpu GROUP BY cpu)",
            );
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, cpu::tag AS cpu, usage_system::float AS usage_system FROM (SELECT time::timestamp AS time, usage_system::float AS usage_system FROM cpu GROUP BY cpu::tag)"
            );

            // Projects cpu tag in outer query separately from aliased cpu tag "foo"
            let stmt = parse_select(
                "SELECT * FROM (SELECT cpu as foo, usage_system FROM cpu GROUP BY cpu)",
            );
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, cpu::tag AS cpu, foo::tag AS foo, usage_system::float AS usage_system FROM (SELECT time::timestamp AS time, cpu::tag AS foo, usage_system::float AS usage_system FROM cpu GROUP BY cpu::tag)"
            );

            // Projects non-existent foo as a tag in the outer query
            let stmt = parse_select(
                "SELECT * FROM (SELECT usage_idle FROM cpu GROUP BY foo) GROUP BY cpu",
            );
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, foo::tag AS foo, usage_idle::float AS usage_idle FROM (SELECT time::timestamp AS time, usage_idle::float AS usage_idle FROM cpu GROUP BY foo) GROUP BY cpu::tag"
            );
            // Normalises time to all leaf subqueries
            let stmt = parse_select(
                "SELECT * FROM (SELECT MAX(value) FROM (SELECT DISTINCT(usage_idle) AS value FROM cpu)) GROUP BY cpu",
            );
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, max::float AS max FROM (SELECT time::timestamp AS time, max(value::float) AS max FROM (SELECT time::timestamp AS time, distinct(usage_idle::float) AS value FROM cpu FILL(NONE)) FILL(NONE)) GROUP BY cpu::tag"
            );

            // Projects non-existent tag, "bytes_free" from cpu and also bytes_free field from disk
            // NOTE: InfluxQL OG does something really strange and arguably incorrect
            //
            // ```
            // SELECT * FROM (SELECT usage_idle FROM cpu GROUP BY bytes_free), (SELECT bytes_free FROM disk) GROUP BY cpu
            // ```
            //
            // The output shows that InfluxQL expanded the non-existent bytes_free tag (bytes_free1)
            // from the cpu measurement, and the bytes_free field from the disk measurement as two
            // separate columns but when producing the results, read the data from the `bytes_free`
            // field for the disk table.
            //
            // ```
            // name: cpu
            // tags: cpu=cpu-total
            // time                bytes_free bytes_free_1 usage_idle
            // ----                ---------- ------------ ----------
            // 1667181600000000000                         2.98
            // 1667181610000000000                         2.99
            // ... trimmed for brevity
            //
            // name: disk
            // tags: cpu=
            // time                bytes_free bytes_free_1 usage_idle
            // ----                ---------- ------------ ----------
            // 1667181600000000000 1234       1234
            // 1667181600000000000 3234       3234
            // ... trimmed for brevity
            // ```
            let stmt = parse_select(
                "SELECT * FROM (SELECT usage_idle FROM cpu GROUP BY bytes_free), (SELECT bytes_free FROM disk) GROUP BY cpu",
            );
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, bytes_free::integer AS bytes_free, bytes_free::tag AS bytes_free_1, usage_idle::float AS usage_idle FROM (SELECT time::timestamp AS time, usage_idle::float AS usage_idle FROM cpu GROUP BY bytes_free), (SELECT time::timestamp AS time, bytes_free::integer AS bytes_free FROM disk) GROUP BY cpu::tag"
            );
        }

        /// `DISTINCT` clause and `distinct` function
        #[test]
        fn projection_distinct() {
            let namespace = MockSchemaProvider::default();

            // COUNT(DISTINCT)
            let stmt = parse_select("SELECT COUNT(DISTINCT bytes_free) FROM disk");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, count(distinct(bytes_free::integer)) AS count FROM disk"
            );

            let stmt = parse_select("SELECT DISTINCT bytes_free FROM disk");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, distinct(bytes_free::integer) AS \"distinct\" FROM disk"
            );
        }

        /// Projections with unary and binary expressions
        #[test]
        fn projection_unary_binary_expr() {
            let namespace = MockSchemaProvider::default();

            // Binary expression
            let stmt = parse_select("SELECT bytes_free+bytes_used FROM disk");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, bytes_free::integer + bytes_used::integer AS bytes_free_bytes_used FROM disk"
            );

            // Unary expressions
            let stmt = parse_select("SELECT -bytes_free FROM disk");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, -1 * bytes_free::integer AS bytes_free FROM disk"
            );
        }

        /// Projections which contain function calls
        #[test]
        fn projection_call_expr() {
            let mut namespace = MockSchemaProvider::default();
            // Add a schema with tags that could conflict with aliasing against an
            // existing call expression, in this case "last"
            namespace.add_schema(
                SchemaBuilder::new()
                    .measurement("conflicts")
                    .timestamp()
                    .tag("last")
                    .influx_field("field_f64", InfluxFieldType::Float)
                    .build()
                    .unwrap(),
            );

            let stmt = parse_select("SELECT COUNT(field_i64) FROM temp_01");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, count(field_i64::integer) AS count FROM temp_01"
            );

            // Duplicate aggregate columns
            let stmt = parse_select("SELECT COUNT(field_i64), COUNT(field_i64) FROM temp_01");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, count(field_i64::integer) AS count, count(field_i64::integer) AS count_1 FROM temp_01"
            );

            let stmt = parse_select("SELECT COUNT(field_f64) FROM temp_01");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, count(field_f64::float) AS count FROM temp_01"
            );

            // Expands all fields
            let stmt = parse_select("SELECT COUNT(*) FROM temp_01");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, count(field_f64::float) AS count_field_f64, count(field_i64::integer) AS count_field_i64, count(field_str::string) AS count_field_str, count(field_u64::unsigned) AS count_field_u64, count(shared_field0::float) AS count_shared_field0 FROM temp_01"
            );

            // Expands matching fields
            let stmt = parse_select("SELECT COUNT(/64$/) FROM temp_01");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, count(field_f64::float) AS count_field_f64, count(field_i64::integer) AS count_field_i64, count(field_u64::unsigned) AS count_field_u64 FROM temp_01"
            );

            // Expands only numeric fields
            let stmt = parse_select("SELECT SUM(*) FROM temp_01");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, sum(field_f64::float) AS sum_field_f64, sum(field_i64::integer) AS sum_field_i64, sum(field_u64::unsigned) AS sum_field_u64, sum(shared_field0::float) AS sum_shared_field0 FROM temp_01"
            );

            // Handles conflicts when call expression is renamed to match an existing tag
            let stmt = parse_select("SELECT LAST(field_f64), last FROM conflicts");
            let stmt = rewrite_select_statement(&namespace, &stmt).unwrap();
            assert_eq!(
                stmt.to_string(),
                "SELECT time::timestamp AS time, last(field_f64::float) AS last, last::tag AS last_1 FROM conflicts"
            );
        }
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
