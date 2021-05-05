//! This module contains a unified Predicate structure for IOx qieries
//! that can select and filter Fields and Tags from the InfluxDB data
//! mode as well as for arbitrary other predicates that are expressed
//! by DataFusion's `Expr` type.

use std::collections::BTreeSet;

use data_types::timestamp::TimestampRange;
use datafusion::logical_plan::Expr;
use datafusion_util::{make_range_expr, AndExprBuilder};
use internal_types::schema::TIME_COLUMN_NAME;

/// This `Predicate` represents the empty predicate (aka that
/// evaluates to true for all rows).
pub const EMPTY_PREDICATE: Predicate = Predicate {
    table_names: None,
    field_columns: None,
    exprs: vec![],
    range: None,
    partition_key: None,
};

/// Represents a parsed predicate for evaluation by the
/// TSDatabase InfluxDB IOx query engine.
///
/// Note that the data model of TSDatabase (e.g. ParsedLine's)
/// distinguishes between some types of columns (tags and fields), and
/// likewise the semantics of this structure has some types of
/// restrictions that only apply to certain types of columns.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Predicate {
    /// Optional table restriction. If present, restricts the results
    /// to only tables whose names are in `table_names`
    pub table_names: Option<BTreeSet<String>>,

    // Optional field restriction. If present, restricts the results to only
    // tables which have *at least one* of the fields in field_columns.
    pub field_columns: Option<BTreeSet<String>>,

    /// Optional arbitrary predicates, represented as list of
    /// DataFusion expressions applied a logical conjuction (aka they
    /// are 'AND'ed together). Only rows that evaluate to TRUE for all
    /// these expressions should be returned. Other rows are excluded
    /// from the results.
    pub exprs: Vec<Expr>,

    /// Optional timestamp range: only rows within this range are included in
    /// results. Other rows are excluded
    pub range: Option<TimestampRange>,

    /// Optional partition key filter
    pub partition_key: Option<String>,
}

impl Predicate {
    /// Return true if this predicate has any general purpose predicates
    pub fn has_exprs(&self) -> bool {
        !self.exprs.is_empty()
    }

    /// Return a DataFusion `Expr` predicate representing the
    /// combination of all predicate (`exprs`) and timestamp
    /// restriction in this Predicate. Returns None if there are no
    /// `Expr`'s restricting the data
    pub fn filter_expr(&self) -> Option<Expr> {
        let mut builder =
            AndExprBuilder::default().append_opt(self.make_timestamp_predicate_expr());

        for expr in &self.exprs {
            builder = builder.append_expr(expr.clone());
        }

        builder.build()
    }

    /// Return true if results from this table should be included in
    /// results
    pub fn should_include_table(&self, table_name: &str) -> bool {
        match &self.table_names {
            None => true, // No table name restriction on predicate
            Some(table_names) => table_names.contains(table_name),
        }
    }

    /// Return true if the field should be included in results
    pub fn should_include_field(&self, field_name: &str) -> bool {
        match &self.field_columns {
            None => true, // No field restriction on predicate
            Some(field_names) => field_names.contains(field_name),
        }
    }

    /// Creates a DataFusion predicate for appliying a timestamp range:
    ///
    /// `range.start <= time and time < range.end`
    fn make_timestamp_predicate_expr(&self) -> Option<Expr> {
        self.range
            .map(|range| make_range_expr(range.start, range.end, TIME_COLUMN_NAME))
    }

    /// Returns true if ths predicate evaluates to true for all rows
    pub fn is_empty(&self) -> bool {
        self == &EMPTY_PREDICATE
    }
}

#[derive(Debug, Default)]
/// Structure for building `Predicate`s
pub struct PredicateBuilder {
    inner: Predicate,
}

impl From<Predicate> for PredicateBuilder {
    fn from(inner: Predicate) -> Self {
        Self { inner }
    }
}

impl PredicateBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the timestamp range
    pub fn timestamp_range(mut self, start: i64, end: i64) -> Self {
        // Without more thought, redefining the timestamp range would
        // lose the old range. Asser that that cannot happen.
        assert!(
            self.inner.range.is_none(),
            "Unexpected re-definition of timestamp range"
        );
        self.inner.range = Some(TimestampRange { start, end });
        self
    }

    /// sets the optional timestamp range, if any
    pub fn timestamp_range_option(mut self, range: Option<TimestampRange>) -> Self {
        // Without more thought, redefining the timestamp range would
        // lose the old range. Asser that that cannot happen.
        assert!(
            range.is_none() || self.inner.range.is_none(),
            "Unexpected re-definition of timestamp range"
        );
        self.inner.range = range;
        self
    }

    /// Adds an expression to the list of general purpose predicates
    pub fn add_expr(mut self, expr: Expr) -> Self {
        self.inner.exprs.push(expr);
        self
    }

    /// Adds an optional table name restriction to the existing list
    pub fn table_option(self, table: Option<String>) -> Self {
        if let Some(table) = table {
            self.tables(vec![table])
        } else {
            self
        }
    }

    /// Set the table restriction to `table`
    pub fn table(self, table: impl Into<String>) -> Self {
        self.tables(vec![table.into()])
    }

    /// Sets table name restrictions from something that can iterate
    /// over items that can be converted into `Strings`
    pub fn tables<I, S>(mut self, tables: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        // We need to distinguish predicates like `table_name In
        // (foo, bar)` and `table_name = foo and table_name = bar` in order to handle
        // this
        assert!(
            self.inner.table_names.is_none(),
            "Multiple table predicate specification not yet supported"
        );

        let table_names: BTreeSet<String> = tables.into_iter().map(|s| s.into()).collect();

        self.inner.table_names = Some(table_names);
        self
    }

    /// Sets field_column restriction
    pub fn field_columns(mut self, columns: Vec<impl Into<String>>) -> Self {
        // We need to distinguish predicates like `column_name In
        // (foo, bar)` and `column_name = foo and column_name = bar` in order to handle
        // this
        if self.inner.field_columns.is_some() {
            unimplemented!("Complex/Multi field predicates are not yet supported");
        }

        let column_names = columns
            .into_iter()
            .map(|s| s.into())
            .collect::<BTreeSet<_>>();

        self.inner.field_columns = Some(column_names);
        self
    }

    /// Set the partition key restriction
    pub fn partition_key(mut self, partition_key: impl Into<String>) -> Self {
        assert!(
            self.inner.partition_key.is_none(),
            "multiple partition key predicates not suported"
        );
        self.inner.partition_key = Some(partition_key.into());
        self
    }

    /// Create a predicate, consuming this builder
    pub fn build(self) -> Predicate {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_predicate_is_empty() {
        let p = Predicate::default();
        assert!(p.is_empty());
    }

    #[test]
    fn test_non_default_predicate_is_not_empty() {
        let p = PredicateBuilder::new().timestamp_range(1, 100).build();

        assert!(!p.is_empty());
    }
}
