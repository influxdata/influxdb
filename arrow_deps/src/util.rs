//! Utility functions for working with arrow

use std::iter::FromIterator;
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    error::ArrowError,
    record_batch::RecordBatch,
};
use datafusion::{
    logical_plan::{binary_expr, col, lit, Expr, Operator},
    scalar::ScalarValue,
};

/// Returns a single column record batch of type Utf8 from the
/// contents of something that can be turned into an iterator over
/// `Option<&str>`
pub fn str_iter_to_batch<Ptr, I>(field_name: &str, iter: I) -> Result<RecordBatch, ArrowError>
where
    I: IntoIterator<Item = Option<Ptr>>,
    Ptr: AsRef<str>,
{
    let array = StringArray::from_iter(iter);

    RecordBatch::try_from_iter(vec![(field_name, Arc::new(array) as ArrayRef)])
}

/// Traits to help creating DataFusion expressions from strings
pub trait AsExpr {
    /// Creates a DataFusion expr
    fn as_expr(&self) -> Expr;

    /// creates a DataFusion SortExpr
    fn as_sort_expr(&self) -> Expr {
        Expr::Sort {
            expr: Box::new(self.as_expr()),
            asc: true, // Sort ASCENDING
            nulls_first: true,
        }
    }
}

impl AsExpr for Arc<String> {
    fn as_expr(&self) -> Expr {
        col(self.as_ref())
    }
}

impl AsExpr for str {
    fn as_expr(&self) -> Expr {
        col(self)
    }
}

impl AsExpr for Expr {
    fn as_expr(&self) -> Expr {
        self.clone()
    }
}

/// Creates expression like:
/// start <= time && time < end
pub fn make_range_expr(start: i64, end: i64, time: impl AsRef<str>) -> Expr {
    // We need to cast the start and end values to timestamps
    // the equivalent of:
    let ts_start = ScalarValue::TimestampNanosecond(Some(start));
    let ts_end = ScalarValue::TimestampNanosecond(Some(end));

    let ts_low = lit(ts_start).lt_eq(col(time.as_ref()));
    let ts_high = col(time.as_ref()).lt(lit(ts_end));

    ts_low.and(ts_high)
}

/// Creates a single expression representing the conjunction (aka
/// AND'ing) together of a set of expressions
#[derive(Debug, Default)]
pub struct AndExprBuilder {
    cur_expr: Option<Expr>,
}

impl AndExprBuilder {
    /// append `new_expr` to the expression chain being built
    pub fn append_opt_ref(self, new_expr: Option<&Expr>) -> Self {
        match new_expr {
            None => self,
            Some(new_expr) => self.append_expr(new_expr.clone()),
        }
    }

    /// append `new_expr` to the expression chain being built
    pub fn append_opt(self, new_expr: Option<Expr>) -> Self {
        match new_expr {
            None => self,
            Some(new_expr) => self.append_expr(new_expr),
        }
    }

    /// Append `new_expr` to the expression chain being built
    pub fn append_expr(self, new_expr: Expr) -> Self {
        let Self { cur_expr } = self;

        let cur_expr = if let Some(cur_expr) = cur_expr {
            binary_expr(cur_expr, Operator::And, new_expr)
        } else {
            new_expr
        };

        let cur_expr = Some(cur_expr);

        Self { cur_expr }
    }

    /// Creates the new filter expression, consuming Self
    pub fn build(self) -> Option<Expr> {
        self.cur_expr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_range_expr() {
        // Test that the generated predicate is correct

        let ts_predicate_expr = make_range_expr(101, 202, "time");
        let expected_string =
            "TimestampNanosecond(101) LtEq #time And #time Lt TimestampNanosecond(202)";
        let actual_string = format!("{:?}", ts_predicate_expr);

        assert_eq!(actual_string, expected_string);
    }
}
