//! Utility functions for working with arrow

use std::iter::FromIterator;
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use datafusion::logical_plan::{binary_expr, col, lit, Expr, Operator};

/// Returns a single column record batch of type Utf8 from the
/// contents of something that can be turned into an iterator over
/// `Option<&str>`
pub fn str_iter_to_batch<Ptr, I>(field_name: &str, iter: I) -> Result<RecordBatch, ArrowError>
where
    I: IntoIterator<Item = Option<Ptr>>,
    Ptr: AsRef<str>,
{
    let schema = Arc::new(Schema::new(vec![Field::new(
        field_name,
        DataType::Utf8,
        false,
    )]));
    let array = StringArray::from_iter(iter);
    let columns: Vec<ArrayRef> = vec![Arc::new(array)];

    RecordBatch::try_new(schema, columns)
}

/// Traits to help creating DataFusion expressions from strings
pub trait IntoExpr {
    /// Creates a DataFusion expr
    fn into_expr(&self) -> Expr;

    /// creates a DataFusion SortExpr
    fn into_sort_expr(&self) -> Expr {
        Expr::Sort {
            expr: Box::new(self.into_expr()),
            asc: true, // Sort ASCENDING
            nulls_first: true,
        }
    }
}

impl IntoExpr for Arc<String> {
    fn into_expr(&self) -> Expr {
        col(self.as_ref())
    }
}

impl IntoExpr for str {
    fn into_expr(&self) -> Expr {
        col(self)
    }
}

impl IntoExpr for Expr {
    fn into_expr(&self) -> Expr {
        self.clone()
    }
}

/// Creates expression like:
/// start <= time && time < end
pub fn make_range_expr(start: i64, end: i64, time: impl AsRef<str>) -> Expr {
    let ts_low = lit(start).lt_eq(col(time.as_ref()));
    let ts_high = col(time.as_ref()).lt(lit(end));

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
        let expected_string = "Int64(101) LtEq #time And #time Lt Int64(202)";
        let actual_string = format!("{:?}", ts_predicate_expr);

        assert_eq!(actual_string, expected_string);
    }
}
