//! Utility functions for working with arrow

use std::iter::FromIterator;
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use datafusion::logical_plan::{col, Expr};

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
