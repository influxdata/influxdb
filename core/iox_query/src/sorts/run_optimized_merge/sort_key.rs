//! Evaluation of sort keys against input batches.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, RecordBatch},
    compute::SortOptions,
};
use datafusion::{error::Result, physical_expr::LexOrdering};

/// The sort direction and null placement of each expression in `expr`.
pub(super) fn sort_options(expr: &LexOrdering) -> Vec<SortOptions> {
    expr.iter().map(|e| e.options).collect()
}

/// The result of evaluating every expression of a [`LexOrdering`] against one
/// record batch.
pub(super) struct SortKeys {
    /// One array per sort expression, each with the same length as the batch.
    pub arrays: Vec<ArrayRef>,

    /// The number of bytes held by arrays that are not themselves columns of
    /// the source batch.
    pub extra_bytes: usize,
}

/// Evaluate every expression of `expr` against `batch`.
///
/// An expression that evaluates to one of the batch's own columns yields an
/// alias of that column, which does not contribute to
/// [`extra_bytes`](SortKeys::extra_bytes).
pub(super) fn evaluate_sort_keys(expr: &LexOrdering, batch: &RecordBatch) -> Result<SortKeys> {
    let num_rows = batch.num_rows();
    let mut arrays = Vec::with_capacity(expr.len());
    let mut extra_bytes = 0;
    for sort_expr in expr.iter() {
        let array = sort_expr.expr.evaluate(batch)?.into_array(num_rows)?;
        if !batch.columns().iter().any(|c| Arc::ptr_eq(c, &array)) {
            extra_bytes += array.get_array_memory_size();
        }
        arrays.push(array);
    }
    Ok(SortKeys {
        arrays,
        extra_bytes,
    })
}
