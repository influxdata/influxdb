//! Functions for computing a sort key based on cardinality of primary key columns.

use crate::data::QueryableBatch;
use schema::sort::SortKey;

/// Given a `QueryableBatch`, compute a sort key based on:
///
/// - The columns that make up the primary key of the schema of this batch
/// - Order those columns from low cardinality to high cardinality based on the data
/// - Always have the time column last
pub fn compute_sort_key(_queryable_batch: &QueryableBatch) -> SortKey {
    unimplemented!()
}
