//! Functions for computing a sort key based on cardinality of primary key columns.

use crate::data::{QueryableBatch, SnapshotBatch};
use observability_deps::tracing::trace;
use query::QueryChunkMeta;
use schema::{
    sort::{SortKey, SortKeyBuilder},
    TIME_COLUMN_NAME,
};
use std::{collections::HashMap, num::NonZeroU64, sync::Arc};

/// Given a `QueryableBatch`, compute a sort key based on:
///
/// - The columns that make up the primary key of the schema of this batch
/// - Order those columns from low cardinality to high cardinality based on the data
/// - Always have the time column last
pub fn compute_sort_key(queryable_batch: &QueryableBatch) -> SortKey {
    let schema = queryable_batch.schema();
    let primary_key = schema.primary_key();

    let cardinalities = distinct_counts(&queryable_batch.data, &primary_key);

    trace!(cardinalities=?cardinalities, "cardinalities of of columns to compute sort key");

    let mut cardinalities: Vec<_> = cardinalities.into_iter().collect();
    // Sort by (cardinality, column_name) to have deterministic order if same cardinality
    cardinalities.sort_by_cached_key(|x| (x.1, x.0.clone()));

    let mut builder = SortKeyBuilder::with_capacity(cardinalities.len() + 1);
    for (col, _) in cardinalities {
        builder = builder.with_col(col)
    }
    builder = builder.with_col(TIME_COLUMN_NAME);

    let key = builder.build();

    trace!(computed_sort_key=?key, "Value of sort key from compute_sort_key");

    key
}

/// Takes batches of data and the columns that make up the primary key. Computes the number of
/// distinct values for each primary key column across all batches, also known as "cardinality".
/// Used to determine sort order.
fn distinct_counts(
    _batches: &[Arc<SnapshotBatch>],
    _primary_key: &[&str],
) -> HashMap<String, NonZeroU64> {
    HashMap::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types2::SequenceNumber;
    use schema::selection::Selection;

    fn lp_to_queryable_batch(line_protocol_batches: &[&str]) -> QueryableBatch {
        let data = line_protocol_batches
            .iter()
            .map(|line_protocol| {
                let (_, mb) = mutable_batch_lp::test_helpers::lp_to_mutable_batch(line_protocol);
                let rb = mb.to_arrow(Selection::All).unwrap();

                Arc::new(SnapshotBatch {
                    min_sequencer_number: SequenceNumber::new(0),
                    max_sequencer_number: SequenceNumber::new(1),
                    data: Arc::new(rb),
                })
            })
            .collect();

        QueryableBatch {
            data,
            delete_predicates: Default::default(),
            table_name: Default::default(),
        }
    }

    #[test]
    fn test_sort_key() {
        // Across these three record batches:
        // - `host` has 2 distinct values: "a", "b"
        // - 'env' has 3 distinct values: "prod", "stage", "dev"
        // host's 2 values appear in each record batch, so the distinct counts could be incorrectly
        // aggregated together as 2 + 2 + 2 = 6. env's 3 values each occur in their own record
        // batch, so they should always be aggregated as 3.
        // host has the lower cardinality, so it should appear first in the sort key.
        let lp1 = r#"
            cpu,host=a,env=prod val=23 1
            cpu,host=b,env=prod val=2 2
        "#;
        let lp2 = r#"
            cpu,host=a,env=stage val=23 3
            cpu,host=b,env=stage val=2 4
        "#;
        let lp3 = r#"
            cpu,host=a,env=dev val=23 5
            cpu,host=b,env=dev val=2 6
        "#;
        let qb = lp_to_queryable_batch(&[lp1, lp2, lp3]);

        let sort_key = compute_sort_key(&qb);

        assert_eq!(sort_key, SortKey::from_columns(["host", "env", "time"]));
    }
}
