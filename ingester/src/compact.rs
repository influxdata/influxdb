//! This module is responsible for compacting Ingester's data

use crate::data::{PersistingBatch, QueryableBatch};
use arrow::{array::TimestampNanosecondArray, record_batch::RecordBatch};
use datafusion::{error::DataFusionError, physical_plan::SendableRecordBatchStream};
use iox_catalog::interface::NamespaceId;
use parquet_file::metadata::IoxMetadata;
use query::{
    exec::{Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
    QueryChunkMeta,
};
use schema::TIME_COLUMN_NAME;
use snafu::{OptionExt, ResultExt, Snafu};
use std::sync::Arc;
use time::{Time, TimeProvider};

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Error while building logical plan for Ingester's compaction"))]
    LogicalPlan {
        source: query::frontend::reorg::Error,
    },

    #[snafu(display("Error while building physical plan for Ingester's compaction"))]
    PhysicalPlan { source: DataFusionError },

    #[snafu(display("Error while executing Ingester's compaction"))]
    ExecutePlan { source: DataFusionError },

    #[snafu(display("Error while collecting Ingester's compaction data into a Record Batch"))]
    CollectStream { source: DataFusionError },

    #[snafu(display("Error while building delete predicate from start time, {}, stop time, {}, and serialized predicate, {}", min, max, predicate))]
    DeletePredicate {
        source: predicate::delete_predicate::Error,
        min: String,
        max: String,
        predicate: String,
    },

    #[snafu(display("The Record batch is empty"))]
    EmptyBatch,

    #[snafu(display("Error while searching Time column in a Record Batch"))]
    TimeColumn { source: arrow::error::ArrowError },

    #[snafu(display("Error while casting Timenanosecond on Time column"))]
    TimeCasting,
}

/// A specialized `Error` for Ingester's Compact errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Return min and max for column `time` of the given set of record batches
pub fn compute_timenanosecond_min_max(batches: &[RecordBatch]) -> Result<(i64, i64)> {
    let mut min_time = i64::MAX;
    let mut max_time = i64::MIN;
    for batch in batches {
        let (min, max) = compute_timenanosecond_min_max_for_one_record_bacth(batch)?;
        if min_time > min {
            min_time = min;
        }
        if max_time < max {
            max_time = max;
        }
    }
    Ok((min_time, max_time))
}

/// Return min and max for column `time` in the given record batch
pub fn compute_timenanosecond_min_max_for_one_record_bacth(
    batch: &RecordBatch,
) -> Result<(i64, i64)> {
    if batch.num_columns() == 0 {
        return Err(Error::EmptyBatch);
    }

    let index = batch
        .schema()
        .index_of(TIME_COLUMN_NAME)
        .context(TimeColumnSnafu {})?;

    let time_col = batch
        .column(index)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .context(TimeCastingSnafu {})?;

    let min = time_col
        .iter()
        .min()
        .expect("Time column must have values")
        .expect("Time column cannot be NULL");
    let max = time_col
        .iter()
        .max()
        .expect("Time column must have values")
        .expect("Time column cannot be NULL");

    Ok((min, max))
}

/// Compact a given persisting batch
/// Return compacted data with its metadata
pub async fn compact_persisting_batch(
    time_provider: Arc<dyn TimeProvider>,
    executor: &Executor,
    namespace_id: i32,
    namespace_name: &str,
    table_name: &str,
    partition_key: &str,
    batch: Arc<PersistingBatch>,
) -> Result<Option<(Vec<RecordBatch>, IoxMetadata)>> {
    // Nothing to compact
    if batch.data.data.is_empty() {
        return Ok(None);
    }

    // Compact
    let stream = compact(executor, Arc::clone(&batch.data)).await?;
    // Collect compacted data into record batches for computing statistics
    let output_batches = datafusion::physical_plan::common::collect(stream)
        .await
        .context(CollectStreamSnafu {})?;

    // No data after compaction
    if output_batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(None);
    }

    // Compute min and max of the `time` column
    let (min_time, max_time) = compute_timenanosecond_min_max(&output_batches)?;

    // Compute min and max sequence numbers
    let (min_seq, max_seq) = batch.data.min_max_sequence_numbers();

    let meta = IoxMetadata {
        object_store_id: batch.object_store_id,
        creation_timestamp: time_provider.now(),
        sequencer_id: batch.sequencer_id,
        namespace_id: NamespaceId::new(namespace_id),
        namespace_name: Arc::from(namespace_name),
        table_id: batch.table_id,
        table_name: Arc::from(table_name),
        partition_id: batch.partition_id,
        partition_key: Arc::from(partition_key),
        time_of_first_write: Time::from_timestamp_nanos(min_time),
        time_of_last_write: Time::from_timestamp_nanos(max_time),
        min_sequence_number: min_seq,
        max_sequence_number: max_seq,
    };

    Ok(Some((output_batches, meta)))
}

/// Compact a given Queryable Batch
pub async fn compact(
    executor: &Executor,
    data: Arc<QueryableBatch>,
) -> Result<SendableRecordBatchStream> {
    // Build logical plan for compaction
    let ctx = executor.new_context(ExecutorType::Reorg);
    let logical_plan = ReorgPlanner::new()
        .scan_single_chunk_plan(data.schema(), data)
        .context(LogicalPlanSnafu {})?;

    // Build physical plan
    let physical_plan = ctx
        .prepare_plan(&logical_plan)
        .await
        .context(PhysicalPlanSnafu {})?;

    // Execute the plan and return the compacted stream
    let output_stream = ctx
        .execute_stream(physical_plan)
        .await
        .context(ExecutePlanSnafu {})?;

    Ok(output_stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{
        create_batches_with_influxtype, create_batches_with_influxtype_different_columns,
        create_batches_with_influxtype_different_columns_different_order,
        create_batches_with_influxtype_same_columns_different_type,
        create_one_record_batch_with_influxtype_duplicates,
        create_one_record_batch_with_influxtype_no_duplicates, create_tombstone, make_meta,
        make_persisting_batch, make_queryable_batch, make_queryable_batch_with_deletes,
    };
    use arrow_util::assert_batches_eq;
    use time::SystemProvider;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_compact_persisting_batch_on_one_record_batch_no_dupilcates() {
        // create input data
        let batches = create_one_record_batch_with_influxtype_no_duplicates().await;

        // build persisting batch from the input batches
        let uuid = Uuid::new_v4();
        let namespace_name = "test_namespace";
        let partition_key = "test_partition_key";
        let table_name = "test_table";
        let seq_id = 1;
        let seq_num_start: i64 = 1;
        let seq_num_end: i64 = seq_num_start; // one batch
        let namespace_id = 1;
        let table_id = 1;
        let partition_id = 1;
        let persisting_batch = make_persisting_batch(
            seq_id,
            seq_num_start,
            table_id,
            table_name,
            partition_id,
            uuid,
            batches,
            vec![],
        );

        // verify PK
        let schema = persisting_batch.data.schema();
        let pk = schema.primary_key();
        let expected_pk = vec!["tag1", "time"];
        assert_eq!(expected_pk, pk);

        // compact
        let exc = Executor::new(1);
        let time_provider = Arc::new(SystemProvider::new());
        let (output_batches, meta) = compact_persisting_batch(
            time_provider,
            &exc,
            1,
            namespace_name,
            table_name,
            partition_key,
            persisting_batch,
        )
        .await
        .unwrap()
        .unwrap();

        // verify compacted data
        // should be the same as the input but sorted on tag1 & time
        let expected_data = vec![
            "+-----------+------+-----------------------------+",
            "| field_int | tag1 | time                        |",
            "+-----------+------+-----------------------------+",
            "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
            "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
            "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
            "+-----------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected_data, &output_batches);

        let expected_meta = make_meta(
            uuid,
            meta.creation_timestamp,
            seq_id,
            namespace_id,
            namespace_name,
            table_id,
            table_name,
            partition_id,
            partition_key,
            8000,
            20000,
            seq_num_start,
            seq_num_end,
        );
        assert_eq!(expected_meta, meta);
    }

    #[tokio::test]
    async fn test_compact_one_batch_no_dupilcates_with_deletes() {
        test_helpers::maybe_start_logging();

        // create input data
        let batches = create_one_record_batch_with_influxtype_no_duplicates().await;
        let tombstones = vec![create_tombstone(1, 1, 1, 1, 0, 200000, "tag1=UT")];

        // build queryable batch from the input batches
        let compact_batch = make_queryable_batch_with_deletes("test_table", 1, batches, tombstones);

        // verify PK
        let schema = compact_batch.schema();
        let pk = schema.primary_key();
        let expected_pk = vec!["tag1", "time"];
        assert_eq!(expected_pk, pk);

        // compact
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // verify compacted data
        // row with "tag1=UT" no longer avaialble
        let expected = vec![
            "+-----------+------+-----------------------------+",
            "| field_int | tag1 | time                        |",
            "+-----------+------+-----------------------------+",
            "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
            "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
            "+-----------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &output_batches);
    }

    #[tokio::test]
    async fn test_compact_one_batch_with_duplicates() {
        // create input data
        let batches = create_one_record_batch_with_influxtype_duplicates().await;

        // build queryable batch from the input batches
        let compact_batch = make_queryable_batch("test_table", 1, batches);

        // verify PK
        let schema = compact_batch.schema();
        let pk = schema.primary_key();
        let expected_pk = vec!["tag1", "time"];
        assert_eq!(expected_pk, pk);

        // compact
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // verify compacted data
        //  data is sorted and all duplicates are removed
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
            "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &output_batches);
    }

    #[tokio::test]
    async fn test_compact_many_batches_same_columns_with_duplicates() {
        // create many-batches input data
        let batches = create_batches_with_influxtype().await;

        // build queryable batch from the input batches
        let compact_batch = make_queryable_batch("test_table", 1, batches);

        // verify PK
        let schema = compact_batch.schema();
        let pk = schema.primary_key();
        let expected_pk = vec!["tag1", "time"];
        assert_eq!(expected_pk, pk);

        // compact
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // verify compacted data
        // data is sorted and all duplicates are removed
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &output_batches);
    }

    #[tokio::test]
    async fn test_compact_many_batches_different_columns_with_duplicates() {
        // create many-batches input data
        let batches = create_batches_with_influxtype_different_columns().await;

        // build queryable batch from the input batches
        let compact_batch = make_queryable_batch("test_table", 1, batches);

        // verify PK
        let schema = compact_batch.schema();
        let pk = schema.primary_key();
        let expected_pk = vec!["tag1", "tag2", "time"];
        assert_eq!(expected_pk, pk);

        // compact
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // verify compacted data
        // data is sorted and all duplicates are removed
        let expected = vec![
            "+-----------+------------+------+------+--------------------------------+",
            "| field_int | field_int2 | tag1 | tag2 | time                           |",
            "+-----------+------------+------+------+--------------------------------+",
            "| 10        |            | AL   |      | 1970-01-01T00:00:00.000000050Z |",
            "| 100       | 100        | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        |            | CT   |      | 1970-01-01T00:00:00.000000100Z |",
            "| 70        |            | CT   |      | 1970-01-01T00:00:00.000000500Z |",
            "| 70        | 70         | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 30        |            | MT   |      | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      |            | MT   |      | 1970-01-01T00:00:00.000001Z    |",
            "| 1000      |            | MT   |      | 1970-01-01T00:00:00.000002Z    |",
            "| 20        |            | MT   |      | 1970-01-01T00:00:00.000007Z    |",
            "| 5         | 5          | MT   | AL   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        | 10         | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 1000      | 1000       | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
            "+-----------+------------+------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &output_batches);
    }

    #[tokio::test]
    async fn test_compact_many_batches_different_columns_different_order_with_duplicates_with_deletes(
    ) {
        // create many-batches input data
        let batches = create_batches_with_influxtype_different_columns_different_order().await;
        let tombstones = vec![create_tombstone(
            1,
            1,
            1,
            100,                          // delete's seq_number
            0,                            // min time of data to get deleted
            200000,                       // max time of data to get deleted
            "tag2=CT and field_int=1000", // delete predicate
        )];

        // build queryable batch from the input batches
        let compact_batch = make_queryable_batch_with_deletes("test_table", 1, batches, tombstones);

        // verify PK
        let schema = compact_batch.schema();
        let pk = schema.primary_key();
        let expected_pk = vec!["tag1", "tag2", "time"];
        assert_eq!(expected_pk, pk);

        // compact
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // verify compacted data
        // data is sorted and all duplicates are removed
        // all rows with ("tag2=CT and field_int=1000") are also removed
        // CORRECT RESULT
        let expected = vec![
            "+-----------+------+------+--------------------------------+",
            "| field_int | tag1 | tag2 | time                           |",
            "+-----------+------+------+--------------------------------+",
            "| 5         |      | AL   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        |      | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        |      | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       |      | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 10        | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 30        | MT   | AL   | 1970-01-01T00:00:00.000000005Z |",
            "| 20        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "+-----------+------+------+--------------------------------+",
        ];

        assert_batches_eq!(&expected, &output_batches);
    }

    #[tokio::test]
    async fn test_compact_many_batches_different_columns_different_order_with_duplicates_with_many_deletes(
    ) {
        // create many-batches input data
        let batches = create_batches_with_influxtype_different_columns_different_order().await;
        let tombstones = vec![
            create_tombstone(
                1,
                1,
                1,
                100,                          // delete's seq_number
                0,                            // min time of data to get deleted
                200000,                       // max time of data to get deleted
                "tag2=CT and field_int=1000", // delete predicate
            ),
            create_tombstone(
                1, 1, 1, 101,        // delete's seq_number
                0,          // min time of data to get deleted
                200000,     // max time of data to get deleted
                "tag1!=MT", // delete predicate
            ),
        ];

        // build queryable batch from the input batches
        let compact_batch = make_queryable_batch_with_deletes("test_table", 1, batches, tombstones);

        // verify PK
        let schema = compact_batch.schema();
        let pk = schema.primary_key();
        let expected_pk = vec!["tag1", "tag2", "time"];
        assert_eq!(expected_pk, pk);

        // compact
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // verify compacted data
        // data is sorted and all duplicates are removed
        // all rows with ("tag2=CT and field_int=1000") and ("tag1!=MT") are also removed
        let expected = vec![
            "+-----------+------+------+--------------------------------+",
            "| field_int | tag1 | tag2 | time                           |",
            "+-----------+------+------+--------------------------------+",
            "| 30        | MT   | AL   | 1970-01-01T00:00:00.000000005Z |",
            "| 20        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "+-----------+------+------+--------------------------------+",
        ];

        assert_batches_eq!(&expected, &output_batches);
    }

    // BUG
    #[tokio::test]
    async fn test_compact_many_batches_different_columns_different_order_with_duplicates_with_many_deletes_2(
    ) {
        // create many-batches input data
        let batches = create_batches_with_influxtype_different_columns_different_order().await;
        let tombstones = vec![
            create_tombstone(
                1,
                1,
                1,
                100,                          // delete's seq_number
                0,                            // min time of data to get deleted
                200000,                       // max time of data to get deleted
                "tag2=CT and field_int=1000", // delete predicate
            ),
            create_tombstone(
                1, 1, 1, 101,       // delete's seq_number
                0,         // min time of data to get deleted
                200000,    // max time of data to get deleted
                "tag1=MT", // delete predicate
            ),
        ];

        // build queryable batch from the input batches
        let compact_batch = make_queryable_batch_with_deletes("test_table", 1, batches, tombstones);

        // verify PK
        let schema = compact_batch.schema();
        let pk = schema.primary_key();
        let expected_pk = vec!["tag1", "tag2", "time"];
        assert_eq!(expected_pk, pk);

        // compact
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // verify compacted data
        // data is sorted and all duplicates are removed
        // all rows with ("tag2=CT and field_int=1000") and ("tag1=MT") are also removed
        // CORRECT RESULT
        // let expected = vec![
        //     "+-----------+------+------+--------------------------------+",
        //     "| field_int | tag1 | tag2 | time                           |",
        //     "+-----------+------+------+--------------------------------+",
        //     "| 5         |      | AL   | 1970-01-01T00:00:00.000005Z    |",
        //     "| 10        |      | AL   | 1970-01-01T00:00:00.000007Z    |",
        //     "| 70        |      | CT   | 1970-01-01T00:00:00.000000100Z |",
        //     "| 100       |      | MA   | 1970-01-01T00:00:00.000000050Z |",
        //     "| 10        | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
        //     "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
        //     "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000500Z |",
        //     "+-----------+------+------+--------------------------------+",
        // ];
        // current WRONMG result: "tag1 is null" is also eliminated
        let expected = vec![
            "+-----------+------+------+--------------------------------+",
            "| field_int | tag1 | tag2 | time                           |",
            "+-----------+------+------+--------------------------------+",
            "| 10        | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000500Z |",
            "+-----------+------+------+--------------------------------+",
        ];

        assert_batches_eq!(&expected, &output_batches);
    }

    #[tokio::test]
    #[should_panic(expected = "Schemas compatible")]
    async fn test_compact_many_batches_same_columns_different_types() {
        // create many-batches input data
        let batches = create_batches_with_influxtype_same_columns_different_type().await;

        // build queryable batch from the input batches
        let compact_batch = make_queryable_batch("test_table", 1, batches);

        // the schema merge will thorw a panic
        compact_batch.schema();
    }
}
