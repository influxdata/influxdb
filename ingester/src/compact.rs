//! This module is responsible for compacting Ingester's data

use datafusion::{error::DataFusionError, physical_plan::SendableRecordBatchStream};
use query::{
    exec::{Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
    QueryChunkMeta,
};
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

use crate::data::QueryableBatch;

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

    #[snafu(display("Error while building delete predicate from start time, {}, stop time, {}, and serialized predicate, {}", min, max, predicate))]
    DeletePredicate {
        source: predicate::delete_predicate::Error,
        min: String,
        max: String,
        predicate: String,
    },
}

/// A specialized `Error` for Ingester's Compact errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Compact the given Ingester's data
/// Note: the given `executor` should be created  with the IngesterServer
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
    use std::num::NonZeroU64;

    use crate::data::SnapshotBatch;

    use super::*;

    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_eq;
    use iox_catalog::interface::SequenceNumber;
    use query::test::{raw_data, TestChunk};

    #[ignore]
    #[tokio::test]
    async fn test_compact_no_dedup_no_delete() {
        let batches = create_record_batches_with_influxtype_no_duplicates().await;
        let compact_batch = make_queryable_batch(batches);
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        println!("output_batches: {:#?}", output_batches);

        let expected = vec![
            "+-----------+------+-------------------------------+",
            "| field_int | tag1 | time                          |",
            "+-----------+------+-------------------------------+",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | 1970-01-01 00:00:00.000000005 |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000002    |",
            "| 20        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000500 |",
            "| 10        | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 30        | MT   | 1970-01-01 00:00:00.000000005 |",
            "+-----------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &output_batches);
    }

    #[ignore]
    #[tokio::test]
    async fn test_compact_with_dedup_no_delete() {
        let batches = create_batches_with_influxtype().await;
        let compact_batch = make_queryable_batch(batches);
        let exc = Executor::new(1);

        let stream = compact(&exc, compact_batch).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // println!("output_batches: {:#?}", output_batches);
        // let table = pretty_format_batches(&[batch]).unwrap();
        let expected = vec![
            "+-----------+------+-------------------------------+",
            "| field_int | tag1 | time                          |",
            "+-----------+------+-------------------------------+",
            "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
            "| 10        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
            "| 100       | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 5         | MT   | 1970-01-01 00:00:00.000000005 |",
            "| 1000      | MT   | 1970-01-01 00:00:00.000002    |",
            "| 20        | MT   | 1970-01-01 00:00:00.000007    |",
            "| 70        | CT   | 1970-01-01 00:00:00.000000500 |",
            "| 10        | AL   | 1970-01-01 00:00:00.000000050 |",
            "| 30        | MT   | 1970-01-01 00:00:00.000000005 |",
            "+-----------+------+-------------------------------+",
        ];
        assert_batches_eq!(&expected, &output_batches);
    }

    // ----------------------------------------------------------------------------------------------
    // Data for testing
    pub fn make_queryable_batch(batches: Vec<Arc<RecordBatch>>) -> Arc<QueryableBatch> {
        // make snapshots for the bacthes
        let mut snapshots = vec![];
        let mut seq_num = 1;
        for batch in batches {
            let seq = SequenceNumber::new(seq_num);
            snapshots.push(make_snapshot_batch(batch, seq, seq));
            seq_num += 1;
        }

        Arc::new(QueryableBatch::new("test_table", snapshots, vec![]))
    }

    pub fn make_snapshot_batch(
        batch: Arc<RecordBatch>,
        min: SequenceNumber,
        max: SequenceNumber,
    ) -> SnapshotBatch {
        SnapshotBatch {
            min_sequencer_number: min,
            max_sequencer_number: max,
            data: batch,
        }
    }

    pub async fn create_record_batches_with_influxtype_no_duplicates() -> Vec<Arc<RecordBatch>> {
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    10,
                    Some(NonZeroU64::new(7).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    10,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_ten_rows_of_data_some_duplicates(),
        );
        let batches = raw_data(&[chunk1]).await;
        let batches: Vec<_> = batches.iter().map(|r| Arc::new(r.clone())).collect();

        // Output data look like this:
        // let expected = vec![
        //     "+-----------+------+-------------------------------+",
        //     "| field_int | tag1 | time                          |",
        //     "+-----------+------+-------------------------------+",
        //     "| 1000      | MT   | 1970-01-01 00:00:00.000001    |",
        //     "| 10        | MT   | 1970-01-01 00:00:00.000007    |",
        //     "| 70        | CT   | 1970-01-01 00:00:00.000000100 |",
        //     "| 100       | AL   | 1970-01-01 00:00:00.000000050 |",
        //     "| 5         | MT   | 1970-01-01 00:00:00.000000005 |",
        //     "| 1000      | MT   | 1970-01-01 00:00:00.000002    |",
        //     "| 20        | MT   | 1970-01-01 00:00:00.000007    |",
        //     "| 70        | CT   | 1970-01-01 00:00:00.000000500 |",
        //     "| 10        | AL   | 1970-01-01 00:00:00.000000050 |",
        //     "| 30        | MT   | 1970-01-01 00:00:00.000000005 |",
        //     "+-----------+------+-------------------------------+",
        // ];

        batches
    }

    // RecordBatches with knowledge of influx metadata
    pub async fn create_batches_with_influxtype() -> Vec<Arc<RecordBatch>> {
        // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
        let mut batches = vec![];

        // This test covers all kind of chunks: overlap, non-overlap without duplicates within, non-overlap with duplicates within
        // todo: may want to simplify the below data of test chunks. these are reuse the current code that cover many commpaction cases
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    10,
                    Some(NonZeroU64::new(7).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    10,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_ten_rows_of_data_some_duplicates(),
        );
        let batch1 = raw_data(&[chunk1]).await[0].clone();
        //println!("BATCH1: {:#?}", batch1);
        batches.push(Arc::new(batch1));

        // chunk2 overlaps with chunk 1
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column_with_full_stats(
                    Some(5),
                    Some(7000),
                    5,
                    Some(NonZeroU64::new(5).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("AL"),
                    Some("MT"),
                    5,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        );
        let batch2 = raw_data(&[chunk2]).await[0].clone();
        //println!("BATCH2: {:#?}", batch2);
        batches.push(Arc::new(batch2));

        // chunk3 no overlap, no duplicates within
        let chunk3 = Arc::new(
            TestChunk::new("t")
                .with_id(3)
                .with_time_column_with_full_stats(
                    Some(8000),
                    Some(20000),
                    3,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("UT"),
                    Some("WA"),
                    3,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_three_rows_of_data(),
        );
        let batch3 = raw_data(&[chunk3]).await[0].clone();
        //println!("BATCH3: {:#?}", batch3);
        batches.push(Arc::new(batch3));

        // chunk3 no overlap, duplicates within
        let chunk4 = Arc::new(
            TestChunk::new("t")
                .with_id(4)
                .with_time_column_with_full_stats(
                    Some(28000),
                    Some(220000),
                    4,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_tag_column_with_full_stats(
                    "tag1",
                    Some("UT"),
                    Some("WA"),
                    4,
                    Some(NonZeroU64::new(3).unwrap()),
                )
                .with_i64_field_column("field_int")
                .with_may_contain_pk_duplicates(true)
                .with_four_rows_of_data(),
        );
        let batch4 = raw_data(&[chunk4]).await[0].clone();
        //println!("BATCH4: {:#?}", batch4);
        batches.push(Arc::new(batch4));

        // todo: show what output data look like in comments for easier debug

        batches
    }
}
