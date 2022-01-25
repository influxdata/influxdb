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
    use crate::data::SnapshotBatch;

    use super::*;

    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_eq;
    use iox_catalog::interface::SequenceNumber;
    use query::test::{raw_data, TestChunk};

    #[tokio::test]
    async fn test_compact_one_batch_no_dupilcates() {
        // create input data
        let batches = create_one_record_batch_with_influxtype_no_duplicates().await;

        // build queryable batch from the inout batches
        let compact_batch = make_queryable_batch(batches);

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
        // should be the same as the input but sorted on tag1 & time
        let expected = vec![
            "+-----------+------+-----------------------------+",
            "| field_int | tag1 | time                        |",
            "+-----------+------+-----------------------------+",
            "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
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

        // build queryable batch from the inout batches
        let compact_batch = make_queryable_batch(batches);

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

        // build queryable batch from the inout batches
        let compact_batch = make_queryable_batch(batches);

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

    // TODO: FIX BUG
    #[ignore]
    #[tokio::test]
    async fn test_compact_many_batches_different_columns_with_duplicates() {
        // create many-batches input data
        let batches = create_batches_with_influxtype_different_columns().await;

        // build queryable batch from the inout batches
        let compact_batch = make_queryable_batch(batches);

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

    pub async fn create_one_record_batch_with_influxtype_no_duplicates() -> Vec<Arc<RecordBatch>> {
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column() //_with_full_stats(
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_three_rows_of_data(),
        );
        let batches = raw_data(&[chunk1]).await;

        // Make sure all dat ain one record batch
        assert_eq!(batches.len(), 1);

        // verify data
        let expected = vec![
            "+-----------+------+-----------------------------+",
            "| field_int | tag1 | time                        |",
            "+-----------+------+-----------------------------+",
            "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
            "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
            "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
            "+-----------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &batches);

        let batches: Vec<_> = batches.iter().map(|r| Arc::new(r.clone())).collect();
        batches
    }

    pub async fn create_one_record_batch_with_influxtype_duplicates() -> Vec<Arc<RecordBatch>> {
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column() //_with_full_stats(
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_ten_rows_of_data_some_duplicates(),
        );
        let batches = raw_data(&[chunk1]).await;

        // Make sure all dat ain one record batch
        assert_eq!(batches.len(), 1);

        // verify data
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
            "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batches);

        let batches: Vec<_> = batches.iter().map(|r| Arc::new(r.clone())).collect();
        batches
    }

    // RecordBatches with knowledge of influx metadata
    pub async fn create_batches_with_influxtype() -> Vec<Arc<RecordBatch>> {
        // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
        let mut batches = vec![];

        // chunk1 with 10 rows and 3 columns
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_ten_rows_of_data_some_duplicates(),
        );
        let batch1 = raw_data(&[chunk1]).await[0].clone();
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
            "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &[batch1.clone()]);
        batches.push(Arc::new(batch1));

        // chunk2 having duplicate data with chunk 1
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column()
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        );
        let batch2 = raw_data(&[chunk2]).await[0].clone();
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &[batch2.clone()]);
        batches.push(Arc::new(batch2));

        // verify data from both batches
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
            "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
            "+-----------+------+--------------------------------+",
        ];
        let b: Vec<_> = batches.iter().map(|b| (**b).clone()).collect();
        assert_batches_eq!(&expected, &b);

        batches
    }

    // RecordBatches with knowledge of influx metadata
    pub async fn create_batches_with_influxtype_different_columns() -> Vec<Arc<RecordBatch>> {
        // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
        let mut batches = vec![];

        // chunk1 with 10 rows and 3 columns
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_ten_rows_of_data_some_duplicates(),
        );
        let batch1 = raw_data(&[chunk1]).await[0].clone();
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |",
            "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &[batch1.clone()]);
        batches.push(Arc::new(batch1));

        // chunk2 having duplicate data with chunk 1
        // mmore columns
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column()
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_tag_column("tag2")
                .with_i64_field_column("field_int2")
                .with_five_rows_of_data(),
        );
        let batch2 = raw_data(&[chunk2]).await[0].clone();
        let expected = vec![
            "+-----------+------------+------+------+--------------------------------+",
            "| field_int | field_int2 | tag1 | tag2 | time                           |",
            "+-----------+------------+------+------+--------------------------------+",
            "| 1000      | 1000       | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | 10         | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | 70         | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | 100        | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | 5          | MT   | AL   | 1970-01-01T00:00:00.000005Z    |",
            "+-----------+------------+------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &[batch2.clone()]);
        batches.push(Arc::new(batch2));

        // verify data from both batches
        let expected = vec![
            "+-----------+------+--------------------------------+----+--------------------------------+",
            "| field_int | tag1 | time                           |    |                                |",
            "+-----------+------+--------------------------------+----+--------------------------------+",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |    |                                |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |    |                                |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |    |                                |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |    |                                |",
            "| 5         | MT   | 1970-01-01T00:00:00.000000005Z |    |                                |",
            "| 1000      | MT   | 1970-01-01T00:00:00.000002Z    |    |                                |",
            "| 20        | MT   | 1970-01-01T00:00:00.000007Z    |    |                                |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000500Z |    |                                |",
            "| 10        | AL   | 1970-01-01T00:00:00.000000050Z |    |                                |",
            "| 30        | MT   | 1970-01-01T00:00:00.000000005Z |    |                                |",
            "| 1000      | 1000 | MT                             | CT | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | 10   | MT                             | AL | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | 70   | CT                             | CT | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | 100  | AL                             | MA | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | 5    | MT                             | AL | 1970-01-01T00:00:00.000005Z    |",
            "+-----------+------+--------------------------------+----+--------------------------------+",
        ];
        let b: Vec<_> = batches.iter().map(|b| (**b).clone()).collect();
        assert_batches_eq!(&expected, &b);

        batches
    }
}
