//! This module is responsible for compacting Ingester's data

use arrow::{array::TimestampNanosecondArray, record_batch::RecordBatch};
use datafusion::{error::DataFusionError, physical_plan::SendableRecordBatchStream};
use iox_catalog::interface::{NamespaceId, PartitionId, SequenceNumber, SequencerId, TableId};
use query::{
    exec::{Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
    QueryChunkMeta,
};
use schema::TIME_COLUMN_NAME;
use snafu::{OptionExt, ResultExt, Snafu};
use std::sync::Arc;
use time::{Time, TimeProvider};
use uuid::Uuid;

use crate::data::{PersistingBatch, QueryableBatch};

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

// todo: this struct will be moved to persist.rs
/// Metadata stored with each parquet file
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IoxMetadata {
    /// The uuid used as the location of the parquet file in the OS.
    /// This uuid will later be used as the catalog's ParquetFileId
    pub object_store_id: Uuid,

    /// Timestamp when this file was created.
    pub creation_timestamp: Time,

    /// sequencer id of the data
    pub sequencer_id: SequencerId,

    /// namesapce id of the data
    pub namespace_id: NamespaceId,

    /// namespace name
    pub namespace_name: Arc<str>,

    /// table id of the data
    pub table_id: TableId,

    /// table name of the data
    pub table_name: Arc<str>,

    /// partition id of the data
    pub partition_id: PartitionId,

    /// parittion key of the data
    pub partition_key: Arc<str>,

    /// Time of the first write of the data
    /// This is also the min value of the column `time`
    pub time_of_first_write: Time,

    /// Time of the last write of the data
    /// This is also the max value of the column `time`
    pub time_of_last_write: Time,

    /// sequence number of the first write
    pub min_sequence_number: SequenceNumber,

    /// sequence number of the last write
    pub max_sequence_number: SequenceNumber,
}

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
pub async fn compact_persting_batch(
    time_provider: Arc<dyn TimeProvider>,
    executor: &Executor,
    namespace_id: i32,
    namespace_name: &str,
    table_name: &str,
    partition_key: &str,
    batch: Arc<PersistingBatch>,
) -> Result<(Vec<RecordBatch>, Option<IoxMetadata>)> {
    // Nothing to compact
    if batch.data.data.is_empty() {
        return Ok((vec![], None));
    }

    // Compact
    let stream = compact(executor, Arc::clone(&batch.data)).await?;
    // Collect compacted data into record batches for computing statistics
    let output_batches = datafusion::physical_plan::common::collect(stream)
        .await
        .context(CollectStreamSnafu {})?;

    // No data after compaction
    if output_batches.iter().all(|b| b.num_rows() == 0) {
        return Ok((vec![], None));
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

    Ok((output_batches, Some(meta)))
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
    use crate::{data::SnapshotBatch, query::create_tombstone};

    use super::*;

    use arrow::{array::TimestampNanosecondArray, record_batch::RecordBatch};
    use arrow_util::assert_batches_eq;
    use iox_catalog::interface::{SequenceNumber, Tombstone};
    use query::test::{raw_data, TestChunk};
    use time::SystemProvider;

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
        );

        // verify PK
        let schema = persisting_batch.data.schema();
        let pk = schema.primary_key();
        let expected_pk = vec!["tag1", "time"];
        assert_eq!(expected_pk, pk);

        // compact
        let exc = Executor::new(1);
        let time_provider = Arc::new(SystemProvider::new());
        let (output_batches, meta) = compact_persting_batch(
            time_provider,
            &exc,
            1,
            namespace_name,
            table_name,
            partition_key,
            persisting_batch,
        )
        .await
        .unwrap();
        let meta = meta.unwrap();

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
            1,
            0,
            200000,
            "tag2=CT and field_int=1000",
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
        // all rows with "tag2=CT and field_int=1000" are also removed
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
    #[should_panic(expected = "Schemas compatible")]
    async fn test_compact_many_batches_same_columns_different_types() {
        // create many-batches input data
        let batches = create_batches_with_influxtype_same_columns_different_type().await;

        // build queryable batch from the input batches
        let compact_batch = make_queryable_batch("test_table", 1, batches);

        // the schema merge will thorw a panic
        compact_batch.schema();
    }

    // ----------------------------------------------------------------------------------------------
    // Data for testing
    #[allow(clippy::too_many_arguments)]
    pub fn make_meta(
        object_store_id: Uuid,
        creation_timestamp: Time,
        sequencer_id: i16,
        namespace_id: i32,
        namespace_name: &str,
        table_id: i32,
        table_name: &str,
        partition_id: i64,
        partition_key: &str,
        min_time: i64,
        max_time: i64,
        min_sequence_number: i64,
        max_sequence_number: i64,
    ) -> IoxMetadata {
        IoxMetadata {
            object_store_id,
            creation_timestamp,
            sequencer_id: SequencerId::new(sequencer_id),
            namespace_id: NamespaceId::new(namespace_id),
            namespace_name: Arc::from(namespace_name),
            table_id: TableId::new(table_id),
            table_name: Arc::from(table_name),
            partition_id: PartitionId::new(partition_id),
            partition_key: Arc::from(partition_key),
            time_of_first_write: Time::from_timestamp_nanos(min_time),
            time_of_last_write: Time::from_timestamp_nanos(max_time),
            min_sequence_number: SequenceNumber::new(min_sequence_number),
            max_sequence_number: SequenceNumber::new(max_sequence_number),
        }
    }

    pub fn make_persisting_batch(
        seq_id: i16,
        seq_num_start: i64,
        table_id: i32,
        table_name: &str,
        partition_id: i64,
        object_store_id: Uuid,
        batches: Vec<Arc<RecordBatch>>,
    ) -> Arc<PersistingBatch> {
        let queryable_batch = make_queryable_batch(table_name, seq_num_start, batches);

        Arc::new(PersistingBatch {
            sequencer_id: SequencerId::new(seq_id),
            table_id: TableId::new(table_id),
            partition_id: PartitionId::new(partition_id),
            object_store_id,
            data: queryable_batch,
        })
    }

    pub fn make_queryable_batch(
        table_name: &str,
        seq_num_start: i64,
        batches: Vec<Arc<RecordBatch>>,
    ) -> Arc<QueryableBatch> {
        make_queryable_batch_with_deletes(table_name, seq_num_start, batches, vec![])
    }

    pub fn make_queryable_batch_with_deletes(
        table_name: &str,
        seq_num_start: i64,
        batches: Vec<Arc<RecordBatch>>,
        tombstones: Vec<Tombstone>,
    ) -> Arc<QueryableBatch> {
        // make snapshots for the bacthes
        let mut snapshots = vec![];
        let mut seq_num = seq_num_start;
        for batch in batches {
            let seq = SequenceNumber::new(seq_num);
            snapshots.push(make_snapshot_batch(batch, seq, seq));
            seq_num += 1;
        }

        Arc::new(QueryableBatch::new(table_name, snapshots, tombstones))
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

        batches
    }

    // RecordBatches with knowledge of influx metadata
    pub async fn create_batches_with_influxtype_different_columns_different_order(
    ) -> Vec<Arc<RecordBatch>> {
        // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
        let mut batches = vec![];

        // chunk1 with 10 rows and 3 columns
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_tag_column("tag2")
                .with_ten_rows_of_data_some_duplicates(),
        );
        let batch1 = raw_data(&[chunk1]).await[0].clone();
        let expected = vec![
            "+-----------+------+------+--------------------------------+",
            "| field_int | tag1 | tag2 | time                           |",
            "+-----------+------+------+--------------------------------+",
            "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | AL   | 1970-01-01T00:00:00.000000005Z |",
            "| 1000      | MT   | CT   | 1970-01-01T00:00:00.000002Z    |",
            "| 20        | MT   | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | CT   | 1970-01-01T00:00:00.000000500Z |",
            "| 10        | AL   | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 30        | MT   | AL   | 1970-01-01T00:00:00.000000005Z |",
            "+-----------+------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &[batch1.clone()]);
        batches.push(Arc::new(batch1.clone()));

        let time_col = batch1
            .column(3)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let min = time_col.iter().min();
        let max = time_col.iter().max();
        println!("---- Min: {:#?}, Max: {:#?}", min, max);

        // chunk2 having duplicate data with chunk 1
        // mmore columns
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column()
                .with_tag_column("tag2")
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        );
        let batch2 = raw_data(&[chunk2]).await[0].clone();
        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag2 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 1000      | CT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | AL   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | MA   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | AL   | 1970-01-01T00:00:00.000005Z    |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &[batch2.clone()]);
        batches.push(Arc::new(batch2));

        batches
    }

    // RecordBatches with knowledge of influx metadata
    pub async fn create_batches_with_influxtype_same_columns_different_type(
    ) -> Vec<Arc<RecordBatch>> {
        // Use the available TestChunk to create chunks and then convert them to raw RecordBatches
        let mut batches = vec![];

        // chunk1
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column()
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_three_rows_of_data(),
        );
        let batch1 = raw_data(&[chunk1]).await[0].clone();
        let expected = vec![
            "+-----------+------+-----------------------------+",
            "| field_int | tag1 | time                        |",
            "+-----------+------+-----------------------------+",
            "| 1000      | WA   | 1970-01-01T00:00:00.000008Z |",
            "| 10        | VT   | 1970-01-01T00:00:00.000010Z |",
            "| 70        | UT   | 1970-01-01T00:00:00.000020Z |",
            "+-----------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &[batch1.clone()]);
        batches.push(Arc::new(batch1));

        // chunk2 having duplicate data with chunk 1
        // mmore columns
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_id(2)
                .with_time_column()
                .with_u64_column("field_int") //  u64 type but on existing name "field_int" used for i64 in chunk 1
                .with_tag_column("tag2")
                .with_three_rows_of_data(),
        );
        let batch2 = raw_data(&[chunk2]).await[0].clone();
        let expected = vec![
            "+-----------+------+-----------------------------+",
            "| field_int | tag2 | time                        |",
            "+-----------+------+-----------------------------+",
            "| 1000      | SC   | 1970-01-01T00:00:00.000008Z |",
            "| 10        | NC   | 1970-01-01T00:00:00.000010Z |",
            "| 70        | RI   | 1970-01-01T00:00:00.000020Z |",
            "+-----------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &[batch2.clone()]);
        batches.push(Arc::new(batch2));

        batches
    }
}
