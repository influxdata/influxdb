//! This module is responsible for compacting Ingester's data

use crate::data::{PersistingBatch, QueryableBatch};
use data_types::{NamespaceId, PartitionInfo};
use datafusion::{error::DataFusionError, physical_plan::SendableRecordBatchStream};
use iox_catalog::interface::INITIAL_COMPACTION_LEVEL;
use iox_query::{
    exec::{Executor, ExecutorType},
    frontend::reorg::ReorgPlanner,
    QueryChunk, QueryChunkMeta,
};
use iox_time::TimeProvider;
use parquet_file::metadata::IoxMetadata;
use schema::sort::{adjust_sort_key_columns, compute_sort_key, SortKey};
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Error while building logical plan for Ingester's compaction"))]
    LogicalPlan {
        source: iox_query::frontend::reorg::Error,
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

    #[snafu(display("Could not convert row count to i64"))]
    RowCountTypeConversion { source: std::num::TryFromIntError },

    #[snafu(display("Error computing min and max for record batches: {}", source))]
    MinMax { source: iox_query::util::Error },
}

/// A specialized `Error` for Ingester's Compact errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Compact a given persisting batch, returning a stream of compacted
/// [`RecordBatch`] and the associated [`IoxMetadata`].
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
pub async fn compact_persisting_batch(
    time_provider: Arc<dyn TimeProvider>,
    executor: &Executor,
    namespace_id: i64,
    partition_info: &PartitionInfo,
    batch: Arc<PersistingBatch>,
) -> Result<Option<(SendableRecordBatchStream, IoxMetadata, Option<SortKey>)>> {
    // Nothing to compact
    if batch.data.data.is_empty() {
        return Ok(None);
    }

    let namespace_name = &partition_info.namespace_name;
    let table_name = &partition_info.table_name;
    let partition_key = &partition_info.partition.partition_key;
    let sort_key = partition_info.partition.sort_key();

    // Get sort key from the catalog or compute it from cardinality. Save a new value for the
    // catalog to store if necessary.
    let (metadata_sort_key, sort_key_update) = match sort_key {
        Some(sk) => {
            // Remove any columns not present in this data from the sort key that will be in this
            // parquet file's metadata.
            // If there are any new columns, add them to the end of the sort key in the catalog and
            // return that to be updated in the catalog.
            adjust_sort_key_columns(&sk, &batch.data.schema().primary_key())
        }
        None => {
            let sort_key = compute_sort_key(
                batch.data.schema().as_ref(),
                batch.data.data.iter().map(|sb| sb.data.as_ref()),
            );
            // Use the sort key computed from the cardinality as the sort key for this parquet
            // file's metadata, also return the sort key to be stored in the catalog
            (sort_key.clone(), Some(sort_key))
        }
    };

    // Compact
    let stream = compact(executor, Arc::clone(&batch.data), metadata_sort_key.clone()).await?;

    // Compute min and max sequence numbers
    let (min_seq, max_seq) = batch.data.min_max_sequence_numbers();

    let meta = IoxMetadata {
        object_store_id: batch.object_store_id,
        creation_timestamp: time_provider.now(),
        sequencer_id: batch.sequencer_id,
        namespace_id: NamespaceId::new(namespace_id),
        namespace_name: Arc::from(namespace_name.as_str()),
        table_id: batch.table_id,
        table_name: Arc::from(table_name.as_str()),
        partition_id: batch.partition_id,
        partition_key: Arc::from(partition_key.as_str()),
        min_sequence_number: min_seq,
        max_sequence_number: max_seq,
        compaction_level: INITIAL_COMPACTION_LEVEL,
        sort_key: Some(metadata_sort_key),
    };

    Ok(Some((stream, meta, sort_key_update)))
}

/// Compact a given Queryable Batch
pub async fn compact(
    executor: &Executor,
    data: Arc<QueryableBatch>,
    sort_key: SortKey,
) -> Result<SendableRecordBatchStream> {
    // Build logical plan for compaction
    let ctx = executor.new_context(ExecutorType::Reorg);
    let logical_plan = ReorgPlanner::new()
        .compact_plan(data.schema(), [data as Arc<dyn QueryChunk>], sort_key)
        .context(LogicalPlanSnafu {})?;

    // Build physical plan
    let physical_plan = ctx
        .create_physical_plan(&logical_plan)
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
        create_batches_with_influxtype, create_batches_with_influxtype_different_cardinality,
        create_batches_with_influxtype_different_columns,
        create_batches_with_influxtype_different_columns_different_order,
        create_batches_with_influxtype_same_columns_different_type,
        create_one_record_batch_with_influxtype_duplicates,
        create_one_record_batch_with_influxtype_no_duplicates,
        create_one_row_record_batch_with_influxtype, create_tombstone, make_meta,
        make_persisting_batch, make_queryable_batch, make_queryable_batch_with_deletes,
    };
    use arrow_util::assert_batches_eq;
    use data_types::{Partition, PartitionId, SequencerId, TableId};
    use iox_time::SystemProvider;
    use mutable_batch_lp::lines_to_batches;
    use schema::selection::Selection;
    use uuid::Uuid;

    // this test was added to guard against https://github.com/influxdata/influxdb_iox/issues/3782
    // where if sending in a single row it would compact into an output of two batches, one of
    // which was empty, which would cause this to panic.
    #[tokio::test]
    async fn test_compact_persisting_batch_on_one_record_batch_with_one_row() {
        // create input data
        let batch = lines_to_batches("cpu bar=2 20", 0)
            .unwrap()
            .get("cpu")
            .unwrap()
            .to_arrow(Selection::All)
            .unwrap();
        let batches = vec![Arc::new(batch)];
        // build persisting batch from the input batches
        let uuid = Uuid::new_v4();
        let namespace_name = "test_namespace";
        let partition_key = "test_partition_key";
        let table_name = "test_table";
        let seq_id = 1;
        let seq_num_start: i64 = 1;
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
        let expected_pk = vec!["time"];
        assert_eq!(expected_pk, pk);

        // compact
        let exc = Executor::new(1);
        let time_provider = Arc::new(SystemProvider::new());
        let partition_info = PartitionInfo {
            namespace_name: namespace_name.into(),
            table_name: table_name.into(),
            partition: Partition {
                id: PartitionId::new(partition_id),
                sequencer_id: SequencerId::new(seq_id),
                table_id: TableId::new(table_id),
                partition_key: partition_key.into(),
                sort_key: None,
            },
        };

        let (stream, _, _) =
            compact_persisting_batch(time_provider, &exc, 1, &partition_info, persisting_batch)
                .await
                .unwrap()
                .unwrap();

        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .expect("should execute plan");

        // verify compacted data
        // should be the same as the input but sorted on tag1 & time
        let expected_data = vec![
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 2   | 1970-01-01T00:00:00.000000020Z |",
            "+-----+--------------------------------+",
        ];
        assert_batches_eq!(&expected_data, &output_batches);
    }

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
        let partition_info = PartitionInfo {
            namespace_name: namespace_name.into(),
            table_name: table_name.into(),
            partition: Partition {
                id: PartitionId::new(partition_id),
                sequencer_id: SequencerId::new(seq_id),
                table_id: TableId::new(table_id),
                partition_key: partition_key.into(),
                sort_key: None,
            },
        };

        let (stream, meta, sort_key_update) =
            compact_persisting_batch(time_provider, &exc, 1, &partition_info, persisting_batch)
                .await
                .unwrap()
                .unwrap();

        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .expect("should execute plan");

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
            seq_num_start,
            seq_num_end,
            INITIAL_COMPACTION_LEVEL,
            Some(SortKey::from_columns(["tag1", "time"])),
        );
        assert_eq!(expected_meta, meta);

        assert_eq!(
            sort_key_update.unwrap(),
            SortKey::from_columns(["tag1", "time"])
        );
    }

    #[tokio::test]
    async fn test_compact_persisting_batch_no_sort_key() {
        // create input data
        let batches = create_batches_with_influxtype_different_cardinality().await;

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
        let expected_pk = vec!["tag1", "tag3", "time"];
        assert_eq!(expected_pk, pk);

        // compact
        let exc = Executor::new(1);
        let time_provider = Arc::new(SystemProvider::new());
        let partition_info = PartitionInfo {
            namespace_name: namespace_name.into(),
            table_name: table_name.into(),
            partition: Partition {
                id: PartitionId::new(partition_id),
                sequencer_id: SequencerId::new(seq_id),
                table_id: TableId::new(table_id),
                partition_key: partition_key.into(),
                // NO SORT KEY from the catalog here, first persisting batch
                sort_key: None,
            },
        };

        let (stream, meta, sort_key_update) =
            compact_persisting_batch(time_provider, &exc, 1, &partition_info, persisting_batch)
                .await
                .unwrap()
                .unwrap();

        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .expect("should execute plan");

        // verify compacted data
        // should be the same as the input but sorted on the computed sort key of tag1, tag3, & time
        let expected_data = vec![
            "+-----------+------+------+-----------------------------+",
            "| field_int | tag1 | tag3 | time                        |",
            "+-----------+------+------+-----------------------------+",
            "| 70        | UT   | OR   | 1970-01-01T00:00:00.000220Z |",
            "| 50        | VT   | AL   | 1970-01-01T00:00:00.000210Z |",
            "| 10        | VT   | PR   | 1970-01-01T00:00:00.000210Z |",
            "| 1000      | WA   | TX   | 1970-01-01T00:00:00.000028Z |",
            "+-----------+------+------+-----------------------------+",
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
            seq_num_start,
            seq_num_end,
            INITIAL_COMPACTION_LEVEL,
            // Sort key should now be set
            Some(SortKey::from_columns(["tag1", "tag3", "time"])),
        );
        assert_eq!(expected_meta, meta);

        assert_eq!(
            sort_key_update.unwrap(),
            SortKey::from_columns(["tag1", "tag3", "time"])
        );
    }

    #[tokio::test]
    async fn test_compact_persisting_batch_with_specified_sort_key() {
        // create input data
        let batches = create_batches_with_influxtype_different_cardinality().await;

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
        let expected_pk = vec!["tag1", "tag3", "time"];
        assert_eq!(expected_pk, pk);

        // compact
        let exc = Executor::new(1);
        let time_provider = Arc::new(SystemProvider::new());
        let partition_info = PartitionInfo {
            namespace_name: namespace_name.into(),
            table_name: table_name.into(),
            partition: Partition {
                id: PartitionId::new(partition_id),
                sequencer_id: SequencerId::new(seq_id),
                table_id: TableId::new(table_id),
                partition_key: partition_key.into(),
                // SPECIFY A SORT KEY HERE to simulate a sort key being stored in the catalog
                // this is NOT what the computed sort key would be based on this data's cardinality
                sort_key: Some("tag3,tag1,time".into()),
            },
        };

        let (stream, meta, sort_key_update) =
            compact_persisting_batch(time_provider, &exc, 1, &partition_info, persisting_batch)
                .await
                .unwrap()
                .unwrap();

        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .expect("should execute plan");

        // verify compacted data
        // should be the same as the input but sorted on the specified sort key of tag3, tag1, &
        // time
        let expected_data = vec![
            "+-----------+------+------+-----------------------------+",
            "| field_int | tag1 | tag3 | time                        |",
            "+-----------+------+------+-----------------------------+",
            "| 50        | VT   | AL   | 1970-01-01T00:00:00.000210Z |",
            "| 70        | UT   | OR   | 1970-01-01T00:00:00.000220Z |",
            "| 10        | VT   | PR   | 1970-01-01T00:00:00.000210Z |",
            "| 1000      | WA   | TX   | 1970-01-01T00:00:00.000028Z |",
            "+-----------+------+------+-----------------------------+",
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
            seq_num_start,
            seq_num_end,
            INITIAL_COMPACTION_LEVEL,
            // The sort key in the metadata should be the same as specified (that is, not
            // recomputed)
            Some(SortKey::from_columns(["tag3", "tag1", "time"])),
        );
        assert_eq!(expected_meta, meta);

        // The sort key does not need to be updated in the catalog
        assert!(sort_key_update.is_none());
    }

    #[tokio::test]
    async fn test_compact_persisting_batch_new_column_for_sort_key() {
        // create input data
        let batches = create_batches_with_influxtype_different_cardinality().await;

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
        let expected_pk = vec!["tag1", "tag3", "time"];
        assert_eq!(expected_pk, pk);

        // compact
        let exc = Executor::new(1);
        let time_provider = Arc::new(SystemProvider::new());
        let partition_info = PartitionInfo {
            namespace_name: namespace_name.into(),
            table_name: table_name.into(),
            partition: Partition {
                id: PartitionId::new(partition_id),
                sequencer_id: SequencerId::new(seq_id),
                table_id: TableId::new(table_id),
                partition_key: partition_key.into(),
                // SPECIFY A SORT KEY HERE to simulate a sort key being stored in the catalog
                // this is NOT what the computed sort key would be based on this data's cardinality
                // The new column, tag1, should get added just before the time column
                sort_key: Some("tag3,time".into()),
            },
        };

        let (stream, meta, sort_key_update) =
            compact_persisting_batch(time_provider, &exc, 1, &partition_info, persisting_batch)
                .await
                .unwrap()
                .unwrap();

        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .expect("should execute plan");

        // verify compacted data
        // should be the same as the input but sorted on the specified sort key of tag3, tag1, &
        // time
        let expected_data = vec![
            "+-----------+------+------+-----------------------------+",
            "| field_int | tag1 | tag3 | time                        |",
            "+-----------+------+------+-----------------------------+",
            "| 50        | VT   | AL   | 1970-01-01T00:00:00.000210Z |",
            "| 70        | UT   | OR   | 1970-01-01T00:00:00.000220Z |",
            "| 10        | VT   | PR   | 1970-01-01T00:00:00.000210Z |",
            "| 1000      | WA   | TX   | 1970-01-01T00:00:00.000028Z |",
            "+-----------+------+------+-----------------------------+",
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
            seq_num_start,
            seq_num_end,
            INITIAL_COMPACTION_LEVEL,
            // The sort key in the metadata should be updated to include the new column just before
            // the time column
            Some(SortKey::from_columns(["tag3", "tag1", "time"])),
        );
        assert_eq!(expected_meta, meta);

        // The sort key in the catalog needs to be updated to include the new column
        assert_eq!(
            sort_key_update.unwrap(),
            SortKey::from_columns(["tag3", "tag1", "time"])
        );
    }

    #[tokio::test]
    async fn test_compact_persisting_batch_missing_column_for_sort_key() {
        // create input data
        let batches = create_batches_with_influxtype_different_cardinality().await;

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
        let expected_pk = vec!["tag1", "tag3", "time"];
        assert_eq!(expected_pk, pk);

        // compact
        let exc = Executor::new(1);
        let time_provider = Arc::new(SystemProvider::new());
        let partition_info = PartitionInfo {
            namespace_name: namespace_name.into(),
            table_name: table_name.into(),
            partition: Partition {
                id: PartitionId::new(partition_id),
                sequencer_id: SequencerId::new(seq_id),
                table_id: TableId::new(table_id),
                partition_key: partition_key.into(),
                // SPECIFY A SORT KEY HERE to simulate a sort key being stored in the catalog
                // this is NOT what the computed sort key would be based on this data's cardinality
                // This contains a sort key, "tag4", that doesn't appear in the data.
                sort_key: Some("tag3,tag1,tag4,time".into()),
            },
        };

        let (stream, meta, sort_key_update) =
            compact_persisting_batch(time_provider, &exc, 1, &partition_info, persisting_batch)
                .await
                .unwrap()
                .unwrap();

        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .expect("should execute plan");

        // verify compacted data
        // should be the same as the input but sorted on the specified sort key of tag3, tag1, &
        // time
        let expected_data = vec![
            "+-----------+------+------+-----------------------------+",
            "| field_int | tag1 | tag3 | time                        |",
            "+-----------+------+------+-----------------------------+",
            "| 50        | VT   | AL   | 1970-01-01T00:00:00.000210Z |",
            "| 70        | UT   | OR   | 1970-01-01T00:00:00.000220Z |",
            "| 10        | VT   | PR   | 1970-01-01T00:00:00.000210Z |",
            "| 1000      | WA   | TX   | 1970-01-01T00:00:00.000028Z |",
            "+-----------+------+------+-----------------------------+",
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
            seq_num_start,
            seq_num_end,
            INITIAL_COMPACTION_LEVEL,
            // The sort key in the metadata should only contain the columns in this file
            Some(SortKey::from_columns(["tag3", "tag1", "time"])),
        );
        assert_eq!(expected_meta, meta);

        // The sort key in the catalog should NOT get a new value
        assert!(sort_key_update.is_none());
    }

    #[tokio::test]
    async fn test_compact_one_row_batch() {
        test_helpers::maybe_start_logging();

        // create input data
        let batches = create_one_row_record_batch_with_influxtype().await;

        // build queryable batch from the input batches
        let compact_batch = make_queryable_batch("test_table", 1, batches);

        // verify PK
        let schema = compact_batch.schema();
        let pk = schema.primary_key();
        let expected_pk = vec!["tag1", "time"];
        assert_eq!(expected_pk, pk);

        let sort_key = compute_sort_key(
            &schema,
            compact_batch.data.iter().map(|sb| sb.data.as_ref()),
        );
        assert_eq!(sort_key, SortKey::from_columns(["tag1", "time"]));

        // compact
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch, sort_key).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();

        // verify no empty record batches - bug #3782
        assert_eq!(output_batches.len(), 1);

        // verify compacted data
        let expected = vec![
            "+-----------+------+-----------------------------+",
            "| field_int | tag1 | time                        |",
            "+-----------+------+-----------------------------+",
            "| 1000      | MA   | 1970-01-01T00:00:00.000001Z |",
            "+-----------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &output_batches);
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

        let sort_key = compute_sort_key(
            &schema,
            compact_batch.data.iter().map(|sb| sb.data.as_ref()),
        );
        assert_eq!(sort_key, SortKey::from_columns(["tag1", "time"]));

        // compact
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch, sort_key).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();
        // verify no empty record bacthes - bug #3782
        assert_eq!(output_batches.len(), 2);
        assert_eq!(output_batches[0].num_rows(), 1);
        assert_eq!(output_batches[1].num_rows(), 1);

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

        let sort_key = compute_sort_key(
            &schema,
            compact_batch.data.iter().map(|sb| sb.data.as_ref()),
        );
        assert_eq!(sort_key, SortKey::from_columns(["tag1", "time"]));

        // compact
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch, sort_key).await.unwrap();
        let output_batches = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();
        // verify no empty record bacthes - bug #3782
        assert_eq!(output_batches.len(), 2);
        assert_eq!(output_batches[0].num_rows(), 6);
        assert_eq!(output_batches[1].num_rows(), 1);

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

        let sort_key = compute_sort_key(
            &schema,
            compact_batch.data.iter().map(|sb| sb.data.as_ref()),
        );
        assert_eq!(sort_key, SortKey::from_columns(["tag1", "time"]));

        // compact
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch, sort_key).await.unwrap();
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

        let sort_key = compute_sort_key(
            &schema,
            compact_batch.data.iter().map(|sb| sb.data.as_ref()),
        );
        assert_eq!(sort_key, SortKey::from_columns(["tag1", "tag2", "time"]));

        // compact
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch, sort_key).await.unwrap();
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

        let sort_key = compute_sort_key(
            &schema,
            compact_batch.data.iter().map(|sb| sb.data.as_ref()),
        );
        assert_eq!(sort_key, SortKey::from_columns(["tag1", "tag2", "time"]));

        // compact
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch, sort_key).await.unwrap();
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

        let sort_key = compute_sort_key(
            &schema,
            compact_batch.data.iter().map(|sb| sb.data.as_ref()),
        );
        assert_eq!(sort_key, SortKey::from_columns(["tag1", "tag2", "time"]));

        // compact
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch, sort_key).await.unwrap();
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

        let sort_key = compute_sort_key(
            &schema,
            compact_batch.data.iter().map(|sb| sb.data.as_ref()),
        );
        assert_eq!(sort_key, SortKey::from_columns(["tag1", "tag2", "time"]));

        // compact
        let exc = Executor::new(1);
        let stream = compact(&exc, compact_batch, sort_key).await.unwrap();
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
