//! Table level data buffer structures.

use std::{collections::BTreeMap, sync::Arc};

use data_types::{
    DeletePredicate, NamespaceId, PartitionKey, SequenceNumber, ShardId, TableId, Timestamp,
};
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use mutable_batch::MutableBatch;
use snafu::ResultExt;
use write_summary::ShardProgress;

use super::partition::{
    resolver::PartitionProvider, PartitionData, PartitionStatus, UnpersistedPartitionData,
};
use crate::lifecycle::LifecycleHandle;

/// Data of a Table in a given Namesapce that belongs to a given Shard
#[derive(Debug)]
pub(crate) struct TableData {
    table_id: TableId,
    table_name: Arc<str>,

    /// The catalog ID of the shard & namespace this table is being populated
    /// from.
    shard_id: ShardId,
    namespace_id: NamespaceId,

    // the max sequence number for a tombstone associated with this table
    tombstone_max_sequence_number: Option<SequenceNumber>,

    /// An abstract constructor of [`PartitionData`] instances for a given
    /// `(key, shard, table)` triplet.
    partition_provider: Arc<dyn PartitionProvider>,

    // Map pf partition key to its data
    pub(super) partition_data: BTreeMap<PartitionKey, PartitionData>,
}

impl TableData {
    /// Initialize new table buffer identified by [`TableId`] in the catalog.
    ///
    /// Optionally the given tombstone max [`SequenceNumber`] identifies the
    /// inclusive upper bound of tombstones associated with this table. Any data
    /// greater than this value is guaranteed to not (yet) have a delete
    /// tombstone that must be resolved.
    ///
    /// The partition provider is used to instantiate a [`PartitionData`]
    /// instance when this [`TableData`] instance observes an op for a partition
    /// for the first time.
    pub(super) fn new(
        table_id: TableId,
        table_name: &str,
        shard_id: ShardId,
        namespace_id: NamespaceId,
        tombstone_max_sequence_number: Option<SequenceNumber>,
        partition_provider: Arc<dyn PartitionProvider>,
    ) -> Self {
        Self {
            table_id,
            table_name: table_name.into(),
            shard_id,
            namespace_id,
            tombstone_max_sequence_number,
            partition_data: Default::default(),
            partition_provider,
        }
    }

    /// Return parquet_max_sequence_number
    pub(super) fn parquet_max_sequence_number(&self) -> Option<SequenceNumber> {
        self.partition_data
            .values()
            .map(|p| p.max_persisted_sequence_number())
            .max()
            .flatten()
    }

    /// Return tombstone_max_sequence_number
    #[allow(dead_code)] // Used in tests
    pub(super) fn tombstone_max_sequence_number(&self) -> Option<SequenceNumber> {
        self.tombstone_max_sequence_number
    }

    // buffers the table write and returns true if the lifecycle manager indicates that
    // ingest should be paused.
    pub(super) async fn buffer_table_write(
        &mut self,
        sequence_number: SequenceNumber,
        batch: MutableBatch,
        partition_key: PartitionKey,
        lifecycle_handle: &dyn LifecycleHandle,
    ) -> Result<bool, super::Error> {
        let partition_data = match self.partition_data.get_mut(&partition_key) {
            Some(p) => p,
            None => {
                let p = self
                    .partition_provider
                    .get_partition(
                        partition_key.clone(),
                        self.shard_id,
                        self.namespace_id,
                        self.table_id,
                        Arc::clone(&self.table_name),
                    )
                    .await;
                // Add the partition to the map.
                assert!(self
                    .partition_data
                    .insert(partition_key.clone(), p)
                    .is_none());
                self.partition_data.get_mut(&partition_key).unwrap()
            }
        };

        // skip the write if it has already been persisted
        if let Some(max) = partition_data.max_persisted_sequence_number() {
            if max >= sequence_number {
                return Ok(false);
            }
        }

        let size = batch.size();
        let rows = batch.rows();
        partition_data.buffer_write(sequence_number, batch)?;

        // Record the write as having been buffered.
        //
        // This should happen AFTER the write is applied, because buffering the
        // op may fail which would lead to a write being recorded, but not
        // applied.
        let should_pause = lifecycle_handle.log_write(
            partition_data.id(),
            self.shard_id,
            self.namespace_id,
            self.table_id,
            sequence_number,
            size,
            rows,
        );

        Ok(should_pause)
    }

    pub(super) async fn buffer_delete(
        &mut self,
        predicate: &DeletePredicate,
        sequence_number: SequenceNumber,
        catalog: &dyn Catalog,
        executor: &Executor,
    ) -> Result<(), super::Error> {
        let min_time = Timestamp::new(predicate.range.start());
        let max_time = Timestamp::new(predicate.range.end());

        let mut repos = catalog.repositories().await;
        let tombstone = repos
            .tombstones()
            .create_or_get(
                self.table_id,
                self.shard_id,
                sequence_number,
                min_time,
                max_time,
                &predicate.expr_sql_string(),
            )
            .await
            .context(super::CatalogSnafu)?;

        // remember "persisted" state
        self.tombstone_max_sequence_number = Some(sequence_number);

        // modify one partition at a time
        for data in self.partition_data.values_mut() {
            data.buffer_tombstone(executor, tombstone.clone()).await;
        }

        Ok(())
    }

    pub(crate) fn unpersisted_partition_data(&self) -> Vec<UnpersistedPartitionData> {
        self.partition_data
            .values()
            .map(|p| UnpersistedPartitionData {
                partition_id: p.id(),
                non_persisted: p
                    .get_non_persisting_data()
                    .expect("get_non_persisting should always work"),
                persisting: p.get_persisting_data(),
                partition_status: PartitionStatus {
                    parquet_max_sequence_number: p.max_persisted_sequence_number(),
                    tombstone_max_sequence_number: self.tombstone_max_sequence_number,
                },
            })
            .collect()
    }

    /// Return progress from this Table
    pub(super) fn progress(&self) -> ShardProgress {
        let progress = ShardProgress::new();
        let progress = match self.parquet_max_sequence_number() {
            Some(n) => progress.with_persisted(n),
            None => progress,
        };

        self.partition_data
            .values()
            .fold(progress, |progress, partition_data| {
                progress.combine(partition_data.progress())
            })
    }

    /// Returns the table ID for this partition.
    pub(super) fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Returns the name of this table.
    pub(crate) fn table_name(&self) -> &Arc<str> {
        &self.table_name
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use data_types::{PartitionId, ShardIndex};
    use mutable_batch::writer;
    use mutable_batch_lp::lines_to_batches;
    use schema::{InfluxColumnType, InfluxFieldType};

    use crate::{
        data::{
            partition::{resolver::MockPartitionProvider, PartitionData},
            Error,
        },
        lifecycle::mock_handle::{MockLifecycleCall, MockLifecycleHandle},
        test_util::populate_catalog,
    };

    use super::*;

    const SHARD_INDEX: ShardIndex = ShardIndex::new(24);
    const TABLE_NAME: &str = "bananas";
    const NAMESPACE_NAME: &str = "platanos";
    const PARTITION_KEY: &str = "platanos";
    const PARTITION_ID: PartitionId = PartitionId::new(0);

    #[tokio::test]
    async fn test_bad_write_memory_counting() {
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> =
            Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));

        // Populate the catalog with the shard / namespace / table
        let (shard_id, ns_id, table_id) =
            populate_catalog(&*catalog, SHARD_INDEX, NAMESPACE_NAME, TABLE_NAME).await;

        // Configure the mock partition provider to return a partition for this
        // table ID.
        let partition_provider = Arc::new(MockPartitionProvider::default().with_partition(
            PartitionData::new(
                PARTITION_ID,
                PARTITION_KEY.into(),
                shard_id,
                ns_id,
                table_id,
                TABLE_NAME.into(),
                None,
            ),
        ));

        let mut table = TableData::new(
            table_id,
            TABLE_NAME,
            shard_id,
            ns_id,
            None,
            partition_provider,
        );

        let batch = lines_to_batches(r#"bananas,bat=man value=24 42"#, 0)
            .unwrap()
            .remove(TABLE_NAME)
            .unwrap();

        // Initialise the mock lifecycle handle and use it to inspect the calls
        // made to the lifecycle manager during buffering.
        let handle = MockLifecycleHandle::default();

        // Assert the table does not contain the test partition
        assert!(table.partition_data.get(&PARTITION_KEY.into()).is_none());

        // Write some test data
        let pause = table
            .buffer_table_write(
                SequenceNumber::new(42),
                batch,
                PARTITION_KEY.into(),
                &handle,
            )
            .await
            .expect("buffer op should succeed");
        assert!(!pause);

        // Referencing the partition should succeed
        assert!(table.partition_data.get(&PARTITION_KEY.into()).is_some());

        // And the lifecycle handle was called with the expected values
        assert_eq!(
            handle.get_log_calls(),
            &[MockLifecycleCall {
                partition_id: PARTITION_ID,
                shard_id,
                namespace_id: ns_id,
                table_id,
                sequence_number: SequenceNumber::new(42),
                bytes_written: 1131,
                rows_written: 1,
            }]
        );

        // Attempt to buffer the second op that contains a type conflict - this
        // should return an error, and not make a call to the lifecycle handle
        // (as no data was buffered)
        //
        // Note the type of value was numeric previously, and here it is a string.
        let batch = lines_to_batches(r#"bananas,bat=man value="platanos" 42"#, 0)
            .unwrap()
            .remove(TABLE_NAME)
            .unwrap();

        let err = table
            .buffer_table_write(
                SequenceNumber::new(42),
                batch,
                PARTITION_KEY.into(),
                &handle,
            )
            .await
            .expect_err("type conflict should error");

        // The buffer op should return a column type error
        assert_matches!(
            err,
            Error::BufferWrite {
                source: mutable_batch::Error::WriterError {
                    source: writer::Error::TypeMismatch {
                        existing: InfluxColumnType::Field(InfluxFieldType::Float),
                        inserted: InfluxColumnType::Field(InfluxFieldType::String),
                        column: col_name,
                    }
                },
            } => { assert_eq!(col_name, "value") }
        );

        // And the lifecycle handle should not be called.
        //
        // It still contains the first call, so the desired length is 1
        // indicating no second call was made.
        assert_eq!(handle.get_log_calls().len(), 1);
    }
}
