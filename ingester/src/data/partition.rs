//! Partition level data buffer structures.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use data_types::{NamespaceId, PartitionId, PartitionKey, SequenceNumber, ShardId, TableId};
use mutable_batch::MutableBatch;
use observability_deps::tracing::*;
use schema::{selection::Selection, sort::SortKey};
use snafu::ResultExt;
use uuid::Uuid;
use write_summary::ShardProgress;

use self::{
    buffer::{BufferBatch, DataBuffer},
    resolver::DeferredSortKey,
};
use crate::{querier_handler::PartitionStatus, query::QueryableBatch};

use super::table::TableName;

mod buffer;
pub mod resolver;

/// Read only copy of the unpersisted data for a partition in the ingester for a specific partition.
#[derive(Debug)]
pub(crate) struct UnpersistedPartitionData {
    pub(crate) partition_id: PartitionId,
    pub(crate) non_persisted: Vec<Arc<SnapshotBatch>>,
    pub(crate) persisting: Option<QueryableBatch>,
    pub(crate) partition_status: PartitionStatus,
}

/// PersistingBatch contains all needed info and data for creating
/// a parquet file for given set of SnapshotBatches
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct PersistingBatch {
    /// Shard id of the data
    pub(crate) shard_id: ShardId,

    /// Table id of the data
    pub(crate) table_id: TableId,

    /// Partition Id of the data
    pub(crate) partition_id: PartitionId,

    /// Id of to-be-created parquet file of this data
    pub(crate) object_store_id: Uuid,

    /// data
    pub(crate) data: Arc<QueryableBatch>,
}

impl PersistingBatch {
    pub(crate) fn object_store_id(&self) -> Uuid {
        self.object_store_id
    }

    pub(crate) fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub(crate) fn table_id(&self) -> TableId {
        self.table_id
    }

    pub(crate) fn partition_id(&self) -> PartitionId {
        self.partition_id
    }
}

/// SnapshotBatch contains data of many contiguous BufferBatches
#[derive(Debug, PartialEq)]
pub(crate) struct SnapshotBatch {
    /// Min sequence number of its combined BufferBatches
    pub(crate) min_sequence_number: SequenceNumber,
    /// Max sequence number of its combined BufferBatches
    pub(crate) max_sequence_number: SequenceNumber,
    /// Data of its combined BufferBatches kept in one RecordBatch
    pub(crate) data: Arc<RecordBatch>,
}

impl SnapshotBatch {
    /// Return only data of the given columns
    pub(crate) fn scan(
        &self,
        selection: Selection<'_>,
    ) -> Result<Option<Arc<RecordBatch>>, super::Error> {
        Ok(match selection {
            Selection::All => Some(Arc::clone(&self.data)),
            Selection::Some(columns) => {
                let schema = self.data.schema();

                let indices = columns
                    .iter()
                    .filter_map(|&column_name| {
                        match schema.index_of(column_name) {
                            Ok(idx) => Some(idx),
                            _ => None, // this batch does not include data of this column_name
                        }
                    })
                    .collect::<Vec<_>>();
                if indices.is_empty() {
                    None
                } else {
                    Some(Arc::new(
                        self.data
                            .project(&indices)
                            .context(super::FilterColumnSnafu {})?,
                    ))
                }
            }
        })
    }

    /// Return progress in this data
    fn progress(&self) -> ShardProgress {
        ShardProgress::new()
            .with_buffered(self.min_sequence_number)
            .with_buffered(self.max_sequence_number)
    }
}

/// The load state of the [`SortKey`] for a given partition.
#[derive(Debug, Clone)]
pub(crate) enum SortKeyState {
    /// The [`SortKey`] has not yet been fetched from the catalog, and will be
    /// lazy loaded (or loaded in the background) by a call to
    /// [`DeferredSortKey::get()`].
    Deferred(Arc<DeferredSortKey>),
    /// The sort key is known and specified.
    Provided(Option<SortKey>),
}

impl SortKeyState {
    pub(crate) async fn get(&self) -> Option<SortKey> {
        match self {
            Self::Deferred(v) => v.get().await,
            Self::Provided(v) => v.clone(),
        }
    }
}

/// Data of an IOx Partition of a given Table of a Namespace that belongs to a
/// given Shard
#[derive(Debug)]
pub struct PartitionData {
    /// The catalog ID of the partition this buffer is for.
    id: PartitionId,
    /// The string partition key for this partition.
    partition_key: PartitionKey,

    /// The sort key of this partition.
    ///
    /// This can known, in which case this field will contain a
    /// [`SortKeyState::Provided`] with the [`SortKey`], or unknown with a value
    /// of [`SortKeyState::Deferred`] causing it to be loaded from the catalog
    /// (potentially) in the background or at read time.
    ///
    /// Callers should use [`Self::sort_key()`] to be abstracted away from these
    /// fetch details.
    sort_key: SortKeyState,

    /// The shard, namespace & table IDs for this partition.
    shard_id: ShardId,
    namespace_id: NamespaceId,
    table_id: TableId,
    /// The name of the table this partition is part of.
    table_name: TableName,

    pub(super) data: DataBuffer,

    /// The max_persisted_sequence number for any parquet_file in this
    /// partition.
    max_persisted_sequence_number: Option<SequenceNumber>,
}

impl PartitionData {
    /// Initialize a new partition data buffer
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: PartitionId,
        partition_key: PartitionKey,
        shard_id: ShardId,
        namespace_id: NamespaceId,
        table_id: TableId,
        table_name: TableName,
        sort_key: SortKeyState,
        max_persisted_sequence_number: Option<SequenceNumber>,
    ) -> Self {
        Self {
            id,
            partition_key,
            sort_key,
            shard_id,
            namespace_id,
            table_id,
            table_name,
            data: Default::default(),
            max_persisted_sequence_number,
        }
    }

    /// Snapshot anything in the buffer and move all snapshot data into a persisting batch
    pub(super) fn snapshot_to_persisting_batch(&mut self) -> Option<Arc<PersistingBatch>> {
        self.data
            .snapshot_to_persisting(self.shard_id, self.table_id, self.id, &self.table_name)
    }

    /// Snapshot whatever is in the buffer and return a new vec of the
    /// arc cloned snapshots
    #[cfg(test)]
    fn snapshot(&mut self) -> Result<Vec<Arc<SnapshotBatch>>, super::Error> {
        self.data
            .generate_snapshot()
            .context(super::SnapshotSnafu)?;
        Ok(self.data.get_snapshots().to_vec())
    }

    /// Return non persisting data
    pub(super) fn get_non_persisting_data(&self) -> Result<Vec<Arc<SnapshotBatch>>, super::Error> {
        self.data.buffer_and_snapshots()
    }

    /// Return persisting data
    pub(super) fn get_persisting_data(&self) -> Option<QueryableBatch> {
        self.data.get_persisting_data()
    }

    /// Write the given mb in the buffer
    pub(super) fn buffer_write(
        &mut self,
        sequence_number: SequenceNumber,
        mb: MutableBatch,
    ) -> Result<(), super::Error> {
        let (min_sequence_number, max_sequence_number) = match &mut self.data.buffer {
            Some(buf) => {
                buf.max_sequence_number = sequence_number.max(buf.max_sequence_number);
                buf.data.extend_from(&mb).context(super::BufferWriteSnafu)?;
                (buf.min_sequence_number, buf.max_sequence_number)
            }
            None => {
                self.data.buffer = Some(BufferBatch {
                    min_sequence_number: sequence_number,
                    max_sequence_number: sequence_number,
                    data: mb,
                });
                (sequence_number, sequence_number)
            }
        };
        trace!(
            min_sequence_number=?min_sequence_number,
            max_sequence_number=?max_sequence_number,
            "buffered write"
        );

        Ok(())
    }

    /// Return the progress from this Partition
    pub(super) fn progress(&self) -> ShardProgress {
        self.data.progress()
    }

    pub(super) fn partition_id(&self) -> PartitionId {
        self.id
    }

    /// Return the [`SequenceNumber`] that forms the (inclusive) persistence
    /// watermark for this partition.
    pub(crate) fn max_persisted_sequence_number(&self) -> Option<SequenceNumber> {
        self.max_persisted_sequence_number
    }

    /// Mark this partition as having completed persistence up to, and
    /// including, the specified [`SequenceNumber`].
    pub(super) fn mark_persisted(&mut self, sequence_number: SequenceNumber) {
        // It is an invariant that partitions are persisted in order so that
        // both the per-shard, and per-partition watermarks are correctly
        // advanced and accurate.
        if let Some(last_persist) = self.max_persisted_sequence_number() {
            assert!(
                sequence_number > last_persist,
                "out of order partition persistence, persisting {}, previously persisted {}",
                sequence_number.get(),
                last_persist.get(),
            );
        }

        self.max_persisted_sequence_number = Some(sequence_number);
        self.data.mark_persisted();
    }

    /// Return the name of the table this [`PartitionData`] is buffering writes
    /// for.
    #[cfg(test)]
    pub(crate) fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    /// Return the shard ID for this partition.
    #[cfg(test)]
    pub(crate) fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Return the table ID for this partition.
    #[cfg(test)]
    pub(crate) fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Return the partition key for this partition.
    pub fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    /// Return the [`NamespaceId`] this partition is a part of.
    pub fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    /// Return the [`SortKey`] for this partition.
    ///
    /// NOTE: this MAY involve querying the catalog with unbounded retries.
    pub(crate) fn sort_key(&self) -> &SortKeyState {
        &self.sort_key
    }

    /// Set the cached [`SortKey`] to the specified value.
    ///
    /// All subsequent calls to [`Self::sort_key`] will return
    /// [`SortKeyState::Provided`]  with the `new`.
    pub(crate) fn update_sort_key(&mut self, new: Option<SortKey>) {
        self.sort_key = SortKeyState::Provided(new);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow_util::assert_batches_sorted_eq;
    use assert_matches::assert_matches;
    use backoff::BackoffConfig;
    use data_types::ShardIndex;
    use iox_catalog::interface::Catalog;
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;

    use crate::test_util::populate_catalog;

    use super::*;

    #[test]
    fn snapshot_buffer_different_but_compatible_schemas() {
        let mut partition_data = PartitionData::new(
            PartitionId::new(1),
            "bananas".into(),
            ShardId::new(1),
            NamespaceId::new(42),
            TableId::new(1),
            "foo".into(),
            SortKeyState::Provided(None),
            None,
        );

        let seq_num1 = SequenceNumber::new(1);
        // Missing tag `t1`
        let (_, mut mutable_batch1) =
            lp_to_mutable_batch(r#"foo iv=1i,uv=774u,fv=1.0,bv=true,sv="hi" 1"#);
        partition_data
            .buffer_write(seq_num1, mutable_batch1.clone())
            .unwrap();

        let seq_num2 = SequenceNumber::new(2);
        // Missing field `iv`
        let (_, mutable_batch2) =
            lp_to_mutable_batch(r#"foo,t1=aoeu uv=1u,fv=12.0,bv=false,sv="bye" 10000"#);

        partition_data
            .buffer_write(seq_num2, mutable_batch2.clone())
            .unwrap();
        partition_data.data.generate_snapshot().unwrap();

        assert!(partition_data.data.buffer.is_none());
        assert_eq!(partition_data.data.snapshots.len(), 1);

        let snapshot = &partition_data.data.snapshots[0];
        assert_eq!(snapshot.min_sequence_number, seq_num1);
        assert_eq!(snapshot.max_sequence_number, seq_num2);

        mutable_batch1.extend_from(&mutable_batch2).unwrap();
        let combined_record_batch = mutable_batch1.to_arrow(Selection::All).unwrap();
        assert_eq!(&*snapshot.data, &combined_record_batch);
    }

    // Test deletes mixed with writes on a single parittion
    #[tokio::test]
    async fn writes() {
        // Make a partition with empty DataBuffer
        let s_id = 1;
        let t_id = 1;
        let p_id = 1;
        let mut p = PartitionData::new(
            PartitionId::new(p_id),
            "bananas".into(),
            ShardId::new(s_id),
            NamespaceId::new(42),
            TableId::new(t_id),
            "restaurant".into(),
            SortKeyState::Provided(None),
            None,
        );

        // ------------------------------------------
        // Fill `buffer`
        // --- seq_num: 1
        let (_, mb) = lp_to_mutable_batch(r#"restaurant,city=Boston day="fri",temp=50 10"#);
        p.buffer_write(SequenceNumber::new(1), mb).unwrap();

        // --- seq_num: 2
        let (_, mb) = lp_to_mutable_batch(r#"restaurant,city=Andover day="thu",temp=44 15"#);

        p.buffer_write(SequenceNumber::new(2), mb).unwrap();

        // verify data
        assert_eq!(
            p.data.buffer.as_ref().unwrap().min_sequence_number,
            SequenceNumber::new(1)
        );
        assert_eq!(
            p.data.buffer.as_ref().unwrap().max_sequence_number,
            SequenceNumber::new(2)
        );
        assert_eq!(p.data.snapshots.len(), 0);
        assert_eq!(p.data.persisting, None);

        // ------------------------------------------
        // Fill `buffer`
        // --- seq_num: 4
        let (_, mb) = lp_to_mutable_batch(
            r#"
                restaurant,city=Medford day="sun",temp=55 22
                restaurant,city=Boston day="sun",temp=57 24
            "#,
        );
        p.buffer_write(SequenceNumber::new(4), mb).unwrap();

        // --- seq_num: 5
        let (_, mb) = lp_to_mutable_batch(r#"restaurant,city=Andover day="tue",temp=56 30"#);

        p.buffer_write(SequenceNumber::new(5), mb).unwrap();

        // verify data
        assert_eq!(
            p.data.buffer.as_ref().unwrap().min_sequence_number,
            SequenceNumber::new(1)
        );
        assert_eq!(
            p.data.buffer.as_ref().unwrap().max_sequence_number,
            SequenceNumber::new(5)
        );
        assert_eq!(p.data.snapshots.len(), 0);
        assert_eq!(p.data.persisting, None);
        assert!(p.data.buffer.is_some());

        // ------------------------------------------
        // Persisting
        let p_batch = p.snapshot_to_persisting_batch().unwrap();

        // verify data
        assert!(p.data.buffer.is_none()); // always empty after issuing persit
        assert_eq!(p.data.snapshots.len(), 0); // always empty after issuing persit
        assert_eq!(p.data.persisting, Some(Arc::clone(&p_batch)));

        // verify data
        assert!(p.data.buffer.is_none());
        assert_eq!(p.data.snapshots.len(), 0); // no snpashots becasue buffer has not data yet and the
                                               // snapshot was empty too
        assert_eq!(p.data.persisting, Some(Arc::clone(&p_batch)));

        // ------------------------------------------
        // Fill `buffer`
        // --- seq_num: 8
        let (_, mb) = lp_to_mutable_batch(
            r#"
                restaurant,city=Wilmington day="sun",temp=55 35
                restaurant,city=Boston day="sun",temp=60 36
                restaurant,city=Boston day="sun",temp=62 38
            "#,
        );
        p.buffer_write(SequenceNumber::new(8), mb).unwrap();

        // verify data
        assert_eq!(
            p.data.buffer.as_ref().unwrap().min_sequence_number,
            SequenceNumber::new(8)
        ); // 1 newly added mutable batch of 3 rows of data
        assert_eq!(p.data.snapshots.len(), 0); // still empty
        assert_eq!(p.data.persisting, Some(Arc::clone(&p_batch)));

        // ------------------------------------------
        // Take snapshot of the `buffer`
        p.snapshot().unwrap();
        // verify data
        assert!(p.data.buffer.is_none()); // empty after snapshot
        assert_eq!(p.data.snapshots.len(), 1); // data moved from buffer
        assert_eq!(p.data.persisting, Some(Arc::clone(&p_batch)));
        // snapshot has three rows moved from buffer
        let data = (*p.data.snapshots[0].data).clone();
        let expected = vec![
            "+------------+-----+------+--------------------------------+",
            "| city       | day | temp | time                           |",
            "+------------+-----+------+--------------------------------+",
            "| Wilmington | sun | 55   | 1970-01-01T00:00:00.000000035Z |",
            "| Boston     | sun | 60   | 1970-01-01T00:00:00.000000036Z |",
            "| Boston     | sun | 62   | 1970-01-01T00:00:00.000000038Z |",
            "+------------+-----+------+--------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &[data]);
        assert_eq!(p.data.snapshots[0].min_sequence_number.get(), 8);
        assert_eq!(p.data.snapshots[0].max_sequence_number.get(), 8);
    }

    #[tokio::test]
    async fn test_update_provided_sort_key() {
        let starting_state =
            SortKeyState::Provided(Some(SortKey::from_columns(["banana", "time"])));

        let mut p = PartitionData::new(
            PartitionId::new(1),
            "bananas".into(),
            ShardId::new(1),
            NamespaceId::new(42),
            TableId::new(1),
            "platanos".into(),
            starting_state,
            None,
        );

        let want = Some(SortKey::from_columns(["banana", "platanos", "time"]));
        p.update_sort_key(want.clone());

        assert_matches!(p.sort_key(), SortKeyState::Provided(_));
        assert_eq!(p.sort_key().get().await, want);
    }

    #[tokio::test]
    async fn test_update_deferred_sort_key() {
        let metrics = Arc::new(metric::Registry::default());
        let backoff_config = BackoffConfig::default();
        let catalog: Arc<dyn Catalog> =
            Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));

        // Populate the catalog with the shard / namespace / table
        let (shard_id, _ns_id, table_id) =
            populate_catalog(&*catalog, ShardIndex::new(1), "bananas", "platanos").await;

        let partition_id = catalog
            .repositories()
            .await
            .partitions()
            .create_or_get("test".into(), shard_id, table_id)
            .await
            .expect("should create")
            .id;

        catalog
            .repositories()
            .await
            .partitions()
            .update_sort_key(partition_id, &["terrific"])
            .await
            .unwrap();

        // Read the just-created sort key (None)
        let fetcher = Arc::new(DeferredSortKey::new(
            partition_id,
            Duration::from_nanos(1),
            Arc::clone(&catalog),
            backoff_config.clone(),
        ));

        let starting_state = SortKeyState::Deferred(fetcher);

        let mut p = PartitionData::new(
            PartitionId::new(1),
            "bananas".into(),
            ShardId::new(1),
            NamespaceId::new(42),
            TableId::new(1),
            "platanos".into(),
            starting_state,
            None,
        );

        let want = Some(SortKey::from_columns(["banana", "platanos", "time"]));
        p.update_sort_key(want.clone());

        assert_matches!(p.sort_key(), SortKeyState::Provided(_));
        assert_eq!(p.sort_key().get().await, want);
    }
}
