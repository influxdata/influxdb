use std::sync::Arc;

use backoff::Backoff;
use data_types::{
    CompactionLevel, NamespaceId, ParquetFileParams, PartitionId, PartitionKey, SequenceNumber,
    ShardId, TableId,
};
use iox_catalog::interface::{get_table_schema_by_id, CasFailure};
use iox_time::{SystemProvider, TimeProvider};
use observability_deps::tracing::*;
use parking_lot::Mutex;
use parquet_file::metadata::IoxMetadata;
use schema::sort::SortKey;
use tokio::{
    sync::{oneshot, OwnedSemaphorePermit},
    time::Instant,
};
use uuid::Uuid;

use crate::{
    buffer_tree::{
        namespace::NamespaceName,
        partition::{persisting::PersistingData, PartitionData, SortKeyState},
        table::TableName,
    },
    deferred_load::DeferredLoad,
    persist::compact::{compact_persisting_batch, CompactedStream},
};

use super::handle::Inner;

/// An internal type that contains all necessary information to run a persist task.
///
/// Used to communicate between actor handles & actor task.
#[derive(Debug)]
pub(super) struct PersistRequest {
    complete: oneshot::Sender<()>,
    partition: Arc<Mutex<PartitionData>>,
    data: PersistingData,
    enqueued_at: Instant,
    permit: OwnedSemaphorePermit,
}

impl PersistRequest {
    /// Construct a [`PersistRequest`] for `data` from `partition`, recording
    /// the current timestamp as the "enqueued at" point.
    pub(super) fn new(
        partition: Arc<Mutex<PartitionData>>,
        data: PersistingData,
        permit: OwnedSemaphorePermit,
        enqueued_at: Instant,
    ) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                complete: tx,
                partition,
                data,
                enqueued_at,
                permit,
            },
            rx,
        )
    }

    /// Return the partition ID of the persisting data.
    pub(super) fn partition_id(&self) -> PartitionId {
        self.data.partition_id()
    }
}

pub(super) struct Context {
    partition: Arc<Mutex<PartitionData>>,
    data: PersistingData,
    inner: Arc<Inner>,
    /// IDs loaded from the partition at construction time.
    namespace_id: NamespaceId,
    table_id: TableId,
    partition_id: PartitionId,

    transition_shard_id: ShardId,

    // The partition key for this partition
    partition_key: PartitionKey,

    /// Deferred strings needed for persistence.
    ///
    /// These [`DeferredLoad`] are given a pre-fetch hint when this [`Context`]
    /// is constructed to load them in the background (if not already resolved)
    /// in order to avoid incurring the query latency when the values are
    /// needed.
    namespace_name: Arc<DeferredLoad<NamespaceName>>,
    table_name: Arc<DeferredLoad<TableName>>,

    /// The [`SortKey`] for the [`PartitionData`] at the time of [`Context`]
    /// construction.
    ///
    /// The [`SortKey`] MUST NOT change during persistence, as updates to the
    /// sort key are not commutative and thus require serialising. This
    /// precludes parallel persists of partitions, unless they can be proven not
    /// to need to update the sort key.
    sort_key: SortKeyState,

    /// A notification signal to indicate to the caller that this partition has
    /// persisted.
    complete: oneshot::Sender<()>,

    /// Timing statistics tracking the timestamp this persist job was first
    /// enqueued, and the timestamp this [`Context`] was constructed (signifying
    /// the start of active persist work, as opposed to passive time spent in
    /// the queue).
    enqueued_at: Instant,
    dequeued_at: Instant,

    /// The persistence permit for this work.
    ///
    /// This permit MUST be retained for the entire duration of the persistence
    /// work, and MUST be released at the end of the persistence AFTER any
    /// references to the persisted data are released.
    permit: OwnedSemaphorePermit,
}

impl Context {
    /// Construct a persistence job [`Context`] from `req`.
    ///
    /// Locks the [`PartitionData`] in `req` to read various properties which
    /// are then cached in the [`Context`].
    pub(super) fn new(req: PersistRequest, inner: Arc<Inner>) -> Self {
        let partition_id = req.data.partition_id();

        // Obtain the partition lock and load the immutable values that will be
        // used during this persistence.
        let s = {
            let PersistRequest {
                complete,
                partition,
                data,
                enqueued_at,
                permit,
            } = req;

            let p = Arc::clone(&partition);
            let guard = p.lock();

            assert_eq!(partition_id, guard.partition_id());

            Self {
                partition,
                data,
                inner,
                namespace_id: guard.namespace_id(),
                table_id: guard.table_id(),
                partition_id,
                partition_key: guard.partition_key().clone(),
                namespace_name: Arc::clone(guard.namespace_name()),
                table_name: Arc::clone(guard.table_name()),

                // Technically the sort key isn't immutable, but MUST NOT change
                // during the execution of this persist.
                sort_key: guard.sort_key().clone(),

                complete,
                enqueued_at,
                dequeued_at: Instant::now(),
                permit,
                transition_shard_id: guard.transition_shard_id(),
            }
        };

        // Pre-fetch the deferred values in a background thread (if not already
        // resolved)
        s.namespace_name.prefetch_now();
        s.table_name.prefetch_now();
        if let SortKeyState::Deferred(ref d) = s.sort_key {
            d.prefetch_now();
        }

        s
    }

    pub(super) async fn compact(&self) -> CompactedStream {
        debug!(
            namespace_id = %self.namespace_id,
            namespace_name = %self.namespace_name,
            table_id = %self.table_id,
            table_name = %self.table_name,
            partition_id = %self.partition_id,
            partition_key = %self.partition_key,
            "compacting partition"
        );

        assert!(!self.data.record_batches().is_empty());

        // Run a compaction sort the data and resolve any duplicate values.
        //
        // This demands the deferred load values and may have to wait for them
        // to be loaded before compaction starts.
        compact_persisting_batch(
            &self.inner.exec,
            self.sort_key.get().await,
            self.table_name.get().await,
            self.data.query_adaptor(),
        )
        .await
        .expect("unable to compact persisting batch")
    }

    pub(super) async fn upload(
        &self,
        compacted: CompactedStream,
    ) -> (Option<SortKey>, ParquetFileParams) {
        let CompactedStream {
            stream: record_stream,
            catalog_sort_key_update,
            data_sort_key,
        } = compacted;

        // Generate a UUID to uniquely identify this parquet file in
        // object storage.
        let object_store_id = Uuid::new_v4();

        debug!(
            namespace_id = %self.namespace_id,
            namespace_name = %self.namespace_name,
            table_id = %self.table_id,
            table_name = %self.table_name,
            partition_id = %self.partition_id,
            partition_key = %self.partition_key,
            %object_store_id,
            sort_key = %data_sort_key,
            "uploading partition parquet"
        );

        // Construct the metadata for this parquet file.
        let iox_metadata = IoxMetadata {
            object_store_id,
            creation_timestamp: SystemProvider::new().now(),
            shard_id: self.transition_shard_id,
            namespace_id: self.namespace_id,
            namespace_name: Arc::clone(&*self.namespace_name.get().await),
            table_id: self.table_id,
            table_name: Arc::clone(&*self.table_name.get().await),
            partition_id: self.partition_id,
            partition_key: self.partition_key.clone(),
            max_sequence_number: SequenceNumber::new(0), // TODO: not ordered!
            compaction_level: CompactionLevel::Initial,
            sort_key: Some(data_sort_key),
        };

        // Save the compacted data to a parquet file in object storage.
        //
        // This call retries until it completes.
        let (md, file_size) = self
            .inner
            .store
            .upload(record_stream, &iox_metadata)
            .await
            .expect("unexpected fatal persist error");

        debug!(
            namespace_id = %self.namespace_id,
            namespace_name = %self.namespace_name,
            table_id = %self.table_id,
            table_name = %self.table_name,
            partition_id = %self.partition_id,
            partition_key = %self.partition_key,
            %object_store_id,
            file_size,
            "partition parquet uploaded"
        );

        // Read the table schema from the catalog to act as a map of column name
        // -> column IDs.
        let table_schema = Backoff::new(&Default::default())
            .retry_all_errors("get table schema", || async {
                let mut repos = self.inner.catalog.repositories().await;
                get_table_schema_by_id(self.table_id, repos.as_mut()).await
            })
            .await
            .expect("retry forever");

        // Build the data that must be inserted into the parquet_files catalog
        // table in order to make the file visible to queriers.
        let parquet_table_data =
            iox_metadata.to_parquet_file(self.partition_id, file_size, &md, |name| {
                table_schema.columns.get(name).expect("unknown column").id
            });

        (catalog_sort_key_update, parquet_table_data)
    }

    pub(crate) async fn update_database(
        self,
        sort_key_update: Option<SortKey>,
        parquet_table_data: ParquetFileParams,
    ) {
        // Extract the object store ID to the local scope so that it can easily
        // be referenced in debug logging to aid correlation of persist events
        // for a specific file.
        let object_store_id = parquet_table_data.object_store_id;

        debug!(
            namespace_id = %self.namespace_id,
            namespace_name = %self.namespace_name,
            table_id = %self.table_id,
            table_name = %self.table_name,
            partition_id = %self.partition_id,
            partition_key = %self.partition_key,
            %object_store_id,
            ?parquet_table_data,
            ?sort_key_update,
            "updating catalog"
        );

        // If necessary, update the partition sort key in the catalog and update
        // the local cached copy in the PartitionData.
        //
        // This update MUST be made visibile before the parquet file, otherwise
        // the consumer of the parquet file will observe an inconsistent sort
        // key.
        if let Some(new_sort_key) = sort_key_update {
            let old_sort_key = self
                .sort_key
                .get()
                .await
                .map(|v| v.to_columns().map(|v| v.to_string()).collect::<Vec<_>>());
            Backoff::new(&Default::default())
                .retry_all_errors("cas_sort_key", || {
                    let mut old_sort_key = old_sort_key.clone();
                    let new_sort_key_str = new_sort_key.to_columns().collect::<Vec<_>>();
                    let catalog = Arc::clone(&self.inner.catalog);
                    async move {
                        let mut repos = catalog.repositories().await;
                        loop {
                            match repos
                                .partitions()
                                .cas_sort_key(
                                    self.partition_id,
                                    old_sort_key.clone(),
                                    &new_sort_key_str,
                                )
                                .await
                            {
                                Ok(_) => break,
                                Err(CasFailure::ValueMismatch(old)) => {
                                    // An ingester concurrently updated the sort
                                    // key.
                                    //
                                    // This breaks a sort-key update invariant -
                                    // sort key updates MUST be serialised. This
                                    // currently cannot be enforced.
                                    //
                                    // See:
                                    //   https://github.com/influxdata/influxdb_iox/issues/6439
                                    //
                                    error!(
                                        expected=?old_sort_key,
                                        observed=?old,
                                        update=?new_sort_key_str,
                                        "detected concurrent sort key update"
                                    );
                                    // Retry using the new CAS value.
                                    old_sort_key = Some(old);
                                }
                                Err(CasFailure::QueryError(e)) => return Err(e),
                            };
                        }
                        Ok(()) as Result<(), iox_catalog::interface::Error>
                    }
                })
                .await
                .expect("retry forever");

            // Update the sort key in the partition cache.
            let old_key;
            {
                let mut guard = self.partition.lock();
                old_key = guard.sort_key().clone();
                guard.update_sort_key(Some(new_sort_key.clone()));
            };

            // Assert the internal (to this instance) serialisation of sort key
            // updates.
            //
            // Both of these get() should not block due to both of the values
            // having been previously resolved / used.
            assert_eq!(old_key.get().await, self.sort_key.get().await);

            debug!(
                namespace_id = %self.namespace_id,
                namespace_name = %self.namespace_name,
                table_id = %self.table_id,
                table_name = %self.table_name,
                partition_id = %self.partition_id,
                partition_key = %self.partition_key,
                %object_store_id,
                ?old_sort_key,
                %new_sort_key,
                "adjusted partition sort key"
            );
        }

        // Add the parquet file to the catalog.
        //
        // This has the effect of allowing the queriers to "discover" the
        // parquet file by polling / querying the catalog.
        Backoff::new(&Default::default())
            .retry_all_errors("add parquet file to catalog", || async {
                let mut repos = self.inner.catalog.repositories().await;
                let parquet_file = repos
                    .parquet_files()
                    .create(parquet_table_data.clone())
                    .await?;

                debug!(
                    namespace_id = %self.namespace_id,
                    namespace_name = %self.namespace_name,
                    table_id = %self.table_id,
                    table_name = %self.table_name,
                    partition_id = %self.partition_id,
                    partition_key = %self.partition_key,
                    %object_store_id,
                    ?parquet_table_data,
                    parquet_file_id=?parquet_file.id,
                    "parquet file added to catalog"
                );

                // compiler insisted on getting told the type of the error :shrug:
                Ok(()) as Result<(), iox_catalog::interface::Error>
            })
            .await
            .expect("retry forever");

        // Mark the partition as having completed persistence, causing it to
        // release the reference to the in-flight persistence data it is
        // holding.
        //
        // This SHOULD cause the data to be dropped, but there MAY be ongoing
        // queries that currently hold a reference to the data. In either case,
        // the persisted data will be dropped "shortly".
        self.partition.lock().mark_persisted(self.data);

        let now = Instant::now();

        info!(
            %object_store_id,
            namespace_id = %self.namespace_id,
            namespace_name = %self.namespace_name,
            table_id = %self.table_id,
            table_name = %self.table_name,
            partition_id = %self.partition_id,
            partition_key = %self.partition_key,
            total_persist_duration = ?now.duration_since(self.enqueued_at),
            active_persist_duration = ?now.duration_since(self.dequeued_at),
            queued_persist_duration = ?self.dequeued_at.duration_since(self.enqueued_at),
            "persisted partition"
        );

        // Explicitly drop the permit before notifying the caller, so that if
        // there's no headroom in the queue, the caller that is woken by the
        // notification is able to push into the queue immediately.
        drop(self.permit);

        // Notify the observer of this persistence task, if any.
        let _ = self.complete.send(());
    }
}

// TODO(test): persist
// TODO(test): persist completion notification
