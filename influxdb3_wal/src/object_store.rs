use crate::snapshot_tracker::{SnapshotTracker, WalPeriod};
use crate::{
    background_wal_flush, OrderedCatalogBatch, SnapshotDetails, SnapshotSequenceNumber, Wal,
    WalConfig, WalContents, WalFileNotifier, WalFileSequenceNumber, WalOp, WriteBatch,
};
use crate::{serialize::verify_file_type_and_deserialize, NoopDetails};
use bytes::Bytes;
use data_types::Timestamp;
use futures_util::stream::StreamExt;
use hashbrown::HashMap;
use iox_time::TimeProvider;
use object_store::path::{Path, PathPart};
use object_store::{ObjectStore, PutPayload};
use observability_deps::tracing::{debug, error, info};
use std::time::Duration;
use std::{str::FromStr, sync::Arc};
use tokio::sync::Mutex;
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
pub struct WalObjectStore {
    object_store: Arc<dyn ObjectStore>,
    host_identifier_prefix: String,
    file_notifier: Arc<dyn WalFileNotifier>,
    added_file_notifiers: parking_lot::Mutex<Vec<Arc<dyn WalFileNotifier>>>,
    /// Buffered wal ops go in here along with the state to track when to snapshot
    flush_buffer: Mutex<FlushBuffer>,
    /// number of snapshotted wal files to retain in object store
    snapshotted_wal_files_to_keep: u64,
    wal_remover: parking_lot::Mutex<WalFileRemover>,
}

impl WalObjectStore {
    /// Creates a new WAL. This will replay files into the notifier and trigger any snapshots that
    /// exist in the WAL files that haven't been cleaned up yet.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        time_provider: Arc<dyn TimeProvider>,
        object_store: Arc<dyn ObjectStore>,
        host_identifier_prefix: impl Into<String> + Send,
        file_notifier: Arc<dyn WalFileNotifier>,
        config: WalConfig,
        last_wal_sequence_number: Option<WalFileSequenceNumber>,
        last_snapshot_sequence_number: Option<SnapshotSequenceNumber>,
        snapshotted_wal_files_to_keep: u64,
    ) -> Result<Arc<Self>, crate::Error> {
        let host_identifier = host_identifier_prefix.into();
        let all_wal_file_paths =
            load_all_wal_file_paths(Arc::clone(&object_store), host_identifier.clone()).await?;
        let flush_interval = config.flush_interval;
        let wal = Self::new_without_replay(
            time_provider,
            object_store,
            host_identifier,
            file_notifier,
            config,
            last_wal_sequence_number,
            last_snapshot_sequence_number,
            &all_wal_file_paths,
            snapshotted_wal_files_to_keep,
        );

        wal.replay(last_wal_sequence_number, &all_wal_file_paths)
            .await?;
        let wal = Arc::new(wal);
        background_wal_flush(Arc::clone(&wal), flush_interval);

        Ok(wal)
    }

    #[allow(clippy::too_many_arguments)]
    fn new_without_replay(
        time_provider: Arc<dyn TimeProvider>,
        object_store: Arc<dyn ObjectStore>,
        host_identifier_prefix: impl Into<String>,
        file_notifier: Arc<dyn WalFileNotifier>,
        config: WalConfig,
        last_wal_sequence_number: Option<WalFileSequenceNumber>,
        last_snapshot_sequence_number: Option<SnapshotSequenceNumber>,
        all_wal_file_paths: &[Path],
        num_wal_files_to_keep: u64,
    ) -> Self {
        let wal_file_sequence_number = last_wal_sequence_number.unwrap_or_default().next();
        let oldest_wal_file_num = oldest_wal_file_num(all_wal_file_paths);

        Self {
            object_store,
            host_identifier_prefix: host_identifier_prefix.into(),
            file_notifier,
            added_file_notifiers: Default::default(),
            flush_buffer: Mutex::new(FlushBuffer::new(
                Arc::clone(&time_provider),
                WalBuffer {
                    time_provider,
                    is_shutdown: false,
                    wal_file_sequence_number,
                    op_limit: config.max_write_buffer_size,
                    op_count: 0,
                    database_to_write_batch: Default::default(),
                    catalog_batches: vec![],
                    write_op_responses: vec![],
                    no_op: None,
                },
                SnapshotTracker::new(
                    config.snapshot_size,
                    config.gen1_duration,
                    last_snapshot_sequence_number,
                ),
            )),
            snapshotted_wal_files_to_keep: num_wal_files_to_keep,
            wal_remover: parking_lot::Mutex::new(WalFileRemover {
                oldest_wal_file: oldest_wal_file_num,
                last_snapshotted_wal_sequence_number: last_wal_sequence_number,
            }),
        }
    }

    /// Loads the WAL files in order from object store, calling the file notifier on each one and
    /// populating the snapshot tracker with the WAL periods.
    pub async fn replay(
        &self,
        last_wal_sequence_number: Option<WalFileSequenceNumber>,
        all_wal_file_paths: &[Path],
    ) -> crate::Result<()> {
        debug!(">>> replaying");
        let paths = self.load_existing_wal_file_paths(last_wal_sequence_number, all_wal_file_paths);

        let last_snapshot_sequence_number = {
            self.flush_buffer
                .lock()
                .await
                .snapshot_tracker
                .last_snapshot_sequence_number()
        };

        async fn get_contents(
            object_store: Arc<dyn ObjectStore>,
            path: Path,
        ) -> Result<WalContents, crate::Error> {
            let file_bytes = object_store.get(&path).await?.bytes().await?;
            Ok(verify_file_type_and_deserialize(file_bytes)?)
        }

        let mut replay_tasks = Vec::new();
        for path in paths {
            let object_store = Arc::clone(&self.object_store);
            replay_tasks.push(tokio::spawn(get_contents(object_store, path)));
        }

        for wal_contents in replay_tasks {
            let wal_contents = wal_contents.await??;

            // add this to the snapshot tracker, so we know what to clear out later if the replay
            // was a wal file that had a snapshot
            self.flush_buffer
                .lock()
                .await
                .replay_wal_period(WalPeriod::new(
                    wal_contents.wal_file_number,
                    Timestamp::new(wal_contents.min_timestamp_ns),
                    Timestamp::new(wal_contents.max_timestamp_ns),
                ));

            info!(
                n_ops = %wal_contents.ops.len(),
                min_timestamp_ns = %wal_contents.min_timestamp_ns,
                max_timestamp_ns = %wal_contents.max_timestamp_ns,
                wal_file_number = %wal_contents.wal_file_number,
                snapshot_details = ?wal_contents.snapshot,
                "replaying WAL file"
            );

            match wal_contents.snapshot {
                // This branch uses so much time
                None => self.file_notifier.notify(Arc::new(wal_contents)).await,
                Some(snapshot_details) => {
                    let snapshot_info = {
                        let mut buffer = self.flush_buffer.lock().await;

                        match buffer.snapshot_tracker.snapshot(snapshot_details.forced) {
                            None => None,
                            Some(info) => {
                                let semaphore = Arc::clone(&buffer.snapshot_semaphore);
                                let permit = semaphore.acquire_owned().await.unwrap();
                                Some((info, permit))
                            }
                        }
                    };
                    if snapshot_details.snapshot_sequence_number <= last_snapshot_sequence_number {
                        // Instead just notify about the WAL, as this snapshot has already been taken
                        // and WAL files may have been cleared.
                        self.file_notifier.notify(Arc::new(wal_contents)).await;
                    } else {
                        let snapshot_done = self
                            .file_notifier
                            .notify_and_snapshot(Arc::new(wal_contents), snapshot_details)
                            .await;
                        let details = snapshot_done.await.unwrap();
                        assert_eq!(snapshot_details, details);
                    }

                    // if the info is there, we have wal files to delete
                    if let Some((snapshot_info, snapshot_permit)) = snapshot_info {
                        self.cleanup_snapshot(snapshot_info, snapshot_permit).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// Stop accepting write operations, flush of buffered writes to a WAL file and return when done.
    pub async fn shutdown(&mut self) {
        // stop accepting writes
        self.flush_buffer.lock().await.wal_buffer.is_shutdown = true;

        // do the flush and wait for the snapshot if that's running
        if let Some((snapshot_done, snapshot_info, snapshot_permit)) =
            self.flush_buffer(false).await
        {
            let snapshot_details = snapshot_done.await.expect("snapshot should complete");
            assert_eq!(snapshot_info, snapshot_details);
            self.remove_snapshot_wal_files(snapshot_info, snapshot_permit)
                .await;
        }
    }

    /// Buffer into a single larger operation in memory. Returns before the operation is persisted.
    async fn buffer_op_unconfirmed(&self, op: WalOp) -> crate::Result<(), crate::Error> {
        self.flush_buffer
            .lock()
            .await
            .wal_buffer
            .buffer_op_unconfirmed(op)
    }

    /// Writes the op into the buffer and waits until the WAL file is persisted. When this returns
    /// the operation is durable in the configured object store.
    async fn write_ops(&self, ops: Vec<WalOp>) -> crate::Result<(), crate::Error> {
        let (tx, rx) = oneshot::channel();
        self.flush_buffer
            .lock()
            .await
            .wal_buffer
            .buffer_ops_with_response(ops, tx)?;

        match rx.await {
            Ok(WriteResult::Success(())) => Ok(()),
            Ok(WriteResult::Error(e)) => Err(crate::Error::WriteError(e)),
            Err(_) => Err(crate::Error::WriteError(
                "oneshot channel closed".to_string(),
            )),
        }
    }

    async fn flush_buffer(
        &self,
        force_snapshot: bool,
    ) -> Option<(
        oneshot::Receiver<SnapshotDetails>,
        SnapshotDetails,
        OwnedSemaphorePermit,
    )> {
        let (wal_contents, responses, snapshot) = {
            let mut flush_buffer = self.flush_buffer.lock().await;
            if flush_buffer.wal_buffer.is_empty() && !force_snapshot {
                return None;
            }
            flush_buffer
                .flush_buffer_into_contents_and_responses(force_snapshot)
                .await
        };
        info!(
            n_ops = %wal_contents.ops.len(),
            min_timestamp_ns = %wal_contents.min_timestamp_ns,
            max_timestamp_ns = %wal_contents.max_timestamp_ns,
            wal_file_number = %wal_contents.wal_file_number,
            "flushing WAL buffer to object store"
        );

        let wal_path = wal_path(&self.host_identifier_prefix, wal_contents.wal_file_number);
        let data = crate::serialize::serialize_to_file_bytes(&wal_contents)
            .expect("unable to serialize wal contents into bytes for file");
        let data = Bytes::from(data);

        let mut retry_count = 0;

        // keep trying to write this to object store forever
        loop {
            match self
                .object_store
                .put(&wal_path, PutPayload::from_bytes(data.clone()))
                .await
            {
                Ok(_) => {
                    break;
                }
                Err(e) => {
                    error!(%e, "error writing wal file to object store");
                    retry_count += 1;
                    if retry_count > 100 {
                        // we're over max retries, the object store must be down, so drop
                        // all these responses and any in the new buffer
                        for response in responses {
                            let _ = response.send(WriteResult::Error(e.to_string()));
                        }

                        self.flush_buffer
                            .lock()
                            .await
                            .flush_buffer_with_failure(WriteResult::Error(e.to_string()))
                            .await;

                        return None;
                    }

                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }

        let wal_contents = Arc::new(wal_contents);

        // now that we've persisted this latest notify and start the snapshot, if set
        let snapshot_response = match wal_contents.snapshot {
            Some(snapshot_details) => {
                info!(?snapshot_details, "snapshotting wal");
                let snapshot_done = self
                    .file_notifier
                    .notify_and_snapshot(Arc::clone(&wal_contents), snapshot_details)
                    .await;
                let (snapshot_info, snapshot_permit) =
                    snapshot.expect("snapshot should be set when snapshot details are set");

                Some((snapshot_done, snapshot_info, snapshot_permit))
            }
            None => {
                debug!(
                    "notify sent to buffer for wal file {}",
                    wal_contents.wal_file_number.as_u64()
                );
                self.file_notifier.notify(Arc::clone(&wal_contents)).await;
                None
            }
        };

        // now send the contents to the extra notifiers
        let notifiers = self.added_file_notifiers.lock().clone();
        for notifier in notifiers {
            // added notifiers don't do anything with the snapshot, so just notify
            notifier.notify(Arc::clone(&wal_contents)).await;
        }

        // send all the responses back to clients
        for response in responses {
            let _ = response.send(WriteResult::Success(()));
        }

        snapshot_response
    }

    fn load_existing_wal_file_paths(
        &self,
        last_wal_sequence_number: Option<WalFileSequenceNumber>,
        all_wal_file_paths: &[Path],
    ) -> Vec<Path> {
        // if we have a last wal path from persisted snapshots, we don't need to load the wal files
        // that came before it:
        all_wal_file_paths
            .iter()
            .filter(|path| {
                if let Some(last_wal_sequence_number) = last_wal_sequence_number {
                    let last_wal_path =
                        wal_path(&self.host_identifier_prefix, last_wal_sequence_number);
                    debug!(
                        ?path,
                        ?last_wal_path,
                        ">>> path and last_wal_path check when replaying"
                    );
                    *path >= &last_wal_path
                } else {
                    true
                }
            })
            .cloned()
            .collect()
    }

    async fn remove_snapshot_wal_files(
        &self,
        snapshot_details: SnapshotDetails,
        snapshot_permit: OwnedSemaphorePermit,
    ) {
        let (oldest, last, curr_num_files) = {
            let (oldest, last) = self.wal_remover.lock().get_current_state();
            let curr_num_files = last - oldest;
            (oldest, last, curr_num_files)
        };
        debug!(
            ?oldest,
            ?last,
            ?curr_num_files,
            ">>> checking num wal files to delete"
        );

        if curr_num_files > self.snapshotted_wal_files_to_keep {
            let num_files_to_delete = curr_num_files - self.snapshotted_wal_files_to_keep;
            let last_to_delete = oldest + num_files_to_delete;

            debug!(
                num_files_to_keep = ?self.snapshotted_wal_files_to_keep,
                ?curr_num_files,
                ?num_files_to_delete,
                ?last_to_delete,
                ">>> more wal files than num files to keep around"
            );

            for idx in oldest..last_to_delete {
                let path = wal_path(
                    &self.host_identifier_prefix,
                    WalFileSequenceNumber::new(idx),
                );
                debug!(?path, ">>> deleting wal file");

                loop {
                    match self.object_store.delete(&path).await {
                        Ok(_) => break,
                        Err(object_store::Error::Generic { store, source }) => {
                            error!(%store, %source, "error deleting wal file");
                            // hopefully just a temporary error, keep trying until we succeed
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        Err(e) => {
                            // this must be configuration or file not there error or something else,
                            // log it and move on
                            error!(%e, "error deleting wal file");
                            break;
                        }
                    }
                }
            }

            {
                self.wal_remover.lock().update_state(
                    last_to_delete,
                    snapshot_details.last_wal_sequence_number.as_u64(),
                );
            }
        } else {
            {
                self.wal_remover
                    .lock()
                    .update_last_wal_num(snapshot_details.last_wal_sequence_number);
            }
        }

        // release the permit so the next snapshot can be run when the time comes
        drop(snapshot_permit);
    }
}

fn oldest_wal_file_num(all_wal_file_paths: &[Path]) -> Option<WalFileSequenceNumber> {
    let file_name = all_wal_file_paths.first()?.filename()?;
    WalFileSequenceNumber::from_str(file_name).ok()
}

async fn load_all_wal_file_paths(
    object_store: Arc<dyn ObjectStore>,
    host_identifier_prefix: String,
) -> Result<Vec<Path>, crate::Error> {
    let mut paths = Vec::new();
    let mut offset: Option<Path> = None;
    let path = Path::from(format!("{host}/wal", host = host_identifier_prefix));
    loop {
        let mut listing = if let Some(offset) = offset {
            object_store.list_with_offset(Some(&path), &offset)
        } else {
            object_store.list(Some(&path))
        };
        let path_count = paths.len();

        while let Some(item) = listing.next().await {
            paths.push(item?.location);
        }

        if path_count == paths.len() {
            paths.sort();
            break;
        }

        paths.sort();
        offset = Some(paths.last().unwrap().clone())
    }
    Ok(paths)
}

#[async_trait::async_trait]
impl Wal for WalObjectStore {
    async fn buffer_op_unconfirmed(&self, op: WalOp) -> crate::Result<(), crate::Error> {
        self.buffer_op_unconfirmed(op).await
    }

    async fn write_ops(&self, ops: Vec<WalOp>) -> crate::Result<(), crate::Error> {
        self.write_ops(ops).await
    }

    async fn flush_buffer(
        &self,
    ) -> Option<(
        oneshot::Receiver<SnapshotDetails>,
        SnapshotDetails,
        OwnedSemaphorePermit,
    )> {
        self.flush_buffer(false).await
    }

    async fn force_flush_buffer(
        &self,
    ) -> Option<(
        oneshot::Receiver<SnapshotDetails>,
        SnapshotDetails,
        OwnedSemaphorePermit,
    )> {
        self.flush_buffer(true).await
    }

    async fn cleanup_snapshot(
        &self,
        snapshot_details: SnapshotDetails,
        snapshot_permit: OwnedSemaphorePermit,
    ) {
        self.remove_snapshot_wal_files(snapshot_details, snapshot_permit)
            .await
    }

    async fn last_wal_sequence_number(&self) -> WalFileSequenceNumber {
        self.flush_buffer
            .lock()
            .await
            .snapshot_tracker
            .last_wal_sequence_number()
    }

    async fn last_snapshot_sequence_number(&self) -> SnapshotSequenceNumber {
        self.flush_buffer
            .lock()
            .await
            .snapshot_tracker
            .last_snapshot_sequence_number()
    }

    async fn shutdown(&self) {
        self.shutdown().await
    }

    fn add_file_notifier(&self, notifier: Arc<dyn WalFileNotifier>) {
        self.added_file_notifiers.lock().push(notifier);
    }
}

#[derive(Debug)]
struct FlushBuffer {
    time_provider: Arc<dyn TimeProvider>,
    wal_buffer: WalBuffer,
    snapshot_tracker: SnapshotTracker,
    snapshot_semaphore: Arc<Semaphore>,
}

impl FlushBuffer {
    fn new(
        time_provider: Arc<dyn TimeProvider>,
        wal_buffer: WalBuffer,
        snapshot_tracker: SnapshotTracker,
    ) -> Self {
        Self {
            time_provider,
            wal_buffer,
            snapshot_tracker,
            snapshot_semaphore: Arc::new(Semaphore::new(1)),
        }
    }

    fn replay_wal_period(&mut self, wal_period: WalPeriod) {
        self.wal_buffer.wal_file_sequence_number = wal_period.wal_file_number.next();
        self.snapshot_tracker.add_wal_period(wal_period);
    }

    /// Converts the wal_buffer into contents and resets it. Returns the channels waiting for
    /// responses. If a snapshot should occur with this flush, a semaphore permit is also returned.
    ///
    /// There are 4 possible scenarios
    /// wal buffer | force_snapshot | outcome
    ///  empty     | true           | noop / snapshot**
    ///  empty     | false          | should not happen (guarded at call site)
    ///  not empty | true           | snapshot**
    ///  not empty | false          | may snapshot (depends on wal periods in tracker)
    ///
    /// snapshot**: These snapshots in theory can still not create snapshot
    /// details, because tracker may not have wal periods. But in practice
    /// force_snapshot will be called when queryable buffer is
    /// full and that means the wal periods should be present in snapshot
    /// tracker.
    async fn flush_buffer_into_contents_and_responses(
        &mut self,
        force_snapshot: bool,
    ) -> (
        WalContents,
        Vec<oneshot::Sender<WriteResult>>,
        Option<(SnapshotDetails, OwnedSemaphorePermit)>,
    ) {
        if force_snapshot && self.wal_buffer.is_empty() {
            self.wal_buffer.add_no_op();
        }
        let (mut wal_contents, responses) = self.flush_buffer_with_responses(force_snapshot);

        self.snapshot_tracker.add_wal_period(WalPeriod {
            wal_file_number: wal_contents.wal_file_number,
            min_time: Timestamp::new(wal_contents.min_timestamp_ns),
            max_time: Timestamp::new(wal_contents.max_timestamp_ns),
        });
        let snapshot_details = self.snapshot_tracker.snapshot(force_snapshot);
        let snapshot = match snapshot_details {
            Some(snapshot_details) => {
                wal_contents.snapshot = Some(snapshot_details);
                Some((snapshot_details, self.acquire_snapshot_permit().await))
            }
            None => None,
        };

        (wal_contents, responses, snapshot)
    }

    async fn acquire_snapshot_permit(&self) -> OwnedSemaphorePermit {
        let snapshot_semaphore = Arc::clone(&self.snapshot_semaphore);
        snapshot_semaphore
            .acquire_owned()
            .await
            .expect("snapshot semaphore permit")
    }

    fn flush_buffer_with_responses(
        &mut self,
        force_snapshot: bool,
    ) -> (WalContents, Vec<oneshot::Sender<WriteResult>>) {
        // swap out the filled buffer with a new one
        let mut new_buffer = WalBuffer {
            time_provider: Arc::clone(&self.time_provider),
            is_shutdown: false,
            wal_file_sequence_number: self.wal_buffer.wal_file_sequence_number.next(),
            op_limit: self.wal_buffer.op_limit,
            op_count: 0,
            database_to_write_batch: Default::default(),
            write_op_responses: vec![],
            catalog_batches: vec![],
            no_op: None,
        };
        std::mem::swap(&mut self.wal_buffer, &mut new_buffer);

        new_buffer.into_wal_contents_and_responses(force_snapshot)
    }

    async fn flush_buffer_with_failure(&mut self, error: WriteResult) {
        let (_, responses) = self.flush_buffer_with_responses(false);
        for response in responses {
            let _ = response.send(error.clone());
        }
    }
}

#[derive(Debug)]
struct WalBuffer {
    time_provider: Arc<dyn TimeProvider>,
    is_shutdown: bool,
    wal_file_sequence_number: WalFileSequenceNumber,
    op_limit: usize,
    op_count: usize,
    database_to_write_batch: HashMap<Arc<str>, WriteBatch>,
    catalog_batches: Vec<OrderedCatalogBatch>,
    write_op_responses: Vec<oneshot::Sender<WriteResult>>,
    no_op: Option<i64>,
}

impl WalBuffer {
    fn is_empty(&self) -> bool {
        self.database_to_write_batch.is_empty()
            && self.catalog_batches.is_empty()
            && self.no_op.is_none()
    }

    fn add_no_op(&mut self) {
        self.no_op = Some(self.time_provider.now().timestamp_nanos());
    }
}

// Writes should only fail if the underlying WAL throws an error. They are validated before they
// are buffered. The WAL should continuously retry the write until it succeeds. But if a timeout
// passes, we can use this to pass the object store error back to the client.
#[derive(Debug, Clone)]
pub enum WriteResult {
    Success(()),
    Error(String),
}

impl WalBuffer {
    fn buffer_op_unconfirmed(&mut self, op: WalOp) -> crate::Result<(), crate::Error> {
        if self.op_count >= self.op_limit {
            return Err(crate::Error::BufferFull(self.op_count));
        }

        match op {
            WalOp::Write(new_write_batch) => {
                let db_name = Arc::clone(&new_write_batch.database_name);

                // insert the database write batch or add to existing
                let write_batch =
                    self.database_to_write_batch
                        .entry(db_name)
                        .or_insert_with(|| WriteBatch {
                            database_id: new_write_batch.database_id,
                            database_name: new_write_batch.database_name,
                            table_chunks: Default::default(),
                            min_time_ns: i64::MAX,
                            max_time_ns: i64::MIN,
                        });
                write_batch.add_write_batch(
                    new_write_batch.table_chunks,
                    new_write_batch.min_time_ns,
                    new_write_batch.max_time_ns,
                );
            }
            WalOp::Catalog(catalog_batch) => {
                self.catalog_batches.push(catalog_batch);
            }
            WalOp::Noop(_) => {}
        }

        Ok(())
    }

    fn buffer_ops_with_response(
        &mut self,
        ops: Vec<WalOp>,
        response: oneshot::Sender<WriteResult>,
    ) -> crate::Result<(), crate::Error> {
        self.write_op_responses.push(response);
        for op in ops {
            self.buffer_op_unconfirmed(op)?;
        }

        Ok(())
    }

    fn into_wal_contents_and_responses(
        self,
        forced_snapshot: bool,
    ) -> (WalContents, Vec<oneshot::Sender<WriteResult>>) {
        // get the min and max data timestamps for writes into this wal file
        let mut min_timestamp_ns = i64::MAX;
        let mut max_timestamp_ns = i64::MIN;

        for write_batch in self.database_to_write_batch.values() {
            min_timestamp_ns = min_timestamp_ns.min(write_batch.min_time_ns);
            max_timestamp_ns = max_timestamp_ns.max(write_batch.max_time_ns);
        }

        for catalog_batch in &self.catalog_batches {
            min_timestamp_ns = min_timestamp_ns.min(catalog_batch.catalog.time_ns);
            max_timestamp_ns = max_timestamp_ns.max(catalog_batch.catalog.time_ns);
        }

        // have the catalog ops come before any writes in ordering
        let mut ops =
            Vec::with_capacity(self.database_to_write_batch.len() + self.catalog_batches.len());

        ops.extend(self.catalog_batches.into_iter().map(WalOp::Catalog));
        ops.extend(self.database_to_write_batch.into_values().map(WalOp::Write));

        ops.sort();

        // We are writing a noop so that wal buffer which is empty can still trigger a forced
        // snapshot and write that noop and snapshot details to a wal file
        if ops.is_empty() && forced_snapshot && self.no_op.is_some() {
            let time = self.no_op.unwrap();
            ops.push(WalOp::Noop(NoopDetails { timestamp_ns: time }));
            // when we leave behind wals, these noops need min/max time as snapshot tracker falls
            // over without them when adding wal period by default
            min_timestamp_ns = time;
            max_timestamp_ns = time + 1;
        }

        (
            WalContents {
                persist_timestamp_ms: self.time_provider.now().timestamp_millis(),
                min_timestamp_ns,
                max_timestamp_ns,
                wal_file_number: self.wal_file_sequence_number,
                ops,
                snapshot: None,
            },
            self.write_op_responses,
        )
    }
}

pub fn wal_path(host_identifier_prefix: &str, wal_file_number: WalFileSequenceNumber) -> Path {
    Path::from(format!(
        "{host_identifier_prefix}/wal/{:011}.wal",
        wal_file_number.0
    ))
}

impl<'a> TryFrom<&'a Path> for WalFileSequenceNumber {
    type Error = crate::Error;

    fn try_from(path: &'a Path) -> Result<Self, Self::Error> {
        let parts: Vec<PathPart<'_>> = path.parts().collect();
        if parts.len() != 3 {
            return Err(crate::Error::InvalidWalFilePath);
        }
        parts[2]
            .as_ref()
            .trim_end_matches(".wal")
            .parse()
            .map_err(|_| crate::Error::InvalidWalFilePath)
    }
}

#[derive(Debug)]
struct WalFileRemover {
    oldest_wal_file: Option<WalFileSequenceNumber>,
    last_snapshotted_wal_sequence_number: Option<WalFileSequenceNumber>,
}

impl WalFileRemover {
    fn get_current_state(&self) -> (u64, u64) {
        (
            self.oldest_wal_file.map(|num| num.as_u64()).unwrap_or(0),
            self.last_snapshotted_wal_sequence_number
                .map(|num| num.as_u64())
                .unwrap_or(0),
        )
    }

    fn update_last_wal_num(&mut self, last_wal: WalFileSequenceNumber) {
        self.last_snapshotted_wal_sequence_number.replace(last_wal);
    }

    fn update_state(&mut self, oldest: u64, last: u64) {
        self.oldest_wal_file = Some(WalFileSequenceNumber::new(oldest));
        self.last_snapshotted_wal_sequence_number = Some(WalFileSequenceNumber::new(last));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        create, Field, FieldData, Gen1Duration, Row, SnapshotSequenceNumber, TableChunk,
        TableChunks,
    };
    use async_trait::async_trait;
    use indexmap::IndexMap;
    use influxdb3_id::{ColumnId, DbId, TableId};
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;
    use std::any::Any;
    use tokio::sync::oneshot::Receiver;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_flush_delete_and_load() {
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
        let wal_config = WalConfig {
            max_write_buffer_size: 100,
            flush_interval: Duration::from_secs(1),
            snapshot_size: 2,
            gen1_duration: Gen1Duration::new_1m(),
        };
        let paths = vec![];
        let wal = WalObjectStore::new_without_replay(
            Arc::clone(&time_provider),
            Arc::clone(&object_store),
            "my_host",
            Arc::clone(&notifier),
            wal_config,
            None,
            None,
            &paths,
            1,
        );

        let db_name: Arc<str> = "db1".into();

        let op1 = WalOp::Write(WriteBatch {
            database_id: DbId::from(0),
            database_name: Arc::clone(&db_name),
            table_chunks: IndexMap::from([(
                TableId::from(0),
                TableChunks {
                    min_time: 1,
                    max_time: 3,
                    chunk_time_to_chunk: HashMap::from([(
                        0,
                        TableChunk {
                            rows: vec![
                                Row {
                                    time: 1,
                                    fields: vec![
                                        Field {
                                            id: ColumnId::from(0),
                                            value: FieldData::Integer(1),
                                        },
                                        Field {
                                            id: ColumnId::from(1),
                                            value: FieldData::Timestamp(1),
                                        },
                                    ],
                                },
                                Row {
                                    time: 3,
                                    fields: vec![
                                        Field {
                                            id: ColumnId::from(0),
                                            value: FieldData::Integer(2),
                                        },
                                        Field {
                                            id: ColumnId::from(1),
                                            value: FieldData::Timestamp(3),
                                        },
                                    ],
                                },
                            ],
                        },
                    )]),
                },
            )])
            .into(),
            min_time_ns: 1,
            max_time_ns: 3,
        });
        wal.buffer_op_unconfirmed(op1.clone()).await.unwrap();

        let op2 = WalOp::Write(WriteBatch {
            database_id: DbId::from(0),
            database_name: Arc::clone(&db_name),
            table_chunks: IndexMap::from([(
                TableId::from(0),
                TableChunks {
                    min_time: 12,
                    max_time: 12,
                    chunk_time_to_chunk: HashMap::from([(
                        0,
                        TableChunk {
                            rows: vec![Row {
                                time: 12,
                                fields: vec![
                                    Field {
                                        id: ColumnId::from(0),
                                        value: FieldData::Integer(3),
                                    },
                                    Field {
                                        id: ColumnId::from(1),
                                        value: FieldData::Timestamp(62_000000000),
                                    },
                                ],
                            }],
                        },
                    )]),
                },
            )])
            .into(),
            min_time_ns: 62_000000000,
            max_time_ns: 62_000000000,
        });
        wal.buffer_op_unconfirmed(op2.clone()).await.unwrap();

        // create wal file 1
        let ret = wal.flush_buffer(false).await;
        assert!(ret.is_none());
        let file_1_contents = create::wal_contents(
            (1, 62_000_000_000, 1),
            [WalOp::Write(WriteBatch {
                database_id: DbId::from(0),
                database_name: "db1".into(),
                table_chunks: IndexMap::from([(
                    TableId::from(0),
                    TableChunks {
                        min_time: 1,
                        max_time: 12,
                        chunk_time_to_chunk: HashMap::from([(
                            0,
                            TableChunk {
                                rows: vec![
                                    Row {
                                        time: 1,
                                        fields: vec![
                                            Field {
                                                id: ColumnId::from(0),
                                                value: FieldData::Integer(1),
                                            },
                                            Field {
                                                id: ColumnId::from(1),
                                                value: FieldData::Timestamp(1),
                                            },
                                        ],
                                    },
                                    Row {
                                        time: 3,
                                        fields: vec![
                                            Field {
                                                id: ColumnId::from(0),
                                                value: FieldData::Integer(2),
                                            },
                                            Field {
                                                id: ColumnId::from(1),
                                                value: FieldData::Timestamp(3),
                                            },
                                        ],
                                    },
                                    Row {
                                        time: 12,
                                        fields: vec![
                                            Field {
                                                id: ColumnId::from(0),
                                                value: FieldData::Integer(3),
                                            },
                                            Field {
                                                id: ColumnId::from(1),
                                                value: FieldData::Timestamp(62_000000000),
                                            },
                                        ],
                                    },
                                ],
                            },
                        )]),
                    },
                )])
                .into(),
                min_time_ns: 1,
                max_time_ns: 62_000000000,
            })],
        );

        // create wal file 2
        wal.buffer_op_unconfirmed(op2.clone()).await.unwrap();
        assert!(wal.flush_buffer(false).await.is_none());

        let file_2_contents = create::wal_contents(
            (62_000_000_000, 62_000_000_000, 2),
            [WalOp::Write(WriteBatch {
                database_id: DbId::from(0),
                database_name: "db1".into(),
                table_chunks: IndexMap::from([(
                    TableId::from(0),
                    TableChunks {
                        min_time: 12,
                        max_time: 12,
                        chunk_time_to_chunk: HashMap::from([(
                            0,
                            TableChunk {
                                rows: vec![Row {
                                    time: 12,
                                    fields: vec![
                                        Field {
                                            id: ColumnId::from(0),
                                            value: FieldData::Integer(3),
                                        },
                                        Field {
                                            id: ColumnId::from(1),
                                            value: FieldData::Timestamp(62_000000000),
                                        },
                                    ],
                                }],
                            },
                        )]),
                    },
                )])
                .into(),
                min_time_ns: 62_000000000,
                max_time_ns: 62_000000000,
            })],
        );

        // before we trigger a snapshot, test replay with a new wal and notifier
        let replay_notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
        let paths = vec![];
        let replay_wal = WalObjectStore::new_without_replay(
            Arc::clone(&time_provider),
            Arc::clone(&object_store),
            "my_host",
            Arc::clone(&replay_notifier),
            wal_config,
            None,
            None,
            &paths,
            1,
        );
        assert_eq!(
            replay_wal.load_existing_wal_file_paths(
                None,
                &[
                    Path::from("my_host/wal/00000000001.wal"),
                    Path::from("my_host/wal/00000000002.wal")
                ]
            ),
            vec![
                Path::from("my_host/wal/00000000001.wal"),
                Path::from("my_host/wal/00000000002.wal")
            ]
        );
        replay_wal
            .replay(
                None,
                &[
                    Path::from("my_host/wal/00000000001.wal"),
                    Path::from("my_host/wal/00000000002.wal"),
                ],
            )
            .await
            .unwrap();
        let replay_notifier = replay_notifier
            .as_any()
            .downcast_ref::<TestNotifier>()
            .unwrap();

        {
            let notified_writes = replay_notifier.notified_writes.lock();
            let notified_refs = notified_writes
                .iter()
                .map(|x| x.as_ref())
                .collect::<Vec<_>>();
            assert_eq!(notified_refs, vec![&file_1_contents, &file_2_contents]);
        }
        assert!(replay_notifier.snapshot_details.lock().is_none());

        // create wal file 3, which should trigger a snapshot
        let op3 = WalOp::Write(WriteBatch {
            database_id: DbId::from(0),
            database_name: Arc::clone(&db_name),
            table_chunks: IndexMap::from([(
                TableId::from(0),
                TableChunks {
                    min_time: 26,
                    max_time: 26,
                    chunk_time_to_chunk: HashMap::from([(
                        0,
                        TableChunk {
                            rows: vec![Row {
                                time: 26,
                                fields: vec![
                                    Field {
                                        id: ColumnId::from(0),
                                        value: FieldData::Integer(3),
                                    },
                                    Field {
                                        id: ColumnId::from(1),
                                        value: FieldData::Timestamp(128_000000000),
                                    },
                                ],
                            }],
                        },
                    )]),
                },
            )])
            .into(),
            min_time_ns: 128_000000000,
            max_time_ns: 128_000000000,
        });
        wal.buffer_op_unconfirmed(op3.clone()).await.unwrap();

        let (snapshot_done, snapshot_info, snapshot_permit) =
            wal.flush_buffer(false).await.unwrap();
        let expected_info = SnapshotDetails {
            snapshot_sequence_number: SnapshotSequenceNumber::new(1),
            end_time_marker: 120000000000,
            first_wal_sequence_number: WalFileSequenceNumber(1),
            last_wal_sequence_number: WalFileSequenceNumber(2),
            forced: false,
        };
        assert_eq!(expected_info, snapshot_info);
        snapshot_done.await.unwrap();

        let file_3_contents = create::wal_contents_with_snapshot(
            (128_000_000_000, 128_000_000_000, 3),
            [WalOp::Write(WriteBatch {
                database_id: DbId::from(0),
                database_name: "db1".into(),
                table_chunks: IndexMap::from([(
                    TableId::from(0),
                    TableChunks {
                        min_time: 26,
                        max_time: 26,
                        chunk_time_to_chunk: HashMap::from([(
                            0,
                            TableChunk {
                                rows: vec![Row {
                                    time: 26,
                                    fields: vec![
                                        Field {
                                            id: ColumnId::from(0),
                                            value: FieldData::Integer(3),
                                        },
                                        Field {
                                            id: ColumnId::from(1),
                                            value: FieldData::Timestamp(128_000000000),
                                        },
                                    ],
                                }],
                            },
                        )]),
                    },
                )])
                .into(),
                min_time_ns: 128_000000000,
                max_time_ns: 128_000000000,
            })],
            SnapshotDetails {
                snapshot_sequence_number: SnapshotSequenceNumber::new(1),
                end_time_marker: 120_000000000,
                first_wal_sequence_number: WalFileSequenceNumber(1),
                last_wal_sequence_number: WalFileSequenceNumber(2),
                forced: false,
            },
        );

        let notifier = notifier.as_any().downcast_ref::<TestNotifier>().unwrap();

        {
            let notified_writes = notifier.notified_writes.lock();
            let notified_refs = notified_writes
                .iter()
                .map(|x| x.as_ref())
                .collect::<Vec<_>>();
            let expected_writes = vec![&file_1_contents, &file_2_contents, &file_3_contents];
            assert_eq!(notified_refs, expected_writes);
            let details = notifier.snapshot_details.lock();
            assert_eq!(details.unwrap(), expected_info);
        }

        wal.remove_snapshot_wal_files(snapshot_info, snapshot_permit)
            .await;

        // test that replay now only has file 3
        let replay_notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
        let paths = vec![];
        let replay_wal = WalObjectStore::new_without_replay(
            Arc::clone(&time_provider),
            object_store,
            "my_host",
            Arc::clone(&replay_notifier),
            wal_config,
            None,
            None,
            &paths,
            1,
        );
        assert_eq!(
            replay_wal
                .load_existing_wal_file_paths(None, &[Path::from("my_host/wal/00000000003.wal")]),
            vec![Path::from("my_host/wal/00000000003.wal")]
        );
        replay_wal
            .replay(None, &[Path::from("my_host/wal/00000000003.wal")])
            .await
            .unwrap();
        let replay_notifier = replay_notifier
            .as_any()
            .downcast_ref::<TestNotifier>()
            .unwrap();
        let notified_writes = replay_notifier.notified_writes.lock();
        let notified_refs = notified_writes
            .iter()
            .map(|x| x.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(notified_refs, vec![&file_3_contents]);
        let snapshot_details = replay_notifier.snapshot_details.lock();
        assert_eq!(*snapshot_details, file_3_contents.snapshot);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_for_empty_buffer_skips_notify() {
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
        let wal_config = WalConfig {
            max_write_buffer_size: 100,
            flush_interval: Duration::from_secs(1),
            snapshot_size: 2,
            gen1_duration: Gen1Duration::new_1m(),
        };
        let paths = vec![];
        let wal = WalObjectStore::new_without_replay(
            time_provider,
            Arc::clone(&object_store),
            "my_host",
            Arc::clone(&notifier),
            wal_config,
            None,
            None,
            &paths,
            10,
        );

        assert!(wal.flush_buffer(false).await.is_none());
        let notifier = notifier.as_any().downcast_ref::<TestNotifier>().unwrap();
        assert!(notifier.notified_writes.lock().is_empty());

        // make sure no wal file was written
        assert!(object_store.list(None).next().await.is_none());
    }

    #[test]
    fn persist_time_is_tracked() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let wal_buffer = WalBuffer {
            time_provider: Arc::clone(&time_provider) as _,
            is_shutdown: false,
            wal_file_sequence_number: WalFileSequenceNumber(0),
            op_limit: 10,
            op_count: 0,
            database_to_write_batch: Default::default(),
            catalog_batches: vec![],
            write_op_responses: vec![],
            no_op: None,
        };
        time_provider.set(Time::from_timestamp_millis(1234).unwrap());
        let (wal_contents, _) = wal_buffer.into_wal_contents_and_responses(false);
        assert_eq!(1234, wal_contents.persist_timestamp_ms);
    }

    #[test_log::test(tokio::test)]
    async fn test_flush_buffer_contents() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let clone = Arc::clone(&time_provider) as _;
        let snapshot_tracker = SnapshotTracker::new(1, Gen1Duration::new_1m(), None);

        let mut flush_buffer = FlushBuffer::new(
            clone,
            WalBuffer {
                time_provider: Arc::clone(&time_provider) as _,
                is_shutdown: false,
                wal_file_sequence_number: WalFileSequenceNumber(0),
                op_limit: 10,
                op_count: 0,
                database_to_write_batch: Default::default(),
                catalog_batches: vec![],
                write_op_responses: vec![],
                no_op: None,
            },
            snapshot_tracker,
        );

        flush_buffer
            .wal_buffer
            .buffer_op_unconfirmed(WalOp::Write(WriteBatch {
                database_id: DbId::from(0),
                database_name: "db1".into(),
                table_chunks: IndexMap::from([(
                    TableId::from(0),
                    TableChunks {
                        min_time: 26,
                        max_time: 26,
                        chunk_time_to_chunk: HashMap::from([(
                            0,
                            TableChunk {
                                rows: vec![Row {
                                    time: 26,
                                    fields: vec![
                                        Field {
                                            id: ColumnId::from(0),
                                            value: FieldData::Integer(3),
                                        },
                                        Field {
                                            id: ColumnId::from(1),
                                            value: FieldData::Timestamp(128_000000000),
                                        },
                                    ],
                                }],
                            },
                        )]),
                    },
                )])
                .into(),
                min_time_ns: 128_000000000,
                max_time_ns: 148_000000000,
            }))
            .unwrap();

        // wal buffer not empty, force snapshot set => snapshot (empty wal buffer
        // content then snapshot immediately)
        let (_, _, maybe_snapshot) = flush_buffer
            .flush_buffer_into_contents_and_responses(true)
            .await;
        let (snapshot_details, _) = maybe_snapshot.unwrap();

        assert_eq!(
            SnapshotDetails {
                snapshot_sequence_number: SnapshotSequenceNumber::new(1),
                end_time_marker: 180_000000000,
                first_wal_sequence_number: WalFileSequenceNumber(0),
                last_wal_sequence_number: WalFileSequenceNumber(0),
                forced: true,
            },
            snapshot_details
        );
        assert_eq!(0, flush_buffer.snapshot_tracker.num_wal_periods());
        assert!(flush_buffer.wal_buffer.is_empty());

        // wal buffer not empty and force snapshot not set. should
        // not snapshot
        flush_buffer
            .wal_buffer
            .buffer_op_unconfirmed(WalOp::Write(WriteBatch {
                database_id: DbId::from(0),
                database_name: "db1".into(),
                table_chunks: IndexMap::from([(
                    TableId::from(0),
                    TableChunks {
                        min_time: 26,
                        max_time: 26,
                        chunk_time_to_chunk: HashMap::from([(
                            0,
                            TableChunk {
                                rows: vec![Row {
                                    time: 26,
                                    fields: vec![
                                        Field {
                                            id: ColumnId::from(0),
                                            value: FieldData::Integer(3),
                                        },
                                        Field {
                                            id: ColumnId::from(1),
                                            value: FieldData::Timestamp(128_000000000),
                                        },
                                    ],
                                }],
                            },
                        )]),
                    },
                )])
                .into(),
                min_time_ns: 128_000000000,
                max_time_ns: 148_000000000,
            }))
            .unwrap();

        let (wal_contents, _, maybe_snapshot) = flush_buffer
            .flush_buffer_into_contents_and_responses(false)
            .await;

        assert_eq!(1, wal_contents.ops.len());
        assert_eq!(1, flush_buffer.snapshot_tracker.num_wal_periods());
        assert!(flush_buffer.wal_buffer.is_empty());
        assert!(maybe_snapshot.is_none());

        // wal buffer empty and force snapshot set. should snapshot
        // if it has wal periods
        assert!(flush_buffer.wal_buffer.is_empty());

        let (wal_contents, _, maybe_snapshot) = flush_buffer
            .flush_buffer_into_contents_and_responses(true)
            .await;

        assert_eq!(1, wal_contents.ops.len());
        assert!(matches!(wal_contents.ops.first().unwrap(), WalOp::Noop(_)));
        assert_eq!(0, flush_buffer.snapshot_tracker.num_wal_periods());
        assert!(flush_buffer.wal_buffer.is_empty());
        let (snapshot_details, _) = maybe_snapshot.unwrap();
        assert_eq!(
            SnapshotDetails {
                snapshot_sequence_number: SnapshotSequenceNumber::new(2),
                end_time_marker: 180_000000000,
                first_wal_sequence_number: WalFileSequenceNumber(1),
                last_wal_sequence_number: WalFileSequenceNumber(2),
                forced: true,
            },
            snapshot_details
        );
    }

    #[derive(Debug, Default)]
    struct TestNotifier {
        notified_writes: parking_lot::Mutex<Vec<Arc<WalContents>>>,
        snapshot_details: parking_lot::Mutex<Option<SnapshotDetails>>,
    }

    #[async_trait]
    impl WalFileNotifier for TestNotifier {
        async fn notify(&self, write: Arc<WalContents>) {
            self.notified_writes.lock().push(write);
        }

        async fn notify_and_snapshot(
            &self,
            write: Arc<WalContents>,
            snapshot_details: SnapshotDetails,
        ) -> Receiver<SnapshotDetails> {
            self.notified_writes.lock().push(write);
            *self.snapshot_details.lock() = Some(snapshot_details);

            let (sender, receiver) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                sender.send(snapshot_details).unwrap();
            });

            receiver
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }
}
