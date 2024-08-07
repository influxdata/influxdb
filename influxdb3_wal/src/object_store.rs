use crate::serialize::verify_file_type_and_deserialize;
use crate::snapshot_tracker::{SnapshotInfo, SnapshotTracker, WalPeriod};
use crate::{
    background_wal_flush, CatalogBatch, SnapshotDetails, Wal, WalConfig, WalContents,
    WalFileNotifier, WalFileSequenceNumber, WalOp, WriteBatch,
};
use bytes::Bytes;
use data_types::Timestamp;
use futures_util::stream::StreamExt;
use hashbrown::HashMap;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use observability_deps::tracing::{debug, error, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
pub struct WalObjectStore {
    object_store: Arc<dyn ObjectStore>,
    host_identifier_prefix: String,
    file_notifier: Arc<dyn WalFileNotifier>,
    /// Buffered wal ops go in here along with the state to track when to snapshot
    flush_buffer: Mutex<FlushBuffer>,
}

impl WalObjectStore {
    /// Creates a new WAL. This will replay files into the notifier and trigger any snapshots that
    /// exist in the WAL files that haven't been cleaned up yet.
    pub async fn new(
        object_store: Arc<dyn ObjectStore>,
        host_identifier_prefix: impl Into<String> + Send,
        file_notifier: Arc<dyn WalFileNotifier>,
        config: WalConfig,
        last_snapshot_wal_sequence: Option<WalFileSequenceNumber>,
    ) -> Result<Arc<Self>, crate::Error> {
        let flush_interval = config.flush_interval;
        let wal = Self::new_without_replay(
            object_store,
            host_identifier_prefix,
            file_notifier,
            config,
            last_snapshot_wal_sequence,
        );

        wal.replay().await?;
        let wal = Arc::new(wal);
        background_wal_flush(Arc::clone(&wal), flush_interval);

        Ok(wal)
    }

    fn new_without_replay(
        object_store: Arc<dyn ObjectStore>,
        host_identifier_prefix: impl Into<String>,
        file_notifier: Arc<dyn WalFileNotifier>,
        config: WalConfig,
        last_snapshot_wal_sequence: Option<WalFileSequenceNumber>,
    ) -> Self {
        let wal_file_sequence_number = last_snapshot_wal_sequence.unwrap_or_default().next();
        Self {
            object_store,
            host_identifier_prefix: host_identifier_prefix.into(),
            file_notifier,
            flush_buffer: Mutex::new(FlushBuffer::new(
                WalBuffer {
                    is_shutdown: false,
                    wal_file_sequence_number,
                    op_limit: config.max_write_buffer_size,
                    op_count: 0,
                    database_to_write_batch: Default::default(),
                    catalog_batches: vec![],
                    write_op_responses: vec![],
                },
                SnapshotTracker::new(config.snapshot_size, config.level_0_duration),
            )),
        }
    }

    /// Loads the WAL files in order from object store, calling the file notifier on each one and
    /// populating the snapshot tracker with the WAL periods.
    pub async fn replay(&self) -> crate::Result<()> {
        let paths = self.load_existing_wal_file_paths().await?;

        for path in paths {
            let file_bytes = self.object_store.get(&path).await?.bytes().await?;
            let wal_contents = verify_file_type_and_deserialize(file_bytes)?;

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

            match wal_contents.snapshot {
                None => self.file_notifier.notify(wal_contents),
                Some(snapshot_details) => {
                    let snapshot_info = {
                        let mut buffer = self.flush_buffer.lock().await;

                        match buffer.snapshot_tracker.snapshot() {
                            None => None,
                            Some(info) => {
                                let semaphore = Arc::clone(&buffer.snapshot_semaphore);
                                let permit = semaphore.acquire_owned().await.unwrap();

                                Some((info, permit))
                            }
                        }
                    };

                    let snapshot_done = self
                        .file_notifier
                        .notify_and_snapshot(wal_contents, snapshot_details)
                        .await;
                    let details = snapshot_done.await.unwrap();
                    assert_eq!(snapshot_details, details);

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
    pub async fn shutdown(&self) {
        // stop accepting writes
        self.flush_buffer.lock().await.wal_buffer.is_shutdown = true;

        // do the flush and wait for the snapshot if that's running
        if let Some((snapshot_done, snapshot_info, snapshot_permit)) = self.flush_buffer().await {
            let snapshot_details = snapshot_done.await.expect("snapshot should complete");
            assert_eq!(snapshot_info.snapshot_details, snapshot_details);
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
    ) -> Option<(
        oneshot::Receiver<SnapshotDetails>,
        SnapshotInfo,
        OwnedSemaphorePermit,
    )> {
        let (wal_contents, responses, snapshot) = {
            let mut flush_buffer = self.flush_buffer.lock().await;
            if flush_buffer.wal_buffer.is_empty() {
                return None;
            }
            flush_buffer
                .flush_buffer_into_contents_and_responses()
                .await
        };

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

        // now that we've persisted this latest notify and start the snapshot, if set
        let snapshot_response = match wal_contents.snapshot {
            Some(snapshot_details) => {
                info!(?snapshot_details, "snapshotting wal");
                let snapshot_done = self
                    .file_notifier
                    .notify_and_snapshot(wal_contents, snapshot_details)
                    .await;
                let (snapshot_info, snapshot_permit) =
                    snapshot.expect("snapshot should be set when snapshot details are set");
                Some((snapshot_done, snapshot_info, snapshot_permit))
            }
            None => {
                debug!(
                    "notify sent to buffer for wal file {}",
                    wal_contents.wal_file_number.get()
                );
                self.file_notifier.notify(wal_contents);
                None
            }
        };

        // send all the responses back to clients
        for response in responses {
            let _ = response.send(WriteResult::Success(()));
        }

        snapshot_response
    }

    async fn load_existing_wal_file_paths(&self) -> crate::Result<Vec<Path>> {
        let mut paths = Vec::new();
        let mut offset: Option<Path> = None;
        let path = Path::from(format!("{host}/wal", host = self.host_identifier_prefix));
        loop {
            let mut listing = if let Some(offset) = offset {
                self.object_store.list_with_offset(Some(&path), &offset)
            } else {
                self.object_store.list(Some(&path))
            };
            let path_count = paths.len();

            while let Some(item) = listing.next().await {
                paths.push(item?.location);
            }

            if path_count == paths.len() {
                break;
            }

            paths.sort();
            offset = Some(paths.last().unwrap().clone())
        }
        paths.sort();

        Ok(paths)
    }

    async fn remove_snapshot_wal_files(
        &self,
        snapshot_info: SnapshotInfo,
        snapshot_permit: OwnedSemaphorePermit,
    ) {
        for period in snapshot_info.wal_periods {
            let path = wal_path(&self.host_identifier_prefix, period.wal_file_number);

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

        // release the permit so the next snapshot can be run when the time comes
        drop(snapshot_permit);
    }
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
        SnapshotInfo,
        OwnedSemaphorePermit,
    )> {
        self.flush_buffer().await
    }

    async fn cleanup_snapshot(
        &self,
        snapshot_info: SnapshotInfo,
        snapshot_permit: OwnedSemaphorePermit,
    ) {
        self.remove_snapshot_wal_files(snapshot_info, snapshot_permit)
            .await
    }

    async fn last_sequence_number(&self) -> WalFileSequenceNumber {
        self.flush_buffer
            .lock()
            .await
            .snapshot_tracker
            .last_sequence_number()
    }

    async fn shutdown(&self) {
        self.shutdown().await
    }
}

#[derive(Debug)]
struct FlushBuffer {
    wal_buffer: WalBuffer,
    snapshot_tracker: SnapshotTracker,
    snapshot_semaphore: Arc<Semaphore>,
}

impl FlushBuffer {
    fn new(wal_buffer: WalBuffer, snapshot_tracker: SnapshotTracker) -> Self {
        Self {
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
    async fn flush_buffer_into_contents_and_responses(
        &mut self,
    ) -> (
        WalContents,
        Vec<oneshot::Sender<WriteResult>>,
        Option<(SnapshotInfo, OwnedSemaphorePermit)>,
    ) {
        // convert into wal contents and resopnses and capture if a snapshot should be taken
        let (mut wal_contents, responses) = self.flush_buffer_with_responses();
        self.snapshot_tracker.add_wal_period(WalPeriod {
            wal_file_number: wal_contents.wal_file_number,
            min_time: Timestamp::new(wal_contents.min_timestamp_ns),
            max_time: Timestamp::new(wal_contents.max_timestamp_ns),
        });

        let snapshot = match self.snapshot_tracker.snapshot() {
            Some(snapshot_info) => {
                wal_contents.snapshot = Some(snapshot_info.snapshot_details);

                Some((snapshot_info, self.acquire_snapshot_permit().await))
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

    fn flush_buffer_with_responses(&mut self) -> (WalContents, Vec<oneshot::Sender<WriteResult>>) {
        // swap out the filled buffer with a new one
        let mut new_buffer = WalBuffer {
            is_shutdown: false,
            wal_file_sequence_number: self.wal_buffer.wal_file_sequence_number.next(),
            op_limit: self.wal_buffer.op_limit,
            op_count: 0,
            database_to_write_batch: Default::default(),
            write_op_responses: vec![],
            catalog_batches: vec![],
        };
        std::mem::swap(&mut self.wal_buffer, &mut new_buffer);

        new_buffer.into_wal_contents_and_responses()
    }

    async fn flush_buffer_with_failure(&mut self, error: WriteResult) {
        let (_, responses) = self.flush_buffer_with_responses();
        for response in responses {
            let _ = response.send(error.clone());
        }
    }
}

#[derive(Debug, Default)]
struct WalBuffer {
    is_shutdown: bool,
    wal_file_sequence_number: WalFileSequenceNumber,
    op_limit: usize,
    op_count: usize,
    database_to_write_batch: HashMap<Arc<str>, WriteBatch>,
    catalog_batches: Vec<CatalogBatch>,
    write_op_responses: Vec<oneshot::Sender<WriteResult>>,
}

impl WalBuffer {
    fn is_empty(&self) -> bool {
        self.database_to_write_batch.is_empty()
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

    fn into_wal_contents_and_responses(self) -> (WalContents, Vec<oneshot::Sender<WriteResult>>) {
        // get the min and max data timestamps for writes into this wal file
        let mut min_timestamp_ns = i64::MAX;
        let mut max_timestamp_ns = i64::MIN;

        for write_batch in self.database_to_write_batch.values() {
            min_timestamp_ns = min_timestamp_ns.min(write_batch.min_time_ns);
            max_timestamp_ns = max_timestamp_ns.max(write_batch.max_time_ns);
        }

        // have the catalog ops come before any writes in ordering
        let mut ops =
            Vec::with_capacity(self.database_to_write_batch.len() + self.catalog_batches.len());

        for catalog_batch in self.catalog_batches {
            ops.push(WalOp::Catalog(catalog_batch));
        }

        for write_batch in self.database_to_write_batch.into_values() {
            ops.push(WalOp::Write(write_batch));
        }

        (
            WalContents {
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

fn wal_path(host_identifier_prefix: &str, wal_file_number: WalFileSequenceNumber) -> Path {
    Path::from(format!(
        "{host_identifier_prefix}/wal/{:011}.wal",
        wal_file_number.0
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Field, FieldData, Row, TableChunk, TableChunks};
    use async_trait::async_trait;
    use object_store::memory::InMemory;
    use std::any::Any;
    use tokio::sync::oneshot::Receiver;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_flush_delete_and_load() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotfiier::default());
        let wal_config = WalConfig {
            max_write_buffer_size: 100,
            flush_interval: Duration::from_secs(1),
            snapshot_size: 2,
            level_0_duration: Duration::from_nanos(10),
        };
        let wal = WalObjectStore::new_without_replay(
            Arc::clone(&object_store),
            "my_host",
            Arc::clone(&notifier),
            wal_config,
            None,
        );

        let db_name: Arc<str> = "db1".into();
        let table_name: Arc<str> = "table1".into();

        let op1 = WalOp::Write(WriteBatch {
            database_name: Arc::clone(&db_name),
            table_chunks: HashMap::from([(
                Arc::clone(&table_name),
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
                                            name: "f1".into(),
                                            value: FieldData::Integer(1),
                                        },
                                        Field {
                                            name: "time".into(),
                                            value: FieldData::Timestamp(1),
                                        },
                                    ],
                                },
                                Row {
                                    time: 3,
                                    fields: vec![
                                        Field {
                                            name: "f1".into(),
                                            value: FieldData::Integer(2),
                                        },
                                        Field {
                                            name: "time".into(),
                                            value: FieldData::Timestamp(3),
                                        },
                                    ],
                                },
                            ],
                        },
                    )]),
                },
            )]),
            min_time_ns: 1,
            max_time_ns: 3,
        });
        wal.buffer_op_unconfirmed(op1.clone()).await.unwrap();

        let op2 = WalOp::Write(WriteBatch {
            database_name: Arc::clone(&db_name),
            table_chunks: HashMap::from([(
                Arc::clone(&table_name),
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
                                        name: "f1".into(),
                                        value: FieldData::Integer(3),
                                    },
                                    Field {
                                        name: "time".into(),
                                        value: FieldData::Timestamp(12),
                                    },
                                ],
                            }],
                        },
                    )]),
                },
            )]),
            min_time_ns: 12,
            max_time_ns: 12,
        });
        wal.buffer_op_unconfirmed(op2.clone()).await.unwrap();

        // create wal file 1
        let ret = wal.flush_buffer().await;
        assert!(ret.is_none());
        let file_1_contents = WalContents {
            min_timestamp_ns: 1,
            max_timestamp_ns: 12,
            wal_file_number: WalFileSequenceNumber(1),
            ops: vec![WalOp::Write(WriteBatch {
                database_name: "db1".into(),
                table_chunks: HashMap::from([(
                    "table1".into(),
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
                                                name: "f1".into(),
                                                value: FieldData::Integer(1),
                                            },
                                            Field {
                                                name: "time".into(),
                                                value: FieldData::Timestamp(1),
                                            },
                                        ],
                                    },
                                    Row {
                                        time: 3,
                                        fields: vec![
                                            Field {
                                                name: "f1".into(),
                                                value: FieldData::Integer(2),
                                            },
                                            Field {
                                                name: "time".into(),
                                                value: FieldData::Timestamp(3),
                                            },
                                        ],
                                    },
                                    Row {
                                        time: 12,
                                        fields: vec![
                                            Field {
                                                name: "f1".into(),
                                                value: FieldData::Integer(3),
                                            },
                                            Field {
                                                name: "time".into(),
                                                value: FieldData::Timestamp(12),
                                            },
                                        ],
                                    },
                                ],
                            },
                        )]),
                    },
                )]),
                min_time_ns: 1,
                max_time_ns: 12,
            })],
            snapshot: None,
        };

        // create wal file 2
        wal.buffer_op_unconfirmed(op2.clone()).await.unwrap();
        assert!(wal.flush_buffer().await.is_none());

        let file_2_contents = WalContents {
            min_timestamp_ns: 12,
            max_timestamp_ns: 12,
            wal_file_number: WalFileSequenceNumber(2),
            ops: vec![WalOp::Write(WriteBatch {
                database_name: "db1".into(),
                table_chunks: HashMap::from([(
                    "table1".into(),
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
                                            name: "f1".into(),
                                            value: FieldData::Integer(3),
                                        },
                                        Field {
                                            name: "time".into(),
                                            value: FieldData::Timestamp(12),
                                        },
                                    ],
                                }],
                            },
                        )]),
                    },
                )]),
                min_time_ns: 12,
                max_time_ns: 12,
            })],
            snapshot: None,
        };

        // before we trigger a snapshot, test replay with a new wal and notifier
        let replay_notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotfiier::default());
        let replay_wal = WalObjectStore::new_without_replay(
            Arc::clone(&object_store),
            "my_host",
            Arc::clone(&replay_notifier),
            WalConfig {
                level_0_duration: Duration::from_secs(10),
                max_write_buffer_size: 10,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 2,
            },
            None,
        );
        assert_eq!(
            replay_wal.load_existing_wal_file_paths().await.unwrap(),
            vec![
                Path::from("my_host/wal/00000000001.wal"),
                Path::from("my_host/wal/00000000002.wal")
            ]
        );
        replay_wal.replay().await.unwrap();
        let replay_notifier = replay_notifier
            .as_any()
            .downcast_ref::<TestNotfiier>()
            .unwrap();

        {
            let notified_writes = replay_notifier.notified_writes.lock();
            assert_eq!(
                *notified_writes,
                vec![file_1_contents.clone(), file_2_contents.clone()]
            );
        }
        assert!(replay_notifier.snapshot_details.lock().is_none());

        // create wal file 3, which should trigger a snapshot
        let op3 = WalOp::Write(WriteBatch {
            database_name: Arc::clone(&db_name),
            table_chunks: HashMap::from([(
                Arc::clone(&table_name),
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
                                        name: "f1".into(),
                                        value: FieldData::Integer(3),
                                    },
                                    Field {
                                        name: "time".into(),
                                        value: FieldData::Timestamp(26),
                                    },
                                ],
                            }],
                        },
                    )]),
                },
            )]),
            min_time_ns: 26,
            max_time_ns: 26,
        });
        wal.buffer_op_unconfirmed(op3.clone()).await.unwrap();

        let (snapshot_done, snapshot_info, snapshot_permit) = wal.flush_buffer().await.unwrap();
        let expected_info = SnapshotInfo {
            snapshot_details: SnapshotDetails {
                end_time_marker: 20,
                last_sequence_number: WalFileSequenceNumber(2),
            },
            wal_periods: vec![
                WalPeriod {
                    wal_file_number: WalFileSequenceNumber(1),
                    min_time: Timestamp::new(1),
                    max_time: Timestamp::new(12),
                },
                WalPeriod {
                    wal_file_number: WalFileSequenceNumber(2),
                    min_time: Timestamp::new(12),
                    max_time: Timestamp::new(12),
                },
            ],
        };
        assert_eq!(expected_info, snapshot_info);
        snapshot_done.await.unwrap();

        let file_3_contents = WalContents {
            min_timestamp_ns: 26,
            max_timestamp_ns: 26,
            wal_file_number: WalFileSequenceNumber(3),
            ops: vec![WalOp::Write(WriteBatch {
                database_name: "db1".into(),
                table_chunks: HashMap::from([(
                    "table1".into(),
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
                                            name: "f1".into(),
                                            value: FieldData::Integer(3),
                                        },
                                        Field {
                                            name: "time".into(),
                                            value: FieldData::Timestamp(26),
                                        },
                                    ],
                                }],
                            },
                        )]),
                    },
                )]),
                min_time_ns: 26,
                max_time_ns: 26,
            })],
            snapshot: Some(SnapshotDetails {
                end_time_marker: 20,
                last_sequence_number: WalFileSequenceNumber(2),
            }),
        };

        let notifier = notifier.as_any().downcast_ref::<TestNotfiier>().unwrap();

        {
            let notified_writes = notifier.notified_writes.lock();
            let expected_writes = vec![file_1_contents, file_2_contents, file_3_contents.clone()];
            assert_eq!(*notified_writes, expected_writes);
            let details = notifier.snapshot_details.lock();
            assert_eq!(details.unwrap(), expected_info.snapshot_details);
        }

        wal.remove_snapshot_wal_files(snapshot_info, snapshot_permit)
            .await;

        // test that replay now only has file 3
        let replay_notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotfiier::default());
        let replay_wal = WalObjectStore::new_without_replay(
            object_store,
            "my_host",
            Arc::clone(&replay_notifier),
            wal_config,
            None,
        );
        assert_eq!(
            replay_wal.load_existing_wal_file_paths().await.unwrap(),
            vec![Path::from("my_host/wal/00000000003.wal")]
        );
        replay_wal.replay().await.unwrap();
        let replay_notifier = replay_notifier
            .as_any()
            .downcast_ref::<TestNotfiier>()
            .unwrap();
        let notified_writes = replay_notifier.notified_writes.lock();
        assert_eq!(*notified_writes, vec![file_3_contents.clone()]);
        let snapshot_details = replay_notifier.snapshot_details.lock();
        assert_eq!(*snapshot_details, file_3_contents.snapshot);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_for_empty_buffer_skips_notify() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotfiier::default());
        let wal_config = WalConfig {
            max_write_buffer_size: 100,
            flush_interval: Duration::from_secs(1),
            snapshot_size: 2,
            level_0_duration: Duration::from_nanos(10),
        };
        let wal = WalObjectStore::new_without_replay(
            Arc::clone(&object_store),
            "my_host",
            Arc::clone(&notifier),
            wal_config,
            None,
        );

        assert!(wal.flush_buffer().await.is_none());
        let notifier = notifier.as_any().downcast_ref::<TestNotfiier>().unwrap();
        assert!(notifier.notified_writes.lock().is_empty());

        // make sure no wal file was written
        assert!(object_store.list(None).next().await.is_none());
    }

    #[derive(Debug, Default)]
    struct TestNotfiier {
        notified_writes: parking_lot::Mutex<Vec<WalContents>>,
        snapshot_details: parking_lot::Mutex<Option<SnapshotDetails>>,
    }

    #[async_trait]
    impl WalFileNotifier for TestNotfiier {
        fn notify(&self, write: WalContents) {
            self.notified_writes.lock().push(write);
        }

        async fn notify_and_snapshot(
            &self,
            write: WalContents,
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
