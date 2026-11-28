use super::*;
use crate::{
    Field, FieldData, Gen1Duration, Row, SnapshotSequenceNumber, TableChunk, TableChunks, create,
};
use async_trait::async_trait;
use indexmap::IndexMap;
use influxdb3_id::{ColumnId, DbId, TableId};
use iox_time::{MockProvider, Time};
use object_store::memory::InMemory;
use object_store_utils::{LostResponseError, LostResponseStore, SelfVerifyingCreateStore};
use rustc_hash::FxHasher;
use std::any::Any;
use std::hash::BuildHasherDefault;
use tokio::sync::oneshot::Receiver;

// Helper function to create IndexMap with FxHasher for tests
type FxBuildHasher = BuildHasherDefault<FxHasher>;
fn fx_index_map<K: std::hash::Hash + Eq, V, const N: usize>(
    entries: [(K, V); N],
) -> IndexMap<K, V, FxBuildHasher> {
    let mut map = IndexMap::with_capacity_and_hasher(N, FxBuildHasher::default());
    map.extend(entries);
    map
}

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
        ..Default::default()
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
        CancellationToken::new(),
    );

    let db_name: Arc<str> = "db1".into();

    let op1 = WalOp::Write(WriteBatch {
        catalog_sequence: 0,
        database_id: DbId::from(0),
        database_name: Arc::clone(&db_name),
        table_chunks: fx_index_map([(
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
    wal.write_ops_unconfirmed(vec![op1.clone()]).await.unwrap();

    let op2 = WalOp::Write(WriteBatch {
        catalog_sequence: 0,
        database_id: DbId::from(0),
        database_name: Arc::clone(&db_name),
        table_chunks: fx_index_map([(
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
    wal.write_ops_unconfirmed(vec![op2.clone()]).await.unwrap();

    // create wal file 1
    let ret = wal.flush_buffer(false).await;
    assert!(ret.is_none());
    let file_1_contents = create::wal_contents(
        (1, 62_000_000_000, 1),
        [WalOp::Write(WriteBatch {
            catalog_sequence: 0,
            database_id: DbId::from(0),
            database_name: "db1".into(),
            table_chunks: fx_index_map([(
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
    wal.write_ops_unconfirmed(vec![op2.clone()]).await.unwrap();
    assert!(wal.flush_buffer(false).await.is_none());

    let file_2_contents = create::wal_contents(
        (62_000_000_000, 62_000_000_000, 2),
        [WalOp::Write(WriteBatch {
            catalog_sequence: 0,
            database_id: DbId::from(0),
            database_name: "db1".into(),
            table_chunks: fx_index_map([(
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
        CancellationToken::new(),
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
            1,
            false,
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
        catalog_sequence: 0,
        database_id: DbId::from(0),
        database_name: Arc::clone(&db_name),
        table_chunks: fx_index_map([(
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
    wal.write_ops_unconfirmed(vec![op3.clone()]).await.unwrap();

    let (snapshot_done, snapshot_info, snapshot_permit) = wal.flush_buffer(false).await.unwrap();
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
            catalog_sequence: 0,
            database_id: DbId::from(0),
            database_name: "db1".into(),
            table_chunks: fx_index_map([(
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
        CancellationToken::new(),
    );
    assert_eq!(
        replay_wal.load_existing_wal_file_paths(None, &[Path::from("my_host/wal/00000000003.wal")]),
        vec![Path::from("my_host/wal/00000000003.wal")]
    );
    replay_wal
        .replay(None, &[Path::from("my_host/wal/00000000003.wal")], 1, true)
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
        ..Default::default()
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
        CancellationToken::new(),
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
        state: WalBufferState::AcceptingWrites,
        wal_file_sequence_number: WalFileSequenceNumber(0),
        op_limit: 10,
        op_count: 0,
        database_to_write_batch: Default::default(),
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
            state: WalBufferState::AcceptingWrites,
            wal_file_sequence_number: WalFileSequenceNumber(0),
            op_limit: 10,
            op_count: 0,
            database_to_write_batch: Default::default(),
            write_op_responses: vec![],
            no_op: None,
        },
        snapshot_tracker,
    );

    flush_buffer
        .wal_buffer
        .write_ops_unconfirmed(vec![WalOp::Write(WriteBatch {
            catalog_sequence: 0,
            database_id: DbId::from(0),
            database_name: "db1".into(),
            table_chunks: fx_index_map([(
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
        })])
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
        .write_ops_unconfirmed(vec![WalOp::Write(WriteBatch {
            catalog_sequence: 0,
            database_id: DbId::from(0),
            database_name: "db1".into(),
            table_chunks: fx_index_map([(
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
        })])
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

#[test_log::test(tokio::test)]
async fn test_wal_file_removal_after_snapshot() {
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
    let wal_config = WalConfig {
        max_write_buffer_size: 100,
        flush_interval: Duration::from_secs(1),
        snapshot_size: 1,
        gen1_duration: Gen1Duration::new_1m(),
        ..Default::default()
    };

    // load some files into OS
    let path1 = wal_path("my_host", WalFileSequenceNumber::new(1));
    let path2 = wal_path("my_host", WalFileSequenceNumber::new(2));
    let path3 = wal_path("my_host", WalFileSequenceNumber::new(3));
    object_store
        .put(&path2, PutPayload::from_static(b"boo"))
        .await
        .unwrap();
    let all_paths = load_all_wal_file_paths(Arc::clone(&object_store), "my_host".to_string())
        .await
        .unwrap();

    debug!(?all_paths, "test: all paths in object store");

    let wal = WalObjectStore::new_without_replay(
        Arc::clone(&time_provider),
        Arc::clone(&object_store),
        "my_host",
        Arc::clone(&notifier),
        wal_config,
        None,
        None,
        &[],
        1,
        CancellationToken::new(),
    );

    {}
    let snapshot_details = SnapshotDetails {
        snapshot_sequence_number: SnapshotSequenceNumber::new(1),
        end_time_marker: 10,
        // snapshot size is 1, so we just snapshotted to a single file
        first_wal_sequence_number: WalFileSequenceNumber::new(1),
        last_wal_sequence_number: WalFileSequenceNumber::new(1),
        forced: false,
    };
    // add that wal file to obj store
    object_store
        .put(&path1, PutPayload::from_static(b"boo"))
        .await
        .unwrap();

    let snapshot_permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
    // this will not delete any files
    wal.remove_snapshot_wal_files(snapshot_details, snapshot_permit)
        .await;

    // now add another snapshot
    let snapshot_details = SnapshotDetails {
        snapshot_sequence_number: SnapshotSequenceNumber::new(2),
        end_time_marker: 20,
        first_wal_sequence_number: WalFileSequenceNumber::new(2),
        last_wal_sequence_number: WalFileSequenceNumber::new(2),
        forced: false,
    };

    // add that wal file to obj store
    object_store
        .put(&path2, PutPayload::from_static(b"boo"))
        .await
        .unwrap();

    let snapshot_permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
    // this will not delete any files - we've added the 2nd file still no deletes
    wal.remove_snapshot_wal_files(snapshot_details, snapshot_permit)
        .await;

    let all_paths = load_all_wal_file_paths(Arc::clone(&object_store), "my_host".to_string())
        .await
        .unwrap();

    debug!(?all_paths, "test: all paths in object store after removal");

    assert!(object_store.get(&path1).await.ok().is_some());
    assert!(object_store.get(&path2).await.ok().is_some());

    // now add 3rd snapshot
    let snapshot_details = SnapshotDetails {
        snapshot_sequence_number: SnapshotSequenceNumber::new(3),
        end_time_marker: 30,
        // snapshot size is 1, so we just snapshotted to a single file
        first_wal_sequence_number: WalFileSequenceNumber::new(3),
        last_wal_sequence_number: WalFileSequenceNumber::new(3),
        forced: false,
    };

    // add that wal file to obj store
    object_store
        .put(&path3, PutPayload::from_static(b"foo"))
        .await
        .unwrap();

    let snapshot_permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
    wal.remove_snapshot_wal_files(snapshot_details, snapshot_permit)
        .await;

    let all_paths = load_all_wal_file_paths(Arc::clone(&object_store), "my_host".to_string())
        .await
        .unwrap();

    debug!(?all_paths, "test: all paths in object store after removal");

    let err = object_store.get(&path1).await.err().unwrap();

    assert!(matches!(
        err,
        object_store::Error::NotFound { path: _, source: _ }
    ));
    assert!(object_store.get(&path2).await.ok().is_some());
    assert!(object_store.get(&path3).await.ok().is_some());
}

#[test_log::test(tokio::test)]
async fn test_wal_file_removal_after_snapshot_worked_out_example() {
    // Say we snapshot every 5 files, and we always want to keep around at least 10 snapshotted files.
    // Then say we're currently in this state
    //     oldest - 20
    //     last_snapshot - 30
    //     latest - 33
    // All good, keep running and we get two more wal files, then we'll get to the state of
    //     oldest - 20
    //     last_snapshot - 30
    //     latest - 35
    // When a snapshot gets triggered. When it's done, you'll have this state:
    //     oldest - 20
    //     last_snapshot - 35
    //     latest - (some number >=35 given we may have received more wal files while the snapshot was running)
    // So now we kick off the deletion, which should end us in this state:
    //     oldest - 25
    //     last_snapshot - 35
    //     latest - >= 35
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
    let wal_config = WalConfig {
        max_write_buffer_size: 100,
        flush_interval: Duration::from_secs(1),
        snapshot_size: 1,
        gen1_duration: Gen1Duration::new_1m(),
        ..Default::default()
    };

    // load some files into OS
    for i in 20..=35 {
        let path = wal_path("my_host", WalFileSequenceNumber::new(i));
        object_store
            .put(&path, PutPayload::from_static(b"boo"))
            .await
            .unwrap();
    }
    let all_paths = load_all_wal_file_paths(Arc::clone(&object_store), "my_host".to_string())
        .await
        .unwrap();

    debug!(?all_paths, "test: all paths in object store");

    let wal = WalObjectStore::new_without_replay(
        Arc::clone(&time_provider),
        Arc::clone(&object_store),
        "my_host",
        Arc::clone(&notifier),
        wal_config,
        Some(WalFileSequenceNumber::new(30)),
        Some(SnapshotSequenceNumber::new(10)),
        &all_paths,
        10,
        CancellationToken::new(),
    );

    let snapshot_details = SnapshotDetails {
        snapshot_sequence_number: SnapshotSequenceNumber::new(20),
        end_time_marker: 10,
        first_wal_sequence_number: WalFileSequenceNumber::new(31),
        last_wal_sequence_number: WalFileSequenceNumber::new(35),
        forced: false,
    };
    let snapshot_permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
    wal.remove_snapshot_wal_files(snapshot_details, snapshot_permit)
        .await;

    let (oldest, last) = wal.wal_remover.get_current_state();
    assert_eq!(25, oldest);
    assert_eq!(35, last);

    for i in 20..=24 {
        let err = object_store
            .get(&wal_path("my_host", WalFileSequenceNumber::new(i)))
            .await
            .err()
            .unwrap();

        assert!(matches!(
            err,
            object_store::Error::NotFound { path: _, source: _ }
        ));
    }

    for i in 25..35 {
        let _ = object_store
            .get(&wal_path("my_host", WalFileSequenceNumber::new(i)))
            .await
            .ok()
            .unwrap();
    }
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

/// Build a WAL over `inner` wrapped in the verifying layer.
fn verifying_wal_over(inner: Arc<dyn ObjectStore>) -> (WalObjectStore, CancellationToken) {
    let registry = metric::Registry::new();
    wal_over(Arc::new(SelfVerifyingCreateStore::new(inner, &registry)))
}

/// Build a WAL over `object_store`, returning the WAL and the shutdown token it
/// will cancel if it decides a foreign writer holds the path.
fn wal_over(object_store: Arc<dyn ObjectStore>) -> (WalObjectStore, CancellationToken) {
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
    let shutdown = CancellationToken::new();
    let wal = WalObjectStore::new_without_replay(
        time_provider,
        object_store,
        "my_host",
        notifier,
        WalConfig {
            max_write_buffer_size: 100,
            flush_interval: Duration::from_secs(1),
            snapshot_size: 2,
            gen1_duration: Gen1Duration::new_1m(),
            ..Default::default()
        },
        None,
        None,
        &[],
        1,
        shutdown.clone(),
    );
    (wal, shutdown)
}

/// A single write op, enough to fill the WAL buffer so a flush persists a file.
fn op() -> WalOp {
    WalOp::Write(WriteBatch {
        catalog_sequence: 0,
        database_id: DbId::from(0),
        database_name: "db1".into(),
        table_chunks: fx_index_map([(
            TableId::from(0),
            TableChunks {
                min_time: 1,
                max_time: 1,
                chunk_time_to_chunk: HashMap::from([(
                    0,
                    TableChunk {
                        rows: vec![Row {
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
                        }],
                    },
                )]),
            },
        )])
        .into(),
        min_time_ns: 1,
        max_time_ns: 1,
    })
}

/// The bug from EAR 7005: the PUT is applied, the response is lost, the
/// client's own retry gets 412. The node must not shut down.
#[tokio::test]
async fn own_lost_put_response_is_not_a_duplicate_writer() {
    let inner = Arc::new(LostResponseStore::new(Arc::new(InMemory::new())));
    let (wal, shutdown) = verifying_wal_over(Arc::clone(&inner) as _);

    wal.write_ops_unconfirmed(vec![op()]).await.unwrap();
    inner.lose_next_put_response_with(LostResponseError::AlreadyExists);

    let flushed = wal.flush_buffer(true).await;

    assert!(!shutdown.is_cancelled(), "node shut down on its own write");
    assert!(
        flushed.is_some(),
        "flush reported failure for a durable write"
    );
}

/// Control: without the verifying layer, the same scenario still shuts the node
/// down. This proves the test above detects the bug, and that a store left
/// unwrapped is unchanged.
#[tokio::test]
async fn an_unwrapped_store_still_shuts_down_on_a_lost_put_response() {
    let inner = Arc::new(LostResponseStore::new(Arc::new(InMemory::new())));
    let (wal, shutdown) = wal_over(Arc::clone(&inner) as _);

    wal.write_ops_unconfirmed(vec![op()]).await.unwrap();
    inner.lose_next_put_response_with(LostResponseError::AlreadyExists);

    let flushed = wal.flush_buffer(true).await;

    assert!(shutdown.is_cancelled());
    assert!(flushed.is_none());
}

/// The WAL's own 100x retry loop re-issues the create. A nonce minted per
/// `put_opts` call would be fresh on the second attempt and read the first
/// attempt's nonce as foreign; a nonce minted per WAL file covers it.
#[tokio::test]
async fn outer_retry_loop_is_covered_by_the_per_file_nonce() {
    let inner = Arc::new(LostResponseStore::new(Arc::new(InMemory::new())));
    let (wal, shutdown) = verifying_wal_over(Arc::clone(&inner) as _);

    wal.write_ops_unconfirmed(vec![op()]).await.unwrap();
    // First attempt lands but returns a retryable error, so the WAL's own loop
    // re-enters put_opts; the second attempt collides with attempt one.
    inner.lose_next_put_response_with(LostResponseError::Generic);

    let flushed = wal.flush_buffer(true).await;

    assert!(
        !shutdown.is_cancelled(),
        "outer-loop retry shut the node down"
    );
    assert!(flushed.is_some());
}

/// A minimal one-row write batch op for buffer-limit tests; `n` distinguishes
/// writes and serves as the row time.
fn small_write_op(n: i64) -> WalOp {
    WalOp::Write(WriteBatch {
        catalog_sequence: 0,
        database_id: DbId::from(0),
        database_name: "db1".into(),
        table_chunks: fx_index_map([(
            TableId::from(0),
            TableChunks {
                min_time: n,
                max_time: n,
                chunk_time_to_chunk: HashMap::from([(
                    0,
                    TableChunk {
                        rows: vec![Row {
                            time: n,
                            fields: vec![
                                Field {
                                    id: ColumnId::from(0),
                                    value: FieldData::Integer(n),
                                },
                                Field {
                                    id: ColumnId::from(1),
                                    value: FieldData::Timestamp(n),
                                },
                            ],
                        }],
                    },
                )]),
            },
        )])
        .into(),
        min_time_ns: n,
        max_time_ns: n,
    })
}

fn small_wal(config: WalConfig) -> WalObjectStore {
    WalObjectStore::new_without_replay(
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
        Arc::new(InMemory::new()),
        "my_host",
        Arc::new(TestNotifier::default()),
        config,
        None,
        None,
        &[],
        1,
        CancellationToken::new(),
    )
}

// https://github.com/influxdata/influxdb_pro/issues/4339
#[tokio::test]
async fn max_write_buffer_size_rejects_writes_when_buffer_full() {
    let wal = small_wal(WalConfig {
        max_write_buffer_size: 1,
        flush_interval: Duration::from_secs(1),
        snapshot_size: 2,
        gen1_duration: Gen1Duration::new_1m(),
        ..Default::default()
    });

    wal.write_ops_unconfirmed(vec![small_write_op(1)])
        .await
        .unwrap();
    let err = wal
        .write_ops_unconfirmed(vec![small_write_op(2)])
        .await
        .expect_err("second buffered write should exceed max_write_buffer_size=1");
    assert!(matches!(err, crate::Error::BufferFull(1)), "{err:?}");
}

#[tokio::test]
async fn max_write_buffer_size_accepts_writes_again_after_flush() {
    let wal = small_wal(WalConfig {
        max_write_buffer_size: 1,
        flush_interval: Duration::from_secs(1),
        snapshot_size: 2,
        gen1_duration: Gen1Duration::new_1m(),
        ..Default::default()
    });

    wal.write_ops_unconfirmed(vec![small_write_op(1)])
        .await
        .unwrap();
    wal.write_ops_unconfirmed(vec![small_write_op(2)])
        .await
        .expect_err("buffer is at its limit");

    wal.flush_buffer(false).await;

    wal.write_ops_unconfirmed(vec![small_write_op(3)])
        .await
        .expect("flush drains the buffer, making room for new writes");
}

// A rejected write's sender must not be registered: `buffer_ops_with_response`
// accepts the ops before pushing the sender, so the next flush cannot signal
// `Success` into a oneshot whose caller already saw `BufferFull`.
#[test]
fn rejected_write_does_not_register_a_response_sender() {
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let mut wal_buffer = WalBuffer {
        time_provider: Arc::clone(&time_provider) as _,
        state: WalBufferState::AcceptingWrites,
        wal_file_sequence_number: WalFileSequenceNumber(0),
        op_limit: 1,
        op_count: 0,
        database_to_write_batch: Default::default(),
        write_op_responses: vec![],
        no_op: None,
    };

    let (tx1, _rx1) = oneshot::channel();
    wal_buffer
        .buffer_ops_with_response(vec![small_write_op(1)], tx1)
        .unwrap();

    let (tx2, _rx2) = oneshot::channel();
    let err = wal_buffer
        .buffer_ops_with_response(vec![small_write_op(2)], tx2)
        .expect_err("buffer is at its limit");
    assert!(matches!(err, crate::Error::BufferFull(1)), "{err:?}");
    assert_eq!(wal_buffer.write_op_responses.len(), 1);
}

// The sync path: a write past the cap fails fast with `BufferFull` instead
// of waiting on the flush, and the accepted (blocked) write still resolves
// `Ok` once the flush lands.
#[tokio::test]
async fn max_write_buffer_size_rejects_sync_writes_when_buffer_full() {
    let wal = Arc::new(small_wal(WalConfig {
        max_write_buffer_size: 1,
        flush_interval: Duration::from_secs(1),
        snapshot_size: 2,
        gen1_duration: Gen1Duration::new_1m(),
        ..Default::default()
    }));

    let blocked = tokio::spawn({
        let wal = Arc::clone(&wal);
        async move { wal.write_ops(vec![small_write_op(1)]).await }
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        while wal.flush_buffer.lock().await.wal_buffer.is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("spawned write should reach the buffer");

    let err = wal
        .write_ops(vec![small_write_op(2)])
        .await
        .expect_err("second sync write should exceed max_write_buffer_size=1");
    assert!(matches!(err, crate::Error::BufferFull(1)), "{err:?}");

    wal.flush_buffer(false).await;
    blocked
        .await
        .unwrap()
        .expect("the accepted write resolves once the flush lands");
}

/// A power failure can leave zero-byte WAL objects after the latest snapshot.
/// Replay skips their contents, but startup must not reuse their names.
#[tokio::test]
async fn zero_byte_wal_after_snapshot_does_not_collide_on_first_flush() {
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
    let shutdown = CancellationToken::new();

    for sequence in 5..=8 {
        object_store
            .put(
                &wal_path("my_host", WalFileSequenceNumber::new(sequence)),
                PutPayload::from_static(b""),
            )
            .await
            .unwrap();
    }

    let paths = load_all_wal_file_paths(Arc::clone(&object_store), "my_host".to_owned())
        .await
        .unwrap();
    let wal = WalObjectStore::new_without_replay(
        time_provider,
        Arc::clone(&object_store),
        "my_host",
        notifier,
        WalConfig {
            max_write_buffer_size: 100,
            flush_interval: Duration::from_secs(1),
            snapshot_size: 2,
            gen1_duration: Gen1Duration::new_1m(),
            ..Default::default()
        },
        Some(WalFileSequenceNumber::new(4)),
        None,
        &paths,
        10,
        shutdown.clone(),
    );

    wal.replay(Some(WalFileSequenceNumber::new(4)), &paths, 2, false)
        .await
        .unwrap();
    assert_eq!(
        wal.flush_buffer
            .lock()
            .await
            .wal_buffer
            .wal_file_sequence_number,
        WalFileSequenceNumber::new(9)
    );

    wal.write_ops_unconfirmed(vec![op()]).await.unwrap();
    assert!(wal.flush_buffer(true).await.is_some());
    assert!(
        !shutdown.is_cancelled(),
        "first post-recovery write shut down the node"
    );

    // The corrupt files are preserved for diagnosis; the new write uses 9.
    for sequence in 5..=8 {
        assert_eq!(
            object_store
                .get(&wal_path("my_host", WalFileSequenceNumber::new(sequence)))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .len(),
            0
        );
    }
    object_store
        .get(&wal_path("my_host", WalFileSequenceNumber::new(9)))
        .await
        .expect("first post-recovery WAL must use sequence 9");
}

/// Serialize `contents` and put it at the WAL path for `sequence`, producing a
/// replayable file (unlike a bare 0-byte put).
async fn put_valid_wal_file(
    object_store: &Arc<dyn ObjectStore>,
    sequence: u64,
    contents: &WalContents,
) {
    let bytes = crate::serialize::serialize_to_file_bytes(contents).unwrap();
    object_store
        .put(
            &wal_path("my_host", WalFileSequenceNumber::new(sequence)),
            PutPayload::from_bytes(Bytes::from(bytes)),
        )
        .await
        .unwrap();
}

fn wal_contents_for(sequence: u64) -> WalContents {
    WalContents {
        persist_timestamp_ms: 0,
        min_timestamp_ns: 1,
        max_timestamp_ns: 1,
        wal_file_number: WalFileSequenceNumber::new(sequence),
        ops: vec![op()],
        snapshot: None,
    }
}

/// The replay of a *valid* file sets the next sequence to its number + 1. If a
/// corrupt file sits at the tail after the last valid file, replaying the valid
/// file rewinds the sequence back onto the corrupt file's name. The skip branch
/// must therefore advance past skipped files, or the first flush collides.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_file_after_valid_file_does_not_collide_on_first_flush() {
    let time_provider: Arc<dyn TimeProvider> =
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
    let shutdown = CancellationToken::new();

    // 5 is a valid WAL file; 6 is a 0-byte file left by an unclean shutdown.
    put_valid_wal_file(&object_store, 5, &wal_contents_for(5)).await;
    object_store
        .put(
            &wal_path("my_host", WalFileSequenceNumber::new(6)),
            PutPayload::from_static(b""),
        )
        .await
        .unwrap();

    let paths = load_all_wal_file_paths(Arc::clone(&object_store), "my_host".to_owned())
        .await
        .unwrap();
    let wal = WalObjectStore::new_without_replay(
        time_provider,
        Arc::clone(&object_store),
        "my_host",
        notifier,
        WalConfig {
            max_write_buffer_size: 100,
            flush_interval: Duration::from_secs(1),
            snapshot_size: 2,
            gen1_duration: Gen1Duration::new_1m(),
            ..Default::default()
        },
        Some(WalFileSequenceNumber::new(4)),
        None,
        &paths,
        10,
        shutdown.clone(),
    );

    wal.replay(Some(WalFileSequenceNumber::new(4)), &paths, 2, false)
        .await
        .unwrap();
    assert_eq!(
        wal.flush_buffer
            .lock()
            .await
            .wal_buffer
            .wal_file_sequence_number,
        WalFileSequenceNumber::new(7),
        "replay of the valid file rewound the sequence onto the corrupt file"
    );

    wal.write_ops_unconfirmed(vec![op()]).await.unwrap();
    assert!(wal.flush_buffer(true).await.is_some());
    assert!(
        !shutdown.is_cancelled(),
        "first post-recovery write shut down the node"
    );
    object_store
        .get(&wal_path("my_host", WalFileSequenceNumber::new(7)))
        .await
        .expect("first post-recovery WAL must use sequence 7");
}

/// Snapshot metadata can be newer than the file listing. Never move backwards,
/// and ignore paths that do not encode a WAL sequence number.
#[tokio::test]
async fn next_wal_sequence_uses_greater_of_snapshot_and_disk() {
    let paths = vec![
        Path::from("my_host/wal/00000000008.wal"),
        Path::from("my_host/wal/invalid.wal"),
        Path::from("my_host/wal/00000000005.wal"),
    ];
    for (snapshot, expected) in [(Some(4), 9), (Some(12), 13), (None, 9)] {
        let wal = WalObjectStore::new_without_replay(
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
            Arc::new(InMemory::new()),
            "my_host",
            Arc::new(TestNotifier::default()),
            WalConfig::default(),
            snapshot.map(WalFileSequenceNumber::new),
            None,
            &paths,
            10,
            CancellationToken::new(),
        );
        assert_eq!(
            wal.flush_buffer
                .lock()
                .await
                .wal_buffer
                .wal_file_sequence_number,
            WalFileSequenceNumber::new(expected)
        );
    }
}

// ---------------------------------------------------------------------------
// Extended recovery coverage for issue #26970.
// ---------------------------------------------------------------------------

fn recovery_config() -> WalConfig {
    WalConfig {
        max_write_buffer_size: 100,
        flush_interval: Duration::from_secs(1),
        snapshot_size: 2,
        gen1_duration: Gen1Duration::new_1m(),
        ..Default::default()
    }
}

/// A snapshot size large enough that no snapshot is ever triggered, so tests
/// can observe WAL sequence numbers in isolation.
fn no_snapshot_config() -> WalConfig {
    WalConfig {
        snapshot_size: 1_000_000,
        ..recovery_config()
    }
}

async fn put_zero_byte_wal(object_store: &Arc<dyn ObjectStore>, sequence: u64) {
    put_zero_byte_wal_at(object_store, "my_host", sequence).await;
}

async fn put_zero_byte_wal_at(object_store: &Arc<dyn ObjectStore>, prefix: &str, sequence: u64) {
    object_store
        .put(
            &wal_path(prefix, WalFileSequenceNumber::new(sequence)),
            PutPayload::from_static(b""),
        )
        .await
        .unwrap();
}

async fn recover_wal_with_config(
    object_store: Arc<dyn ObjectStore>,
    notifier: Arc<dyn WalFileNotifier>,
    config: WalConfig,
    last_wal_sequence_number: Option<WalFileSequenceNumber>,
    fail_on_error: bool,
) -> Result<WalObjectStore, crate::Error> {
    let paths = load_all_wal_file_paths(Arc::clone(&object_store), "my_host".to_owned())
        .await
        .unwrap();
    let wal = WalObjectStore::new_without_replay(
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
        Arc::clone(&object_store),
        "my_host",
        notifier,
        config,
        last_wal_sequence_number,
        None,
        &paths,
        10,
        CancellationToken::new(),
    );
    wal.replay(last_wal_sequence_number, &paths, 2, fail_on_error)
        .await?;
    Ok(wal)
}

async fn recover_wal(
    object_store: Arc<dyn ObjectStore>,
    notifier: Arc<dyn WalFileNotifier>,
    last_wal_sequence_number: Option<WalFileSequenceNumber>,
) -> WalObjectStore {
    recover_wal_with_config(
        object_store,
        notifier,
        recovery_config(),
        last_wal_sequence_number,
        false,
    )
    .await
    .unwrap()
}

async fn next_sequence(wal: &WalObjectStore) -> u64 {
    wal.flush_buffer
        .lock()
        .await
        .wal_buffer
        .wal_file_sequence_number
        .as_u64()
}

async fn wal_file_len(object_store: &Arc<dyn ObjectStore>, sequence: u64) -> usize {
    object_store
        .get(&wal_path("my_host", WalFileSequenceNumber::new(sequence)))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .len()
}

/// Buffer a single write and flush it without forcing a snapshot. Returns the
/// sequence number the flushed file was written to.
async fn write_and_flush(wal: &WalObjectStore, object_store: &Arc<dyn ObjectStore>) -> u64 {
    let before = next_sequence(wal).await;
    wal.write_ops_unconfirmed(vec![op()]).await.unwrap();
    wal.flush_buffer(false).await;
    assert!(
        object_store
            .get(&wal_path("my_host", WalFileSequenceNumber::new(before)))
            .await
            .is_ok(),
        "expected a WAL file at sequence {before}"
    );
    assert_eq!(next_sequence(wal).await, before + 1);
    before
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_zero_byte_wal_recovers_without_collision() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
    put_zero_byte_wal(&object_store, 5).await;

    let wal = recover_wal(
        Arc::clone(&object_store),
        Arc::clone(&notifier),
        Some(WalFileSequenceNumber::new(4)),
    )
    .await;

    assert_eq!(next_sequence(&wal).await, 6);
    assert_eq!(write_and_flush(&wal, &object_store).await, 6);
    assert_eq!(
        wal_file_len(&object_store, 5).await,
        0,
        "corrupt file changed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_consecutive_zero_byte_wals_recover_without_collision() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
    for sequence in 5..=8 {
        put_zero_byte_wal(&object_store, sequence).await;
    }

    let wal = recover_wal(
        Arc::clone(&object_store),
        Arc::clone(&notifier),
        Some(WalFileSequenceNumber::new(4)),
    )
    .await;

    assert_eq!(next_sequence(&wal).await, 9);
    assert_eq!(write_and_flush(&wal, &object_store).await, 9);
    for sequence in 5..=8 {
        assert_eq!(wal_file_len(&object_store, sequence).await, 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_files_mixed_with_valid_files_recover_in_order() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let notifier = Arc::new(TestNotifier::default());
    // 5 valid, 6 corrupt, 7 valid, 8 corrupt.
    put_valid_wal_file(&object_store, 5, &wal_contents_for(5)).await;
    put_zero_byte_wal(&object_store, 6).await;
    put_valid_wal_file(&object_store, 7, &wal_contents_for(7)).await;
    put_zero_byte_wal(&object_store, 8).await;

    let wal = recover_wal(
        Arc::clone(&object_store),
        Arc::clone(&notifier) as Arc<dyn WalFileNotifier>,
        Some(WalFileSequenceNumber::new(4)),
    )
    .await;

    assert_eq!(next_sequence(&wal).await, 9);
    // Both valid files were replayed into the notifier, in order.
    let replayed: Vec<u64> = notifier
        .notified_writes
        .lock()
        .iter()
        .map(|contents| contents.wal_file_number.as_u64())
        .collect();
    assert_eq!(replayed, vec![5, 7]);

    assert_eq!(write_and_flush(&wal, &object_store).await, 9);
    assert_eq!(
        wal_file_len(&object_store, 6).await,
        0,
        "corrupt file changed"
    );
    assert_eq!(
        wal_file_len(&object_store, 8).await,
        0,
        "corrupt file changed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_behind_existing_wal_files_uses_disk_max() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
    put_valid_wal_file(&object_store, 6, &wal_contents_for(6)).await;
    put_zero_byte_wal(&object_store, 8).await;

    // Snapshot metadata lags the files on disk (4 < 8).
    let wal = recover_wal(
        Arc::clone(&object_store),
        Arc::clone(&notifier),
        Some(WalFileSequenceNumber::new(4)),
    )
    .await;

    assert_eq!(next_sequence(&wal).await, 9);
    assert_eq!(write_and_flush(&wal, &object_store).await, 9);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_ahead_of_existing_wal_files_is_not_rewound() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
    for sequence in 5..=8 {
        put_zero_byte_wal(&object_store, sequence).await;
    }

    // Snapshot metadata is ahead of every file on disk; those files are older
    // than the snapshot and must not be replayed, and the sequence must not
    // move backwards.
    let wal = recover_wal(
        Arc::clone(&object_store),
        Arc::clone(&notifier),
        Some(WalFileSequenceNumber::new(12)),
    )
    .await;

    assert_eq!(next_sequence(&wal).await, 13);
    assert_eq!(write_and_flush(&wal, &object_store).await, 13);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_wal_directory_starts_at_sequence_one() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());

    let wal = recover_wal(Arc::clone(&object_store), Arc::clone(&notifier), None).await;

    assert_eq!(next_sequence(&wal).await, 1);
    assert_eq!(write_and_flush(&wal, &object_store).await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_wal_filenames_are_ignored_when_choosing_sequence() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
    object_store
        .put(
            &Path::from("my_host/wal/not-a-number.wal"),
            PutPayload::from_static(b"junk"),
        )
        .await
        .unwrap();
    object_store
        .put(
            &Path::from("my_host/wal/README.txt"),
            PutPayload::from_static(b"notes"),
        )
        .await
        .unwrap();
    put_valid_wal_file(&object_store, 8, &wal_contents_for(8)).await;

    let wal = recover_wal(
        Arc::clone(&object_store),
        Arc::clone(&notifier),
        Some(WalFileSequenceNumber::new(4)),
    )
    .await;

    assert_eq!(next_sequence(&wal).await, 9);
    assert_eq!(write_and_flush(&wal, &object_store).await, 9);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_writes_and_flushes_after_recovery_increase_sequence() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
    put_zero_byte_wal(&object_store, 5).await;

    let wal = recover_wal_with_config(
        Arc::clone(&object_store),
        Arc::clone(&notifier),
        no_snapshot_config(),
        Some(WalFileSequenceNumber::new(4)),
        false,
    )
    .await
    .unwrap();

    assert_eq!(write_and_flush(&wal, &object_store).await, 6);
    assert_eq!(write_and_flush(&wal, &object_store).await, 7);
    assert_eq!(write_and_flush(&wal, &object_store).await, 8);
    assert_eq!(next_sequence(&wal).await, 9);
    // The corrupt file is still present and untouched.
    assert_eq!(wal_file_len(&object_store, 5).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_restart_and_replay_cycles_do_not_collide() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());

    // A 0-byte file left behind by the first unclean shutdown.
    put_zero_byte_wal(&object_store, 5).await;

    for expected_first_sequence in (6..).take(3) {
        let wal = recover_wal_with_config(
            Arc::clone(&object_store),
            Arc::clone(&notifier),
            no_snapshot_config(),
            Some(WalFileSequenceNumber::new(4)),
            false,
        )
        .await
        .unwrap();

        let written = write_and_flush(&wal, &object_store).await;
        assert_eq!(written, expected_first_sequence);

        // Shutdown flushes nothing new; the file must survive for the next cycle.
        wal.shutdown().await;
        assert!(
            object_store
                .get(&wal_path("my_host", WalFileSequenceNumber::new(written)))
                .await
                .is_ok()
        );
    }

    assert_eq!(wal_file_len(&object_store, 5).await, 0);
}

/// Node identifiers are arbitrary strings and may contain `/`, which makes WAL
/// object paths deeper than three segments. The replay guard must still read
/// the sequence number, or a corrupt tail after a valid file collides on the
/// first flush.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_node_identifier_recovers_corrupt_tail_after_valid_file() {
    const PREFIX: &str = "org/host";
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());

    let bytes = crate::serialize::serialize_to_file_bytes(&wal_contents_for(5)).unwrap();
    object_store
        .put(
            &wal_path(PREFIX, WalFileSequenceNumber::new(5)),
            PutPayload::from_bytes(Bytes::from(bytes)),
        )
        .await
        .unwrap();
    put_zero_byte_wal_at(&object_store, PREFIX, 6).await;

    let paths = load_all_wal_file_paths(Arc::clone(&object_store), PREFIX.to_owned())
        .await
        .unwrap();
    let wal = WalObjectStore::new_without_replay(
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
        Arc::clone(&object_store),
        PREFIX,
        notifier,
        no_snapshot_config(),
        Some(WalFileSequenceNumber::new(4)),
        None,
        &paths,
        10,
        CancellationToken::new(),
    );
    wal.replay(Some(WalFileSequenceNumber::new(4)), &paths, 2, false)
        .await
        .unwrap();

    assert_eq!(next_sequence(&wal).await, 7);
    wal.write_ops_unconfirmed(vec![op()]).await.unwrap();
    wal.flush_buffer(false).await;
    object_store
        .get(&wal_path(PREFIX, WalFileSequenceNumber::new(7)))
        .await
        .expect("first post-recovery WAL must use sequence 7");
    assert_eq!(
        object_store
            .get(&wal_path(PREFIX, WalFileSequenceNumber::new(6)))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .len(),
        0,
        "corrupt file changed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn strict_replay_mode_still_fails_on_corrupt_files() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let notifier: Arc<dyn WalFileNotifier> = Arc::new(TestNotifier::default());
    put_valid_wal_file(&object_store, 5, &wal_contents_for(5)).await;
    put_zero_byte_wal(&object_store, 6).await;

    let err = recover_wal_with_config(
        Arc::clone(&object_store),
        Arc::clone(&notifier),
        recovery_config(),
        Some(WalFileSequenceNumber::new(4)),
        true,
    )
    .await
    .expect_err("strict replay must surface corruption");

    assert!(
        matches!(err, crate::Error::Serialize(_)),
        "unexpected error: {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_preserves_valid_historical_records() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let notifier = Arc::new(TestNotifier::default());
    put_valid_wal_file(&object_store, 5, &wal_contents_for(5)).await;
    let valid_bytes = object_store
        .get(&wal_path("my_host", WalFileSequenceNumber::new(5)))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    let wal = recover_wal(
        Arc::clone(&object_store),
        Arc::clone(&notifier) as Arc<dyn WalFileNotifier>,
        Some(WalFileSequenceNumber::new(4)),
    )
    .await;

    // The historical record reached the notifier.
    assert_eq!(notifier.notified_writes.lock().len(), 1);
    assert_eq!(write_and_flush(&wal, &object_store).await, 6);
    // The historical file is byte-for-byte intact.
    assert_eq!(
        object_store
            .get(&wal_path("my_host", WalFileSequenceNumber::new(5)))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        valid_bytes
    );
}
