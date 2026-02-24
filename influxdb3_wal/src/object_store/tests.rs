use super::*;
use crate::{
    Field, FieldData, Gen1Duration, Row, SnapshotSequenceNumber, TableChunk, TableChunks, create,
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
    wal.write_ops_unconfirmed(vec![op1.clone()]).await.unwrap();

    let op2 = WalOp::Write(WriteBatch {
        catalog_sequence: 0,
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
    wal.write_ops_unconfirmed(vec![op2.clone()]).await.unwrap();
    assert!(wal.flush_buffer(false).await.is_none());

    let file_2_contents = create::wal_contents(
        (62_000_000_000, 62_000_000_000, 2),
        [WalOp::Write(WriteBatch {
            catalog_sequence: 0,
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
