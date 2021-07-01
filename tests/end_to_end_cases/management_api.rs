use std::{collections::HashSet, fs::set_permissions, os::unix::fs::PermissionsExt};

use generated_types::{
    google::protobuf::{Duration, Empty},
    influxdata::iox::management::v1::{database_rules::RoutingRules, *},
};
use influxdb_iox_client::{management::CreateDatabaseError, operations, write::WriteError};

use test_helpers::assert_contains;

use super::scenario::{
    create_readable_database, create_two_partition_database, create_unreadable_database, rand_name,
};
use crate::common::server_fixture::ServerFixture;
use std::time::Instant;
use tonic::Code;

#[tokio::test]
async fn test_serving_readiness() {
    let server_fixture = ServerFixture::create_single_use().await;
    let mut mgmt_client = server_fixture.management_client();
    let mut write_client = server_fixture.write_client();

    let name = "foo";
    let lp_data = "bar baz=1 10";

    mgmt_client
        .update_server_id(42)
        .await
        .expect("set ID failed");
    server_fixture.wait_server_initialized().await;
    mgmt_client
        .create_database(DatabaseRules {
            name: name.to_string(),
            ..Default::default()
        })
        .await
        .expect("create database failed");

    mgmt_client.set_serving_readiness(false).await.unwrap();
    let err = write_client.write(name, lp_data).await.unwrap_err();
    assert!(
        matches!(&err, WriteError::ServerError(status) if status.code() == Code::Unavailable),
        "{}",
        &err
    );

    mgmt_client.set_serving_readiness(true).await.unwrap();
    write_client.write(name, lp_data).await.unwrap();
}

#[tokio::test]
async fn test_list_update_remotes() {
    let server_fixture = ServerFixture::create_single_use().await;
    let mut client = server_fixture.management_client();

    const TEST_REMOTE_ID_1: u32 = 42;
    const TEST_REMOTE_ADDR_1: &str = "1.2.3.4:1234";
    const TEST_REMOTE_ID_2: u32 = 84;
    const TEST_REMOTE_ADDR_2: &str = "4.3.2.1:4321";
    const TEST_REMOTE_ADDR_2_UPDATED: &str = "40.30.20.10:4321";

    let res = client.list_remotes().await.expect("list remotes failed");
    assert_eq!(res.len(), 0);

    client
        .update_remote(TEST_REMOTE_ID_1, TEST_REMOTE_ADDR_1)
        .await
        .expect("update failed");

    let res = client.list_remotes().await.expect("list remotes failed");
    assert_eq!(res.len(), 1);

    client
        .update_remote(TEST_REMOTE_ID_2, TEST_REMOTE_ADDR_2)
        .await
        .expect("update failed");

    let res = client.list_remotes().await.expect("list remotes failed");
    assert_eq!(res.len(), 2);
    assert_eq!(res[0].id, TEST_REMOTE_ID_1);
    assert_eq!(res[0].connection_string, TEST_REMOTE_ADDR_1);
    assert_eq!(res[1].id, TEST_REMOTE_ID_2);
    assert_eq!(res[1].connection_string, TEST_REMOTE_ADDR_2);

    client
        .delete_remote(TEST_REMOTE_ID_1)
        .await
        .expect("delete failed");

    client
        .delete_remote(TEST_REMOTE_ID_1)
        .await
        .expect_err("expected delete to fail");

    let res = client.list_remotes().await.expect("list remotes failed");
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].id, TEST_REMOTE_ID_2);
    assert_eq!(res[0].connection_string, TEST_REMOTE_ADDR_2);

    client
        .update_remote(TEST_REMOTE_ID_2, TEST_REMOTE_ADDR_2_UPDATED)
        .await
        .expect("update failed");

    let res = client.list_remotes().await.expect("list remotes failed");
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].id, TEST_REMOTE_ID_2);
    assert_eq!(res[0].connection_string, TEST_REMOTE_ADDR_2_UPDATED);
}

#[tokio::test]
async fn test_set_get_writer_id() {
    let server_fixture = ServerFixture::create_single_use().await;
    let mut client = server_fixture.management_client();

    const TEST_ID: u32 = 42;

    client
        .update_server_id(TEST_ID)
        .await
        .expect("set ID failed");

    let got = client.get_server_id().await.expect("get ID failed");

    assert_eq!(got.get(), TEST_ID);
}

#[tokio::test]
async fn test_create_database_duplicate_name() {
    let server_fixture = ServerFixture::create_shared().await;
    let mut client = server_fixture.management_client();

    let db_name = rand_name();

    client
        .create_database(DatabaseRules {
            name: db_name.clone(),
            ..Default::default()
        })
        .await
        .expect("create database failed");

    let err = client
        .create_database(DatabaseRules {
            name: db_name,
            ..Default::default()
        })
        .await
        .expect_err("create database failed");

    assert!(matches!(
        dbg!(err),
        CreateDatabaseError::DatabaseAlreadyExists
    ))
}

#[tokio::test]
async fn test_create_database_invalid_name() {
    let server_fixture = ServerFixture::create_shared().await;
    let mut client = server_fixture.management_client();

    let err = client
        .create_database(DatabaseRules {
            name: "my_example\ndb".to_string(),
            ..Default::default()
        })
        .await
        .expect_err("expected request to fail");

    assert!(matches!(dbg!(err), CreateDatabaseError::InvalidArgument(_)));
}

#[tokio::test]
async fn test_list_databases() {
    let server_fixture = ServerFixture::create_shared().await;
    let mut client = server_fixture.management_client();

    let name = rand_name();
    client
        .create_database(DatabaseRules {
            name: name.clone(),
            ..Default::default()
        })
        .await
        .expect("create database failed");

    let names = client
        .list_databases()
        .await
        .expect("list databases failed");
    assert!(names.contains(&name));
}

#[tokio::test]
async fn test_create_get_update_database() {
    let server_fixture = ServerFixture::create_shared().await;
    let mut client = server_fixture.management_client();

    let db_name = rand_name();

    // Specify everything to allow direct comparison between request and response
    // Otherwise would expect difference due to server-side defaulting
    let mut rules = DatabaseRules {
        name: db_name.clone(),
        partition_template: Some(PartitionTemplate {
            parts: vec![partition_template::Part {
                part: Some(partition_template::part::Part::Table(Empty {})),
            }],
        }),
        lifecycle_rules: Some(LifecycleRules {
            buffer_size_hard: 553,
            sort_order: Some(lifecycle_rules::SortOrder {
                order: Order::Asc as _,
                sort: Some(lifecycle_rules::sort_order::Sort::CreatedAtTime(Empty {})),
            }),
            catalog_transactions_until_checkpoint: 13,
            late_arrive_window_seconds: 423,
            worker_backoff_millis: 15,
            persist_row_threshold: 342,
            persist_age_threshold_seconds: 700,
            ..Default::default()
        }),
        routing_rules: None,
        worker_cleanup_avg_sleep: Some(Duration {
            seconds: 2,
            nanos: 0,
        }),
        write_buffer_connection_string: "".into(),
    };

    client
        .create_database(rules.clone())
        .await
        .expect("create database failed");

    let response = client
        .get_database(&db_name)
        .await
        .expect("get database failed");

    assert_eq!(response.routing_rules, None);

    rules.routing_rules = Some(RoutingRules::ShardConfig(ShardConfig {
        ignore_errors: true,
        ..Default::default()
    }));

    let updated_rules = client
        .update_database(rules.clone())
        .await
        .expect("update database failed");

    assert_eq!(updated_rules, rules);

    let response = client
        .get_database(&db_name)
        .await
        .expect("get database failed");

    assert!(matches!(
            response.routing_rules,
            Some(RoutingRules::ShardConfig(cfg)) if cfg.ignore_errors,
    ));
}

#[tokio::test]
async fn test_chunk_get() {
    use generated_types::influxdata::iox::management::v1::{Chunk, ChunkStorage};

    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    let lp_lines = vec![
        "cpu,region=west user=23.2 100",
        "cpu,region=west user=21.0 150",
        "disk,region=east bytes=99i 200",
    ];

    write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    let mut chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");

    // ensure the output order is consistent
    chunks.sort_by(|c1, c2| c1.partition_key.cmp(&c2.partition_key));

    // make sure there were timestamps prior to normalization
    assert!(
        chunks[0].time_of_first_write.is_some()
            && chunks[0].time_of_last_write.is_some()
            && chunks[0].time_closed.is_none(), // chunk is not yet closed
        "actual:{:#?}",
        chunks[0]
    );

    let chunks = normalize_chunks(chunks);

    let expected: Vec<Chunk> = vec![
        Chunk {
            partition_key: "cpu".into(),
            table_name: "cpu".into(),
            id: 0,
            storage: ChunkStorage::OpenMutableBuffer as i32,
            estimated_bytes: 100,
            row_count: 2,
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
        },
        Chunk {
            partition_key: "disk".into(),
            table_name: "disk".into(),
            id: 0,
            storage: ChunkStorage::OpenMutableBuffer as i32,
            estimated_bytes: 82,
            row_count: 1,
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
        },
    ];
    assert_eq!(
        expected, chunks,
        "expected:\n\n{:#?}\n\nactual:{:#?}",
        expected, chunks
    );
}

#[tokio::test]
async fn test_chunk_get_errors() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let db_name = rand_name();

    let err = management_client
        .list_chunks(&db_name)
        .await
        .expect_err("no db had been created");

    assert_contains!(
        err.to_string(),
        "Some requested entity was not found: Resource database"
    );

    create_unreadable_database(&db_name, fixture.grpc_channel()).await;
}

#[tokio::test]
async fn test_partition_list() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();

    let db_name = rand_name();
    create_two_partition_database(&db_name, fixture.grpc_channel()).await;

    let mut partitions = management_client
        .list_partitions(&db_name)
        .await
        .expect("listing partition");

    // ensure the output order is consistent
    partitions.sort_by(|p1, p2| p1.key.cmp(&p2.key));

    let expected = vec![
        Partition {
            key: "cpu".to_string(),
        },
        Partition {
            key: "mem".to_string(),
        },
    ];

    assert_eq!(
        expected, partitions,
        "expected:\n\n{:#?}\n\nactual:{:#?}",
        expected, partitions
    );
}

#[tokio::test]
async fn test_partition_list_error() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();

    let err = management_client
        .list_partitions("this database does not exist")
        .await
        .expect_err("expected error");

    assert_contains!(err.to_string(), "Database not found");
}

#[tokio::test]
async fn test_partition_get() {
    use generated_types::influxdata::iox::management::v1::Partition;

    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();

    let db_name = rand_name();
    create_two_partition_database(&db_name, fixture.grpc_channel()).await;

    let partition_key = "cpu";
    let partition = management_client
        .get_partition(&db_name, partition_key)
        .await
        .expect("getting partition");

    let expected = Partition { key: "cpu".into() };

    assert_eq!(
        expected, partition,
        "expected:\n\n{:#?}\n\nactual:{:#?}",
        expected, partition
    );
}

#[tokio::test]
async fn test_partition_get_error() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();

    let err = management_client
        .list_partitions("this database does not exist")
        .await
        .expect_err("expected error");

    assert_contains!(err.to_string(), "Database not found");

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    let lp_lines =
        vec!["processes,host=foo running=4i,sleeping=514i,total=519i 1591894310000000000"];

    write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    let err = management_client
        .get_partition(&db_name, "non existent partition")
        .await
        .expect_err("exepcted error getting partition");

    assert_contains!(err.to_string(), "Partition not found");
}

#[tokio::test]
async fn test_list_partition_chunks() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    let lp_lines = vec![
        "cpu,region=west user=23.2 100",
        "cpu,region=west user=21.0 150",
        "disk,region=east bytes=99i 200",
    ];

    write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    let partition_key = "cpu";
    let chunks = management_client
        .list_partition_chunks(&db_name, partition_key)
        .await
        .expect("getting partition chunks");

    let chunks = normalize_chunks(chunks);

    let expected: Vec<Chunk> = vec![Chunk {
        partition_key: "cpu".into(),
        table_name: "cpu".into(),
        id: 0,
        storage: ChunkStorage::OpenMutableBuffer as i32,
        estimated_bytes: 100,
        row_count: 2,
        time_of_first_write: None,
        time_of_last_write: None,
        time_closed: None,
    }];

    assert_eq!(
        expected, chunks,
        "expected:\n\n{:#?}\n\nactual:{:#?}",
        expected, chunks
    );
}

#[tokio::test]
async fn test_list_partition_chunk_errors() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let db_name = rand_name();

    let err = management_client
        .list_partition_chunks(&db_name, "cpu")
        .await
        .expect_err("no db had been created");

    assert_contains!(
        err.to_string(),
        "Some requested entity was not found: Resource database"
    );
}

#[tokio::test]
async fn test_new_partition_chunk() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    let lp_lines = vec!["cpu,region=west user=23.2 100"];

    write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");

    assert_eq!(chunks.len(), 1, "Chunks: {:#?}", chunks);
    let partition_key = "cpu";
    let table_name = "cpu";

    // Rollover the a second chunk
    management_client
        .new_partition_chunk(&db_name, table_name, partition_key)
        .await
        .expect("new partition chunk");

    // Load some more data and now expect that we have a second chunk

    let lp_lines = vec!["cpu,region=west user=21.0 150"];

    write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeeded");

    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");

    assert_eq!(chunks.len(), 2, "Chunks: {:#?}", chunks);

    // Made all chunks in the same partition
    assert_eq!(
        chunks.iter().filter(|c| c.partition_key == "cpu").count(),
        2,
        "Chunks: {:#?}",
        chunks
    );

    // Rollover a (currently non existent) partition which is not OK
    let err = management_client
        .new_partition_chunk(&db_name, table_name, "non_existent_partition")
        .await
        .expect_err("new partition chunk");

    assert_eq!(
        "Resource partition/cpu:non_existent_partition not found",
        err.to_string()
    );

    // Rollover a (currently non existent) table in an existing partition which is not OK
    let err = management_client
        .new_partition_chunk(&db_name, "non_existing_table", partition_key)
        .await
        .expect_err("new partition chunk");

    assert_eq!(
        "Resource table/non_existing_table not found",
        err.to_string()
    );
}

#[tokio::test]
async fn test_new_partition_chunk_error() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();

    let err = management_client
        .new_partition_chunk(
            "this database does not exist",
            "nor_does_this_table",
            "nor_does_this_partition",
        )
        .await
        .expect_err("expected error");

    assert_contains!(
        err.to_string(),
        "Resource database/this database does not exist not found"
    );
}

#[tokio::test]
async fn test_close_partition_chunk() {
    use influxdb_iox_client::management::generated_types::operation_metadata::Job;
    use influxdb_iox_client::management::generated_types::ChunkStorage;

    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();
    let mut operations_client = fixture.operations_client();

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    let partition_key = "cpu";
    let table_name = "cpu";
    let lp_lines = vec!["cpu,region=west user=23.2 100"];

    write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");

    assert_eq!(chunks.len(), 1, "Chunks: {:#?}", chunks);
    assert_eq!(chunks[0].id, 0);
    assert_eq!(chunks[0].storage, ChunkStorage::OpenMutableBuffer as i32);

    // Move the chunk to read buffer
    let operation = management_client
        .close_partition_chunk(&db_name, table_name, partition_key, 0)
        .await
        .expect("new partition chunk");

    println!("Operation response is {:?}", operation);
    let operation_id = operation.id();

    let meta = operations::ClientOperation::try_new(operation)
        .unwrap()
        .metadata();

    // ensure we got a legit job description back
    if let Some(Job::CloseChunk(close_chunk)) = meta.job {
        assert_eq!(close_chunk.db_name, db_name);
        assert_eq!(close_chunk.partition_key, partition_key);
        assert_eq!(close_chunk.chunk_id, 0);
    } else {
        panic!("unexpected job returned")
    };

    // wait for the job to be done
    operations_client
        .wait_operation(operation_id, Some(std::time::Duration::from_secs(1)))
        .await
        .expect("failed to wait operation");

    // And now the chunk  should be good
    let mut chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");
    chunks.sort_by(|c1, c2| c1.id.cmp(&c2.id));

    assert_eq!(chunks.len(), 1, "Chunks: {:#?}", chunks);
    assert_eq!(chunks[0].id, 0);
    assert_eq!(chunks[0].storage, ChunkStorage::ReadBuffer as i32);
}

#[tokio::test]
async fn test_close_partition_chunk_error() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();

    let err = management_client
        .close_partition_chunk(
            "this database does not exist",
            "nor_does_this_table",
            "nor_does_this_partition",
            0,
        )
        .await
        .expect_err("expected error");

    assert_contains!(err.to_string(), "Database not found");
}

#[tokio::test]
async fn test_chunk_lifecycle() {
    use influxdb_iox_client::management::generated_types::ChunkStorage;

    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    management_client
        .create_database(DatabaseRules {
            name: db_name.clone(),
            lifecycle_rules: Some(LifecycleRules {
                mutable_linger_seconds: 1,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();

    let lp_lines = vec!["cpu,region=west user=23.2 100"];

    write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].storage, ChunkStorage::OpenMutableBuffer as i32);

    let start = Instant::now();
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let chunks = management_client
            .list_chunks(&db_name)
            .await
            .expect("listing chunks");

        assert_eq!(chunks.len(), 1);
        if chunks[0].storage == ChunkStorage::ReadBuffer as i32 {
            break;
        }

        if start.elapsed().as_secs_f64() > 10. {
            panic!("chunk failed to transition to read buffer after 10 seconds")
        }
    }
}

#[tokio::test]
async fn test_wipe_preserved_catalog() {
    use influxdb_iox_client::management::generated_types::operation_metadata::Job;

    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let mut operations_client = fixture.operations_client();

    let db_name = rand_name();

    let operation = management_client
        .wipe_persisted_catalog(&db_name)
        .await
        .expect("wipe persisted catalog");

    println!("Operation response is {:?}", operation);
    let operation_id = operation.id();

    let meta = operations::ClientOperation::try_new(operation)
        .unwrap()
        .metadata();

    // ensure we got a legit job description back
    if let Some(Job::WipePreservedCatalog(wipe_persisted_catalog)) = meta.job {
        assert_eq!(wipe_persisted_catalog.db_name, db_name);
    } else {
        panic!("unexpected job returned")
    };

    // wait for the job to be done
    operations_client
        .wait_operation(operation_id, Some(std::time::Duration::from_secs(1)))
        .await
        .expect("failed to wait operation");
}

/// Normalizes a set of Chunks for comparison by removing timestamps
fn normalize_chunks(chunks: Vec<Chunk>) -> Vec<Chunk> {
    chunks
        .into_iter()
        .map(|summary| {
            let Chunk {
                partition_key,
                table_name,
                id,
                storage,
                estimated_bytes,
                row_count,
                ..
            } = summary;
            Chunk {
                partition_key,
                table_name,
                id,
                storage,
                estimated_bytes,
                row_count,
                time_of_first_write: None,
                time_of_last_write: None,
                time_closed: None,
            }
        })
        .collect::<Vec<_>>()
}

#[tokio::test]
async fn test_get_server_status_ok() {
    let server_fixture = ServerFixture::create_single_use().await;
    let mut client = server_fixture.management_client();

    // not initalized
    let status = client.get_server_status().await.unwrap();
    assert!(!status.initialized);

    // initialize
    client.update_server_id(42).await.expect("set ID failed");
    server_fixture.wait_server_initialized().await;

    // now initalized
    let status = client.get_server_status().await.unwrap();
    assert!(status.initialized);

    // create DBs
    let db_name1 = rand_name();
    let db_name2 = rand_name();
    client
        .create_database(DatabaseRules {
            name: db_name1.clone(),
            ..Default::default()
        })
        .await
        .expect("create database failed");

    client
        .create_database(DatabaseRules {
            name: db_name2.clone(),
            ..Default::default()
        })
        .await
        .expect("create database failed");

    // databases are listed
    let status = client.get_server_status().await.unwrap();
    let names_actual: HashSet<_> = status
        .database_statuses
        .iter()
        .map(|db_status| db_status.db_name.clone())
        .collect();
    let names_expected: HashSet<_> = [db_name1, db_name2].iter().cloned().collect();
    assert_eq!(names_actual, names_expected);
}

#[tokio::test]
async fn test_get_server_status_global_error() {
    let server_fixture = ServerFixture::create_single_use().await;
    let mut client = server_fixture.management_client();

    // we need to "break" the object store AFTER the server was started, otherwise the server process will exit
    // immediately
    let metadata = server_fixture.dir().metadata().unwrap();
    let mut permissions = metadata.permissions();
    permissions.set_mode(0o000);
    set_permissions(server_fixture.dir(), permissions).unwrap();

    // setup server
    client.update_server_id(42).await.expect("set ID failed");

    let check = async {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));

        loop {
            let status = client.get_server_status().await.unwrap();
            if let Some(err) = status.error {
                assert!(dbg!(err.message).starts_with("store error:"));
                return;
            }

            interval.tick().await;
        }
    };
    let check = tokio::time::timeout(std::time::Duration::from_secs(10), check);
    check.await.unwrap();
}

#[tokio::test]
async fn test_get_server_status_db_error() {
    let server_fixture = ServerFixture::create_single_use().await;
    let mut client = server_fixture.management_client();

    // create malformed DB config
    let mut path = server_fixture.dir().to_path_buf();
    path.push("42");
    path.push("my_db");
    std::fs::create_dir_all(path.clone()).unwrap();
    path.push("rules.pb");
    std::fs::write(path, "foo").unwrap();

    // initialize
    client.update_server_id(42).await.expect("set ID failed");
    server_fixture.wait_server_initialized().await;

    // check for errors
    let status = client.get_server_status().await.unwrap();
    assert!(status.initialized);
    assert_eq!(status.error, None);
    assert_eq!(status.database_statuses.len(), 1);
    let db_status = &status.database_statuses[0];
    assert_eq!(db_status.db_name, "my_db");
    assert!(dbg!(&db_status.error.as_ref().unwrap().message)
        .starts_with("error deserializing database rules from protobuf:"));
}
