use std::{fs::set_permissions, os::unix::fs::PermissionsExt};

use arrow_util::assert_batches_sorted_eq;
use generated_types::{
    google::protobuf::{Duration, Empty},
    influxdata::iox::management::v1::{
        database_rules::RoutingRules, database_status::DatabaseState, *,
    },
};
use influxdb_iox_client::{
    management::{Client, CreateDatabaseError},
    operations,
    write::WriteError,
};

use test_helpers::assert_contains;

use super::scenario::{
    create_readable_database, create_two_partition_database, create_unreadable_database, rand_name,
};
use crate::{
    common::server_fixture::ServerFixture,
    end_to_end_cases::scenario::{
        fixture_broken_catalog, wait_for_exact_chunk_states, DatabaseBuilder,
    },
};
use chrono::{DateTime, Utc};
use std::convert::TryInto;
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

    let name1 = rand_name();
    let rules1 = DatabaseRules {
        name: name1.clone(),
        ..Default::default()
    };
    client
        .create_database(rules1)
        .await
        .expect("create database failed");

    let name2 = rand_name();
    // Only set the worker cleanup rules.
    let rules2 = DatabaseRules {
        name: name2.clone(),
        worker_cleanup_avg_sleep: Some(Duration {
            seconds: 2,
            nanos: 0,
        }),
        ..Default::default()
    };
    client
        .create_database(rules2)
        .await
        .expect("create database failed");

    // By default, should get both databases names back
    let omit_defaults = false;
    let databases: Vec<_> = client
        .list_databases(omit_defaults)
        .await
        .expect("list databases failed")
        .into_iter()
        // names may contain the names of other databases created by
        // concurrent tests as well
        .filter(|rules| rules.name == name1 || rules.name == name2)
        .collect();

    let names: Vec<_> = databases.iter().map(|rules| rules.name.clone()).collect();

    assert!(dbg!(&names).contains(&name1));
    assert!(dbg!(&names).contains(&name2));

    // validate that both rules have the defaults filled in
    for rules in &databases {
        assert!(rules.lifecycle_rules.is_some());
    }

    // validate that neither database appears in the list of deleted databases
    let deleted_databases = client
        .list_deleted_databases()
        .await
        .expect("list deleted databases failed");
    let names: Vec<_> = deleted_databases.into_iter().map(|db| db.db_name).collect();
    assert!(!names.contains(&name1));
    assert!(!names.contains(&name2));

    // now fetch without defaults, and neither should have their rules filled in
    let omit_defaults = true;
    let databases: Vec<_> = client
        .list_databases(omit_defaults)
        .await
        .expect("list databases failed")
        .into_iter()
        // names may contain the names of other databases created by
        // concurrent tests as well
        .filter(|rules| rules.name == name1 || rules.name == name2)
        .collect();

    let names: Vec<_> = databases.iter().map(|rules| rules.name.clone()).collect();
    assert!(dbg!(&names).contains(&name1));
    assert!(dbg!(&names).contains(&name2));

    for rules in &databases {
        assert!(rules.lifecycle_rules.is_none());
    }

    // now delete one of the databases; it should not appear whether we're omitting defaults or not
    client.delete_database(&name1).await.unwrap();

    let omit_defaults = false;
    let databases: Vec<_> = client
        .list_databases(omit_defaults)
        .await
        .expect("list databases failed")
        .into_iter()
        // names may contain the names of other databases created by
        // concurrent tests as well
        .filter(|rules| rules.name == name1 || rules.name == name2)
        .collect();

    let names: Vec<_> = databases.iter().map(|rules| rules.name.clone()).collect();

    assert!(!dbg!(&names).contains(&name1));
    assert!(dbg!(&names).contains(&name2));

    let omit_defaults = true;
    let databases: Vec<_> = client
        .list_databases(omit_defaults)
        .await
        .expect("list databases failed")
        .into_iter()
        // names may contain the names of other databases created by
        // concurrent tests as well
        .filter(|rules| rules.name == name1 || rules.name == name2)
        .collect();

    let names: Vec<_> = databases.iter().map(|rules| rules.name.clone()).collect();
    assert!(!dbg!(&names).contains(&name1));
    assert!(dbg!(&names).contains(&name2));

    // The deleted database should be included in the list of deleted databases
    let deleted_databases = client
        .list_deleted_databases()
        .await
        .expect("list deleted databases failed");

    assert!(
        deleted_databases
            .iter()
            .any(|db| db.db_name == name1 && db.generation_id == 0),
        "could not find expected database in {:?}",
        deleted_databases
    );
}

#[tokio::test]
async fn test_create_get_update_delete_database() {
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
            catalog_transactions_until_checkpoint: 13,
            late_arrive_window_seconds: 423,
            worker_backoff_millis: 15,
            max_active_compactions_cfg: Some(
                lifecycle_rules::MaxActiveCompactionsCfg::MaxActiveCompactions(8),
            ),
            persist_row_threshold: 342,
            persist_age_threshold_seconds: 700,
            mub_row_threshold: 1343,
            ..Default::default()
        }),
        routing_rules: None,
        worker_cleanup_avg_sleep: Some(Duration {
            seconds: 2,
            nanos: 0,
        }),
        write_buffer_connection: None,
    };

    client
        .create_database(rules.clone())
        .await
        .expect("create database failed");

    let response = client
        .get_database(&db_name, false)
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
        .get_database(&db_name, false)
        .await
        .expect("get database failed");

    assert!(matches!(
            response.routing_rules,
            Some(RoutingRules::ShardConfig(cfg)) if cfg.ignore_errors,
    ));

    client
        .delete_database(&db_name)
        .await
        .expect("delete database failed");

    let err = client
        .get_database(&db_name, false)
        .await
        .expect_err("get database should have failed but didn't");

    assert_contains!(err.to_string(), "Database not found");
}

/// gets configuration both with and without defaults, and verifies
/// that the worker_cleanup_avg_sleep field is the same and that
/// lifecycle_rules are not present except when defaults are filled in
async fn assert_rule_defaults(client: &mut Client, db_name: &str, provided_rules: &DatabaseRules) {
    assert!(provided_rules.lifecycle_rules.is_none());

    // Get the configuration, but do not get any defaults
    // No lifecycle rules should be present
    let response = client
        .get_database(db_name, true)
        .await
        .expect("get database failed");
    assert!(response.lifecycle_rules.is_none());
    assert_eq!(
        provided_rules.worker_cleanup_avg_sleep,
        response.worker_cleanup_avg_sleep
    );

    // Get the configuration, *with* defaults, and the lifecycle rules should be present
    let response = client
        .get_database(db_name, false) // with defaults
        .await
        .expect("get database failed");
    assert!(response.lifecycle_rules.is_some());
    assert_eq!(
        provided_rules.worker_cleanup_avg_sleep,
        response.worker_cleanup_avg_sleep
    );
}

#[tokio::test]
async fn test_create_get_update_database_omit_defaults() {
    // Test to ensure that the database remembers only the
    // configuration that it was sent, not including the default
    // values
    let server_fixture = ServerFixture::create_shared().await;
    let mut client = server_fixture.management_client();

    let db_name = rand_name();

    // Only set the worker cleanup rules.
    let mut rules = DatabaseRules {
        name: db_name.clone(),
        worker_cleanup_avg_sleep: Some(Duration {
            seconds: 2,
            nanos: 0,
        }),
        ..Default::default()
    };

    client
        .create_database(rules.clone())
        .await
        .expect("create database failed");

    assert_rule_defaults(&mut client, &db_name, &rules).await;

    // Now, modify the worker to cleanup rules
    rules.worker_cleanup_avg_sleep = Some(Duration {
        seconds: 20,
        nanos: 0,
    });
    let updated_rules = client
        .update_database(rules.clone())
        .await
        .expect("update database failed");
    assert!(updated_rules.lifecycle_rules.is_some());
    assert_eq!(
        rules.worker_cleanup_avg_sleep,
        updated_rules.worker_cleanup_avg_sleep
    );

    assert_rule_defaults(&mut client, &db_name, &rules).await;
}

#[tokio::test]
async fn test_chunk_get() {
    use generated_types::influxdata::iox::management::v1::{
        Chunk, ChunkLifecycleAction, ChunkStorage,
    };

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

    let lifecycle_action = ChunkLifecycleAction::Unspecified.into();

    let expected: Vec<Chunk> = vec![
        Chunk {
            partition_key: "cpu".into(),
            table_name: "cpu".into(),
            id: 0,
            storage: ChunkStorage::OpenMutableBuffer.into(),
            lifecycle_action,
            memory_bytes: 1016,
            object_store_bytes: 0,
            row_count: 2,
            time_of_last_access: None,
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
        },
        Chunk {
            partition_key: "disk".into(),
            table_name: "disk".into(),
            id: 0,
            storage: ChunkStorage::OpenMutableBuffer.into(),
            lifecycle_action,
            memory_bytes: 1018,
            object_store_bytes: 0,
            row_count: 1,
            time_of_last_access: None,
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
        storage: ChunkStorage::OpenMutableBuffer.into(),
        lifecycle_action: ChunkLifecycleAction::Unspecified.into(),
        memory_bytes: 1016,
        object_store_bytes: 0,
        row_count: 2,
        time_of_last_access: None,
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
                late_arrive_window_seconds: 1,
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
    let db_name = rand_name();

    //
    // Try to load broken catalog and error
    //

    let fixture = fixture_broken_catalog(&db_name).await;

    let mut management_client = fixture.management_client();
    let mut operations_client = fixture.operations_client();

    let status = fixture.wait_server_initialized().await;
    assert_eq!(status.database_statuses.len(), 1);

    let load_error = &status.database_statuses[0].error.as_ref().unwrap().message;
    assert_contains!(
        load_error,
        "error loading catalog: Cannot load preserved catalog"
    );

    //
    // Recover by wiping preserved catalog
    //

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

    let status = fixture.wait_server_initialized().await;
    assert_eq!(status.database_statuses.len(), 1);
    assert!(status.database_statuses[0].error.is_none());
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
                lifecycle_action,
                memory_bytes,
                object_store_bytes,
                row_count,
                ..
            } = summary;
            Chunk {
                partition_key,
                table_name,
                id,
                storage,
                lifecycle_action,
                row_count,
                time_of_last_access: None,
                time_of_first_write: None,
                time_of_last_write: None,
                time_closed: None,
                memory_bytes,
                object_store_bytes,
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
    // output is sorted by db name
    let (db_name1, db_name2) = if db_name1 < db_name2 {
        (db_name1, db_name2)
    } else {
        (db_name2, db_name1)
    };
    let status = client.get_server_status().await.unwrap();
    let names: Vec<_> = status
        .database_statuses
        .iter()
        .map(|db_status| db_status.db_name.clone())
        .collect();
    let errors: Vec<_> = status
        .database_statuses
        .iter()
        .map(|db_status| db_status.error.clone())
        .collect();
    let states: Vec<_> = status
        .database_statuses
        .iter()
        .map(|db_status| DatabaseState::from_i32(db_status.state).unwrap())
        .collect();
    assert_eq!(names, vec![db_name1, db_name2]);
    assert_eq!(errors, vec![None, None]);
    assert_eq!(
        states,
        vec![DatabaseState::Initialized, DatabaseState::Initialized]
    );
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
                assert!(dbg!(err.message).starts_with("error listing databases in object storage:"));
                assert!(status.database_statuses.is_empty());
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
    path.push("0");
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
        .contains("error deserializing database rules"));
    assert_eq!(
        DatabaseState::from_i32(db_status.state).unwrap(),
        DatabaseState::DatabaseObjectStoreFound
    );
}

#[tokio::test]
async fn test_unload_read_buffer() {
    use data_types::chunk_metadata::ChunkStorage;

    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();
    let mut management_client = fixture.management_client();

    let db_name = rand_name();
    DatabaseBuilder::new(db_name.clone())
        .persist(true)
        .persist_age_threshold_seconds(1)
        .late_arrive_window_seconds(1)
        .build(fixture.grpc_channel())
        .await;

    let lp_lines: Vec<_> = (0..1_000)
        .map(|i| format!("data,tag1=val{} x={} {}", i, i * 10, i))
        .collect();

    let num_lines_written = write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("successful write");
    assert_eq!(num_lines_written, 1000);

    wait_for_exact_chunk_states(
        &fixture,
        &db_name,
        vec![ChunkStorage::ReadBufferAndObjectStore],
        std::time::Duration::from_secs(5),
    )
    .await;

    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");
    assert_eq!(chunks.len(), 1);
    let chunk_id = chunks[0].id;
    let partition_key = &chunks[0].partition_key;

    management_client
        .unload_partition_chunk(&db_name, "data", &partition_key[..], chunk_id)
        .await
        .unwrap();
    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");
    assert_eq!(chunks.len(), 1);
    let storage: generated_types::influxdata::iox::management::v1::ChunkStorage =
        ChunkStorage::ObjectStoreOnly.into();
    let storage: i32 = storage.into();
    assert_eq!(chunks[0].storage, storage);
}

#[tokio::test]
async fn test_chunk_access_time() {
    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();
    let mut management_client = fixture.management_client();
    let mut flight_client = fixture.flight_client();

    let db_name = rand_name();
    DatabaseBuilder::new(db_name.clone())
        .build(fixture.grpc_channel())
        .await;

    write_client.write(&db_name, "cpu foo=1 10").await.unwrap();

    let to_datetime = |a: Option<&generated_types::google::protobuf::Timestamp>| -> DateTime<Utc> {
        a.unwrap().clone().try_into().unwrap()
    };

    let chunks = management_client.list_chunks(&db_name).await.unwrap();
    assert_eq!(chunks.len(), 1);
    let t0 = to_datetime(chunks[0].time_of_last_access.as_ref());

    flight_client
        .perform_query(&db_name, "select * from cpu;")
        .await
        .unwrap();

    let chunks = management_client.list_chunks(&db_name).await.unwrap();
    assert_eq!(chunks.len(), 1);
    let t1 = to_datetime(chunks[0].time_of_last_access.as_ref());

    flight_client
        .perform_query(&db_name, "select * from cpu;")
        .await
        .unwrap();

    let chunks = management_client.list_chunks(&db_name).await.unwrap();
    assert_eq!(chunks.len(), 1);
    let t2 = to_datetime(chunks[0].time_of_last_access.as_ref());

    write_client.write(&db_name, "cpu foo=1 20").await.unwrap();

    let chunks = management_client.list_chunks(&db_name).await.unwrap();
    assert_eq!(chunks.len(), 1);
    let t3 = to_datetime(chunks[0].time_of_last_access.as_ref());

    // This chunk should be pruned out and therefore not accessed by the query
    flight_client
        .perform_query(&db_name, "select * from cpu where foo = 2;")
        .await
        .unwrap();

    let chunks = management_client.list_chunks(&db_name).await.unwrap();
    assert_eq!(chunks.len(), 1);
    let t4 = to_datetime(chunks[0].time_of_last_access.as_ref());

    assert!(t0 < t1, "{} {}", t0, t1);
    assert!(t1 < t2, "{} {}", t1, t2);
    assert!(t2 < t3, "{} {}", t2, t3);
    assert_eq!(t3, t4)
}

#[tokio::test]
async fn test_drop_partition() {
    use data_types::chunk_metadata::ChunkStorage;

    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();
    let mut management_client = fixture.management_client();

    let db_name = rand_name();
    DatabaseBuilder::new(db_name.clone())
        .persist(true)
        .persist_age_threshold_seconds(1)
        .late_arrive_window_seconds(1)
        .build(fixture.grpc_channel())
        .await;

    let lp_lines: Vec<_> = (0..1_000)
        .map(|i| format!("data,tag1=val{} x={} {}", i, i * 10, i))
        .collect();

    let num_lines_written = write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("successful write");
    assert_eq!(num_lines_written, 1000);

    wait_for_exact_chunk_states(
        &fixture,
        &db_name,
        vec![ChunkStorage::ReadBufferAndObjectStore],
        std::time::Duration::from_secs(5),
    )
    .await;

    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");
    assert_eq!(chunks.len(), 1);
    let partition_key = &chunks[0].partition_key;

    management_client
        .drop_partition(&db_name, "data", &partition_key[..])
        .await
        .unwrap();
    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");
    assert_eq!(chunks.len(), 0);
}

#[tokio::test]
async fn test_drop_partition_error() {
    use data_types::chunk_metadata::ChunkStorage;

    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();
    let mut management_client = fixture.management_client();

    let db_name = rand_name();
    DatabaseBuilder::new(db_name.clone())
        .persist(true)
        .persist_age_threshold_seconds(1_000)
        .late_arrive_window_seconds(1_000)
        .build(fixture.grpc_channel())
        .await;

    let lp_lines: Vec<_> = (0..1_000)
        .map(|i| format!("data,tag1=val{} x={} {}", i, i * 10, i))
        .collect();

    let num_lines_written = write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("successful write");
    assert_eq!(num_lines_written, 1000);

    wait_for_exact_chunk_states(
        &fixture,
        &db_name,
        vec![ChunkStorage::OpenMutableBuffer],
        std::time::Duration::from_secs(5),
    )
    .await;

    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");
    assert_eq!(chunks.len(), 1);
    let partition_key = &chunks[0].partition_key;

    let err = management_client
        .drop_partition(&db_name, "data", &partition_key[..])
        .await
        .unwrap_err();
    assert_contains!(err.to_string(), "Cannot drop unpersisted chunk");
}

#[tokio::test]
async fn test_delete() {
    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();
    let mut management_client = fixture.management_client();
    let mut flight_client = fixture.flight_client();

    // DB name and rules
    let db_name = rand_name();
    let rules = DatabaseRules {
        name: db_name.clone(),
        ..Default::default()
    };

    // create that db
    management_client
        .create_database(rules.clone())
        .await
        .expect("create database failed");

    // Load a few rows of data
    let lp_lines = vec![
        "cpu,region=west user=23.2 100",
        "cpu,region=west user=21.0 150",
        "disk,region=east bytes=99i 200",
    ];

    let num_lines_written = write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    assert_eq!(num_lines_written, 3);

    // Query cpu
    let mut query_results = flight_client
        .perform_query(db_name.clone(), "select * from cpu")
        .await
        .unwrap();
    let batches = query_results.to_batches().await.unwrap();
    let expected = [
        "+--------+--------------------------------+------+",
        "| region | time                           | user |",
        "+--------+--------------------------------+------+",
        "| west   | 1970-01-01T00:00:00.000000100Z | 23.2 |",
        "| west   | 1970-01-01T00:00:00.000000150Z | 21   |",
        "+--------+--------------------------------+------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    // Delete some data
    // todo

    // query to verify data deleted
    // todo: when the delete is done and integrated, the below test must fail
    let mut query_results = flight_client
        .perform_query(db_name, "select * from cpu")
        .await
        .unwrap();
    let batches = query_results.to_batches().await.unwrap();
    let expected = [
        "+--------+--------------------------------+------+",
        "| region | time                           | user |",
        "+--------+--------------------------------+------+",
        "| west   | 1970-01-01T00:00:00.000000100Z | 23.2 |",
        "| west   | 1970-01-01T00:00:00.000000150Z | 21   |",
        "+--------+--------------------------------+------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    // Negative Delete test to get error messages
    // todo
}

#[tokio::test]
async fn test_persist_partition() {
    use data_types::chunk_metadata::ChunkStorage;

    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();
    let mut management_client = fixture.management_client();

    let db_name = rand_name();
    DatabaseBuilder::new(db_name.clone())
        .persist(true)
        .persist_age_threshold_seconds(1_000)
        .late_arrive_window_seconds(1)
        .build(fixture.grpc_channel())
        .await;

    let num_lines_written = write_client
        .write(&db_name, "data foo=1 10")
        .await
        .expect("successful write");
    assert_eq!(num_lines_written, 1);

    wait_for_exact_chunk_states(
        &fixture,
        &db_name,
        vec![ChunkStorage::OpenMutableBuffer],
        std::time::Duration::from_secs(5),
    )
    .await;

    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");
    assert_eq!(chunks.len(), 1);
    let partition_key = &chunks[0].partition_key;

    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    management_client
        .persist_partition(&db_name, "data", &partition_key[..])
        .await
        .unwrap();

    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");
    assert_eq!(chunks.len(), 1);
    assert_eq!(
        chunks[0].storage,
        generated_types::influxdata::iox::management::v1::ChunkStorage::ReadBufferAndObjectStore
            as i32
    );
}

#[tokio::test]
async fn test_persist_partition_error() {
    use data_types::chunk_metadata::ChunkStorage;

    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();
    let mut management_client = fixture.management_client();

    let db_name = rand_name();
    DatabaseBuilder::new(db_name.clone())
        .persist(true)
        .persist_age_threshold_seconds(1_000)
        .late_arrive_window_seconds(1_000)
        .build(fixture.grpc_channel())
        .await;

    let num_lines_written = write_client
        .write(&db_name, "data foo=1 10")
        .await
        .expect("successful write");
    assert_eq!(num_lines_written, 1);

    wait_for_exact_chunk_states(
        &fixture,
        &db_name,
        vec![ChunkStorage::OpenMutableBuffer],
        std::time::Duration::from_secs(5),
    )
    .await;

    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");
    assert_eq!(chunks.len(), 1);
    let partition_key = &chunks[0].partition_key;

    // there is no old data (late arrival window is 1000s) that can be persisted
    let err = management_client
        .persist_partition(&db_name, "data", &partition_key[..])
        .await
        .unwrap_err();
    assert_contains!(
        err.to_string(),
        "Cannot persist partition because it cannot be flushed at the moment"
    );
}
