use crate::{
    common::server_fixture::{ServerFixture, ServerType, TestConfig, DEFAULT_SERVER_ID},
    end_to_end_cases::{
        management_cli::setup_load_and_persist_two_partition_chunks,
        scenario::{
            create_readable_database, create_two_partition_database, create_unreadable_database,
            fixture_broken_catalog, rand_name, wait_for_exact_chunk_states, DatabaseBuilder,
        },
    },
};
use bytes::Bytes;
use data_types::chunk_metadata::ChunkId;
use generated_types::google::protobuf::{Duration, Empty};
use influxdb_iox_client::{
    error::Error,
    google::{PreconditionViolation, ResourceType},
    management::{
        generated_types::{database_status::DatabaseState, operation_metadata::Job, *},
        Client,
    },
};
use std::{fs::set_permissions, num::NonZeroU32, os::unix::fs::PermissionsExt, time::Instant};
use test_helpers::{assert_contains, assert_error};
use uuid::Uuid;

#[tokio::test]
async fn test_server_id() {
    let server_fixture = ServerFixture::create_single_use(ServerType::Database).await;
    let mut client = server_fixture.management_client();
    let r = client.list_chunks("foo").await;
    match r {
        Err(Error::FailedPrecondition(e)) => {
            let details = e.details.unwrap();
            assert_eq!(details, PreconditionViolation::ServerIdNotSet)
        }
        _ => panic!("unexpected response: {:?}", r),
    }
}

#[tokio::test]
async fn test_create_database_duplicate_name() {
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut client = server_fixture.management_client();

    let db_name = rand_name();

    client
        .create_database(DatabaseRules {
            name: db_name.clone(),
            ..Default::default()
        })
        .await
        .expect("create database failed");

    let r = client
        .create_database(DatabaseRules {
            name: db_name,
            ..Default::default()
        })
        .await;

    assert_error!(r, Error::AlreadyExists(_));
}

#[tokio::test]
async fn test_create_database_invalid_name() {
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut client = server_fixture.management_client();

    let r = client
        .create_database(DatabaseRules {
            name: "my_example\ndb".to_string(),
            ..Default::default()
        })
        .await;

    match r {
        Err(Error::InvalidArgument(e)) => {
            let details = e.details.unwrap();
            assert_eq!(&details.field, "rules.name")
        }
        _ => panic!("unexpected response: {:?}", r),
    }
}

#[tokio::test]
async fn test_list_databases() {
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;
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

    // now release one of the databases; it should not appear whether we're omitting defaults or not
    client.release_database(&name1, None).await.unwrap();

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
}

#[tokio::test]
async fn test_create_get_update_release_claim_database() {
    test_helpers::maybe_start_logging();
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;
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
            catalog_transaction_prune_age: Some(generated_types::google::protobuf::Duration {
                seconds: 11,
                nanos: 22,
            }),
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
        worker_cleanup_avg_sleep: Some(Duration {
            seconds: 2,
            nanos: 0,
        }),
        write_buffer_connection: None,
    };

    let created_uuid = client
        .create_database(rules.clone())
        .await
        .expect("create database failed");

    let response = client
        .get_database(&db_name, false)
        .await
        .expect("get database failed");

    assert_eq!(
        response
            .lifecycle_rules
            .unwrap()
            .catalog_transactions_until_checkpoint,
        13
    );

    rules
        .lifecycle_rules
        .as_mut()
        .unwrap()
        .catalog_transactions_until_checkpoint = 42;
    let updated_rules = client
        .update_database(rules.clone())
        .await
        .expect("update database failed");

    assert_eq!(updated_rules, rules);

    let response = client
        .get_database(&db_name, false)
        .await
        .expect("get database failed");

    assert_eq!(
        response
            .lifecycle_rules
            .unwrap()
            .catalog_transactions_until_checkpoint,
        42
    );

    assert_eq!(get_uuid(&mut client, &db_name).await, Some(created_uuid));

    let released_uuid = client.release_database(&db_name, None).await.unwrap();
    assert_eq!(created_uuid, released_uuid);

    let err = client.get_database(&db_name, false).await.unwrap_err();
    assert_contains!(
        err.to_string(),
        format!("Resource database/{} not found", db_name)
    );

    client.claim_database(released_uuid, false).await.unwrap();

    client.get_database(&db_name, false).await.unwrap();

    let err = client
        .claim_database(released_uuid, false)
        .await
        .unwrap_err();
    assert_contains!(
        err.to_string(),
        format!("Resource database_uuid/{} already exists", released_uuid)
    );

    match err {
        Error::AlreadyExists(e) => {
            let details = e.details.unwrap();
            assert_eq!(details.resource_type, ResourceType::DatabaseUuid);
            assert_eq!(details.resource_name, released_uuid.to_string());
        }
        _ => panic!("unexpected variant: {}", err),
    }

    let unknown_uuid = Uuid::new_v4();
    let err = client
        .claim_database(unknown_uuid, false)
        .await
        .unwrap_err();
    assert_contains!(
        err.to_string(),
        format!("Resource database_uuid/{} not found", unknown_uuid)
    );

    client.release_database(&db_name, None).await.unwrap();

    let newly_created_uuid = client
        .create_database(rules.clone())
        .await
        .expect("create database failed");

    assert_ne!(released_uuid, newly_created_uuid);

    let err = client
        .claim_database(released_uuid, false)
        .await
        .unwrap_err();
    assert_contains!(
        err.to_string(),
        format!("Resource database/{} already exists", db_name)
    );
}

/// queries the server and returns the uuid, if any, for the specified database
async fn get_uuid(client: &mut Client, db_name: &str) -> Option<Uuid> {
    let databases: Vec<_> = client
        .get_server_status()
        .await
        .expect("get_server_status failed")
        .database_statuses
        .into_iter()
        // names may contain the names of other databases created by
        // concurrent tests as well
        .filter(|db| db.db_name == db_name)
        .collect();

    assert!(
        databases.len() <= 1,
        "found more than one entry for {}: {:?}",
        db_name,
        databases
    );

    databases.into_iter().next().and_then(|db| {
        if !db.uuid.is_empty() {
            Some(Uuid::from_slice(&db.uuid).unwrap())
        } else {
            None
        }
    })
}

#[tokio::test]
async fn release_database() {
    test_helpers::maybe_start_logging();
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut client = server_fixture.management_client();

    let db_name = rand_name();
    let rules = DatabaseRules {
        name: db_name.clone(),
        ..Default::default()
    };

    // Create a database on one server
    let created_uuid = client.create_database(rules.clone()).await.unwrap();

    // Release database returns the UUID
    let released_uuid = client.release_database(&db_name, None).await.unwrap();
    assert_eq!(created_uuid, released_uuid);

    // Released database is no longer in this server's database list
    assert_eq!(get_uuid(&mut client, &db_name).await, None);

    // Releasing the same database again is an error
    let err = client.release_database(&db_name, None).await.unwrap_err();
    assert_contains!(
        err.to_string(),
        format!("Resource database/{} not found", db_name)
    );

    // Create another database
    let created_uuid = client.create_database(rules.clone()).await.unwrap();

    // If an optional UUID is specified, don't release the database if the UUID doesn't match
    let incorrect_uuid = Uuid::new_v4();
    let err = client
        .release_database(&db_name, Some(incorrect_uuid))
        .await
        .unwrap_err();
    assert_contains!(
        err.to_string(),
        format!(
            "Could not release {}: the UUID specified ({}) does not match the current UUID ({})",
            db_name, incorrect_uuid, created_uuid,
        )
    );

    // If an optional UUID is specified, release the database if the UUID does match
    let released_uuid = client
        .release_database(&db_name, Some(created_uuid))
        .await
        .unwrap();
    assert_eq!(created_uuid, released_uuid);
}

#[tokio::test]
async fn claim_database() {
    test_helpers::maybe_start_logging();
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut client = server_fixture.management_client();

    let db_name = rand_name();
    let rules = DatabaseRules {
        name: db_name.clone(),
        ..Default::default()
    };

    // Create a database on one server
    let created_uuid = client.create_database(rules.clone()).await.unwrap();

    // Release database returns the UUID
    let deleted_uuid = client.release_database(&db_name, None).await.unwrap();
    assert_eq!(created_uuid, deleted_uuid);

    client.claim_database(deleted_uuid, false).await.unwrap();

    // Claimed database is back in this server's database list
    assert_eq!(get_uuid(&mut client, &db_name).await, Some(deleted_uuid));

    // Claiming the same database again is an error
    let err = client
        .claim_database(deleted_uuid, false)
        .await
        .unwrap_err();
    assert_contains!(
        err.to_string(),
        format!("Resource database_uuid/{} already exists", deleted_uuid)
    );
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
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;
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

    let fixture = ServerFixture::create_shared(ServerType::Database).await;
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
        .write_lp(&db_name, lp_lines.join("\n"), 0)
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
        chunks[0].time_of_first_write.is_some() && chunks[0].time_of_last_write.is_some(), // chunk is not yet closed
        "actual:{:#?}",
        chunks[0]
    );

    let chunks = normalize_chunks(chunks);

    let lifecycle_action = ChunkLifecycleAction::Unspecified.into();

    let expected: Vec<Chunk> = vec![
        Chunk {
            partition_key: "cpu".into(),
            table_name: "cpu".into(),
            id: ChunkId::new_test(0).into(),
            storage: ChunkStorage::OpenMutableBuffer.into(),
            lifecycle_action,
            memory_bytes: 1144,
            object_store_bytes: 0,
            row_count: 2,
            time_of_last_access: None,
            time_of_first_write: None,
            time_of_last_write: None,
            order: 1,
        },
        Chunk {
            partition_key: "disk".into(),
            table_name: "disk".into(),
            id: ChunkId::new_test(0).into(),
            storage: ChunkStorage::OpenMutableBuffer.into(),
            lifecycle_action,
            memory_bytes: 1146,
            object_store_bytes: 0,
            row_count: 1,
            time_of_last_access: None,
            time_of_first_write: None,
            time_of_last_write: None,
            order: 1,
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
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
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
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
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
            key: "cpu".into(),
            table_name: "cpu".into(),
        },
        Partition {
            key: "mem".into(),
            table_name: "mem".into(),
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
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut management_client = fixture.management_client();

    let err = management_client
        .list_partitions("this database does not exist")
        .await
        .expect_err("expected error");

    assert_contains!(
        err.to_string(),
        "Resource database/this database does not exist not found"
    );

    match err {
        Error::NotFound(e) => {
            let details = e.details.unwrap();
            assert_eq!(details.resource_type, ResourceType::Database);
            assert_eq!(&details.resource_name, "this database does not exist")
        }
        _ => panic!("unexpected variant: {}", err),
    }
}

#[tokio::test]
async fn test_partition_get() {
    use generated_types::influxdata::iox::management::v1::Partition;

    let fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut management_client = fixture.management_client();

    let db_name = rand_name();
    create_two_partition_database(&db_name, fixture.grpc_channel()).await;

    let partition_key = "cpu";
    let partition = management_client
        .get_partition(&db_name, partition_key)
        .await
        .expect("getting partition");

    let expected = Partition {
        table_name: "cpu".into(),
        key: "cpu".into(),
    };

    assert_eq!(
        expected, partition,
        "expected:\n\n{:#?}\n\nactual:{:#?}",
        expected, partition
    );
}

#[tokio::test]
async fn test_partition_get_error() {
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();

    let err = management_client
        .list_partitions("this database does not exist")
        .await
        .expect_err("expected error");

    assert_contains!(
        err.to_string(),
        "Resource database/this database does not exist not found"
    );

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    let lp_lines =
        vec!["processes,host=foo running=4i,sleeping=514i,total=519i 1591894310000000000"];

    write_client
        .write_lp(&db_name, lp_lines.join("\n"), 0)
        .await
        .expect("write succeded");

    let err = management_client
        .get_partition(&db_name, "non existent partition")
        .await
        .expect_err("expected error getting partition");

    // TODO(raphael): this should be a 404
    assert_contains!(
        err.to_string(),
        "Violation for field \"partition\": Field is required"
    );
}

#[tokio::test]
async fn test_list_partition_chunks() {
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
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
        .write_lp(&db_name, lp_lines.join("\n"), 0)
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
        id: ChunkId::new_test(0).into(),
        storage: ChunkStorage::OpenMutableBuffer.into(),
        lifecycle_action: ChunkLifecycleAction::Unspecified.into(),
        memory_bytes: 1144,
        object_store_bytes: 0,
        row_count: 2,
        time_of_last_access: None,
        time_of_first_write: None,
        time_of_last_write: None,
        order: 1,
    }];

    assert_eq!(
        expected, chunks,
        "expected:\n\n{:#?}\n\nactual:{:#?}",
        expected, chunks
    );
}

#[tokio::test]
async fn test_list_partition_chunk_errors() {
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
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
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    let lp_lines = vec!["cpu,region=west user=23.2 100"];

    write_client
        .write_lp(&db_name, lp_lines.join("\n"), 0)
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
        .write_lp(&db_name, lp_lines.join("\n"), 0)
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

    assert_contains!(
        err.to_string(),
        "Resource partition/cpu:non_existent_partition not found"
    );

    // Rollover a (currently non existent) table in an existing partition which is not OK
    let err = management_client
        .new_partition_chunk(&db_name, "non_existing_table", partition_key)
        .await
        .expect_err("new partition chunk");

    assert_contains!(
        err.to_string(),
        "Resource table/non_existing_table not found"
    );
}

#[tokio::test]
async fn test_new_partition_chunk_error() {
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
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
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();
    let mut operations_client = fixture.operations_client();

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    let partition_key = "cpu";
    let table_name = "cpu";
    let lp_lines = vec!["cpu,region=west user=23.2 100"];

    write_client
        .write_lp(&db_name, lp_lines.join("\n"), 0)
        .await
        .expect("write succeded");

    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");

    assert_eq!(chunks.len(), 1, "Chunks: {:#?}", chunks);
    assert_eq!(chunks[0].storage, ChunkStorage::OpenMutableBuffer as i32);
    let chunk_id = chunks[0].id.clone();

    // Move the chunk to read buffer
    let iox_operation = management_client
        .close_partition_chunk(&db_name, table_name, partition_key, chunk_id)
        .await
        .expect("new partition chunk");

    println!("Operation response is {:?}", iox_operation);
    let operation_id = iox_operation.operation.id();

    // ensure we got a legit job description back
    match iox_operation.metadata.job {
        Some(Job::CompactChunks(job)) => {
            assert_eq!(job.chunks.len(), 1);
            assert_eq!(&job.db_name, &db_name);
            assert_eq!(job.partition_key.as_str(), partition_key);
            assert_eq!(job.table_name.as_str(), table_name);
        }
        job => panic!("unexpected job returned {:#?}", job),
    }

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
    assert_eq!(chunks[0].storage, ChunkStorage::ReadBuffer as i32);
}

#[tokio::test]
async fn test_close_partition_chunk_error() {
    let fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut management_client = fixture.management_client();

    let err = management_client
        .close_partition_chunk(
            "this database does not exist",
            "nor_does_this_table",
            "nor_does_this_partition",
            ChunkId::new_test(0).into(),
        )
        .await
        .expect_err("expected error");

    assert_contains!(
        err.to_string(),
        "Resource database/this database does not exist not found"
    );
}

#[tokio::test]
async fn test_chunk_lifecycle() {
    use influxdb_iox_client::management::generated_types::ChunkStorage;

    let fixture = ServerFixture::create_shared(ServerType::Database).await;
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
        .write_lp(&db_name, lp_lines.join("\n"), 0)
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

    let iox_operation = management_client
        .wipe_preserved_catalog(&db_name)
        .await
        .expect("wipe persisted catalog");

    println!("Operation response is {:?}", iox_operation);
    let operation_id = iox_operation.operation.id();

    // ensure we got a legit job description back
    if let Some(Job::WipePreservedCatalog(wipe_persisted_catalog)) = iox_operation.metadata.job {
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

#[tokio::test]
async fn test_rebuild_preserved_catalog() {
    let test_config =
        TestConfig::new(ServerType::Database).with_env("INFLUXDB_IOX_WIPE_CATALOG_ON_ERROR", "no");

    let fixture = ServerFixture::create_single_use_with_config(test_config).await;

    fixture
        .deployment_client()
        .update_server_id(NonZeroU32::new(DEFAULT_SERVER_ID).unwrap())
        .await
        .unwrap();

    fixture.wait_server_initialized().await;

    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();

    let db_name = rand_name();

    let uuid = management_client
        .create_database(DatabaseRules {
            name: db_name.clone(),
            ..Default::default()
        })
        .await
        .unwrap();

    write_client
        .write_lp(&db_name, "cpu,tag=foo val=1 1", 0)
        .await
        .unwrap();

    let chunks = management_client.list_chunks(&db_name).await.unwrap();
    assert_eq!(chunks.len(), 1);
    let partition_key = &chunks[0].partition_key;

    management_client
        .persist_partition(&db_name, "cpu", partition_key, true)
        .await
        .unwrap();

    let chunks = management_client.list_chunks(&db_name).await.unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(
        chunks[0].storage,
        ChunkStorage::ReadBufferAndObjectStore as i32
    );

    fixture.poison_catalog(uuid);
    let fixture = fixture.restart_server().await;

    let status = fixture.wait_server_initialized().await;

    let db_status = &status.database_statuses[0];
    assert_eq!(db_status.db_name, db_name);
    assert_contains!(
        &db_status.error.as_ref().unwrap().message,
        "error loading catalog"
    );
    assert_eq!(db_status.state, DatabaseState::CatalogLoadError as i32);

    let mut management_client = fixture.management_client();
    let mut operations_client = fixture.operations_client();
    let iox_operation = management_client
        .rebuild_preserved_catalog(&db_name, false)
        .await
        .unwrap();

    if let Some(Job::RebuildPreservedCatalog(rebuild)) = iox_operation.metadata.job {
        assert_eq!(rebuild.db_name, db_name);
    } else {
        panic!("unexpected job returned")
    };

    let operation_id = iox_operation.operation.id();

    operations_client
        .wait_operation(operation_id, Some(std::time::Duration::from_secs(1)))
        .await
        .unwrap();

    let status = fixture.wait_server_initialized().await;
    assert!(status.initialized);

    let db_status = &status.database_statuses[0];
    assert_eq!(db_status.db_name, db_name);
    assert!(db_status.error.is_none());

    // Should have restored the prior state of the catalog
    let new_chunks = management_client.list_chunks(&db_name).await.unwrap();
    assert_eq!(new_chunks.len(), 1);
    assert_eq!(new_chunks[0].storage, ChunkStorage::ObjectStoreOnly as i32);
    assert_eq!(new_chunks[0].id, chunks[0].id);
    assert_eq!(new_chunks[0].order, chunks[0].order);
    assert_eq!(new_chunks[0].partition_key, chunks[0].partition_key);
    assert_eq!(new_chunks[0].table_name, chunks[0].table_name);
    assert_eq!(new_chunks[0].row_count, chunks[0].row_count);
    assert_eq!(
        new_chunks[0].time_of_first_write,
        chunks[0].time_of_first_write
    );
    assert_eq!(
        new_chunks[0].time_of_last_write,
        chunks[0].time_of_last_write
    );
}

/// Normalizes a set of Chunks for comparison by removing timestamps
fn normalize_chunks(chunks: Vec<Chunk>) -> Vec<Chunk> {
    chunks
        .into_iter()
        .map(|summary| {
            let Chunk {
                partition_key,
                table_name,
                storage,
                lifecycle_action,
                memory_bytes,
                object_store_bytes,
                row_count,
                order,
                ..
            } = summary;
            Chunk {
                partition_key,
                table_name,
                id: ChunkId::new_test(0).into(),
                storage,
                lifecycle_action,
                row_count,
                time_of_last_access: None,
                time_of_first_write: None,
                time_of_last_write: None,
                memory_bytes,
                object_store_bytes,
                order,
            }
        })
        .collect::<Vec<_>>()
}

#[tokio::test]
async fn test_get_server_status_ok() {
    let server_fixture = ServerFixture::create_single_use(ServerType::Database).await;
    let mut deployment_client = server_fixture.deployment_client();
    let mut management_client = server_fixture.management_client();

    // not initalized
    let status = management_client.get_server_status().await.unwrap();
    assert!(!status.initialized);

    // initialize
    deployment_client
        .update_server_id(NonZeroU32::new(42).unwrap())
        .await
        .expect("set ID failed");
    server_fixture.wait_server_initialized().await;

    // now initalized
    let status = management_client.get_server_status().await.unwrap();
    assert!(status.initialized);

    // create DBs
    let db_name1 = rand_name();
    let db_name2 = rand_name();
    management_client
        .create_database(DatabaseRules {
            name: db_name1.clone(),
            ..Default::default()
        })
        .await
        .expect("create database failed");

    management_client
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
    let status = management_client.get_server_status().await.unwrap();
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
    let server_fixture = ServerFixture::create_single_use(ServerType::Database).await;
    let mut deployment_client = server_fixture.deployment_client();
    let mut management_client = server_fixture.management_client();

    // we need to "break" the object store AFTER the server was started, otherwise the server
    // process will exit immediately
    let metadata = server_fixture.dir().metadata().unwrap();
    let mut permissions = metadata.permissions();
    permissions.set_mode(0o000);
    set_permissions(server_fixture.dir(), permissions).unwrap();

    // setup server
    deployment_client
        .update_server_id(NonZeroU32::new(42).unwrap())
        .await
        .expect("set ID failed");

    let check = async {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));

        loop {
            let status = management_client.get_server_status().await.unwrap();
            if let Some(err) = status.error {
                assert_contains!(
                    err.message,
                    "error getting server config from object storage"
                );
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
    let server_fixture = ServerFixture::create_single_use(ServerType::Database).await;
    let mut deployment_client = server_fixture.deployment_client();
    let mut management_client = server_fixture.management_client();

    // Valid content of the owner.pb file
    let owner_info = OwnerInfo {
        id: 42,
        location: "arbitrary".to_string(),
        transactions: vec![],
    };
    let mut owner_info_bytes = bytes::BytesMut::new();
    generated_types::server_config::encode_database_owner_info(&owner_info, &mut owner_info_bytes)
        .expect("owner info serialization should be valid");
    let owner_info_bytes = owner_info_bytes.freeze();

    // create valid owner info but malformed DB rules that will put DB in an error state
    let my_db_uuid = Uuid::new_v4();
    let mut path = server_fixture.dir().to_path_buf();
    path.push("dbs");
    path.push(my_db_uuid.to_string());
    std::fs::create_dir_all(path.clone()).unwrap();
    let mut owner_info_path = path.clone();
    owner_info_path.push("owner.pb");
    std::fs::write(owner_info_path, &owner_info_bytes).unwrap();
    path.push("rules.pb");
    std::fs::write(path, "foo").unwrap();

    // create the server config listing the ownership of this database
    let mut path = server_fixture.dir().to_path_buf();
    path.push("nodes");
    path.push("42");
    std::fs::create_dir_all(path.clone()).unwrap();
    path.push("config.pb");

    let data = ServerConfig {
        databases: vec![(String::from("my_db"), format!("dbs/{}", my_db_uuid))]
            .into_iter()
            .collect(),
    };

    let mut encoded = bytes::BytesMut::new();
    generated_types::server_config::encode_persisted_server_config(&data, &mut encoded)
        .expect("server config serialization should be valid");
    let encoded = encoded.freeze();
    std::fs::write(path, encoded).unwrap();

    // initialize
    deployment_client
        .update_server_id(NonZeroU32::new(42).unwrap())
        .await
        .expect("set ID failed");
    server_fixture.wait_server_initialized().await;

    // check for errors
    let status = management_client.get_server_status().await.unwrap();
    assert!(status.initialized);
    assert_eq!(status.error, None);
    assert_eq!(status.database_statuses.len(), 1);

    let db_status = &status.database_statuses[0];
    assert_eq!(db_status.db_name, "my_db");
    assert_contains!(
        &db_status.error.as_ref().unwrap().message,
        "error deserializing database rules"
    );
    assert_eq!(db_status.state, DatabaseState::RulesLoadError as i32);
}

#[tokio::test]
async fn test_restart_wipe() {
    let fixture = ServerFixture::create_single_use(ServerType::Database).await;

    let mut deployment_client = fixture.deployment_client();
    let mut management_client = fixture.management_client();
    let mut operations_client = fixture.operations_client();
    let mut write_client = fixture.write_client();

    deployment_client
        .update_server_id(NonZeroU32::new(1).unwrap())
        .await
        .unwrap();

    fixture.wait_server_initialized().await;

    let db_name = rand_name();
    DatabaseBuilder::new(db_name.clone())
        .persist(true)
        .persist_age_threshold_seconds(1)
        .late_arrive_window_seconds(1)
        .build(fixture.grpc_channel())
        .await;

    // Check database is running
    let status = management_client.get_server_status().await.unwrap();
    assert_eq!(status.database_statuses.len(), 1);
    assert_eq!(
        DatabaseState::from_i32(status.database_statuses[0].state).unwrap(),
        DatabaseState::Initialized
    );

    write_client
        .write_lp(&db_name, "cpu,region=west user=23.2 100", 0)
        .await
        .unwrap();

    let chunks = management_client.list_chunks(&db_name).await.unwrap();
    assert_eq!(chunks.len(), 1);

    // Persist data
    management_client
        .persist_partition(&db_name, "cpu", &chunks[0].partition_key, true)
        .await
        .unwrap();

    // Shutdown database
    management_client.shutdown_database(&db_name).await.unwrap();
    let status = management_client.get_server_status().await.unwrap();
    assert_eq!(status.database_statuses.len(), 1);
    assert_eq!(
        DatabaseState::from_i32(status.database_statuses[0].state).unwrap(),
        DatabaseState::Shutdown
    );

    // Start it up again
    management_client
        .restart_database(&db_name, false)
        .await
        .unwrap();
    let status = management_client.get_server_status().await.unwrap();
    assert_eq!(status.database_statuses.len(), 1);
    assert_eq!(
        DatabaseState::from_i32(status.database_statuses[0].state).unwrap(),
        DatabaseState::Initialized
    );

    // Should still have data
    let chunks = management_client.list_chunks(&db_name).await.unwrap();
    assert_eq!(chunks.len(), 1);

    // Shut it down
    management_client.shutdown_database(&db_name).await.unwrap();
    let status = management_client.get_server_status().await.unwrap();
    assert_eq!(status.database_statuses.len(), 1);
    assert_eq!(
        DatabaseState::from_i32(status.database_statuses[0].state).unwrap(),
        DatabaseState::Shutdown
    );

    // Wipe it
    let operation = management_client
        .wipe_preserved_catalog(&db_name)
        .await
        .unwrap();

    operations_client
        .wait_operation(operation.operation.id(), None)
        .await
        .unwrap();

    // Wipe will complete and then startup the server
    let status = management_client.get_server_status().await.unwrap();
    assert_eq!(status.database_statuses.len(), 1);
    assert_eq!(
        DatabaseState::from_i32(status.database_statuses[0].state).unwrap(),
        DatabaseState::Initialized
    );

    // Data should have been deleted
    let chunks = management_client.list_chunks(&db_name).await.unwrap();
    assert_eq!(chunks.len(), 0);
}

#[tokio::test]
async fn test_unload_read_buffer() {
    use data_types::chunk_metadata::ChunkStorage;

    let fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut write_client = fixture.write_client();
    let mut management_client = fixture.management_client();
    let mut operations_client = fixture.operations_client();

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
        .write_lp(&db_name, lp_lines.join("\n"), 0)
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
    let chunk_id = chunks[0].id.clone();
    let table_name = &chunks[0].table_name;
    let partition_key = &chunks[0].partition_key;

    management_client
        .unload_partition_chunk(&db_name, "data", &partition_key[..], chunk_id.clone())
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

    let iox_operation = management_client
        .load_partition_chunk(&db_name, "data", &partition_key[..], chunk_id.clone())
        .await
        .unwrap();

    let operation_id = iox_operation.operation.id();

    // ensure we got a legit job description back
    match iox_operation.metadata.job {
        Some(Job::LoadReadBufferChunk(job)) => {
            assert_eq!(job.chunk_id, chunk_id);
            assert_eq!(&job.db_name, &db_name);
            assert_eq!(job.partition_key.as_str(), partition_key);
            assert_eq!(job.table_name.as_str(), table_name);
        }
        job => panic!("unexpected job returned {:#?}", job),
    }

    // wait for the job to be done
    operations_client
        .wait_operation(operation_id, Some(std::time::Duration::from_secs(1)))
        .await
        .expect("failed to wait operation");

    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");

    assert_eq!(chunks.len(), 1);
    let storage: generated_types::influxdata::iox::management::v1::ChunkStorage =
        ChunkStorage::ReadBufferAndObjectStore.into();
    let storage: i32 = storage.into();
    assert_eq!(chunks[0].storage, storage);
}

#[tokio::test]
async fn test_drop_partition() {
    use data_types::chunk_metadata::ChunkStorage;

    let fixture = ServerFixture::create_shared(ServerType::Database).await;
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
        .write_lp(&db_name, lp_lines.join("\n"), 0)
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

    let fixture = ServerFixture::create_shared(ServerType::Database).await;
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
        .write_lp(&db_name, lp_lines.join("\n"), 0)
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
async fn test_persist_partition() {
    use data_types::chunk_metadata::ChunkStorage;

    let fixture = ServerFixture::create_shared(ServerType::Database).await;
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
        .write_lp(&db_name, "data foo=1 10", 0)
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

    management_client
        .persist_partition(&db_name, "data", &partition_key[..], true)
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

    let fixture = ServerFixture::create_shared(ServerType::Database).await;
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
        .write_lp(&db_name, "data foo=1 10", 0)
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
        .persist_partition(&db_name, "data", &partition_key[..], false)
        .await
        .unwrap_err();
    assert_contains!(
        err.to_string(),
        "Cannot persist partition because it cannot be flushed at the moment"
    );

    // Can force persistence
    management_client
        .persist_partition(&db_name, "data", &partition_key[..], true)
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
async fn test_compact_os_chunks() {
    // Make 2 persisted chunks for a partition
    let (fixture, db_name, _addr, chunk_ids) = setup_load_and_persist_two_partition_chunks().await;
    assert!(chunk_ids.len() > 1);
    let mut management_client = fixture.management_client();
    let mut operations_client = fixture.operations_client();

    let c_ids: Vec<Bytes> = chunk_ids
        .iter()
        .map(|id| {
            let id_uuid = Uuid::parse_str(id).unwrap();
            id_uuid.as_bytes().to_vec().into()
        })
        .collect();

    // Compact all 2 OS chunks of the partition
    // note that both partition and table_name are "cpu" in the setup
    let iox_operation = management_client
        .compact_object_store_chunks(&db_name, "cpu", "cpu", c_ids.clone())
        .await
        .unwrap();

    let operation_id = iox_operation.operation.id();

    // ensure we got a legit job description back
    match iox_operation.metadata.job {
        Some(Job::CompactObjectStoreChunks(job)) => {
            assert_eq!(&job.db_name, &db_name);
            assert_eq!(job.partition_key.as_str(), "cpu");
            assert_eq!(job.table_name.as_str(), "cpu");
        }
        job => panic!("unexpected job returned {:#?}", job),
    }

    // wait for the job to be done
    operations_client
        .wait_operation(operation_id, Some(std::time::Duration::from_secs(1)))
        .await
        .expect("failed to wait operation");

    // verify chunks after compaction
    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");
    assert_eq!(chunks.len(), 1);
    assert_eq!(
        chunks[0].storage,
        generated_types::influxdata::iox::management::v1::ChunkStorage::ObjectStoreOnly as i32
    );
    let new_chunk_id = chunks[0].id.clone();
    assert_ne!(new_chunk_id, c_ids[0]);
    assert_ne!(new_chunk_id, c_ids[1]);
}

#[tokio::test]
async fn test_compact_os_partition() {
    // Make 2 persisted chunks for a partition
    let (fixture, db_name, _addr, chunk_ids) = setup_load_and_persist_two_partition_chunks().await;
    let mut management_client = fixture.management_client();
    let mut operations_client = fixture.operations_client();

    // Compact all 2 OS chunks of the partition
    // note that both partition and table_name are "cpu" in the setup
    let iox_operation = management_client
        .compact_object_store_partition(&db_name, "cpu", "cpu")
        .await
        .unwrap();

    let operation_id = iox_operation.operation.id();

    // ensure we got a legit job description back
    // note that since compact_object_store_partition invokes compact_object_store_chunks,
    // its job is recorded as CompactObjectStoreChunks
    match iox_operation.metadata.job {
        Some(Job::CompactObjectStoreChunks(job)) => {
            assert_eq!(&job.db_name, &db_name);
            assert_eq!(job.partition_key.as_str(), "cpu");
            assert_eq!(job.table_name.as_str(), "cpu");
        }
        job => panic!("unexpected job returned {:#?}", job),
    }

    // wait for the job to be done
    operations_client
        .wait_operation(operation_id, Some(std::time::Duration::from_secs(1)))
        .await
        .expect("failed to wait operation");

    // verify chunks after compaction
    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");
    assert_eq!(chunks.len(), 1);
    assert_eq!(
        chunks[0].storage,
        generated_types::influxdata::iox::management::v1::ChunkStorage::ObjectStoreOnly as i32
    );
    let new_chunk_id = chunks[0].id.clone();
    assert_ne!(new_chunk_id, chunk_ids[0]);
    assert_ne!(new_chunk_id, chunk_ids[1]);
}
