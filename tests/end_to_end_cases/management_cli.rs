use std::sync::Arc;
use std::time::Duration;

use assert_cmd::Command;
use predicates::prelude::*;

use data_types::chunk_metadata::ChunkAddr;
use data_types::{
    chunk_metadata::ChunkStorage,
    job::{Job, Operation},
};
use test_helpers::make_temp_file;
use write_buffer::maybe_skip_kafka_integration;

use crate::{
    common::server_fixture::ServerFixture,
    end_to_end_cases::scenario::{wait_for_exact_chunk_states, DatabaseBuilder},
};

use super::scenario::{create_readable_database, rand_name};
use crate::end_to_end_cases::scenario::{fixture_broken_catalog, fixture_replay_broken};

#[tokio::test]
async fn test_server_id() {
    let server_fixture = ServerFixture::create_single_use().await;
    let addr = server_fixture.grpc_base();
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("set")
        .arg("32")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("get")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("32"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("set")
        .arg("42")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("id already set"));
}

#[tokio::test]
async fn test_create_database() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();
    let db = &db_name;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("get")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Database not found"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("create")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    // Listing the databases includes the newly created database
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(db));

    // Retrieving the database includes the name and a mutable buffer configuration
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("get")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(
            predicate::str::contains(db)
                .and(predicate::str::contains(format!(r#""name": "{}"#, db)))
                // validate the defaults have been set reasonably
                .and(predicate::str::contains("%Y-%m-%d %H:00:00"))
                .and(predicate::str::contains(r#""bufferSizeHard": 104857600"#))
                .and(predicate::str::contains("lifecycleRules")),
        );
}

#[tokio::test]
async fn test_create_database_size() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();
    let db = &db_name;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("create")
        .arg(db)
        .arg("--buffer-size-hard")
        .arg("1000")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("get")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(
            predicate::str::contains(r#""bufferSizeHard": 1000"#)
                .and(predicate::str::contains("lifecycleRules")),
        );
}

#[tokio::test]
async fn test_create_database_immutable() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();
    let db = &db_name;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("create")
        .arg(db)
        .arg("--immutable")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("get")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        // Should not have a mutable buffer
        .stdout(predicate::str::contains(r#""immutable": true"#));
}

#[tokio::test]
async fn test_get_chunks() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    let lp_data = vec![
        "cpu,region=west user=23.2 100",
        "cpu,region=west user=21.0 150",
    ];

    load_lp(addr, &db_name, lp_data);

    let predicate = predicate::str::contains(r#""partition_key": "cpu","#)
        .and(predicate::str::contains(r#""id": 0,"#))
        .and(predicate::str::contains(
            r#""storage": "OpenMutableBuffer","#,
        ))
        .and(predicate::str::contains(r#""memory_bytes": 100"#))
        // Check for a non empty timestamp such as
        // "time_of_first_write": "2021-03-30T17:11:10.723866Z",
        .and(predicate::str::contains(r#""time_of_first_write": "20"#));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("chunk")
        .arg("list")
        .arg(&db_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate);
}

#[tokio::test]
async fn test_list_chunks_error() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    // note don't make the database, expect error

    // list the chunks
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("chunk")
        .arg("list")
        .arg(&db_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("Some requested entity was not found: Resource database")
                .and(predicate::str::contains(&db_name)),
        );
}

#[tokio::test]
async fn test_remotes() {
    let server_fixture = ServerFixture::create_single_use().await;
    let addr = server_fixture.grpc_base();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("set")
        .arg("32")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("remote")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("no remotes configured"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("remote")
        .arg("set")
        .arg("1")
        .arg("http://1.2.3.4:1234")
        .arg("--host")
        .arg(addr)
        .assert()
        .success();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("remote")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("http://1.2.3.4:1234"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("remote")
        .arg("remove")
        .arg("1")
        .arg("--host")
        .arg(addr)
        .assert()
        .success();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("remote")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("no remotes configured"));
}

#[tokio::test]
async fn test_list_partitions() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    let lp_data = vec![
        "cpu,region=west user=23.2 100",
        "mem,region=west free=100000 150",
    ];
    load_lp(addr, &db_name, lp_data);

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("list")
        .arg(&db_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("cpu").and(predicate::str::contains("mem")));
}

#[tokio::test]
async fn test_list_partitions_error() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("list")
        .arg("non_existent_database")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Database not found"));
}

#[tokio::test]
async fn test_get_partition() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    let lp_data = vec![
        "cpu,region=west user=23.2 100",
        "mem,region=west free=100000 150",
    ];
    load_lp(addr, &db_name, lp_data);

    let expected = r#""key": "cpu""#;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("get")
        .arg(&db_name)
        .arg("cpu")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(expected));
}

#[tokio::test]
async fn test_get_partition_error() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("get")
        .arg("cpu")
        .arg("non_existent_database")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Database not found"));
}

#[tokio::test]
async fn test_list_partition_chunks() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    let lp_data = vec![
        "cpu,region=west user=23.2 100",
        "cpu2,region=west user=21.0 150",
    ];

    load_lp(addr, &db_name, lp_data);

    let expected = r#"
    "partition_key": "cpu",
    "table_name": "cpu",
    "id": 0,
    "storage": "OpenMutableBuffer",
"#;

    let partition_key = "cpu";
    // should not contain anything related to cpu2 partition
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("list-chunks")
        .arg(&db_name)
        .arg(&partition_key)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(expected).and(predicate::str::contains("cpu2").not()));
}

#[tokio::test]
async fn test_list_partition_chunks_error() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    // note don't make the database, expect error

    // list the chunks
    let partition_key = "cpu";
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("list-chunks")
        .arg(&db_name)
        .arg(&partition_key)
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("Some requested entity was not found: Resource database")
                .and(predicate::str::contains(&db_name)),
        );
}

#[tokio::test]
async fn test_new_partition_chunk() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    let lp_data = vec!["cpu,region=west user=23.2 100"];
    load_lp(addr, &db_name, lp_data);

    let expected = "Ok";
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("new-chunk")
        .arg(&db_name)
        .arg("cpu")
        .arg("cpu")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(expected));

    wait_for_exact_chunk_states(
        &server_fixture,
        &db_name,
        vec![ChunkStorage::ReadBuffer],
        std::time::Duration::from_secs(5),
    )
    .await;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("chunk")
        .arg("list")
        .arg(&db_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("ReadBuffer"));
}

#[tokio::test]
async fn test_new_partition_chunk_error() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("new-chunk")
        .arg("non_existent_database")
        .arg("non_existent_partition")
        .arg("non_existent_table")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Resource database/non_existent_database not found",
        ));
}

#[tokio::test]
async fn test_close_partition_chunk() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    let lp_data = vec!["cpu,region=west user=23.2 100"];
    load_lp(addr, &db_name, lp_data);

    let stdout: Operation = serde_json::from_slice(
        &Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("database")
            .arg("partition")
            .arg("close-chunk")
            .arg(&db_name)
            .arg("cpu")
            .arg("cpu")
            .arg("0")
            .arg("--host")
            .arg(addr)
            .assert()
            .success()
            .get_output()
            .stdout,
    )
    .expect("Expected JSON output");

    let expected_job = Job::CompactChunk {
        chunk: ChunkAddr {
            db_name: Arc::from(db_name.as_str()),
            table_name: Arc::from("cpu"),
            partition_key: Arc::from("cpu"),
            chunk_id: 0,
        },
    };

    assert_eq!(
        Some(expected_job),
        stdout.job,
        "operation was {:#?}",
        stdout
    );
}

#[tokio::test]
async fn test_close_partition_chunk_error() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("close-chunk")
        .arg("non_existent_database")
        .arg("non_existent_partition")
        .arg("non_existent_table")
        .arg("0")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Database not found"));
}

#[tokio::test]
async fn test_wipe_persisted_catalog() {
    let db_name = rand_name();
    let server_fixture = fixture_broken_catalog(&db_name).await;
    let addr = server_fixture.grpc_base();

    let stdout: Operation = serde_json::from_slice(
        &Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("database")
            .arg("recover")
            .arg("wipe")
            .arg(&db_name)
            .arg("--host")
            .arg(addr)
            .arg("--force")
            .assert()
            .success()
            .get_output()
            .stdout,
    )
    .expect("Expected JSON output");

    let expected_job = Job::WipePreservedCatalog {
        db_name: Arc::from(db_name.as_str()),
    };

    assert_eq!(
        Some(expected_job),
        stdout.job,
        "operation was {:#?}",
        stdout
    );
}

#[tokio::test]
async fn test_wipe_persisted_catalog_error_force() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("recover")
        .arg("wipe")
        .arg(&db_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Need to pass `--force`"));
}

#[tokio::test]
async fn test_wipe_persisted_catalog_error_db_exists() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    let expected_err = format!("Failed precondition: database ({}) in invalid state (Initialized) for transition (WipePreservedCatalog)", db_name);

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("recover")
        .arg("wipe")
        .arg(&db_name)
        .arg("--host")
        .arg(addr)
        .arg("--force")
        .assert()
        .failure()
        .stderr(predicate::str::contains(&expected_err));
}

#[tokio::test]
async fn test_skip_replay() {
    let kafka_connection = maybe_skip_kafka_integration!();
    let db_name = rand_name();
    let server_fixture = fixture_replay_broken(&db_name, &kafka_connection).await;
    let addr = server_fixture.grpc_base();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("recover")
        .arg("skip-replay")
        .arg(&db_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));
}

#[tokio::test]
async fn test_skip_replay_error_db_exists() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    let expected_err = format!("Failed precondition: database ({}) in invalid state (Initialized) for transition (SkipReplay)", db_name);

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("recover")
        .arg("skip-replay")
        .arg(&db_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains(&expected_err));
}

/// Loads the specified lines into the named database
fn load_lp(addr: &str, db_name: &str, lp_data: Vec<&str>) {
    let lp_data_file = make_temp_file(lp_data.join("\n"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("write")
        .arg(&db_name)
        .arg(lp_data_file.as_ref())
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Lines OK"));
}

#[tokio::test]
async fn test_unload_partition_chunk() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();
    let db_name = rand_name();

    DatabaseBuilder::new(db_name.clone())
        .persist(true)
        .persist_age_threshold_seconds(1)
        .late_arrive_window_seconds(1)
        .build(fixture.grpc_channel())
        .await;

    let lp_data = vec!["cpu,region=west user=23.2 10"];
    load_lp(addr, &db_name, lp_data);

    let mut chunks = wait_for_exact_chunk_states(
        &fixture,
        &db_name,
        vec![ChunkStorage::ReadBufferAndObjectStore],
        std::time::Duration::from_secs(5),
    )
    .await;
    let chunk = chunks.pop().unwrap();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("unload-chunk")
        .arg(&db_name)
        .arg("cpu")
        .arg("cpu")
        .arg(chunk.id.to_string())
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));
}

#[tokio::test]
async fn test_unload_partition_chunk_error() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    let lp_data = vec!["cpu,region=west user=23.2 100"];
    load_lp(addr, &db_name, lp_data);

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("unload-chunk")
        .arg(&db_name)
        .arg("cpu")
        .arg("cpu")
        .arg("0")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("wrong chunk lifecycle"));
}

#[tokio::test]
async fn test_drop_partition() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();
    let db_name = rand_name();

    DatabaseBuilder::new(db_name.clone())
        .persist(true)
        .persist_age_threshold_seconds(1)
        .late_arrive_window_seconds(1)
        .build(fixture.grpc_channel())
        .await;

    let lp_data = vec!["cpu,region=west user=23.2 10"];
    load_lp(addr, &db_name, lp_data);

    wait_for_exact_chunk_states(
        &fixture,
        &db_name,
        vec![ChunkStorage::ReadBufferAndObjectStore],
        std::time::Duration::from_secs(5),
    )
    .await;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("drop")
        .arg(&db_name)
        .arg("cpu")
        .arg("cpu")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));
}

#[tokio::test]
async fn test_drop_partition_error() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();
    let db_name = rand_name();

    DatabaseBuilder::new(db_name.clone())
        .persist(true)
        .persist_age_threshold_seconds(1_000)
        .late_arrive_window_seconds(1_000)
        .build(fixture.grpc_channel())
        .await;

    let lp_data = vec!["cpu,region=west user=23.2 10"];
    load_lp(addr, &db_name, lp_data);

    wait_for_exact_chunk_states(
        &fixture,
        &db_name,
        vec![ChunkStorage::OpenMutableBuffer],
        std::time::Duration::from_secs(5),
    )
    .await;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("drop")
        .arg(&db_name)
        .arg("cpu")
        .arg("cpu")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Cannot drop unpersisted chunk"));
}

#[tokio::test]
async fn test_persist_partition() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();
    let db_name = rand_name();

    DatabaseBuilder::new(db_name.clone())
        .persist(true)
        .persist_age_threshold_seconds(1_000)
        .late_arrive_window_seconds(1)
        .build(fixture.grpc_channel())
        .await;

    let lp_data = vec!["cpu,region=west user=23.2 10"];
    load_lp(addr, &db_name, lp_data);

    wait_for_exact_chunk_states(
        &fixture,
        &db_name,
        vec![ChunkStorage::OpenMutableBuffer],
        std::time::Duration::from_secs(5),
    )
    .await;

    tokio::time::sleep(Duration::from_millis(1500)).await;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("persist")
        .arg(&db_name)
        .arg("cpu")
        .arg("cpu")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));
}

#[tokio::test]
async fn test_persist_partition_error() {
    let fixture = ServerFixture::create_shared().await;
    let addr = fixture.grpc_base();
    let db_name = rand_name();

    DatabaseBuilder::new(db_name.clone())
        .persist(true)
        .persist_age_threshold_seconds(1_000)
        .late_arrive_window_seconds(1_000)
        .build(fixture.grpc_channel())
        .await;

    let lp_data = vec!["cpu,region=west user=23.2 10"];
    load_lp(addr, &db_name, lp_data);

    wait_for_exact_chunk_states(
        &fixture,
        &db_name,
        vec![ChunkStorage::OpenMutableBuffer],
        std::time::Duration::from_secs(5),
    )
    .await;

    // there is no old data (late arrival window is 1000s) that can be persisted
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("persist")
        .arg(&db_name)
        .arg("cpu")
        .arg("cpu")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Error persisting partition:").and(
            predicate::str::contains(
                "Cannot persist partition because it cannot be flushed at the moment",
            ),
        ));
}
