use crate::common::server_fixture::ServerFixture;
use assert_cmd::Command;
use data_types::job::{Job, Operation};
use predicates::prelude::*;

#[tokio::test]
async fn test_start_stop() {
    let server_fixture = ServerFixture::create_single_use().await;
    let addr = server_fixture.grpc_base();
    let duration = std::time::Duration::from_secs(10).as_nanos() as u64;

    let stdout: Operation = serde_json::from_slice(
        &Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("operation")
            .arg("test")
            .arg(duration.to_string())
            .arg("--host")
            .arg(addr)
            .assert()
            .success()
            .get_output()
            .stdout,
    )
    .expect("expected JSON output");

    assert_eq!(stdout.task_count, 1);
    match stdout.job {
        Some(Job::Dummy { nanos }) => assert_eq!(nanos, vec![duration]),
        _ => panic!("expected dummy job got {:?}", stdout.job),
    }

    let stdout: Vec<Operation> = serde_json::from_slice(
        &Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("operation")
            .arg("list")
            .arg("--host")
            .arg(addr)
            .assert()
            .success()
            .get_output()
            .stdout,
    )
    .expect("expected JSON output");

    assert_eq!(stdout.len(), 1);
    match &stdout[0].job {
        Some(Job::Dummy { nanos }) => {
            assert_eq!(nanos.len(), 1);
            assert_eq!(nanos[0], duration);
        }
        _ => panic!("expected dummy job got {:?}", &stdout[0].job),
    }

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("operation")
        .arg("cancel")
        .arg(stdout[0].id.to_string())
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));
}
