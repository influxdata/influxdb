use crate::common::server_fixture::ServerFixture;
use assert_cmd::Command;
use data_types::job::{Job, Operation, OperationStatus};
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

    assert_eq!(stdout.total_count, 1);
    match stdout.job {
        Some(Job::Dummy { nanos, .. }) => assert_eq!(nanos, vec![duration]),
        _ => panic!("expected dummy job got {:?}", stdout.job),
    }

    let operations: Vec<Operation> = serde_json::from_slice(
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

    assert_eq!(operations.len(), 1);
    match &operations[0].job {
        Some(Job::Dummy { nanos, .. }) => {
            assert_eq!(nanos.len(), 1);
            assert_eq!(nanos[0], duration);
        }
        _ => panic!("expected dummy job got {:?}", &operations[0].job),
    }

    let id = operations[0].id;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("operation")
        .arg("cancel")
        .arg(id.to_string())
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    let completed: Operation = serde_json::from_slice(
        &Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("operation")
            .arg("wait")
            .arg(id.to_string())
            .arg("--host")
            .arg(addr)
            .assert()
            .success()
            .get_output()
            .stdout,
    )
    .expect("expected JSON output");

    assert_eq!(completed.pending_count, 0);
    assert_eq!(completed.total_count, 1);
    assert_eq!(completed.cancelled_count, 1);
    assert_eq!(completed.status, OperationStatus::Cancelled);
    assert_eq!(&completed.job, &operations[0].job)
}
