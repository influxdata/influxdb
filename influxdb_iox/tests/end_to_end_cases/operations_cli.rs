use crate::common::server_fixture::ServerFixture;
use assert_cmd::Command;
use generated_types::google::longrunning::IoxOperation;
use generated_types::influxdata::iox::management::v1::{operation_metadata::Job, Dummy};
use predicates::prelude::*;

#[tokio::test]
async fn test_start_stop() {
    let server_fixture = ServerFixture::create_single_use().await;
    let addr = server_fixture.grpc_base();
    let duration = std::time::Duration::from_secs(10).as_nanos() as u64;

    let stdout: IoxOperation = serde_json::from_slice(
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

    assert_eq!(stdout.metadata.total_count, 1);
    match stdout.metadata.job {
        Some(Job::Dummy(Dummy { nanos, .. })) => assert_eq!(nanos, vec![duration]),
        _ => panic!("expected dummy job got {:?}", stdout.metadata.job),
    }

    let operations: Vec<IoxOperation> = serde_json::from_slice(
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
    match &operations[0].metadata.job {
        Some(Job::Dummy(Dummy { nanos, .. })) => {
            assert_eq!(nanos.len(), 1);
            assert_eq!(nanos[0], duration);
        }
        _ => panic!("expected dummy job got {:?}", &operations[0].metadata.job),
    }

    let name = &operations[0].operation.name;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("operation")
        .arg("cancel")
        .arg(name.clone())
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    let completed: IoxOperation = serde_json::from_slice(
        &Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("operation")
            .arg("wait")
            .arg(name.to_string())
            .arg("--host")
            .arg(addr)
            .assert()
            .success()
            .get_output()
            .stdout,
    )
    .expect("expected JSON output");

    assert_eq!(completed.metadata.pending_count, 0);
    assert_eq!(completed.metadata.total_count, 1);
    assert_eq!(completed.metadata.cancelled_count, 1);
    assert_eq!(&completed.metadata.job, &operations[0].metadata.job)
}
