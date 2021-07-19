use crate::common::server_fixture::ServerFixture;
use influxdb_iox_client::{management::generated_types::*, operations};
use std::time::Duration;

#[tokio::test]
async fn test_operations() {
    let server_fixture = ServerFixture::create_single_use().await;
    let mut management_client = server_fixture.management_client();
    let mut operations_client = server_fixture.operations_client();

    let running_ops = operations_client
        .list_operations()
        .await
        .expect("list operations failed");

    assert_eq!(running_ops.len(), 0);

    let nanos = vec![Duration::from_secs(20).as_nanos() as _, 1];

    let operation = management_client
        .create_dummy_job(nanos.clone())
        .await
        .expect("create dummy job failed");

    let running_ops = operations_client
        .list_operations()
        .await
        .expect("list operations failed");

    assert_eq!(running_ops.len(), 1);
    assert_eq!(running_ops[0].name(), operation.name);

    let id = operation.name.parse().expect("not an integer");

    let meta = operations_client
        .client_operation(id)
        .await
        .unwrap()
        .metadata();

    let job = meta.job.expect("expected a job");

    assert_eq!(meta.total_count, 2);
    assert_eq!(meta.pending_count, 1);
    assert_eq!(job, operation_metadata::Job::Dummy(Dummy { nanos }));

    // Check wait times out correctly
    let fetched = operations_client
        .wait_operation(id, Some(Duration::from_micros(10)))
        .await
        .expect("failed to wait operation");

    assert!(!fetched.done);
    // Shouldn't specify wall_nanos as not complete
    assert_eq!(meta.wall_nanos, 0);

    let wait = tokio::spawn(async move {
        let mut operations_client = server_fixture.operations_client();
        operations_client
            .wait_operation(id, None)
            .await
            .expect("failed to wait operation")
    });

    operations_client
        .cancel_operation(id)
        .await
        .expect("failed to cancel operation");

    let waited = wait.await.unwrap();
    let meta = operations::ClientOperation::try_new(waited.clone())
        .unwrap()
        .metadata();

    assert!(waited.done);
    assert!(meta.wall_nanos > 0);
    assert!(meta.cpu_nanos > 0);
    assert_eq!(meta.pending_count, 0);
    assert_eq!(meta.total_count, 2);
    assert_eq!(meta.cancelled_count, 1);

    match waited.result {
        Some(operations::generated_types::operation::Result::Error(status)) => {
            assert_eq!(status.code, tonic::Code::Cancelled as i32)
        }
        _ => panic!("expected error"),
    }
}
