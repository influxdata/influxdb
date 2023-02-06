use std::path::Path;

use futures::FutureExt;
use influxdb_iox_client::connection::Connection;
use test_helpers::assert_contains;
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState};

#[tokio::test]
pub async fn test_panic() {
    let database_url = maybe_skip_integration!();
    let mut cluster = MiniCluster::create_shared2(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![Step::Custom(Box::new(move |state: &mut StepTestState| {
            async move {
                let querier = state.cluster().querier();
                assert_panic_logging(querier.querier_grpc_connection(), querier.log_path().await)
                    .await;
            }
            .boxed()
        }))],
    )
    .run()
    .await;
}

async fn assert_panic_logging(connection: Connection, log_path: Box<Path>) {
    // trigger panic
    let mut client = influxdb_iox_client::test::Client::new(connection);
    let err = client.error().await.unwrap_err();
    if let influxdb_iox_client::error::Error::Internal(err) = err {
        assert_eq!(&err.message, "This is a test panic");
    } else {
        panic!("wrong error type. got {err:?}");
    }

    // check logs
    let logs = std::fs::read_to_string(log_path).unwrap();
    let expected_error = "'This is a test panic', service_grpc_testing/src/lib.rs:18:9";
    assert_contains!(logs, expected_error);
}
