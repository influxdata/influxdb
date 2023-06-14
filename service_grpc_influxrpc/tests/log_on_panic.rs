use generated_types::TestErrorRequest;
use panic_logging::SendPanicsToTracing;
use service_grpc_influxrpc::test_util::Fixture;
use test_helpers::{assert_contains, tracing::TracingCapture};

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// !!!!!!!!!!!!!!!!!!!!    IMPORTANT    !!!!!!!!!!!!!!!!!!!!
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//
// This file MUST only contain a single test, otherwise
// libtest's panic hooks will interfer with our custom
// panic hook.
//
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

#[tokio::test]
async fn test() {
    // Send a message to a route that causes a panic and ensure:
    // 1. We don't use up all executors 2. The panic message
    // message ends up in the log system

    // Normally, the global panic logger is set at program start
    let _f = SendPanicsToTracing::new();

    // capture all tracing messages
    let tracing_capture = TracingCapture::new();

    // Start a test gRPC server on a randomally allocated port
    let mut fixture = Fixture::new().await.expect("Connecting to test server");

    let request = TestErrorRequest {};

    // Test response from storage server
    let response = fixture.iox_client.test_error(request).await;

    match &response {
        Ok(_) => {
            panic!("Unexpected success: {response:?}");
        }
        Err(status) => {
            assert_eq!(status.code(), tonic::Code::Cancelled);
            assert_contains!(
                status.message(),
                "http2 error: stream error received: stream no longer needed"
            );
        }
    };

    // Ensure that the logs captured the panic
    let captured_logs = tracing_capture.to_string();
    // Note we don't include the actual line / column in the
    // expected panic message to avoid needing to update the test
    // whenever the source code file changed.
    let expected_error = "'This is a test panic', service_grpc_testing/src/lib.rs:";
    assert_contains!(captured_logs, expected_error);

    // Ensure that panics don't exhaust the tokio executor by
    // running 100 times (success is if we can make a successful
    // call after this)
    for _ in 0usize..100 {
        let request = TestErrorRequest {};

        // Test response from storage server
        let response = fixture.iox_client.test_error(request).await;
        assert!(response.is_err(), "Got an error response: {response:?}");
    }

    // Ensure there are still threads to answer actual client queries
    let caps = fixture.storage_client.capabilities().await.unwrap();
    assert!(!caps.is_empty(), "Caps: {caps:?}");
}
