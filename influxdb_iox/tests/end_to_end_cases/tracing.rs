use super::scenario::{collect_query, Scenario};
use crate::common::{
    server_fixture::{ServerFixture, TestConfig},
    udp_listener::UdpCapture,
};
use futures::TryStreamExt;
use generated_types::{storage_client::StorageClient, ReadFilterRequest};

async fn setup() -> (UdpCapture, ServerFixture) {
    let udp_capture = UdpCapture::new().await;

    let test_config = TestConfig::new()
        .with_env("TRACES_EXPORTER", "jaeger")
        .with_env("TRACES_EXPORTER_JAEGER_AGENT_HOST", udp_capture.ip())
        .with_env("TRACES_EXPORTER_JAEGER_AGENT_PORT", udp_capture.port())
        .with_env(
            "TRACES_EXPORTER_JAEGER_TRACE_CONTEXT_HEADER_NAME",
            "custom-trace-header",
        )
        .with_client_header("custom-trace-header", "4:3:2:1");

    let server_fixture = ServerFixture::create_single_use_with_config(test_config).await;

    let mut management_client = server_fixture.management_client();

    management_client.update_server_id(1).await.unwrap();
    server_fixture.wait_server_initialized().await;

    (udp_capture, server_fixture)
}

async fn run_sql_query(server_fixture: &ServerFixture) {
    let scenario = Scenario::new();
    scenario
        .create_database(&mut server_fixture.management_client())
        .await;
    scenario.load_data(&server_fixture.influxdb2_client()).await;

    // run a query, ensure we get traces
    let sql_query = "select * from cpu_load_short";
    let mut client = server_fixture.flight_client();

    let query_results = client
        .perform_query(scenario.database_name(), sql_query)
        .await
        .unwrap();

    collect_query(query_results).await;
}

#[tokio::test]
pub async fn test_tracing_sql() {
    let (udp_capture, server_fixture) = setup().await;
    run_sql_query(&server_fixture).await;

    //  "shallow" packet inspection and verify the UDP server got
    //  something that had some expected results (maybe we could
    //  eventually verify the payload here too)
    udp_capture
        .wait_for(|m| m.to_string().contains("IOxReadFilterNode"))
        .await;

    // debugging assistance
    //println!("Traces received (1):\n\n{:#?}", udp_capture.messages());

    // wait for the UDP server to shutdown
    udp_capture.stop().await
}

#[tokio::test]
pub async fn test_tracing_storage_api() {
    let (udp_capture, server_fixture) = setup().await;

    let scenario = Scenario::new();
    scenario
        .create_database(&mut server_fixture.management_client())
        .await;
    scenario.load_data(&server_fixture.influxdb2_client()).await;

    // run a query via gRPC, ensure we get traces
    let read_source = scenario.read_source();
    let range = scenario.timestamp_range();
    let predicate = None;
    let read_filter_request = tonic::Request::new(ReadFilterRequest {
        read_source,
        range,
        predicate,
    });
    let mut storage_client = StorageClient::new(server_fixture.grpc_channel());
    let read_response = storage_client
        .read_filter(read_filter_request)
        .await
        .unwrap();

    read_response
        .into_inner()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    //  "shallow" packet inspection and verify the UDP server got
    //  something that had some expected results (maybe we could
    //  eventually verify the payload here too)
    udp_capture
        .wait_for(|m| m.to_string().contains("IOxReadFilterNode"))
        .await;

    // debugging assistance
    //println!("Traces received (2):\n\n{:#?}", udp_capture.messages());

    // wait for the UDP server to shutdown
    udp_capture.stop().await
}

#[tokio::test]
pub async fn test_tracing_create_trace() {
    let udp_capture = UdpCapture::new().await;

    let test_config = TestConfig::new()
        .with_env("TRACES_EXPORTER", "jaeger")
        .with_env("TRACES_EXPORTER_JAEGER_AGENT_HOST", udp_capture.ip())
        .with_env("TRACES_EXPORTER_JAEGER_AGENT_PORT", udp_capture.port())
        // setup a custom debug name (to ensure it gets plumbed through)
        .with_env("TRACES_EXPORTER_JAEGER_DEBUG_NAME", "force-trace")
        .with_client_header("force-trace", "some-debug-id");

    let server_fixture = ServerFixture::create_single_use_with_config(test_config).await;

    let mut management_client = server_fixture.management_client();

    management_client.update_server_id(1).await.unwrap();
    server_fixture.wait_server_initialized().await;

    run_sql_query(&server_fixture).await;

    //  "shallow" packet inspection and verify the UDP server got
    //  something that had some expected results (maybe we could
    //  eventually verify the payload here too)
    udp_capture
        .wait_for(|m| m.to_string().contains("IOxReadFilterNode"))
        .await;

    // debugging assistance
    //println!("Traces received (1):\n\n{:#?}", udp_capture.messages());

    // wait for the UDP server to shutdown
    udp_capture.stop().await
}
