use futures::{prelude::*, FutureExt};
use test_helpers_end_to_end::{
    maybe_skip_integration, GrpcRequestBuilder, MiniCluster, Step, StepTest, StepTestState,
    TestConfig, UdpCapture,
};
use trace_exporters::DEFAULT_JAEGER_TRACE_CONTEXT_HEADER_NAME;

#[tokio::test]
pub async fn test_tracing_sql() {
    let database_url = maybe_skip_integration!();
    let table_name = "the_table";
    let udp_capture = UdpCapture::new().await;
    let test_config = TestConfig::new_all_in_one(Some(database_url)).with_tracing(&udp_capture);
    let mut cluster = MiniCluster::create_all_in_one(test_config).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Query {
                sql: format!("select * from {table_name}"),
                expected: vec![
                    "+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | time                           | val |",
                    "+------+------+--------------------------------+-----+",
                    "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                    "| A    | C    | 1970-01-01T00:00:00.000123457Z | 43  |",
                    "+------+------+--------------------------------+-----+",
                ],
            },
        ],
    )
    .run()
    .await;

    // "shallow" packet inspection and verify the UDP server got omething that had some expected
    // results (maybe we could eventually verify the payload here too)
    udp_capture
        .wait_for(|m| m.to_string().contains("RecordBatchesExec"))
        .await;

    // debugging assistance
    // println!("Traces received (1):\n\n{:#?}", udp_capture.messages());

    // wait for the UDP server to shutdown
    udp_capture.stop().await;
}

#[tokio::test]
pub async fn test_tracing_storage_api() {
    let database_url = maybe_skip_integration!();
    let table_name = "the_table";
    let udp_capture = UdpCapture::new().await;
    let test_config = TestConfig::new_all_in_one(Some(database_url)).with_tracing(&udp_capture);
    let mut cluster = MiniCluster::create_all_in_one(test_config).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                let cluster = state.cluster();
                let mut storage_client = cluster.querier_storage_client();

                let read_filter_request = GrpcRequestBuilder::new()
                    .source(state.cluster())
                    .build_read_filter();

                async move {
                    let read_response = storage_client
                        .read_filter(read_filter_request)
                        .await
                        .unwrap();

                    let _responses: Vec<_> =
                        read_response.into_inner().try_collect().await.unwrap();
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await;

    // "shallow" packet inspection and verify the UDP server got omething that had some expected
    // results (maybe we could eventually verify the payload here too)
    udp_capture
        .wait_for(|m| m.to_string().contains("RecordBatchesExec"))
        .await;

    // debugging assistance
    // println!("Traces received (1):\n\n{:#?}", udp_capture.messages());

    // wait for the UDP server to shutdown
    udp_capture.stop().await;
}

#[tokio::test]
pub async fn test_tracing_create_trace() {
    let database_url = maybe_skip_integration!();
    let table_name = "the_table";
    let udp_capture = UdpCapture::new().await;
    let test_config = TestConfig::new_all_in_one(Some(database_url))
        .with_tracing(&udp_capture)
        .with_tracing_debug_name("force-trace");
    let mut cluster = MiniCluster::create_all_in_one(test_config).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Query {
                sql: format!("select * from {table_name}"),
                expected: vec![
                    "+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | time                           | val |",
                    "+------+------+--------------------------------+-----+",
                    "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                    "| A    | C    | 1970-01-01T00:00:00.000123457Z | 43  |",
                    "+------+------+--------------------------------+-----+",
                ],
            },
        ],
    )
    .run()
    .await;

    // "shallow" packet inspection and verify the UDP server got omething that had some expected
    // results (maybe we could eventually verify the payload here too)
    udp_capture
        .wait_for(|m| m.to_string().contains("RecordBatchesExec"))
        .await;

    // debugging assistance
    // println!("Traces received (1):\n\n{:#?}", udp_capture.messages());

    // wait for the UDP server to shutdown
    udp_capture.stop().await;
}

#[tokio::test]
pub async fn test_tracing_create_ingester_query_trace() {
    let database_url = maybe_skip_integration!();
    let table_name = "the_table";
    let udp_capture = UdpCapture::new().await;
    let test_config = TestConfig::new_all_in_one(Some(database_url))
        .with_tracing(&udp_capture)
        // use the header attached with --gen-trace-id flag
        .with_tracing_debug_name(DEFAULT_JAEGER_TRACE_CONTEXT_HEADER_NAME)
        .with_ingester_never_persist();
    let mut cluster = MiniCluster::create_all_in_one(test_config).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{table_name},tag1=A,tag2=B val=42i 123456\n\
                 {table_name},tag1=A,tag2=C val=43i 123457"
            )),
            Step::Query {
                sql: format!("select * from {table_name}"),
                expected: vec![
                    "+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | time                           | val |",
                    "+------+------+--------------------------------+-----+",
                    "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                    "| A    | C    | 1970-01-01T00:00:00.000123457Z | 43  |",
                    "+------+------+--------------------------------+-----+",
                ],
            },
        ],
    )
    .run()
    .await;

    // "shallow" packet inspection and verify the UDP server got omething that had some expected
    // results (maybe we could eventually verify the payload here too)
    udp_capture
        .wait_for(|m| m.to_string().contains("frame encoding"))
        .await;

    // debugging assistance
    // println!("Traces received (1):\n\n{:#?}", udp_capture.messages());

    // wait for the UDP server to shutdown
    udp_capture.stop().await;
}

#[tokio::test]
async fn test_tracing_create_compactor_trace() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let ingester_config = TestConfig::new_ingester(&database_url);
    let router_config = TestConfig::new_router(&ingester_config);
    let querier_config = TestConfig::new_querier(&ingester_config).with_querier_mem_pool_bytes(1);
    let udp_capture = UdpCapture::new().await;
    let compactor_config = TestConfig::new_compactor(&ingester_config).with_tracing(&udp_capture);

    let mut cluster = MiniCluster::new()
        .with_router(router_config)
        .await
        .with_ingester(ingester_config)
        .await
        .with_querier(querier_config)
        .await
        .with_compactor_config(compactor_config);

    StepTest::new(
        &mut cluster,
        vec![
            Step::RecordNumParquetFiles,
            Step::WriteLineProtocol(String::from(
                "my_awesome_table,tag1=A,tag2=B val=42i 123456",
            )),
            // wait for partitions to be persisted
            Step::WaitForPersisted {
                expected_increase: 1,
            },
            // Run the compactor
            Step::Compact,
        ],
    )
    .run()
    .await;

    // "shallow" packet inspection and verify the UDP server got omething that had some expected
    // results.  We could look for any text of any of the compaction spans.  The name of the span
    // for acquiring permit is arbitrarily chosen.
    udp_capture
        .wait_for(|m| m.to_string().contains("acquire_permit"))
        .await;

    // debugging assistance
    //println!("Traces received (1):\n\n{:#?}", udp_capture.messages());

    // wait for the UDP server to shutdown
    udp_capture.stop().await;
}
