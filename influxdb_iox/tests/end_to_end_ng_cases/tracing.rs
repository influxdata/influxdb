use futures::{prelude::*, FutureExt};
use generated_types::{storage_client::StorageClient, ReadFilterRequest, ReadSource};
use prost::Message;
use test_helpers_end_to_end_ng::{
    maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState, TestConfig, UdpCapture,
};

#[tokio::test]
pub async fn test_tracing_sql() {
    let database_url = maybe_skip_integration!();
    let table_name = "the_table";
    let udp_capture = UdpCapture::new().await;
    let test_config = TestConfig::new_all_in_one(database_url).with_tracing(&udp_capture);
    let mut cluster = MiniCluster::create_all_in_one(test_config).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{},tag1=A,tag2=B val=42i 123456\n\
                 {},tag1=A,tag2=C val=43i 123457",
                table_name, table_name
            )),
            Step::WaitForReadable,
            Step::Query {
                sql: format!("select * from {}", table_name),
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
        .wait_for(|m| m.to_string().contains("IOxReadFilterNode"))
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
    let test_config = TestConfig::new_all_in_one(database_url).with_tracing(&udp_capture);
    let mut cluster = MiniCluster::create_all_in_one(test_config).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{},tag1=A,tag2=B val=42i 123456\n\
                 {},tag1=A,tag2=C val=43i 123457",
                table_name, table_name
            )),
            Step::WaitForReadable,
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                let cluster = state.cluster();
                let mut storage_client =
                    StorageClient::new(cluster.querier().querier_grpc_connection());

                let org_id = cluster.org_id();
                let bucket_id = cluster.bucket_id();
                let org_id = u64::from_str_radix(org_id, 16).unwrap();
                let bucket_id = u64::from_str_radix(bucket_id, 16).unwrap();

                let partition_id = u64::from(u32::MAX);
                let read_source = ReadSource {
                    org_id,
                    bucket_id,
                    partition_id,
                };

                // Do the magic to-any conversion
                let mut d = bytes::BytesMut::new();
                read_source.encode(&mut d).unwrap();
                let read_source = generated_types::google::protobuf::Any {
                    type_url: "/TODO".to_string(),
                    value: d.freeze(),
                };

                let range = None;
                let predicate = None;

                let read_filter_request = tonic::Request::new(ReadFilterRequest {
                    read_source: Some(read_source),
                    range,
                    predicate,
                    ..Default::default()
                });

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
        .wait_for(|m| m.to_string().contains("IOxReadFilterNode"))
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
    let test_config = TestConfig::new_all_in_one(database_url)
        .with_tracing(&udp_capture)
        .with_tracing_debug_name("force-trace");
    let mut cluster = MiniCluster::create_all_in_one(test_config).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(format!(
                "{},tag1=A,tag2=B val=42i 123456\n\
                 {},tag1=A,tag2=C val=43i 123457",
                table_name, table_name
            )),
            Step::WaitForReadable,
            Step::Query {
                sql: format!("select * from {}", table_name),
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
        .wait_for(|m| m.to_string().contains("IOxReadFilterNode"))
        .await;

    // debugging assistance
    // println!("Traces received (1):\n\n{:#?}", udp_capture.messages());

    // wait for the UDP server to shutdown
    udp_capture.stop().await;
}
