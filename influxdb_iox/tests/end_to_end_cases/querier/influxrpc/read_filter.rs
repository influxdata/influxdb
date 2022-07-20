use super::{dump::dump_data_frames, read_group_data, run_data_test};
use futures::{prelude::*, FutureExt};
use generated_types::{
    read_response::frame::Data, storage_client::StorageClient, ReadFilterRequest,
};
use influxdb_iox_client::connection::Connection;
use std::sync::Arc;
use test_helpers_end_to_end::{
    maybe_skip_integration, DataGenerator, GrpcRequestBuilder, MiniCluster, Step, StepTest,
    StepTestState,
};

#[tokio::test]
async fn read_filter() {
    let generator = Arc::new(DataGenerator::new());
    run_data_test(Arc::clone(&generator), Box::new(move |state: &mut StepTestState| {
        async move {
            let mut storage_client =
                StorageClient::new(state.cluster().querier().querier_grpc_connection());

            let read_filter_request = GrpcRequestBuilder::new()
                .source(state.cluster())
                .timestamp_range(generator.min_time(), generator.max_time())
                .tag_predicate("host", "server01")
                .build_read_filter();

            let read_response = storage_client
                .read_filter(read_filter_request)
                .await
                .unwrap();

            let responses: Vec<_> = read_response.into_inner().try_collect().await.unwrap();
            let frames: Vec<Data> = responses
                .into_iter()
                .flat_map(|r| r.frames)
                .flat_map(|f| f.data)
                .collect();

            let actual_frames = dump_data_frames(&frames);

            let expected_frames = generator.substitute_nanos(&[
                "SeriesFrame, tags: _field=value,_measurement=cpu_load_short,host=server01, type: 0",
                "FloatPointsFrame, timestamps: [ns1], values: \"27.99\"",
                "SeriesFrame, tags: _field=value,_measurement=cpu_load_short,host=server01,region=us-east, type: 0",
                "FloatPointsFrame, timestamps: [ns3], values: \"1234567.891011\"",
                "SeriesFrame, tags: _field=value,_measurement=cpu_load_short,host=server01,region=us-west, type: 0",
                "FloatPointsFrame, timestamps: [ns0, ns4], values: \"0.64,0.000003\"",
                "SeriesFrame, tags: _field=in,_measurement=swap,host=server01,name=disk0, type: 1",
                "IntegerPointsFrame, timestamps: [ns6], values: \"3\"",
                "SeriesFrame, tags: _field=out,_measurement=swap,host=server01,name=disk0, type: 1",
                "IntegerPointsFrame, timestamps: [ns6], values: \"4\""
            ]);

            assert_eq!(
                expected_frames,
                actual_frames,
                "Expected:\n{}\nActual:\n{}",
                expected_frames.join("\n"),
                actual_frames.join("\n")
            )
        }.boxed()
    })).await
}

#[tokio::test]
pub async fn read_filter_regex_operator() {
    do_read_filter_test(
        read_group_data(),
        GrpcRequestBuilder::new()
            .timestamp_range(0, 2001) // include all data
            .regex_match_predicate("host", "^b.+"),
        vec![
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"20,21\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"81,82\"",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"40,41\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"51,52\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_empty_tag_eq() {
    do_read_filter_test(
        vec!["cpu value=1 1000", "cpu,host=server01 value=2 2000"],
        GrpcRequestBuilder::new()
            .timestamp_range(0, 2001) // include all data
            // https://github.com/influxdata/influxdb_iox/issues/3430
            // host = '' means where host is not present
            .tag_predicate("host", ""),
        vec![
            "SeriesFrame, tags: _field=value,_measurement=cpu, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"1\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_empty_tag_not_regex() {
    do_read_filter_test(
        vec!["cpu value=1 1000", "cpu,host=server01 value=2 2000"],
        GrpcRequestBuilder::new()
            .timestamp_range(0, 2001) // include all data
            // https://github.com/influxdata/influxdb_iox/issues/3434
            // host !~ /^server01$/ means where host doesn't start with `server01`
            .not_regex_match_predicate("host", "^server01"),
        vec![
            "SeriesFrame, tags: _field=value,_measurement=cpu, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"1\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_empty_tag_regex() {
    do_read_filter_test(
        vec!["cpu value=1 1000", "cpu,host=server01 value=2 2000"],
        GrpcRequestBuilder::new()
            .timestamp_range(0, 2001) // include all data
            // host =~ /.+/ means where host is at least one character
            .regex_match_predicate("host", ".+"),
        vec![
            "SeriesFrame, tags: _field=value,_measurement=cpu,host=server01, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"2\"",
        ],
    )
    .await
}

/// Sends the specified line protocol to a server with the timestamp/ predicate
/// predicate, and compares it against expected frames
async fn do_read_filter_test(
    input_lines: Vec<&str>,
    request_builder: GrpcRequestBuilder,
    expected_frames: impl IntoIterator<Item = &str>,
) {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();
    let expected_frames: Vec<String> = expected_frames.into_iter().map(|s| s.to_string()).collect();

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    let line_protocol = input_lines.join("\n");
    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(line_protocol),
            Step::WaitForReadable,
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut storage_client =
                        StorageClient::new(state.cluster().querier().querier_grpc_connection());

                    println!("Sending read_filter request with {:#?}", request_builder);

                    let read_filter_request =
                        request_builder.source(state.cluster()).build_read_filter();

                    assert_eq!(
                        do_read_filter_request(&mut storage_client, read_filter_request).await,
                        expected_frames,
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

/// Make a read_group request and returns the results in a comparable format
async fn do_read_filter_request(
    storage_client: &mut StorageClient<Connection>,
    request: tonic::Request<ReadFilterRequest>,
) -> Vec<String> {
    let read_filter_response = storage_client
        .read_filter(request)
        .await
        .expect("successful read_filter call");

    let responses: Vec<_> = read_filter_response
        .into_inner()
        .try_collect()
        .await
        .unwrap();

    let frames: Vec<_> = responses
        .into_iter()
        .flat_map(|r| r.frames)
        .flat_map(|f| f.data)
        .collect();

    dump_data_frames(&frames)
}
