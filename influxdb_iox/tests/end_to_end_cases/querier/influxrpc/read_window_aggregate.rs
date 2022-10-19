use super::dump::dump_data_frames;
use futures::{prelude::*, FutureExt};
use generated_types::aggregate::AggregateType;
use test_helpers_end_to_end::{
    maybe_skip_integration, GrpcRequestBuilder, MiniCluster, Step, StepTest, StepTestState,
};

// Standalone test that all the pipes are hooked up for read window aggregate
#[tokio::test]
pub async fn read_window_aggregate_test() {
    do_read_window_aggregate_test(
        vec![
            "h2o,state=MA,city=Boston temp=70.0 100",
            "h2o,state=MA,city=Boston temp=71.0 200",
            "h2o,state=MA,city=Boston temp=72.0 300",
            "h2o,state=MA,city=Boston temp=73.0 400",
            "h2o,state=MA,city=Boston temp=74.0 500",
            "h2o,state=MA,city=Cambridge temp=80.0 100",
            "h2o,state=MA,city=Cambridge temp=81.0 200",
            "h2o,state=MA,city=Cambridge temp=82.0 300",
            "h2o,state=MA,city=Cambridge temp=83.0 400",
            "h2o,state=MA,city=Cambridge temp=84.0 500",
            "h2o,state=CA,city=LA temp=90.0 100",
            "h2o,state=CA,city=LA temp=91.0 200",
            "h2o,state=CA,city=LA temp=92.0 300",
            "h2o,state=CA,city=LA temp=93.0 400",
            "h2o,state=CA,city=LA temp=94.0 500",
        ],
        GrpcRequestBuilder::new()
            .timestamp_range(200, 1000)
            .tag_predicate("state", "MA")
            .window_every(200)
            .offset(0)
            .aggregate_type(AggregateType::Sum),
        vec![
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [400, 600], values: \"143,147\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Cambridge,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [400, 600], values: \"163,167\"",
        ],
    )
    .await
}

// Standalone test that all the pipes are hooked up for read window aggregate
#[tokio::test]
pub async fn read_window_aggregate_test_with_periods() {
    do_read_window_aggregate_test(
        vec![
            "measurement.one,tag.one=foo field.one=1,field.two=100 1000",
            "measurement.one,tag.one=bar field.one=2,field.two=200 2000",
        ],
        GrpcRequestBuilder::new()
            .timestamp_range(0, 2001)
            .field_predicate("field.two")
            .window_every(200)
            .offset(0)
            .aggregate_type(AggregateType::Sum),
        vec![
            "SeriesFrame, tags: _field=field.two,_measurement=measurement.one,tag.one=bar, type: 0",
            "FloatPointsFrame, timestamps: [2200], values: \"200\"",
            "SeriesFrame, tags: _field=field.two,_measurement=measurement.one,tag.one=foo, type: 0",
            "FloatPointsFrame, timestamps: [1200], values: \"100\"",
        ],
    )
    .await
}

/// Sends the specified line protocol to a server, runs a read_grou
/// gRPC request, and compares it against expected frames
async fn do_read_window_aggregate_test(
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
                    let mut storage_client = state.cluster().querier_storage_client();

                    let request = request_builder
                        .source(state.cluster())
                        .build_read_window_aggregate();

                    println!("Sending read_window_aggregate request {:#?}", request);

                    let response = storage_client.read_window_aggregate(request).await.unwrap();
                    let responses: Vec<_> = response.into_inner().try_collect().await.unwrap();
                    let frames: Vec<_> = responses
                        .into_iter()
                        .flat_map(|r| r.frames)
                        .flat_map(|f| f.data)
                        .collect();

                    let actual_frames = dump_data_frames(&frames);

                    assert_eq!(
                        expected_frames, actual_frames,
                        "\n\nExpected:\n{:#?}\nActual:\n{:#?}",
                        expected_frames, actual_frames,
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}
