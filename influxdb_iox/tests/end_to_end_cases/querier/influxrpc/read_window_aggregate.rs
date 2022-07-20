use super::dump::dump_data_frames;
use futures::{prelude::*, FutureExt};
use generated_types::{aggregate::AggregateType, storage_client::StorageClient};
use test_helpers_end_to_end::{
    maybe_skip_integration, GrpcRequestBuilder, MiniCluster, Step, StepTest, StepTestState,
};

// Standalone test that all the pipes are hooked up for read window aggregate
#[tokio::test]
pub async fn read_window_aggregate_test() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    let line_protocol = vec![
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
    ]
    .join("\n");

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(line_protocol),
            Step::WaitForReadable,
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let mut storage_client =
                        StorageClient::new(state.cluster().querier().querier_grpc_connection());


                    let request = GrpcRequestBuilder::new()
                        .source(state.cluster())
                        .timestamp_range(200, 1000)
                        .tag_predicate("state", "MA")
                        .window_every(200)
                        .offset(0)
                        .aggregate_type(AggregateType::Sum)
                        .build_read_window_aggregate();

                    println!("Sending read_window_aggregate request {:#?}",
                             request);

                    let response = storage_client.read_window_aggregate(request).await.unwrap();
                    let responses: Vec<_> = response.into_inner().try_collect().await.unwrap();
                    let frames: Vec<_> = responses
                        .into_iter()
                        .flat_map(|r| r.frames)
                        .flat_map(|f| f.data)
                        .collect();

                    let actual_frames = dump_data_frames(&frames);

                    let expected_frames = vec![
                        "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
                        "FloatPointsFrame, timestamps: [400, 600], values: \"143,147\"",
                        "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Cambridge,state=MA, type: 0",
                        "FloatPointsFrame, timestamps: [400, 600], values: \"163,167\""
                    ];

                    assert_eq!(
                        expected_frames,
                        actual_frames,
                        "Expected:\n{}\nActual:\n{}",
                        expected_frames.join("\n"),
                        actual_frames.join("\n")
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}
