use super::{dump::dump_data_frames, run_data_test};
use futures::{prelude::*, FutureExt};
use generated_types::{read_response::frame::Data, storage_client::StorageClient};
use std::sync::Arc;
use test_helpers_end_to_end_ng::{DataGenerator, GrpcRequestBuilder, StepTestState};

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
                "SeriesFrame, tags: _measurement=cpu_load_short,host=server01,_field=value, type: 0",
                "FloatPointsFrame, timestamps: [ns1], values: \"27.99\"",
                "SeriesFrame, tags: _measurement=cpu_load_short,host=server01,region=us-east,_field=value, type: 0",
                "FloatPointsFrame, timestamps: [ns3], values: \"1234567.891011\"",
                "SeriesFrame, tags: _measurement=cpu_load_short,host=server01,region=us-west,_field=value, type: 0",
                "FloatPointsFrame, timestamps: [ns0, ns4], values: \"0.64,0.000003\"",
                "SeriesFrame, tags: _measurement=swap,host=server01,name=disk0,_field=in, type: 1",
                "IntegerPointsFrame, timestamps: [ns6], values: \"3\"",
                "SeriesFrame, tags: _measurement=swap,host=server01,name=disk0,_field=out, type: 1",
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
