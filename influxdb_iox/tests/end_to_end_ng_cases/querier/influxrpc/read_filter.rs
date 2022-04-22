use std::sync::Arc;

use futures::prelude::*;
use futures::FutureExt;
use generated_types::{
    read_response::frame::Data, storage_client::StorageClient, ReadFilterRequest,
};
use test_helpers_end_to_end_ng::StepTestState;

use crate::end_to_end_ng_cases::querier::influxrpc::dump::dump_data_frames;

use super::make_read_source;
use super::run_data_test;
use super::{data::DataGenerator, exprs};

#[tokio::test]
async fn read_filter() {
    let generator = Arc::new(DataGenerator::new());
    run_data_test(Arc::clone(&generator), Box::new(move |state: &mut StepTestState| {
        let mut storage_client =
            StorageClient::new(state.cluster().querier().querier_grpc_connection());
        let read_source = make_read_source(state.cluster());
        let range = generator.timestamp_range();

        let predicate = exprs::make_tag_predicate("host", "server01");
        let predicate = Some(predicate);

        let read_filter_request = tonic::Request::new(ReadFilterRequest {
            read_source,
            range,
            predicate,
            ..Default::default()
        });

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

        async move {
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
