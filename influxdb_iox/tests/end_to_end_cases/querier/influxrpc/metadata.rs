use super::{run_data_test, run_no_data_test, InfluxRpcTest};
use futures::{prelude::*, FutureExt};
use generated_types::{
    google::protobuf::Empty, offsets_response::PartitionOffsetResponse, OffsetsResponse,
};
use influxdb_storage_client::tag_key_bytes_to_strings;
use std::sync::Arc;
use test_helpers_end_to_end::{DataGenerator, GrpcRequestBuilder, StepTestState};

mod measurement_fields;
mod measurement_names;
mod tag_keys;
mod tag_values;

#[tokio::test]
/// Validate that capabilities storage endpoint is hooked up
async fn capabilities() {
    run_no_data_test(Box::new(|state: &mut StepTestState| {
        async move {
            let mut storage_client = state.cluster().querier_storage_client();
            let capabilities_response = storage_client
                .capabilities(Empty {})
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                capabilities_response.caps.len(),
                4,
                "Response: {capabilities_response:?}"
            );
        }
        .boxed()
    }))
    .await
}

#[tokio::test]
/// Validate that storage offsets endpoint is hooked up (required by internal Influx cloud)
async fn offsets() {
    run_no_data_test(Box::new(|state: &mut StepTestState| {
        async move {
            let mut storage_client = state.cluster().querier_storage_client();
            let offsets_response = storage_client.offsets(Empty {}).await.unwrap().into_inner();
            let expected = OffsetsResponse {
                partitions: vec![PartitionOffsetResponse { id: 0, offset: 1 }],
            };
            assert_eq!(offsets_response, expected);
        }
        .boxed()
    }))
    .await
}

#[tokio::test]
async fn measurement_tag_keys() {
    let generator = Arc::new(DataGenerator::new());
    run_data_test(
        Arc::clone(&generator),
        Box::new(move |state: &mut StepTestState| {
            let generator = Arc::clone(&generator);
            async move {
                let mut storage_client = state.cluster().querier_storage_client();

                let measurement_tag_keys_request = GrpcRequestBuilder::new()
                    .source(state.cluster())
                    .timestamp_range(generator.min_time(), generator.max_time())
                    .tag_predicate("host", "server01")
                    .build_measurement_tag_keys("cpu_load_short");

                let measurement_tag_keys_response = storage_client
                    .measurement_tag_keys(measurement_tag_keys_request)
                    .await
                    .unwrap();
                let responses: Vec<_> = measurement_tag_keys_response
                    .into_inner()
                    .try_collect()
                    .await
                    .unwrap();

                let values = &responses[0].values;
                let values: Vec<_> = values
                    .iter()
                    .map(|v| tag_key_bytes_to_strings(v.clone()))
                    .collect();

                assert_eq!(values, vec!["_m(0x00)", "host", "region", "_f(0xff)"]);
            }
            .boxed()
        }),
    )
    .await
}

#[tokio::test]
async fn measurement_tag_values() {
    let generator = Arc::new(DataGenerator::new());
    run_data_test(
        Arc::clone(&generator),
        Box::new(move |state: &mut StepTestState| {
            let generator = Arc::clone(&generator);
            async move {
                let mut storage_client = state.cluster().querier_storage_client();

                let measurement_tag_values_request = GrpcRequestBuilder::new()
                    .source(state.cluster())
                    .timestamp_range(generator.min_time(), generator.max_time())
                    .tag_predicate("host", "server01")
                    .build_measurement_tag_values("cpu_load_short", "host");

                let measurement_tag_values_response = storage_client
                    .measurement_tag_values(measurement_tag_values_request)
                    .await
                    .unwrap();
                let responses: Vec<_> = measurement_tag_values_response
                    .into_inner()
                    .try_collect()
                    .await
                    .unwrap();

                let values = &responses[0].values;
                let values: Vec<_> = values
                    .iter()
                    .map(|v| tag_key_bytes_to_strings(v.clone()))
                    .collect();

                assert_eq!(values, vec!["server01"]);
            }
            .boxed()
        }),
    )
    .await
}
