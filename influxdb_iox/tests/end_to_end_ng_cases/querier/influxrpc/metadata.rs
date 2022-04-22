use super::make_read_source;
use super::run_data_test;
use super::run_no_data_test;

use std::sync::Arc;

use futures::prelude::*;
use futures::FutureExt;
use generated_types::{
    google::protobuf::Empty, measurement_fields_response::FieldType,
    offsets_response::PartitionOffsetResponse, storage_client::StorageClient,
    MeasurementFieldsRequest, MeasurementNamesRequest, MeasurementTagKeysRequest,
    MeasurementTagValuesRequest, OffsetsResponse, TagKeysRequest, TagValuesRequest,
};
use influxdb_storage_client::tag_key_bytes_to_strings;
use test_helpers_end_to_end_ng::StepTestState;

use super::{data::DataGenerator, exprs};

#[tokio::test]
/// Validate that capabilities storage endpoint is hooked up
async fn capabilities() {
    run_no_data_test(Box::new(|state: &mut StepTestState| {
        async move {
            let mut storage_client =
                StorageClient::new(state.cluster().querier().querier_grpc_connection());
            let capabilities_response = storage_client
                .capabilities(Empty {})
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                capabilities_response.caps.len(),
                3,
                "Response: {:?}",
                capabilities_response
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
            let mut storage_client =
                StorageClient::new(state.cluster().querier().querier_grpc_connection());
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
async fn tag_keys() {
    let generator = Arc::new(DataGenerator::new());
    run_data_test(
        Arc::clone(&generator),
        Box::new(move |state: &mut StepTestState| {
            let mut storage_client =
                StorageClient::new(state.cluster().querier().querier_grpc_connection());
            let read_source = make_read_source(state.cluster());
            let range = generator.timestamp_range();
            let predicate = exprs::make_tag_predicate("host", "server01");
            let predicate = Some(predicate);

            let tag_keys_request = tonic::Request::new(TagKeysRequest {
                tags_source: read_source,
                range,
                predicate,
            });

            async move {
                let tag_keys_response = storage_client.tag_keys(tag_keys_request).await.unwrap();
                let responses: Vec<_> = tag_keys_response.into_inner().try_collect().await.unwrap();

                let keys = &responses[0].values;
                let keys: Vec<_> = keys
                    .iter()
                    .map(|v| tag_key_bytes_to_strings(v.clone()))
                    .collect();

                assert_eq!(keys, vec!["_m(0x00)", "host", "name", "region", "_f(0xff)"]);
            }
            .boxed()
        }),
    )
    .await
}

#[tokio::test]
async fn tag_values() {
    let generator = Arc::new(DataGenerator::new());
    run_data_test(
        Arc::clone(&generator),
        Box::new(move |state: &mut StepTestState| {
            let mut storage_client =
                StorageClient::new(state.cluster().querier().querier_grpc_connection());
            let read_source = make_read_source(state.cluster());
            let range = generator.timestamp_range();
            let predicate = exprs::make_tag_predicate("host", "server01");
            let predicate = Some(predicate);

            let tag_values_request = tonic::Request::new(TagValuesRequest {
                tags_source: read_source,
                range,
                predicate,
                tag_key: b"host".to_vec(),
            });

            async move {
                let tag_values_response =
                    storage_client.tag_values(tag_values_request).await.unwrap();
                let responses: Vec<_> = tag_values_response
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

#[tokio::test]
async fn measurement_names() {
    let generator = Arc::new(DataGenerator::new());
    run_data_test(
        Arc::clone(&generator),
        Box::new(move |state: &mut StepTestState| {
            let mut storage_client =
                StorageClient::new(state.cluster().querier().querier_grpc_connection());
            let read_source = make_read_source(state.cluster());
            let range = generator.timestamp_range();

            let measurement_names_request = tonic::Request::new(MeasurementNamesRequest {
                source: read_source,
                range,
                predicate: None,
            });

            async move {
                let measurement_names_response = storage_client
                    .measurement_names(measurement_names_request)
                    .await
                    .unwrap();
                let responses: Vec<_> = measurement_names_response
                    .into_inner()
                    .try_collect()
                    .await
                    .unwrap();

                let values = &responses[0].values;
                let values: Vec<_> = values
                    .iter()
                    .map(|s| std::str::from_utf8(s).unwrap())
                    .collect();

                assert_eq!(
                    values,
                    vec!["attributes", "cpu_load_short", "status", "swap", "system"]
                );
            }
            .boxed()
        }),
    )
    .await
}

#[tokio::test]
async fn measurement_tag_keys() {
    let generator = Arc::new(DataGenerator::new());
    run_data_test(
        Arc::clone(&generator),
        Box::new(move |state: &mut StepTestState| {
            let mut storage_client =
                StorageClient::new(state.cluster().querier().querier_grpc_connection());
            let read_source = make_read_source(state.cluster());
            let range = generator.timestamp_range();

            let predicate = exprs::make_tag_predicate("host", "server01");
            let predicate = Some(predicate);

            let measurement_tag_keys_request = tonic::Request::new(MeasurementTagKeysRequest {
                source: read_source,
                measurement: String::from("cpu_load_short"),
                range,
                predicate,
            });

            async move {
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
            let mut storage_client =
                StorageClient::new(state.cluster().querier().querier_grpc_connection());
            let read_source = make_read_source(state.cluster());
            let range = generator.timestamp_range();

            let predicate = exprs::make_tag_predicate("host", "server01");
            let predicate = Some(predicate);

            let measurement_tag_values_request = tonic::Request::new(MeasurementTagValuesRequest {
                source: read_source,
                measurement: String::from("cpu_load_short"),
                tag_key: String::from("host"),
                range,
                predicate,
            });

            async move {
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

#[tokio::test]
async fn measurement_fields() {
    let generator = Arc::new(DataGenerator::new());
    run_data_test(
        Arc::clone(&generator),
        Box::new(move |state: &mut StepTestState| {
            let mut storage_client =
                StorageClient::new(state.cluster().querier().querier_grpc_connection());
            let read_source = make_read_source(state.cluster());
            let range = generator.timestamp_range();

            let predicate = exprs::make_tag_predicate("host", "server01");
            let predicate = Some(predicate);

            let measurement_fields_request = tonic::Request::new(MeasurementFieldsRequest {
                source: read_source,
                measurement: String::from("cpu_load_short"),
                range,
                predicate,
            });

            let ns_since_epoch = generator.ns_since_epoch();
            async move {
                let measurement_fields_response = storage_client
                    .measurement_fields(measurement_fields_request)
                    .await
                    .unwrap();
                let responses: Vec<_> = measurement_fields_response
                    .into_inner()
                    .try_collect()
                    .await
                    .unwrap();

                let fields = &responses[0].fields;
                assert_eq!(fields.len(), 1);

                let field = &fields[0];
                assert_eq!(field.key, "value");
                assert_eq!(field.r#type(), FieldType::Float);
                assert_eq!(field.timestamp, ns_since_epoch + 4);
            }
            .boxed()
        }),
    )
    .await
}
