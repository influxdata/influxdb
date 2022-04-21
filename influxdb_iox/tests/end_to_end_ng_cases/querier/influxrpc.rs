//! Tests for influxrpc / Storage gRPC endpoints

mod data;
mod dump;
mod exprs;

use std::sync::Arc;

use futures::prelude::*;
use futures::FutureExt;
use generated_types::ReadSource;
use generated_types::{
    google::protobuf::Empty, measurement_fields_response::FieldType,
    offsets_response::PartitionOffsetResponse, read_response::frame::Data,
    storage_client::StorageClient, MeasurementFieldsRequest, MeasurementNamesRequest,
    MeasurementTagKeysRequest, MeasurementTagValuesRequest, OffsetsResponse, ReadFilterRequest,
    TagKeysRequest, TagValuesRequest,
};
use influxdb_storage_client::tag_key_bytes_to_strings;
use prost::Message;
use test_helpers_end_to_end_ng::FCustom;
use test_helpers_end_to_end_ng::{
    maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState,
};

use self::data::DataGenerator;

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

/// Runs the specified custom function on a cluster with no data
async fn run_no_data_test(custom: FCustom) {
    let database_url = maybe_skip_integration!();

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_standard(database_url).await;

    StepTest::new(&mut cluster, vec![Step::Custom(custom)])
        .run()
        .await
}

#[tokio::test]
async fn read_filter() {
    let generator = Arc::new(data::DataGenerator::new());
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

            let actual_frames = dump::dump_data_frames(&frames);

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
async fn tag_keys() {
    let generator = Arc::new(data::DataGenerator::new());
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
    let generator = Arc::new(data::DataGenerator::new());
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
    let generator = Arc::new(data::DataGenerator::new());
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
    let generator = Arc::new(data::DataGenerator::new());
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
    let generator = Arc::new(data::DataGenerator::new());
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
    let generator = Arc::new(data::DataGenerator::new());
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

/// Run the custom test function with a cluster that has had the data from `generator` loaded
async fn run_data_test(generator: Arc<DataGenerator>, custom: FCustom) {
    let database_url = maybe_skip_integration!();

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_standard(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(generator.line_protocol().to_string()),
            Step::WaitForReadable,
            Step::Custom(custom),
        ],
    )
    .run()
    .await
}

/// Creates the appropriate `Any` protobuf magic for a read source
/// with a specified org and bucket name
fn make_read_source(cluster: &MiniCluster) -> Option<generated_types::google::protobuf::Any> {
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

    Some(read_source)
}
