use super::scenario::{substitute_nanos, Scenario};
use crate::common::server_fixture::ServerFixture;

use futures::prelude::*;
use generated_types::{
    aggregate::AggregateType,
    google::protobuf::{Any, Empty},
    measurement_fields_response::FieldType,
    node::{Comparison, Type as NodeType, Value},
    read_group_request::Group,
    read_response::{frame::Data, *},
    storage_client::StorageClient,
    Aggregate, MeasurementFieldsRequest, MeasurementNamesRequest, MeasurementTagKeysRequest,
    MeasurementTagValuesRequest, Node, Predicate, ReadFilterRequest, ReadGroupRequest,
    ReadWindowAggregateRequest, Tag, TagKeysRequest, TagValuesRequest, TimestampRange,
};
use std::str;
use test_helpers::tag_key_bytes_to_strings;
use tonic::transport::Channel;

#[tokio::test]
pub async fn test() {
    let storage_fixture = ServerFixture::create_shared().await;

    let influxdb2 = storage_fixture.influxdb2_client();
    let mut storage_client = StorageClient::new(storage_fixture.grpc_channel());
    let mut management_client = storage_fixture.management_client();

    let scenario = Scenario::new();
    scenario.create_database(&mut management_client).await;
    scenario.load_data(&influxdb2).await;

    capabilities_endpoint(&mut storage_client).await;
    read_filter_endpoint(&mut storage_client, &scenario).await;
    tag_keys_endpoint(&mut storage_client, &scenario).await;
    tag_values_endpoint(&mut storage_client, &scenario).await;
    measurement_names_endpoint(&mut storage_client, &scenario).await;
    measurement_tag_keys_endpoint(&mut storage_client, &scenario).await;
    measurement_tag_values_endpoint(&mut storage_client, &scenario).await;
    measurement_fields_endpoint(&mut storage_client, &scenario).await;
}

/// Validate that capabilities storage endpoint is hooked up
async fn capabilities_endpoint(storage_client: &mut StorageClient<Channel>) {
    let capabilities_response = storage_client.capabilities(Empty {}).await.unwrap();
    let capabilities_response = capabilities_response.into_inner();
    assert_eq!(
        capabilities_response.caps.len(),
        2,
        "Response: {:?}",
        capabilities_response
    );
}

async fn read_filter_endpoint(storage_client: &mut StorageClient<Channel>, scenario: &Scenario) {
    let read_source = scenario.read_source();
    let range = scenario.timestamp_range();

    let predicate = make_tag_predicate("host", "server01");
    let predicate = Some(predicate);

    let read_filter_request = tonic::Request::new(ReadFilterRequest {
        read_source,
        range,
        predicate,
    });
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

    let expected_frames = substitute_nanos(scenario.ns_since_epoch(), &[
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

    let actual_frames = dump_data_frames(&frames);

    assert_eq!(
        expected_frames,
        actual_frames,
        "Expected:\n{}\nActual:\n{}",
        expected_frames.join("\n"),
        actual_frames.join("\n")
    );
}

async fn tag_keys_endpoint(storage_client: &mut StorageClient<Channel>, scenario: &Scenario) {
    let read_source = scenario.read_source();
    let range = scenario.timestamp_range();
    let predicate = make_tag_predicate("host", "server01");
    let predicate = Some(predicate);

    let tag_keys_request = tonic::Request::new(TagKeysRequest {
        tags_source: read_source,
        range,
        predicate,
    });

    let tag_keys_response = storage_client.tag_keys(tag_keys_request).await.unwrap();
    let responses: Vec<_> = tag_keys_response.into_inner().try_collect().await.unwrap();

    let keys = &responses[0].values;
    let keys: Vec<_> = keys
        .iter()
        .map(|v| tag_key_bytes_to_strings(v.clone()))
        .collect();

    assert_eq!(keys, vec!["_m(0x00)", "host", "name", "region", "_f(0xff)"]);
}

async fn tag_values_endpoint(storage_client: &mut StorageClient<Channel>, scenario: &Scenario) {
    let read_source = scenario.read_source();
    let range = scenario.timestamp_range();
    let predicate = make_tag_predicate("host", "server01");
    let predicate = Some(predicate);

    let tag_values_request = tonic::Request::new(TagValuesRequest {
        tags_source: read_source,
        range,
        predicate,
        tag_key: b"host".to_vec(),
    });

    let tag_values_response = storage_client.tag_values(tag_values_request).await.unwrap();
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

async fn measurement_names_endpoint(
    storage_client: &mut StorageClient<Channel>,
    scenario: &Scenario,
) {
    let read_source = scenario.read_source();
    let range = scenario.timestamp_range();

    let measurement_names_request = tonic::Request::new(MeasurementNamesRequest {
        source: read_source,
        range,
        predicate: None,
    });

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
    let values: Vec<_> = values.iter().map(|s| str::from_utf8(s).unwrap()).collect();

    assert_eq!(
        values,
        vec!["attributes", "cpu_load_short", "status", "swap", "system"]
    );
}

async fn measurement_tag_keys_endpoint(
    storage_client: &mut StorageClient<Channel>,
    scenario: &Scenario,
) {
    let read_source = scenario.read_source();
    let range = scenario.timestamp_range();

    let predicate = make_tag_predicate("host", "server01");
    let predicate = Some(predicate);

    let measurement_tag_keys_request = tonic::Request::new(MeasurementTagKeysRequest {
        source: read_source,
        measurement: String::from("cpu_load_short"),
        range,
        predicate,
    });

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

async fn measurement_tag_values_endpoint(
    storage_client: &mut StorageClient<Channel>,
    scenario: &Scenario,
) {
    let read_source = scenario.read_source();
    let range = scenario.timestamp_range();

    let predicate = make_tag_predicate("host", "server01");
    let predicate = Some(predicate);

    let measurement_tag_values_request = tonic::Request::new(MeasurementTagValuesRequest {
        source: read_source,
        measurement: String::from("cpu_load_short"),
        tag_key: String::from("host"),
        range,
        predicate,
    });

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

async fn measurement_fields_endpoint(
    storage_client: &mut StorageClient<Channel>,
    scenario: &Scenario,
) {
    let read_source = scenario.read_source();
    let range = scenario.timestamp_range();

    let predicate = make_tag_predicate("host", "server01");
    let predicate = Some(predicate);

    let measurement_fields_request = tonic::Request::new(MeasurementFieldsRequest {
        source: read_source,
        measurement: String::from("cpu_load_short"),
        range,
        predicate,
    });

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
    assert_eq!(field.timestamp, scenario.ns_since_epoch() + 4);
}

#[tokio::test]
pub async fn regex_operator_test() {
    let fixture = ServerFixture::create_shared().await;
    let mut management = fixture.management_client();
    let mut storage_client = StorageClient::new(fixture.grpc_channel());
    let influxdb2 = fixture.influxdb2_client();

    let scenario = Scenario::new();
    scenario.create_database(&mut management).await;

    load_read_group_data(&influxdb2, &scenario).await;

    let read_source = scenario.read_source();

    // read_group(group_keys: region, agg: None)
    let read_filter_request = ReadFilterRequest {
        read_source: read_source.clone(),
        range: Some(TimestampRange {
            start: 0,
            end: 2001, // include all data
        }),
        predicate: Some(make_regex_match_predicate("host", "^b.+")),
    };

    let expected_frames = vec![
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"20,21\"",
        "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"81,82\"",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"40,41\"",
        "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"51,52\"",
    ];

    assert_eq!(
        do_read_filter_request(&mut storage_client, read_filter_request).await,
        expected_frames,
    );
}

#[tokio::test]
pub async fn read_group_test() {
    let fixture = ServerFixture::create_shared().await;
    let mut management = fixture.management_client();
    let mut storage_client = StorageClient::new(fixture.grpc_channel());
    let influxdb2 = fixture.influxdb2_client();

    let scenario = Scenario::new();
    scenario.create_database(&mut management).await;

    load_read_group_data(&influxdb2, &scenario).await;

    let read_source = scenario.read_source();

    test_read_group_none_agg(&mut storage_client, &read_source).await;
    test_read_group_none_agg_with_predicate(&mut storage_client, &read_source).await;
    test_read_group_sum_agg(&mut storage_client, &read_source).await;
    test_read_group_last_agg(&mut storage_client, &read_source).await;
}

async fn load_read_group_data(client: &influxdb2_client::Client, scenario: &Scenario) {
    let line_protocol = vec![
        "cpu,cpu=cpu1,host=foo  usage_user=71.0,usage_system=10.0 1000",
        "cpu,cpu=cpu1,host=foo  usage_user=72.0,usage_system=11.0 2000",
        "cpu,cpu=cpu1,host=bar  usage_user=81.0,usage_system=20.0 1000",
        "cpu,cpu=cpu1,host=bar  usage_user=82.0,usage_system=21.0 2000",
        "cpu,cpu=cpu2,host=foo  usage_user=61.0,usage_system=30.0 1000",
        "cpu,cpu=cpu2,host=foo  usage_user=62.0,usage_system=31.0 2000",
        "cpu,cpu=cpu2,host=bar  usage_user=51.0,usage_system=40.0 1000",
        "cpu,cpu=cpu2,host=bar  usage_user=52.0,usage_system=41.0 2000",
    ]
    .join("\n");

    client
        .write_line_protocol(
            scenario.org_id_str(),
            scenario.bucket_id_str(),
            line_protocol,
        )
        .await
        .expect("Wrote cpu line protocol data");
}

// Standalone test for read_group with group keys and no aggregate
// assumes that load_read_group_data has been previously run
async fn test_read_group_none_agg(
    storage_client: &mut StorageClient<tonic::transport::Channel>,
    read_source: &std::option::Option<Any>,
) {
    // read_group(group_keys: region, agg: None)
    let read_group_request = ReadGroupRequest {
        read_source: read_source.clone(),
        range: Some(TimestampRange {
            start: 0,
            end: 2001, // include all data
        }),
        predicate: None,
        group_keys: vec![String::from("cpu")],
        group: Group::By as i32,
        aggregate: Some(Aggregate {
            r#type: AggregateType::None as i32,
        }),
        hints: 0,
    };

    let expected_group_frames = vec![
        "GroupFrame, tag_keys: cpu, partition_key_vals: cpu1",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"20,21\"",
        "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"81,82\"",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=foo, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"10,11\"",
        "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=foo, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"71,72\"",
        "GroupFrame, tag_keys: cpu, partition_key_vals: cpu2",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"40,41\"",
        "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"51,52\"",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=foo, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"30,31\"",
        "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=foo, type: 0",
        "FloatPointsFrame, timestamps: [1000, 2000], values: \"61,62\"",
    ];

    let actual_group_frames = do_read_group_request(storage_client, read_group_request).await;

    assert_eq!(
        expected_group_frames,
        actual_group_frames,
        "Expected:\n{}\nActual:\n{}",
        expected_group_frames.join("\n"),
        actual_group_frames.join("\n")
    );
}

/// Test that predicates make it through
async fn test_read_group_none_agg_with_predicate(
    storage_client: &mut StorageClient<tonic::transport::Channel>,
    read_source: &std::option::Option<Any>,
) {
    let read_group_request = ReadGroupRequest {
        read_source: read_source.clone(),
        range: Some(TimestampRange {
            start: 0,
            end: 2000, // do not include data at timestamp 2000
        }),
        predicate: Some(make_field_predicate("usage_system")),
        group_keys: vec![String::from("cpu")],
        group: Group::By as i32,
        aggregate: Some(Aggregate {
            r#type: AggregateType::None as i32,
        }),
        hints: 0,
    };

    let expected_group_frames = vec![
        "GroupFrame, tag_keys: cpu, partition_key_vals: cpu1",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [1000], values: \"20\"",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=foo, type: 0",
        "FloatPointsFrame, timestamps: [1000], values: \"10\"",
        "GroupFrame, tag_keys: cpu, partition_key_vals: cpu2",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [1000], values: \"40\"",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=foo, type: 0",
        "FloatPointsFrame, timestamps: [1000], values: \"30\"",
    ];

    let actual_group_frames = do_read_group_request(storage_client, read_group_request).await;

    assert_eq!(
        expected_group_frames,
        actual_group_frames,
        "Expected:\n{}\nActual:\n{}",
        expected_group_frames.join("\n"),
        actual_group_frames.join("\n")
    );
}

// Standalone test for read_group with group keys and an actual
// "aggregate" (not a "selector" style).  assumes that
// load_read_group_data has been previously run
async fn test_read_group_sum_agg(
    storage_client: &mut StorageClient<tonic::transport::Channel>,
    read_source: &std::option::Option<Any>,
) {
    // read_group(group_keys: region, agg: Sum)
    let read_group_request = ReadGroupRequest {
        read_source: read_source.clone(),
        range: Some(TimestampRange {
            start: 0,
            end: 2001, // include all data
        }),
        predicate: None,
        group_keys: vec![String::from("cpu")],
        group: Group::By as i32,
        aggregate: Some(Aggregate {
            r#type: AggregateType::Sum as i32,
        }),
        hints: 0,
    };

    let expected_group_frames = vec![
        "GroupFrame, tag_keys: cpu, partition_key_vals: cpu1",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"41\"",
        "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"163\"",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=foo, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"21\"",
        "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=foo, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"143\"",
        "GroupFrame, tag_keys: cpu, partition_key_vals: cpu2",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"81\"",
        "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"103\"",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=foo, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"61\"",
        "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=foo, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"123\"",
    ];

    let actual_group_frames = do_read_group_request(storage_client, read_group_request).await;

    assert_eq!(
        expected_group_frames,
        actual_group_frames,
        "Expected:\n{}\nActual:\n{}",
        expected_group_frames.join("\n"),
        actual_group_frames.join("\n")
    );
}

// Standalone test for read_group with group keys and an actual
// "selector" function last.  assumes that
// load_read_group_data has been previously run
async fn test_read_group_last_agg(
    storage_client: &mut StorageClient<tonic::transport::Channel>,
    read_source: &std::option::Option<Any>,
) {
    // read_group(group_keys: region, agg: Last)
    let read_group_request = ReadGroupRequest {
        read_source: read_source.clone(),
        range: Some(TimestampRange {
            start: 0,
            end: 2001, // include all data
        }),
        predicate: None,
        group_keys: vec![String::from("cpu")],
        group: Group::By as i32,
        aggregate: Some(Aggregate {
            r#type: AggregateType::Last as i32,
        }),
        hints: 0,
    };

    let expected_group_frames = vec![
        "GroupFrame, tag_keys: cpu, partition_key_vals: cpu1",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"21\"",
        "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"82\"",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=foo, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"11\"",
        "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=foo, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"72\"",
        "GroupFrame, tag_keys: cpu, partition_key_vals: cpu2",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"41\"",
        "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"52\"",
        "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=foo, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"31\"",
        "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=foo, type: 0",
        "FloatPointsFrame, timestamps: [2000], values: \"62\"",
    ];

    let actual_group_frames = do_read_group_request(storage_client, read_group_request).await;

    assert_eq!(
        expected_group_frames,
        actual_group_frames,
        "Expected:\n{}\nActual:\n{}",
        expected_group_frames.join("\n"),
        actual_group_frames.join("\n")
    );
}

// Standalone test that all the pipes are hooked up for read window aggregate
#[tokio::test]
pub async fn read_window_aggregate_test() {
    let fixture = ServerFixture::create_shared().await;
    let mut management = fixture.management_client();
    let mut storage_client = StorageClient::new(fixture.grpc_channel());
    let influxdb2 = fixture.influxdb2_client();

    let scenario = Scenario::new();
    let read_source = scenario.read_source();

    scenario.create_database(&mut management).await;

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

    influxdb2
        .write_line_protocol(
            scenario.org_id_str(),
            scenario.bucket_id_str(),
            line_protocol,
        )
        .await
        .expect("Wrote h20 line protocol");

    // now, query using read window aggregate

    let request = ReadWindowAggregateRequest {
        read_source: read_source.clone(),
        range: Some(TimestampRange {
            start: 200,
            end: 1000,
        }),
        predicate: Some(make_tag_predicate("state", "MA")),
        window_every: 200,
        offset: 0,
        aggregate: vec![Aggregate {
            r#type: AggregateType::Sum as i32,
        }],
        window: None,
    };

    let response = storage_client.read_window_aggregate(request).await.unwrap();

    let responses: Vec<_> = response.into_inner().try_collect().await.unwrap();

    let frames: Vec<_> = responses
        .into_iter()
        .flat_map(|r| r.frames)
        .flat_map(|f| f.data)
        .collect();

    let expected_frames = vec![
        "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
        "FloatPointsFrame, timestamps: [400, 600], values: \"143,147\"",
        "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Cambridge,state=MA, type: 0",
        "FloatPointsFrame, timestamps: [400, 600], values: \"163,167\"",
    ];

    let actual_frames = dump_data_frames(&frames);

    assert_eq!(
        expected_frames,
        actual_frames,
        "Expected:\n{}\nActual:\n{}",
        expected_frames.join("\n"),
        actual_frames.join("\n")
    );
}

/// Create a predicate representing tag_name=tag_value in the horrible gRPC
/// structs
fn make_tag_predicate(tag_name: impl Into<String>, tag_value: impl Into<String>) -> Predicate {
    Predicate {
        root: Some(Node {
            node_type: NodeType::ComparisonExpression as i32,
            children: vec![
                Node {
                    node_type: NodeType::TagRef as i32,
                    children: vec![],
                    value: Some(Value::TagRefValue(tag_name.into().into())),
                },
                Node {
                    node_type: NodeType::Literal as i32,
                    children: vec![],
                    value: Some(Value::StringValue(tag_value.into())),
                },
            ],
            value: Some(Value::Comparison(Comparison::Equal as _)),
        }),
    }
}

// Create a predicate representing tag_name ~= /pattern/
//
// The constitution of this request was formed by looking at a real request
// made to storage, which looked like this:
//
// root:<
//         node_type:COMPARISON_EXPRESSION
//         children:<node_type:TAG_REF tag_ref_value:"tag_key_name" >
//         children:<node_type:LITERAL regex_value:"pattern" >
//         comparison:REGEX
// >
fn make_regex_match_predicate(
    tag_key_name: impl Into<String>,
    pattern: impl Into<String>,
) -> Predicate {
    Predicate {
        root: Some(Node {
            node_type: NodeType::ComparisonExpression as i32,
            children: vec![
                Node {
                    node_type: NodeType::TagRef as i32,
                    children: vec![],
                    value: Some(Value::TagRefValue(tag_key_name.into().into())),
                },
                Node {
                    node_type: NodeType::Literal as i32,
                    children: vec![],
                    value: Some(Value::RegexValue(pattern.into())),
                },
            ],
            value: Some(Value::Comparison(Comparison::Regex as _)),
        }),
    }
}

/// Create a predicate representing _f=field_name in the horrible gRPC structs
fn make_field_predicate(field_name: impl Into<String>) -> Predicate {
    Predicate {
        root: Some(Node {
            node_type: NodeType::ComparisonExpression as i32,
            children: vec![
                Node {
                    node_type: NodeType::TagRef as i32,
                    children: vec![],
                    value: Some(Value::TagRefValue([255].to_vec())),
                },
                Node {
                    node_type: NodeType::Literal as i32,
                    children: vec![],
                    value: Some(Value::StringValue(field_name.into())),
                },
            ],
            value: Some(Value::Comparison(Comparison::Equal as _)),
        }),
    }
}

/// Make a read_group request and returns the results in a comparable format
async fn do_read_filter_request(
    storage_client: &mut StorageClient<tonic::transport::Channel>,
    request: ReadFilterRequest,
) -> Vec<String> {
    let request = tonic::Request::new(request);

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

/// Make a read_group request and returns the results in a comparable format
async fn do_read_group_request(
    storage_client: &mut StorageClient<tonic::transport::Channel>,
    request: ReadGroupRequest,
) -> Vec<String> {
    let request = tonic::Request::new(request);

    let read_group_response = storage_client
        .read_group(request)
        .await
        .expect("successful read_group call");

    let responses: Vec<_> = read_group_response
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

fn dump_data_frames(frames: &[Data]) -> Vec<String> {
    frames.iter().map(|f| dump_data(f)).collect()
}

fn dump_data(data: &Data) -> String {
    match Some(data) {
        Some(Data::Series(SeriesFrame { tags, data_type })) => format!(
            "SeriesFrame, tags: {}, type: {:?}",
            dump_tags(tags),
            data_type
        ),
        Some(Data::FloatPoints(FloatPointsFrame { timestamps, values })) => format!(
            "FloatPointsFrame, timestamps: {:?}, values: {:?}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::IntegerPoints(IntegerPointsFrame { timestamps, values })) => format!(
            "IntegerPointsFrame, timestamps: {:?}, values: {:?}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::BooleanPoints(BooleanPointsFrame { timestamps, values })) => format!(
            "BooleanPointsFrame, timestamps: {:?}, values: {}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::StringPoints(StringPointsFrame { timestamps, values })) => format!(
            "StringPointsFrame, timestamps: {:?}, values: {}",
            timestamps,
            dump_values(values)
        ),
        Some(Data::Group(GroupFrame {
            tag_keys,
            partition_key_vals,
        })) => format!(
            "GroupFrame, tag_keys: {}, partition_key_vals: {}",
            dump_u8_vec(tag_keys),
            dump_u8_vec(partition_key_vals),
        ),
        None => "<NO data field>".into(),
        _ => ":thinking_face: unknown frame type".into(),
    }
}

fn dump_values<T>(v: &[T]) -> String
where
    T: std::fmt::Display,
{
    v.iter()
        .map(|item| format!("{}", item))
        .collect::<Vec<_>>()
        .join(",")
}

fn dump_u8_vec(encoded_strings: &[Vec<u8>]) -> String {
    encoded_strings
        .iter()
        .map(|b| String::from_utf8_lossy(b))
        .collect::<Vec<_>>()
        .join(",")
}

fn dump_tags(tags: &[Tag]) -> String {
    tags.iter()
        .map(|tag| {
            format!(
                "{}={}",
                String::from_utf8_lossy(&tag.key),
                String::from_utf8_lossy(&tag.value),
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}
