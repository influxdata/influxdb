use super::{dump::dump_data_frames, read_group_data, InfluxRpcTest};
use async_trait::async_trait;
use futures::{prelude::*, FutureExt};
use generated_types::{
    aggregate::AggregateType, read_group_request::Group, storage_client::StorageClient,
    ReadGroupRequest,
};
use influxdb_iox_client::connection::GrpcConnection;
use std::sync::Arc;
use test_helpers_end_to_end::{
    maybe_skip_integration, GrpcRequestBuilder, MiniCluster, Step, StepTest, StepTestState,
};

/// Standalone test for read_group with group keys and no aggregate
#[tokio::test]
async fn test_read_group_none_agg() {
    do_read_group_test(
        read_group_data(),
        GrpcRequestBuilder::new()
            .timestamp_range(0, 2001) // include all data
            .group_keys(["cpu"])
            .group(Group::By)
            .aggregate_type(AggregateType::None),
        vec![
            "GroupFrame, tag_keys: _field,_measurement,cpu,host, partition_key_vals: cpu1",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"20,21\"",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=foo, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"10,11\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"81,82\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=foo, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"71,72\"",
            "GroupFrame, tag_keys: _field,_measurement,cpu,host, partition_key_vals: cpu2",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"40,41\"",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=foo, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"30,31\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"51,52\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=foo, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"61,62\"",
        ],
    )
    .await
}

/// Test that predicates make it through
#[tokio::test]
async fn test_read_group_none_agg_with_predicate() {
    do_read_group_test(
        read_group_data(),
        GrpcRequestBuilder::new()
            // do not include data at timestamp 2000
            .timestamp_range(0, 2000)
            .field_predicate("usage_system")
            .group_keys(["cpu"])
            .group(Group::By)
            .aggregate_type(AggregateType::None),
        vec![
            "GroupFrame, tag_keys: _field,_measurement,cpu,host, partition_key_vals: cpu1",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"20\"",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=foo, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"10\"",
            "GroupFrame, tag_keys: _field,_measurement,cpu,host, partition_key_vals: cpu2",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"40\"",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=foo, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"30\"",
        ],
    )
    .await
}

// Standalone test for read_group with group keys and an actual
// "aggregate" (not a "selector" style).  assumes that
// read_group_data has been previously loaded
#[tokio::test]
async fn test_read_group_sum_agg() {
    do_read_group_test(
        read_group_data(),
        // read_group(group_keys: region, agg: Sum)
        GrpcRequestBuilder::new()
            .timestamp_range(0, 2001) // include all data
            .group_keys(["cpu"])
            .group(Group::By)
            .aggregate_type(AggregateType::Sum),
        vec![
            "GroupFrame, tag_keys: _field,_measurement,cpu,host, partition_key_vals: cpu1",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"41\"",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=foo, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"21\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"163\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=foo, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"143\"",
            "GroupFrame, tag_keys: _field,_measurement,cpu,host, partition_key_vals: cpu2",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"81\"",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=foo, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"61\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"103\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=foo, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"123\"",
        ],
    )
    .await;
}

// Standalone test for read_group with group keys the count aggregate
// (returns a different type than the field types)
#[tokio::test]
async fn test_read_group_count_agg() {
    do_read_group_test(
        read_group_data(),
        // read_group(group_keys: region, agg: Count)
        GrpcRequestBuilder::new()
            .timestamp_range(0, 2001) // include all data
            .group_keys(["cpu"])
            .group(Group::By)
            .aggregate_type(AggregateType::Count),
        vec![
            "GroupFrame, tag_keys: _field,_measurement,cpu,host, partition_key_vals: cpu1",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=bar, type: 1",
            "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=foo, type: 1",
            "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=bar, type: 1",
            "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=foo, type: 1",
            "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
            "GroupFrame, tag_keys: _field,_measurement,cpu,host, partition_key_vals: cpu2",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=bar, type: 1",
            "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=foo, type: 1",
            "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=bar, type: 1",
            "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=foo, type: 1",
            "IntegerPointsFrame, timestamps: [2000], values: \"2\"",
        ],
    )
    .await
}

// Standalone test for read_group with group keys and an actual
// "selector" function last.  assumes that
// read_group_data has been previously loaded
#[tokio::test]
async fn test_read_group_last_agg() {
    do_read_group_test(
        read_group_data(),
        // read_group(group_keys: region, agg: Last)
        GrpcRequestBuilder::new()
            .timestamp_range(0, 2001) // include all data
            .group_keys(["cpu"])
            .group(Group::By)
            .aggregate_type(AggregateType::Last),
        vec![
            "GroupFrame, tag_keys: _field,_measurement,cpu,host, partition_key_vals: cpu1",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"21\"",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=foo, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"11\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"82\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=foo, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"72\"",
            "GroupFrame, tag_keys: _field,_measurement,cpu,host, partition_key_vals: cpu2",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"41\"",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=foo, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"31\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"52\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=foo, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"62\"",
        ],
    )
    .await
}

/// Ensure we can group on tags that have periods
#[tokio::test]
async fn test_read_group_periods() {
    do_read_group_test(
        vec![
            "measurement.one,tag.one=foo field.one=1,field.two=100 1000",
            "measurement.one,tag.one=bar field.one=2,field.two=200 2000",
        ],
        // read_group(group_keys: region, agg: Last)
        GrpcRequestBuilder::new()
            .timestamp_range(0, 2001) // include all data
            .field_predicate("field.two")
            .group_keys(["tag.one"])
            .group(Group::By)
            .aggregate_type(AggregateType::Last),
        vec![
            "GroupFrame, tag_keys: _field,_measurement,tag.one, partition_key_vals: bar",
            "SeriesFrame, tags: _field=field.two,_measurement=measurement.one,tag.one=bar, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"200\"",
            "GroupFrame, tag_keys: _field,_measurement,tag.one, partition_key_vals: foo",
            "SeriesFrame, tags: _field=field.two,_measurement=measurement.one,tag.one=foo, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"100\"",
        ],
    )
    .await
}

#[tokio::test]
async fn test_group_key_not_found() {
    do_test_invalid_group_key(InvalidGroupKey::ColNotFound).await;
}

#[tokio::test]
async fn test_not_a_tag() {
    do_test_invalid_group_key(InvalidGroupKey::NotATag).await;
}

#[tokio::test]
async fn test_duplicate_group_keys() {
    do_test_invalid_group_key(InvalidGroupKey::DuplicateKeys).await;
}

#[tokio::test]
async fn test_group_by_time() {
    do_test_invalid_group_key(InvalidGroupKey::Time).await;
}

#[derive(Clone, Copy)]
enum InvalidGroupKey {
    ColNotFound,
    NotATag,
    DuplicateKeys,
    Time,
}

async fn do_test_invalid_group_key(variant: InvalidGroupKey) {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol("measurement,tag=foo field=1 1000".to_string()),
            Step::WaitForReadable,
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                async move {
                    let grpc_connection = state
                        .cluster()
                        .querier()
                        .querier_grpc_connection()
                        .into_grpc_connection();
                    let mut storage_client = StorageClient::new(grpc_connection);

                    let group_keys = match variant {
                        InvalidGroupKey::ColNotFound => ["tag", "unknown_tag"],
                        InvalidGroupKey::NotATag => ["tag", "field"],
                        InvalidGroupKey::DuplicateKeys => ["tag", "tag"],
                        InvalidGroupKey::Time => ["tag", "time"],
                    };

                    let read_group_request = GrpcRequestBuilder::new()
                        .timestamp_range(0, 2000)
                        .field_predicate("field")
                        .group_keys(group_keys)
                        .group(Group::By)
                        .aggregate_type(AggregateType::Last)
                        .source(state.cluster())
                        .build_read_group();

                    let status = storage_client
                        .read_group(read_group_request)
                        .await
                        .unwrap_err();
                    assert_eq!(
                        status.code(),
                        tonic::Code::InvalidArgument,
                        "Wrong status code: {}\n\nStatus:\n{}",
                        status.code(),
                        status,
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

/// Sends the specified line protocol to a server, runs a read_grou
/// gRPC request, and compares it against expected frames
async fn do_read_group_test(
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
                let request_builder = request_builder.clone();
                let expected_frames = expected_frames.clone();
                async move {
                    let grpc_connection = state
                        .cluster()
                        .querier()
                        .querier_grpc_connection()
                        .into_grpc_connection();
                    let mut storage_client = StorageClient::new(grpc_connection);

                    println!("Sending read_group request with {:#?}", request_builder);

                    let read_group_request =
                        request_builder.source(state.cluster()).build_read_group();

                    let actual_frames =
                        do_read_group_request(&mut storage_client, read_group_request).await;

                    assert_eq!(
                        expected_frames, actual_frames,
                        "\n\nExpected:\n{:#?}\n\nActual:\n{:#?}\n\n",
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

/// Make a read_group request and returns the results in a comparable format
async fn do_read_group_request(
    storage_client: &mut StorageClient<GrpcConnection>,
    request: tonic::Request<ReadGroupRequest>,
) -> Vec<String> {
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

#[tokio::test]
async fn no_tag_columns_count() {
    Arc::new(ReadGroupTest {
        setup_name: "OneMeasurementNoTags2",
        aggregate_type: AggregateType::Count,
        group_keys: vec![],
        request: GrpcRequestBuilder::new(),
        expected_results: vec![
            "GroupFrame, tag_keys: _field,_measurement, partition_key_vals: ",
            "SeriesFrame, tags: _field=foo,_measurement=m0, type: 1",
            "IntegerPointsFrame, timestamps: [2], values: \"2\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn no_tag_columns_min() {
    Arc::new(ReadGroupTest {
        setup_name: "OneMeasurementNoTags2",
        aggregate_type: AggregateType::Min,
        group_keys: vec![],
        request: GrpcRequestBuilder::new(),
        expected_results: vec![
            "GroupFrame, tag_keys: _field,_measurement, partition_key_vals: ",
            "SeriesFrame, tags: _field=foo,_measurement=m0, type: 0",
            "FloatPointsFrame, timestamps: [1], values: \"1\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn tag_predicates() {
    Arc::new(ReadGroupTest {
        setup_name: "OneMeasurementForAggs",
        aggregate_type: AggregateType::Sum,
        group_keys: vec!["state"],
        request: GrpcRequestBuilder::new()
            .tag_predicate("city", "LA")
            .timestamp_range(190, 210),
        expected_results: vec![
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: CA",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [200], values: \"90\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn field_predicates() {
    Arc::new(ReadGroupTest {
        setup_name: "OneMeasurementForAggs",
        aggregate_type: AggregateType::Sum,
        group_keys: vec!["state"],
        request: GrpcRequestBuilder::new().field_predicate("temp"),
        expected_results: vec![
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: CA",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [350], values: \"180\"",
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: MA",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [250], values: \"142.8\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_sum() {
    Arc::new(ReadGroupTest {
        setup_name: "AnotherMeasurementForAggs",
        aggregate_type: AggregateType::Sum,
        group_keys: vec!["state"],
        request: GrpcRequestBuilder::new()
            .or_tag_predicates(
                // city=Boston OR city=Cambridge (filters out LA rows)
                [("city", "Boston"), ("city", "Cambridge")].into_iter(),
            )
            // fiter out first Cambridge row
            .timestamp_range(100, 1000),
        expected_results: vec![
            // The null field (after predicates) are not sent as series. Note order of city key
            // (boston --> cambridge)
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: MA",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [400], values: \"141\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Cambridge,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [200], values: \"163\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_count() {
    Arc::new(ReadGroupTest {
        setup_name: "AnotherMeasurementForAggs",
        aggregate_type: AggregateType::Count,
        group_keys: vec!["state"],
        request: GrpcRequestBuilder::new()
            .or_tag_predicates(
                // city=Boston OR city=Cambridge (filters out LA rows)
                [("city", "Boston"), ("city", "Cambridge")].into_iter(),
            )
            // fiter out first Cambridge row
            .timestamp_range(100, 1000),
        expected_results: vec![
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: MA",
            "SeriesFrame, tags: _field=humidity,_measurement=h2o,city=Boston,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [400], values: \"0\"",
            "SeriesFrame, tags: _field=humidity,_measurement=h2o,city=Cambridge,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [200], values: \"0\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [400], values: \"2\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Cambridge,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [200], values: \"2\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_mean() {
    Arc::new(ReadGroupTest {
        setup_name: "AnotherMeasurementForAggs",
        aggregate_type: AggregateType::Mean,
        group_keys: vec!["state"],
        request: GrpcRequestBuilder::new()
            .or_tag_predicates(
                // city=Boston OR city=Cambridge (filters out LA rows)
                [("city", "Boston"), ("city", "Cambridge")].into_iter(),
            )
            // fiter out first Cambridge row
            .timestamp_range(100, 1000),
        expected_results: vec![
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: MA",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [400], values: \"70.5\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Cambridge,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [200], values: \"81.5\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_count_measurement_predicate() {
    Arc::new(ReadGroupTest {
        setup_name: "TwoMeasurementForAggs",
        aggregate_type: AggregateType::Count,
        group_keys: vec!["state"],
        request: GrpcRequestBuilder::new()
            .measurement_predicate("o2")
            .or_tag_predicates([("city", "Boston")].into_iter()),
        expected_results: vec![
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: CA",
            "SeriesFrame, tags: _field=temp,_measurement=o2,city=LA,state=CA, type: 1",
            "IntegerPointsFrame, timestamps: [350], values: \"2\"",
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: MA",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [250], values: \"2\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_first() {
    Arc::new(ReadGroupTest {
        setup_name: "MeasurementForSelectors",
        aggregate_type: AggregateType::First,
        group_keys: vec!["state"],
        request: GrpcRequestBuilder::new()
            // fiter out first row (ts 1000)
            .timestamp_range(1001, 4001),
        expected_results: vec![
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: MA",
            "SeriesFrame, tags: _field=b,_measurement=h2o,city=Cambridge,state=MA, type: 3",
            "BooleanPointsFrame, timestamps: [2000], values: true",
            "SeriesFrame, tags: _field=f,_measurement=h2o,city=Cambridge,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"7\"",
            "SeriesFrame, tags: _field=i,_measurement=h2o,city=Cambridge,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [2000], values: \"7\"",
            "SeriesFrame, tags: _field=s,_measurement=h2o,city=Cambridge,state=MA, type: 4",
            "StringPointsFrame, timestamps: [2000], values: c",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_first_with_nulls() {
    Arc::new(ReadGroupTest {
        setup_name: "TwoMeasurementsManyFields",
        aggregate_type: AggregateType::First,
        group_keys: vec!["state"],
        request: GrpcRequestBuilder::new()
            // return three rows, but one series
            // "h2o,state=MA,city=Boston temp=70.4 50",
            // "h2o,state=MA,city=Boston other_temp=70.4 250",
            // "h2o,state=MA,city=Boston temp=70.4,moisture=43.0 100000"
            .tag_predicate("state", "MA")
            .tag_predicate("city", "Boston")
            .measurement_predicate("h2o"),
        expected_results: vec![
            // expect timestamps to be present for all three series
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: MA",
            "SeriesFrame, tags: _field=moisture,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [100000], values: \"43\"",
            "SeriesFrame, tags: _field=other_temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [250], values: \"70.4\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [50], values: \"70.4\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_last() {
    Arc::new(ReadGroupTest {
        setup_name: "MeasurementForSelectors",
        aggregate_type: AggregateType::Last,
        group_keys: vec!["state"],
        request: GrpcRequestBuilder::new()
            // fiter out last row (ts 4000)
            .timestamp_range(100, 3999),
        expected_results: vec![
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: MA",
            "SeriesFrame, tags: _field=b,_measurement=h2o,city=Cambridge,state=MA, type: 3",
            "BooleanPointsFrame, timestamps: [3000], values: false",
            "SeriesFrame, tags: _field=f,_measurement=h2o,city=Cambridge,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [3000], values: \"6\"",
            "SeriesFrame, tags: _field=i,_measurement=h2o,city=Cambridge,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [3000], values: \"6\"",
            "SeriesFrame, tags: _field=s,_measurement=h2o,city=Cambridge,state=MA, type: 4",
            "StringPointsFrame, timestamps: [3000], values: b",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_last_with_nulls() {
    Arc::new(ReadGroupTest {
        setup_name: "TwoMeasurementsManyFields",
        aggregate_type: AggregateType::Last,
        group_keys: vec!["state"],
        request: GrpcRequestBuilder::new()
            // return two three:
            .tag_predicate("state", "MA")
            .tag_predicate("city", "Boston")
            .measurement_predicate("h2o"),
        expected_results: vec![
            // expect timestamps to be present for all three series
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: MA",
            "SeriesFrame, tags: _field=moisture,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [100000], values: \"43\"",
            "SeriesFrame, tags: _field=other_temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [250], values: \"70.4\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [100000], values: \"70.4\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_min() {
    Arc::new(ReadGroupTest {
        setup_name: "MeasurementForMin",
        aggregate_type: AggregateType::Min,
        group_keys: vec!["state"],
        request: GrpcRequestBuilder::new()
            // fiter out last row (ts 4000)
            .timestamp_range(100, 3999),
        expected_results: vec![
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: MA",
            "SeriesFrame, tags: _field=b,_measurement=h2o,city=Cambridge,state=MA, type: 3",
            "BooleanPointsFrame, timestamps: [1000], values: false",
            "SeriesFrame, tags: _field=f,_measurement=h2o,city=Cambridge,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [3000], values: \"6\"",
            "SeriesFrame, tags: _field=i,_measurement=h2o,city=Cambridge,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [3000], values: \"6\"",
            "SeriesFrame, tags: _field=s,_measurement=h2o,city=Cambridge,state=MA, type: 4",
            "StringPointsFrame, timestamps: [2000], values: a",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_max() {
    Arc::new(ReadGroupTest {
        setup_name: "MeasurementForMax",
        aggregate_type: AggregateType::Max,
        group_keys: vec!["state"],
        request: GrpcRequestBuilder::new()
            // fiter out first row (ts 1000)
            .timestamp_range(1001, 4001),
        expected_results: vec![
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: MA",
            "SeriesFrame, tags: _field=b,_measurement=h2o,city=Cambridge,state=MA, type: 3",
            "BooleanPointsFrame, timestamps: [3000], values: true",
            "SeriesFrame, tags: _field=f,_measurement=h2o,city=Cambridge,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"7\"",
            "SeriesFrame, tags: _field=i,_measurement=h2o,city=Cambridge,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [2000], values: \"7\"",
            "SeriesFrame, tags: _field=s,_measurement=h2o,city=Cambridge,state=MA, type: 4",
            "StringPointsFrame, timestamps: [4000], values: z",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_group_by_state_city() {
    Arc::new(ReadGroupTest {
        setup_name: "MeasurementForGroupKeys",
        aggregate_type: AggregateType::Sum,
        group_keys: vec!["state", "city"],
        request: GrpcRequestBuilder::new(),
        expected_results: vec![
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: CA,LA",
            "SeriesFrame, tags: _field=humidity,_measurement=h2o,city=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [600], values: \"21\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [600], values: \"181\"",
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: MA,Boston",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [400], values: \"141\"",
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: \
            MA,Cambridge",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Cambridge,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [200], values: \"243\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_group_by_city_state() {
    Arc::new(ReadGroupTest {
        setup_name: "MeasurementForGroupKeys",
        aggregate_type: AggregateType::Sum,
        group_keys: vec!["city", "state"], // <-- note different order from previous test
        request: GrpcRequestBuilder::new(),
        expected_results: vec![
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: Boston,MA",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [400], values: \"141\"",
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: \
            Cambridge,MA",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Cambridge,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [200], values: \"243\"",
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: LA,CA",
            "SeriesFrame, tags: _field=humidity,_measurement=h2o,city=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [600], values: \"21\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [600], values: \"181\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_group_aggregate_none() {
    Arc::new(ReadGroupTest {
        setup_name: "MeasurementForGroupKeys",
        aggregate_type: AggregateType::None,
        group_keys: vec!["city", "state"],
        request: GrpcRequestBuilder::new(),
        expected_results: vec![
            // Expect order of the columns to begin with city/state
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: Boston,MA",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [300, 400], values: \"70,71\"",
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: \
            Cambridge,MA",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Cambridge,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [50, 100, 200], values: \"80,81,82\"",
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: LA,CA",
            "SeriesFrame, tags: _field=humidity,_measurement=h2o,city=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [500, 600], values: \"10,11\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [500, 600], values: \"90,91\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_group_by_field_none() {
    Arc::new(ReadGroupTest {
        setup_name: "MeasurementForGroupByField",
        aggregate_type: AggregateType::None,
        group_keys: vec!["_field"],
        request: GrpcRequestBuilder::new(),
        expected_results: vec![
            // Expect the data is grouped so all the distinct values of load1 are before the values
            // for load2
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: load1",
            "SeriesFrame, tags: _field=load1,_measurement=aa_system,host=local,region=C, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"100.1\"",
            "SeriesFrame, tags: _field=load1,_measurement=system,host=local,region=A, type: 0",
            "FloatPointsFrame, timestamps: [100, 200], values: \"1.1,1.2\"",
            "SeriesFrame, tags: _field=load1,_measurement=system,host=local,region=C, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"100.1\"",
            "SeriesFrame, tags: _field=load1,_measurement=system,host=remote,region=B, type: 0",
            "FloatPointsFrame, timestamps: [100, 200], values: \"10.1,10.2\"",
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: load2",
            "SeriesFrame, tags: _field=load2,_measurement=aa_system,host=local,region=C, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"200.1\"",
            "SeriesFrame, tags: _field=load2,_measurement=system,host=local,region=A, type: 0",
            "FloatPointsFrame, timestamps: [100, 200], values: \"2.1,2.2\"",
            "SeriesFrame, tags: _field=load2,_measurement=system,host=local,region=C, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"200.1\"",
            "SeriesFrame, tags: _field=load2,_measurement=system,host=remote,region=B, type: 0",
            "FloatPointsFrame, timestamps: [100, 200], values: \"2.1,20.2\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_group_by_field_and_tag_none() {
    Arc::new(ReadGroupTest {
        setup_name: "MeasurementForGroupByField",
        aggregate_type: AggregateType::None,
        group_keys: vec!["_field", "region"],
        request: GrpcRequestBuilder::new(),
        expected_results: vec![
            // Expect the data is grouped so all the distinct values of load1 are before the values
            // for load2, grouped by region
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: load1,A",
            "SeriesFrame, tags: _field=load1,_measurement=system,host=local,region=A, type: 0",
            "FloatPointsFrame, timestamps: [100, 200], values: \"1.1,1.2\"",
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: load1,B",
            "SeriesFrame, tags: _field=load1,_measurement=system,host=remote,region=B, type: 0",
            "FloatPointsFrame, timestamps: [100, 200], values: \"10.1,10.2\"",
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: load1,C",
            "SeriesFrame, tags: _field=load1,_measurement=aa_system,host=local,region=C, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"100.1\"",
            "SeriesFrame, tags: _field=load1,_measurement=system,host=local,region=C, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"100.1\"",
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: load2,A",
            "SeriesFrame, tags: _field=load2,_measurement=system,host=local,region=A, type: 0",
            "FloatPointsFrame, timestamps: [100, 200], values: \"2.1,2.2\"",
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: load2,B",
            "SeriesFrame, tags: _field=load2,_measurement=system,host=remote,region=B, type: 0",
            "FloatPointsFrame, timestamps: [100, 200], values: \"2.1,20.2\"",
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: load2,C",
            "SeriesFrame, tags: _field=load2,_measurement=aa_system,host=local,region=C, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"200.1\"",
            "SeriesFrame, tags: _field=load2,_measurement=system,host=local,region=C, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"200.1\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_group_by_tag_and_field_none() {
    Arc::new(ReadGroupTest {
        setup_name: "MeasurementForGroupByField",
        aggregate_type: AggregateType::None,
        group_keys: vec!["region", "_field"], // note group by the tag first then the field
        request: GrpcRequestBuilder::new(),
        expected_results: vec![
            // Output shoud be sorted on region first and then _field
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: A,load1",
            "SeriesFrame, tags: _field=load1,_measurement=system,host=local,region=A, type: 0",
            "FloatPointsFrame, timestamps: [100, 200], values: \"1.1,1.2\"",
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: A,load2",
            "SeriesFrame, tags: _field=load2,_measurement=system,host=local,region=A, type: 0",
            "FloatPointsFrame, timestamps: [100, 200], values: \"2.1,2.2\"",
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: B,load1",
            "SeriesFrame, tags: _field=load1,_measurement=system,host=remote,region=B, type: 0",
            "FloatPointsFrame, timestamps: [100, 200], values: \"10.1,10.2\"",
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: B,load2",
            "SeriesFrame, tags: _field=load2,_measurement=system,host=remote,region=B, type: 0",
            "FloatPointsFrame, timestamps: [100, 200], values: \"2.1,20.2\"",
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: C,load1",
            "SeriesFrame, tags: _field=load1,_measurement=aa_system,host=local,region=C, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"100.1\"",
            "SeriesFrame, tags: _field=load1,_measurement=system,host=local,region=C, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"100.1\"",
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: C,load2",
            "SeriesFrame, tags: _field=load2,_measurement=aa_system,host=local,region=C, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"200.1\"",
            "SeriesFrame, tags: _field=load2,_measurement=system,host=local,region=C, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"200.1\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_group_measurement_tag_count() {
    Arc::new(ReadGroupTest {
        setup_name: "MeasurementForGroupByField",
        aggregate_type: AggregateType::Count,
        group_keys: vec!["_measurement", "region"],
        request: GrpcRequestBuilder::new(),
        expected_results: vec![
            // Expect the data is grouped so output is sorted by measurement and then region
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: \
            aa_system,C",
            "SeriesFrame, tags: _field=load1,_measurement=aa_system,host=local,region=C, type: 1",
            "IntegerPointsFrame, timestamps: [100], values: \"1\"",
            "SeriesFrame, tags: _field=load2,_measurement=aa_system,host=local,region=C, type: 1",
            "IntegerPointsFrame, timestamps: [100], values: \"1\"",
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: system,A",
            "SeriesFrame, tags: _field=load1,_measurement=system,host=local,region=A, type: 1",
            "IntegerPointsFrame, timestamps: [200], values: \"2\"",
            "SeriesFrame, tags: _field=load2,_measurement=system,host=local,region=A, type: 1",
            "IntegerPointsFrame, timestamps: [200], values: \"2\"",
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: system,B",
            "SeriesFrame, tags: _field=load1,_measurement=system,host=remote,region=B, type: 1",
            "IntegerPointsFrame, timestamps: [200], values: \"2\"",
            "SeriesFrame, tags: _field=load2,_measurement=system,host=remote,region=B, type: 1",
            "IntegerPointsFrame, timestamps: [200], values: \"2\"",
            "GroupFrame, tag_keys: _field,_measurement,host,region, partition_key_vals: system,C",
            "SeriesFrame, tags: _field=load1,_measurement=system,host=local,region=C, type: 1",
            "IntegerPointsFrame, timestamps: [100], values: \"1\"",
            "SeriesFrame, tags: _field=load2,_measurement=system,host=local,region=C, type: 1",
            "IntegerPointsFrame, timestamps: [100], values: \"1\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_group_field_start_stop() {
    Arc::new(ReadGroupTest {
        setup_name: "TwoMeasurementsManyFieldsOneChunk",
        aggregate_type: AggregateType::Count,
        group_keys: vec!["_start", "_stop", "state"],
        request: GrpcRequestBuilder::new().measurement_predicate("o2"),
        expected_results: vec![
            // Expect the data is grouped so output is sorted by state, with blank partition values
            // for _start and _stop (mirroring TSM)
            "GroupFrame, tag_keys: _field,_measurement,state, partition_key_vals: ,,CA",
            "SeriesFrame, tags: _field=reading,_measurement=o2,state=CA, type: 1",
            "IntegerPointsFrame, timestamps: [300], values: \"0\"",
            "SeriesFrame, tags: _field=temp,_measurement=o2,state=CA, type: 1",
            "IntegerPointsFrame, timestamps: [300], values: \"1\"",
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: ,,MA",
            "SeriesFrame, tags: _field=reading,_measurement=o2,city=Boston,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [50], values: \"1\"",
            "SeriesFrame, tags: _field=temp,_measurement=o2,city=Boston,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [50], values: \"1\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_group_field_stop_start() {
    Arc::new(ReadGroupTest {
        setup_name: "TwoMeasurementsManyFieldsOneChunk",
        aggregate_type: AggregateType::Count,
        // note the order is different from the previous test
        group_keys: vec!["_stop", "_start", "state"],
        request: GrpcRequestBuilder::new().measurement_predicate("o2"),
        expected_results: vec![
            // Note the expected results are the same as the previous test
            "GroupFrame, tag_keys: _field,_measurement,state, partition_key_vals: ,,CA",
            "SeriesFrame, tags: _field=reading,_measurement=o2,state=CA, type: 1",
            "IntegerPointsFrame, timestamps: [300], values: \"0\"",
            "SeriesFrame, tags: _field=temp,_measurement=o2,state=CA, type: 1",
            "IntegerPointsFrame, timestamps: [300], values: \"1\"",
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: ,,MA",
            "SeriesFrame, tags: _field=reading,_measurement=o2,city=Boston,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [50], values: \"1\"",
            "SeriesFrame, tags: _field=temp,_measurement=o2,city=Boston,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [50], values: \"1\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_group_field_pred_and_null_fields() {
    Arc::new(ReadGroupTest {
        setup_name: "TwoMeasurementsManyFieldsOneChunk",
        aggregate_type: AggregateType::Count,
        group_keys: vec!["state", "_field"],
        request: GrpcRequestBuilder::new().measurement_predicate("o2"),
        expected_results: vec![
            // Expect the data is grouped so output is sorted by measurement state
            "GroupFrame, tag_keys: _field,_measurement,state, partition_key_vals: CA,reading",
            "SeriesFrame, tags: _field=reading,_measurement=o2,state=CA, type: 1",
            "IntegerPointsFrame, timestamps: [300], values: \"0\"",
            "GroupFrame, tag_keys: _field,_measurement,state, partition_key_vals: CA,temp",
            "SeriesFrame, tags: _field=temp,_measurement=o2,state=CA, type: 1",
            "IntegerPointsFrame, timestamps: [300], values: \"1\"",
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: MA,reading",
            "SeriesFrame, tags: _field=reading,_measurement=o2,city=Boston,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [50], values: \"1\"",
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: MA,temp",
            "SeriesFrame, tags: _field=temp,_measurement=o2,city=Boston,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [50], values: \"1\"",
        ],
    })
    .run()
    .await;
}

// See issue: https://github.com/influxdata/influxdb_iox/issues/2845
//
// This test adds coverage for filtering on _field when executing a read_group plan.
#[tokio::test]
async fn grouped_series_set_plan_group_field_pred_filter_on_field() {
    Arc::new(ReadGroupTest {
        setup_name: "TwoMeasurementsManyFieldsOneChunk",
        aggregate_type: AggregateType::Count,
        group_keys: vec!["state", "_field"],
        request: GrpcRequestBuilder::new()
            .field_predicate("reading")
            .measurement_predicate("o2"),
        expected_results: vec![
            // Expect the data is grouped so output is sorted by measurement and then region
            "GroupFrame, tag_keys: _field,_measurement,state, partition_key_vals: CA,reading",
            "SeriesFrame, tags: _field=reading,_measurement=o2,state=CA, type: 1",
            "IntegerPointsFrame, timestamps: [300], values: \"0\"",
            "GroupFrame, tag_keys: _field,_measurement,city,state, partition_key_vals: MA,reading",
            "SeriesFrame, tags: _field=reading,_measurement=o2,city=Boston,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [50], values: \"1\"",
        ],
    })
    .run()
    .await;
}

// See issue: https://github.com/influxdata/influxdb_iox/issues/2691
//
// This test adds coverage for filtering on _value when executing a read_group plan.
#[tokio::test]
async fn grouped_series_set_plan_group_field_pred_filter_on_value() {
    Arc::new(ReadGroupTest {
        setup_name: "MeasurementForDefect2691",
        aggregate_type: AggregateType::Max,
        group_keys: vec!["_field"],
        request: GrpcRequestBuilder::new()
            // 2018-05-22T19:53:26Z, stop: 2018-05-24T00:00:00Z
            .timestamp_range(1_527_018_806_000_000_000, 1_527_120_000_000_000_000)
            .field_value_predicate(1.77),
        expected_results: vec![
            // Expect the data is grouped so output is sorted by measurement and then region
            "GroupFrame, tag_keys: _field,_measurement,host, partition_key_vals: load4",
            "SeriesFrame, tags: _field=load4,_measurement=system,host=host.local, type: 0",
            "FloatPointsFrame, timestamps: [1527018806000000000], values: \"1.77\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_group_field_pred_filter_on_multiple_value() {
    Arc::new(ReadGroupTest {
        setup_name: "MeasurementForDefect2691",
        aggregate_type: AggregateType::Max,
        group_keys: vec!["_field"],
        request: GrpcRequestBuilder::new()
            // 2018-05-22T19:53:26Z, stop: 2018-05-24T00:00:00Z
            .timestamp_range(1_527_018_806_000_000_000, 1_527_120_000_000_000_000)
            .or_field_value_predicates([1.77, 1.72].into_iter()),
        expected_results: vec![
            // Expect the data is grouped so output is sorted by measurement and then region
            "GroupFrame, tag_keys: _field,_measurement,host, partition_key_vals: load3",
            "SeriesFrame, tags: _field=load3,_measurement=system,host=host.local, type: 0",
            "FloatPointsFrame, timestamps: [1527018806000000000], values: \"1.72\"",
            "GroupFrame, tag_keys: _field,_measurement,host, partition_key_vals: load4",
            "SeriesFrame, tags: _field=load4,_measurement=system,host=host.local, type: 0",
            "FloatPointsFrame, timestamps: [1527018806000000000], values: \"1.77\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn grouped_series_set_plan_group_field_pred_filter_on_value_sum() {
    Arc::new(ReadGroupTest {
        setup_name: "MeasurementForDefect2691",
        aggregate_type: AggregateType::Sum,
        group_keys: vec!["_field"],
        request: GrpcRequestBuilder::new()
            // 2018-05-22T19:53:26Z, stop: 2018-05-24T00:00:00Z
            .timestamp_range(1_527_018_806_000_000_000, 1_527_120_000_000_000_000)
            .field_value_predicate(1.77),
        expected_results: vec![
            // Expect the data is grouped so output is sorted by measurement and then region
            "GroupFrame, tag_keys: _field,_measurement,host, partition_key_vals: load4",
            "SeriesFrame, tags: _field=load4,_measurement=system,host=host.local, type: 0",
            "FloatPointsFrame, timestamps: [1527018826000000000], values: \"3.54\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn read_group_with_periods() {
    Arc::new(ReadGroupTest {
        setup_name: "PeriodsInNames",
        aggregate_type: AggregateType::Sum,
        group_keys: vec!["_field"],
        request: GrpcRequestBuilder::new()
            .timestamp_range(0, 1_700_000_001_000_000_000)
            .field_predicate("field.one"),
        expected_results: vec![
            "GroupFrame, tag_keys: _field,_measurement,tag.one,tag.two, partition_key_vals: \
            field.one",
            "SeriesFrame, tags: \
            _field=field.one,_measurement=measurement.one,tag.one=value,tag.two=other, type: 0",
            "FloatPointsFrame, timestamps: [1609459201000000001], values: \"1\"",
            "SeriesFrame, tags: \
            _field=field.one,_measurement=measurement.one,tag.one=value2,tag.two=other2, type: 0",
            "FloatPointsFrame, timestamps: [1609459201000000002], values: \"1\"",
        ],
    })
    .run()
    .await;
}

#[derive(Debug)]
struct ReadGroupTest {
    setup_name: &'static str,
    aggregate_type: AggregateType,
    group_keys: Vec<&'static str>,
    request: GrpcRequestBuilder,
    expected_results: Vec<&'static str>,
}

#[async_trait]
impl InfluxRpcTest for ReadGroupTest {
    fn setup_name(&self) -> &'static str {
        self.setup_name
    }

    async fn request_and_assert(&self, cluster: &MiniCluster) {
        let mut storage_client = cluster.querier_storage_client();

        let read_group_request = self
            .request
            .clone()
            .source(cluster)
            .aggregate_type(self.aggregate_type)
            .group_keys(self.group_keys.clone().into_iter())
            .group(Group::By)
            .build_read_group();

        let read_group_response = storage_client
            .read_group(read_group_request)
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

        let results = dump_data_frames(&frames);

        assert_eq!(results, self.expected_results);
    }
}
