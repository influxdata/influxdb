use super::{dump::dump_data_frames, InfluxRpcTest};
use async_trait::async_trait;
use futures::{prelude::*, FutureExt};
use generated_types::aggregate::AggregateType;
use std::sync::Arc;
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

/// Sends the specified line protocol to a server, runs a read_window_aggregate
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
    let mut cluster = MiniCluster::create_shared2(database_url).await;

    let line_protocol = input_lines.join("\n");
    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(line_protocol),
            Step::Custom(Box::new(move |state: &mut StepTestState| {
                let request_builder = request_builder.clone();
                let expected_frames = expected_frames.clone();
                async move {
                    let mut storage_client = state.cluster().querier_storage_client();

                    let request = request_builder
                        .source(state.cluster())
                        .build_read_window_aggregate();

                    println!("Sending read_window_aggregate request {request:#?}");

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
                        "\n\nExpected:\n{expected_frames:#?}\nActual:\n{actual_frames:#?}",
                    );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}

#[tokio::test]
async fn nanoseconds() {
    Arc::new(ReadWindowAggregateTest {
        setup_name: "MeasurementForWindowAggregate",
        aggregate_type: AggregateType::Mean,
        every: 200,
        offset: 0,
        request: GrpcRequestBuilder::new()
            .or_tag_predicates([("city", "Boston"), ("city", "LA")].into_iter())
            .timestamp_range(100, 450),
        expected_results: vec![
            // note the name of the field is "temp" even though it is the average
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [200, 400, 600], values: \"70,71.5,73\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [200, 400, 600], values: \"90,91.5,93\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn nanoseconds_measurement_predicate() {
    Arc::new(ReadWindowAggregateTest {
        setup_name: "MeasurementForWindowAggregate",
        aggregate_type: AggregateType::Mean,
        every: 200,
        offset: 0,
        request: GrpcRequestBuilder::new()
            .not_measurement_predicate("other")
            .tag_predicate("city", "LA")
            .or_tag_predicates([("city", "Boston")].into_iter())
            .timestamp_range(100, 450),
        expected_results: vec![
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [200, 400, 600], values: \"70,71.5,73\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [200, 400, 600], values: \"90,91.5,93\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn nanoseconds_measurement_count() {
    Arc::new(ReadWindowAggregateTest {
        setup_name: "MeasurementForWindowAggregate",
        aggregate_type: AggregateType::Count,
        every: 200,
        offset: 0,
        request: GrpcRequestBuilder::new().timestamp_range(100, 450),
        expected_results: vec![
            // Expect that the type of `Count` is Integer
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [200, 400, 600], values: \"1,2,1\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Cambridge,state=MA, type: 1",
            "IntegerPointsFrame, timestamps: [200, 400, 600], values: \"1,2,1\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=LA,state=CA, type: 1",
            "IntegerPointsFrame, timestamps: [200, 400, 600], values: \"1,2,1\"",
        ],
    })
    .run()
    .await;
}

// See <https://github.com/influxdata/influxdb_iox/issues/2697>
#[tokio::test]
async fn min_defect_2697() {
    Arc::new(ReadWindowAggregateTest {
        setup_name: "MeasurementForDefect2697",
        aggregate_type: AggregateType::Min,
        every: 10,
        offset: 0,
        request: GrpcRequestBuilder::new()
            // time >= '2021-01-01T00:00:01.000000001Z' AND time <= '2021-01-01T00:00:01.000000031Z'
            .timestamp_range(1_609_459_201_000_000_001, 1_609_459_201_000_000_031),
        expected_results: vec![
            // Because the windowed aggregate is using a selector aggregate (one of MIN,
            // MAX, FIRST, LAST) we need to run a plan that brings along the timestamps
            // for the chosen aggregate in the window.
            "SeriesFrame, tags: _field=bar,_measurement=mm,section=1a, type: 0",
            "FloatPointsFrame, timestamps: [1609459201000000011], values: \"5\"",
            "SeriesFrame, tags: _field=foo,_measurement=mm,section=1a, type: 0",
            "FloatPointsFrame, timestamps: [1609459201000000001, 1609459201000000024], \
            values: \"1,11.24\"",
            "SeriesFrame, tags: _field=bar,_measurement=mm,section=2b, type: 0",
            "FloatPointsFrame, \
            timestamps: [1609459201000000009, 1609459201000000015, 1609459201000000022], \
            values: \"4,6,1.2\"",
            "SeriesFrame, tags: _field=foo,_measurement=mm,section=2b, type: 0",
            "FloatPointsFrame, timestamps: [1609459201000000002], values: \"2\"",
        ],
    })
    .run()
    .await;
}

// See <https://github.com/influxdata/influxdb_iox/issues/2697>
#[tokio::test]
async fn sum_defect_2697() {
    Arc::new(ReadWindowAggregateTest {
        setup_name: "MeasurementForDefect2697",
        aggregate_type: AggregateType::Sum,
        every: 10,
        offset: 0,
        request: GrpcRequestBuilder::new()
            // time >= '2021-01-01T00:00:01.000000001Z' AND time <= '2021-01-01T00:00:01.000000031Z'
            .timestamp_range(1_609_459_201_000_000_001, 1_609_459_201_000_000_031),
        expected_results: vec![
            // The windowed aggregate is using a non-selector aggregate (SUM, COUNT, MEAN).
            // For each distinct series the window defines the `time` column
            "SeriesFrame, tags: _field=bar,_measurement=mm,section=1a, type: 0",
            "FloatPointsFrame, timestamps: [1609459201000000020], values: \"5\"",
            "SeriesFrame, tags: _field=foo,_measurement=mm,section=1a, type: 0",
            "FloatPointsFrame, timestamps: [1609459201000000010, 1609459201000000030], \
            values: \"4,11.24\"",
            "SeriesFrame, tags: _field=bar,_measurement=mm,section=2b, type: 0",
            "FloatPointsFrame, \
            timestamps: [1609459201000000010, 1609459201000000020, 1609459201000000030], \
            values: \"4,6,1.2\"",
            "SeriesFrame, tags: _field=foo,_measurement=mm,section=2b, type: 0",
            "FloatPointsFrame, timestamps: [1609459201000000010], values: \"2\"",
        ],
    })
    .run()
    .await;
}

// See <https://github.com/influxdata/influxdb_iox/issues/2845>
//
// Adds coverage to window_aggregate plan for filtering on _field.
#[tokio::test]
async fn field_predicates() {
    Arc::new(ReadWindowAggregateTest {
        setup_name: "MeasurementForDefect2697",
        aggregate_type: AggregateType::Sum,
        every: 10,
        offset: 0,
        request: GrpcRequestBuilder::new()
            .field_predicate("foo")
            // time >= '2021-01-01T00:00:01.000000001Z' AND time <= '2021-01-01T00:00:01.000000031Z'
            .timestamp_range(1_609_459_201_000_000_001, 1_609_459_201_000_000_031),
        expected_results: vec![
            // The windowed aggregate is using a non-selector aggregate (SUM, COUNT, MEAN).
            // For each distinct series the window defines the `time` column
            "SeriesFrame, tags: _field=foo,_measurement=mm,section=1a, type: 0",
            "FloatPointsFrame, timestamps: [1609459201000000010, 1609459201000000030], \
            values: \"4,11.24\"",
            "SeriesFrame, tags: _field=foo,_measurement=mm,section=2b, type: 0",
            "FloatPointsFrame, timestamps: [1609459201000000010], values: \"2\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn overflow() {
    Arc::new(ReadWindowAggregateTest {
        setup_name: "MeasurementForDefect2890",
        aggregate_type: AggregateType::Max,
        // Note the giant window
        every: i64::MAX,
        offset: 0,
        request: GrpcRequestBuilder::new()
            .timestamp_range(1_609_459_201_000_000_001, 1_609_459_201_000_000_024),
        expected_results: vec![
            // The windowed aggregate is using a non-selector aggregate (SUM, COUNT, MEAN).
            // For each distinct series the window defines the `time` column
            "SeriesFrame, tags: _field=bar,_measurement=mm, type: 0",
            "FloatPointsFrame, timestamps: [1609459201000000015], values: \"6\"",
            "SeriesFrame, tags: _field=foo,_measurement=mm, type: 0",
            "FloatPointsFrame, timestamps: [1609459201000000005], values: \"3\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn periods() {
    Arc::new(ReadWindowAggregateTest {
        setup_name: "PeriodsInNames",
        aggregate_type: AggregateType::Max,
        every: 500_000_000_000,
        offset: 0,
        request: GrpcRequestBuilder::new().timestamp_range(0, 1_700_000_001_000_000_000),
        expected_results: vec![
            "SeriesFrame, tags: \
        _field=field.one,_measurement=measurement.one,tag.one=value,tag.two=other, type: 0",
            "FloatPointsFrame, timestamps: [1609459201000000001], values: \"1\"",
            "SeriesFrame, tags: \
        _field=field.two,_measurement=measurement.one,tag.one=value,tag.two=other, type: 3",
            "BooleanPointsFrame, timestamps: [1609459201000000001], values: true",
            "SeriesFrame, tags: \
        _field=field.one,_measurement=measurement.one,tag.one=value2,tag.two=other2, type: 0",
            "FloatPointsFrame, timestamps: [1609459201000000002], values: \"1\"",
            "SeriesFrame, tags: \
        _field=field.two,_measurement=measurement.one,tag.one=value2,tag.two=other2, type: 3",
            "BooleanPointsFrame, timestamps: [1609459201000000002], values: false",
        ],
    })
    .run()
    .await;
}

#[derive(Debug)]
struct ReadWindowAggregateTest {
    setup_name: &'static str,
    aggregate_type: AggregateType,
    every: i64,
    offset: i64,
    request: GrpcRequestBuilder,
    expected_results: Vec<&'static str>,
}

#[async_trait]
impl InfluxRpcTest for ReadWindowAggregateTest {
    fn setup_name(&self) -> &'static str {
        self.setup_name
    }

    async fn request_and_assert(&self, cluster: &MiniCluster) {
        let mut storage_client = cluster.querier_storage_client();

        let read_window_aggregate_request = self
            .request
            .clone()
            .source(cluster)
            .aggregate_type(self.aggregate_type)
            .window_every(self.every)
            .offset(self.offset)
            .build_read_window_aggregate();

        let read_window_aggregate_response = storage_client
            .read_window_aggregate(read_window_aggregate_request)
            .await
            .expect("successful read_window_aggregate call");

        let responses: Vec<_> = read_window_aggregate_response
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
