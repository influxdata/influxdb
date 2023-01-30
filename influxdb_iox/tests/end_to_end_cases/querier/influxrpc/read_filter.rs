use super::{dump::dump_data_frames, read_group_data, run_data_test, InfluxRpcTest};
use async_trait::async_trait;
use futures::{prelude::*, FutureExt};
use generated_types::{
    node::Logical, read_response::frame::Data, storage_client::StorageClient, ReadFilterRequest,
};
use influxdb_iox_client::connection::GrpcConnection;
use std::sync::Arc;
use test_helpers_end_to_end::{
    maybe_skip_integration, DataGenerator, GrpcRequestBuilder, MiniCluster, Step, StepTest,
    StepTestState,
};

#[tokio::test]
async fn read_filter() {
    let generator = Arc::new(DataGenerator::new());
    run_data_test(Arc::clone(&generator), Box::new(move |state: &mut StepTestState| {
        let generator = Arc::clone(&generator);
        async move {
            let mut storage_client = state.cluster().querier_storage_client();

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
pub async fn read_filter_regex_operator() {
    do_read_filter_test(
        read_group_data(),
        GrpcRequestBuilder::new()
            .timestamp_range(0, 2001) // include all data
            .regex_match_predicate("host", "^b.+"),
        vec![
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"20,21\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu1,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"81,82\"",
            "SeriesFrame, tags: _field=usage_system,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"40,41\"",
            "SeriesFrame, tags: _field=usage_user,_measurement=cpu,cpu=cpu2,host=bar, type: 0",
            "FloatPointsFrame, timestamps: [1000, 2000], values: \"51,52\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_empty_tag_eq() {
    do_read_filter_test(
        vec!["cpu value=1 1000", "cpu,host=server01 value=2 2000"],
        GrpcRequestBuilder::new()
            .timestamp_range(0, 2001) // include all data
            // https://github.com/influxdata/influxdb_iox/issues/3430
            // host = '' means where host is not present
            .tag_predicate("host", ""),
        vec![
            "SeriesFrame, tags: _field=value,_measurement=cpu, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"1\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_empty_tag_not_regex() {
    do_read_filter_test(
        vec!["cpu value=1 1000", "cpu,host=server01 value=2 2000"],
        GrpcRequestBuilder::new()
            .timestamp_range(0, 2001) // include all data
            // https://github.com/influxdata/influxdb_iox/issues/3434
            // host !~ /^server01$/ means where host doesn't start with `server01`
            .not_regex_match_predicate("host", "^server01"),
        vec![
            "SeriesFrame, tags: _field=value,_measurement=cpu, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"1\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_empty_tag_regex() {
    do_read_filter_test(
        vec!["cpu value=1 1000", "cpu,host=server01 value=2 2000"],
        GrpcRequestBuilder::new()
            .timestamp_range(0, 2001) // include all data
            // host =~ /.+/ means where host is at least one character
            .regex_match_predicate("host", ".+"),
        vec![
            "SeriesFrame, tags: _field=value,_measurement=cpu,host=server01, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"2\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_periods() {
    do_read_filter_test(
        vec![
            "measurement.one,tag.one=foo field.one=1,field.two=100 1000",
            "measurement.one,tag.one=bar field.one=2,field.two=200 2000",
        ],
        GrpcRequestBuilder::new().timestamp_range(0, 2001),
        vec![
            "SeriesFrame, tags: _field=field.one,_measurement=measurement.one,tag.one=bar, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"2\"",
            "SeriesFrame, tags: _field=field.two,_measurement=measurement.one,tag.one=bar, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"200\"",
            "SeriesFrame, tags: _field=field.one,_measurement=measurement.one,tag.one=foo, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"1\"",
            "SeriesFrame, tags: _field=field.two,_measurement=measurement.one,tag.one=foo, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"100\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_periods_tag_predicate() {
    do_read_filter_test(
        vec![
            "measurement.one,tag.one=foo field.one=1,field.two=100 1000",
            "measurement.one,tag.one=bar field.one=2,field.two=200 2000",
        ],
        GrpcRequestBuilder::new()
            .tag_predicate("tag.one", "foo")
            .timestamp_range(0, 2001),
        vec![
            "SeriesFrame, tags: _field=field.one,_measurement=measurement.one,tag.one=foo, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"1\"",
            "SeriesFrame, tags: _field=field.two,_measurement=measurement.one,tag.one=foo, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"100\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_periods_multi_tag_predicate() {
    do_read_filter_test(
        // this setup has tags and fields that would be the same except for the period
        vec![
            "h2o,state=CA temp=90.0 100",
            "h2o,state=CA,city.state=LA temp=90.0 200",
            "h2o,state=CA,state.city=LA temp=91.0 300",
        ],
        GrpcRequestBuilder::new()
            .tag_predicate("city.state", "LA")
            .timestamp_range(0, 2001),
        // only the one series (that has city.state), should not get state as well
        vec![
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city.state=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [200], values: \"90\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_periods_multi_tag_predicate2() {
    do_read_filter_test(
        // this setup has tags and fields that would be the same except for the period
        vec![
            "h2o,state=CA temp=90.0 100",
            "h2o,state=CA,city.state=LA temp=90.0 200",
        ],
        GrpcRequestBuilder::new()
            .tag_predicate("state", "CA")
            .timestamp_range(0, 2001),
        // all the series (should not filter out the city.state one)
        vec![
            "SeriesFrame, tags: _field=temp,_measurement=h2o,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"90\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city.state=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [200], values: \"90\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_periods_multi_tag_predicate3() {
    do_read_filter_test(
        // this setup has tags and fields that would be the same except for the period
        vec!["h2o,.state=CA temp=90.0 100", "h2o,.state=CA temp=90.0 200"],
        GrpcRequestBuilder::new()
            .tag_predicate(".state", "CA")
            .timestamp_range(0, 2001),
        // all the series (should not filter out the city.state one)
        vec![
            "SeriesFrame, tags: .state=CA,_field=temp,_measurement=h2o, type: 0",
            "FloatPointsFrame, timestamps: [100, 200], values: \"90,90\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_periods_field_predicate() {
    do_read_filter_test(
        vec![
            "measurement.one,tag.one=foo field.one=1,field.two=100 1000",
            "measurement.one,tag.one=bar field.one=2,field.two=200 2000",
        ],
        GrpcRequestBuilder::new()
            .field_predicate("field.two")
            .timestamp_range(0, 2001),
        vec![
            "SeriesFrame, tags: _field=field.two,_measurement=measurement.one,tag.one=bar, type: 0",
            "FloatPointsFrame, timestamps: [2000], values: \"200\"",
            "SeriesFrame, tags: _field=field.two,_measurement=measurement.one,tag.one=foo, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"100\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_periods_multi_field_prediate() {
    do_read_filter_test(
        // this setup has tags and fields that would be the same except for the period
        vec![
            "h2o,state=CA temp=90.0 100",
            "h2o,state=CA,city.state=LA temp=90.0 200",
            "h2o,state=CA,state.city=LA temp=91.0 300",
            "h2o,state=CA,state.city=LA temp.foo=92.0 400",
            "h2o,state=CA,state.city=LA foo.temp=93.0 500",
        ],
        GrpcRequestBuilder::new()
            .field_predicate("temp")
            .timestamp_range(0, 2001),
        // expect not to see temp.foo
        vec![
            "SeriesFrame, tags: _field=temp,_measurement=h2o,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"90\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,state=CA,state.city=LA, type: 0",
            "FloatPointsFrame, timestamps: [300], values: \"91\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city.state=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [200], values: \"90\"",
        ],
    )
    .await
}

#[tokio::test]
pub async fn read_filter_periods_multi_field_predicate2() {
    do_read_filter_test(
        // this setup has tags and fields that would be the same except for the period
        vec![
            "h2o,state=CA temp=90.0 100",
            "h2o,state=CA,city.state=LA temp=90.0 200",
            "h2o,state=CA,state.city=LA temp=91.0 300",
            "h2o,state=CA,state.city=LA temp.foo=92.0 400",
            "h2o,state=CA,state.city=LA foo.temp=93.0 500",
        ],
        GrpcRequestBuilder::new()
            .field_predicate("temp.foo")
            .timestamp_range(0, 2001),
        // expect only one series
        vec![
            "SeriesFrame, tags: _field=temp.foo,_measurement=h2o,state=CA,state.city=LA, type: 0",
            "FloatPointsFrame, timestamps: [400], values: \"92\"",
        ],
    )
    .await
}

/// Sends the specified line protocol to a server with the timestamp/ predicate
/// predicate, and compares it against expected frames
async fn do_read_filter_test(
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

                    println!("Sending read_filter request with {request_builder:#?}");

                    let read_filter_request =
                        request_builder.source(state.cluster()).build_read_filter();

                    let actual_frames =
                        do_read_filter_request(&mut storage_client, read_filter_request).await;

                    assert_eq!(
                        expected_frames, actual_frames,
                        "\n\nExpected:\n{expected_frames:#?}\n\nActual:\n{actual_frames:#?}\n\n",
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
async fn do_read_filter_request(
    storage_client: &mut StorageClient<GrpcConnection>,
    request: tonic::Request<ReadFilterRequest>,
) -> Vec<String> {
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

#[tokio::test]
async fn no_predicate() {
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsMultiSeries",
        request: GrpcRequestBuilder::new(),
        expected_results: vec![
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [100, 250], values: \"70.4,72.4\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [200, 350], values: \"90,90\"",
            "SeriesFrame, tags: _field=reading,_measurement=o2,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [100, 250], values: \"50,51\"",
            "SeriesFrame, tags: _field=temp,_measurement=o2,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [100, 250], values: \"50.4,53.4\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn exclusive_timestamp_range_predicate() {
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsMultiSeries",
        request: GrpcRequestBuilder::new()
            // should *not* return the 350 row as the predicate is range.start <= ts < range.end
            .timestamp_range(349, 350),
        expected_results: vec![],
    })
    .run()
    .await;
}

#[tokio::test]
async fn inclusive_timestamp_range_predicate() {
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsMultiSeries",
        request: GrpcRequestBuilder::new()
            // *should* return the 350 row as the predicate is range.start <= ts < range.end
            .timestamp_range(350, 351),
        expected_results: vec![
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [350], values: \"90\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn exact_timestamp_range_predicate_multiple_results() {
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsMultiSeries",
        request: GrpcRequestBuilder::new().timestamp_range(250, 251),
        expected_results: vec![
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [250], values: \"72.4\"",
            "SeriesFrame, tags: _field=reading,_measurement=o2,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [250], values: \"51\"",
            "SeriesFrame, tags: _field=temp,_measurement=o2,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [250], values: \"53.4\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn tag_predicate_always_true() {
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurements",
        request: GrpcRequestBuilder::new().tag_to_tag_predicate("region", "region"),
        expected_results: vec![
            "SeriesFrame, tags: _field=user,_measurement=cpu,region=west, type: 0",
            "FloatPointsFrame, timestamps: [100, 150], values: \"23.2,21\"",
            "SeriesFrame, tags: _field=bytes,_measurement=disk,region=east, type: 1",
            "IntegerPointsFrame, timestamps: [200], values: \"99\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn unknown_columns_in_predicate_no_results() {
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurements",
        request: GrpcRequestBuilder::new()
            // bar is not a column that appears in the data; produce no results
            .tag_predicate("bar", "baz"),
        expected_results: vec![],
    })
    .run()
    .await;
}

#[tokio::test]
async fn tag_predicate_containing_field() {
    // Columns in the RPC predicate must be treated as tags:
    // https://github.com/influxdata/idpe/issues/16238
    Arc::new(ReadFilterTest {
        setup_name: "StringFieldWithNumericValue",
        request: GrpcRequestBuilder::new()
            // fld exists in the table, but only as a field, not a tag, so no data should be
            // returned.
            .tag_predicate("fld", "200"),
        expected_results: vec![],
    })
    .run()
    .await;
}

#[tokio::test]
async fn tag_predicates() {
    let expected_results = vec![
        "SeriesFrame, tags: _field=temp,_measurement=h2o,city=LA,state=CA, type: 0",
        "FloatPointsFrame, timestamps: [200], values: \"90\"",
    ];

    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsMultiSeries",
        request: GrpcRequestBuilder::new()
            .timestamp_range(200, 300)
            // filter to one row in h2o
            .tag_predicate("state", "CA"),
        expected_results: expected_results.clone(),
    })
    .run()
    .await;

    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsMultiSeries",
        request: GrpcRequestBuilder::new()
            .timestamp_range(200, 300)
            // Same results via a != predicate.
            .not_tag_predicate("state", "MA"),
        expected_results,
    })
    .run()
    .await;
}

#[tokio::test]
async fn field_and_tag_predicates() {
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsManyFields",
        request: GrpcRequestBuilder::new()
            // filter to one row in h2o
            .field_predicate("other_temp")
            .tag_predicate("state", "CA"),
        expected_results: vec![
            "SeriesFrame, tags: _field=other_temp,_measurement=h2o,city=Boston,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [350], values: \"72.4\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn field_exact_match_predicate() {
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsManyFields",
        request: GrpcRequestBuilder::new().field_predicate("temp"),
        expected_results: vec![
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [50, 100000], values: \"70.4,70.4\"",
            "SeriesFrame, tags: _field=temp,_measurement=o2,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [300], values: \"79\"",
            "SeriesFrame, tags: _field=temp,_measurement=o2,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [50], values: \"53.4\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn not_field_predicate() {
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsManyFields",
        request: GrpcRequestBuilder::new().not_field_predicate("temp"),
        expected_results: vec![
            "SeriesFrame, tags: _field=other_temp,_measurement=h2o,city=Boston,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [350], values: \"72.4\"",
            "SeriesFrame, tags: _field=moisture,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [100000], values: \"43\"",
            "SeriesFrame, tags: _field=other_temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [250], values: \"70.4\"",
            "SeriesFrame, tags: _field=reading,_measurement=o2,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [50], values: \"51\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn field_regex_match_predicates() {
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsManyFields",
        request: GrpcRequestBuilder::new().regex_match_predicate("_field", "temp"),
        expected_results: vec![
            // Should see results for temp and other_temp (but not reading)
            "SeriesFrame, tags: _field=other_temp,_measurement=h2o,city=Boston,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [350], values: \"72.4\"",
            "SeriesFrame, tags: _field=other_temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [250], values: \"70.4\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [50, 100000], values: \"70.4,70.4\"",
            "SeriesFrame, tags: _field=temp,_measurement=o2,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [300], values: \"79\"",
            "SeriesFrame, tags: _field=temp,_measurement=o2,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [50], values: \"53.4\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn field_and_measurement_predicates() {
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsManyFields",
        request: GrpcRequestBuilder::new()
            .field_predicate("temp")
            .measurement_predicate("h2o"),
        expected_results: vec![
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [50, 100000], values: \"70.4,70.4\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn multi_field_and_measurement_predicates() {
    // Predicate should pick 'temp' field from h2o and 'other_temp' from o2
    //
    // (_field = 'other_temp' AND _measurement = 'h2o') OR (_field = 'temp' AND _measurement = 'o2')
    let p1 = GrpcRequestBuilder::new()
        .field_predicate("other_temp")
        .measurement_predicate("h2o");
    let p2 = GrpcRequestBuilder::new()
        .field_predicate("temp")
        .measurement_predicate("o2");
    let node2 = p2.predicate.unwrap().root.unwrap();

    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsManyFields",
        request: p1.combine_predicate(Logical::Or, node2),
        expected_results: vec![
            // SHOULD NOT contain temp from h2o
            "SeriesFrame, tags: _field=other_temp,_measurement=h2o,city=Boston,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [350], values: \"72.4\"",
            "SeriesFrame, tags: _field=other_temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [250], values: \"70.4\"",
            "SeriesFrame, tags: _field=temp,_measurement=o2,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [300], values: \"79\"",
            "SeriesFrame, tags: _field=temp,_measurement=o2,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [50], values: \"53.4\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn measurement_predicates() {
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsManyFields",
        request: GrpcRequestBuilder::new()
            // use an expr on table name to pick just the last row from o2
            .timestamp_range(200, 400)
            .measurement_predicate("o2"),
        expected_results: vec![
            "SeriesFrame, tags: _field=temp,_measurement=o2,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [300], values: \"79\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn predicate_no_columns() {
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurements",
        request: GrpcRequestBuilder::new()
            // Predicate with no columns, only literals.
            .lit_lit_predicate("foo", "foo"),
        expected_results: vec![
            "SeriesFrame, tags: _field=user,_measurement=cpu,region=west, type: 0",
            "FloatPointsFrame, timestamps: [100, 150], values: \"23.2,21\"",
            "SeriesFrame, tags: _field=bytes,_measurement=disk,region=east, type: 1",
            "IntegerPointsFrame, timestamps: [200], values: \"99\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn tag_regex_match_predicates() {
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsMultiSeries",
        request: GrpcRequestBuilder::new()
            .timestamp_range(200, 300)
            // will match CA state
            .regex_match_predicate("state", "C.*"),
        expected_results: vec![
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [200], values: \"90\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn tag_regex_not_match_predicates() {
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsMultiSeries",
        request: GrpcRequestBuilder::new()
            .timestamp_range(200, 300)
            // will filter out any rows with a state that matches "CA"
            .not_regex_match_predicate("state", "C.*"),
        expected_results: vec![
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [250], values: \"72.4\"",
            "SeriesFrame, tags: _field=reading,_measurement=o2,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [250], values: \"51\"",
            "SeriesFrame, tags: _field=temp,_measurement=o2,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [250], values: \"53.4\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn tag_regex_escaped_predicates() {
    Arc::new(ReadFilterTest {
        setup_name: "MeasurementStatusCode",
        request: GrpcRequestBuilder::new()
            // Came from InfluxQL as:
            //
            // ```text
            // SELECT value
            // FROM db0.rp0.status_code
            // WHERE url =~ /https\:\/\/influxdb\.com/
            // ```
            .regex_match_predicate("url", r#"https\://influxdb\.com"#),
        expected_results: vec![
            // expect one series with influxdb.com
            "SeriesFrame, tags: _field=value,_measurement=status_code,url=https://influxdb.com, \
            type: 0",
            "FloatPointsFrame, timestamps: [1527018816000000000], values: \"418\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn tag_not_match_regex_escaped_predicates() {
    Arc::new(ReadFilterTest {
        setup_name: "MeasurementStatusCode",
        request: GrpcRequestBuilder::new()
            // Came from InfluxQL as:
            //
            // ```text
            // SELECT value
            // FROM db0.rp0.status_code
            // WHERE url !~ /https\:\/\/influxdb\.com/
            // ```
            .not_regex_match_predicate("url", r#"https\://influxdb\.com"#),
        expected_results: vec![
            // expect one series with example.com
            "SeriesFrame, tags: _field=value,_measurement=status_code,url=http://www.example.com, \
            type: 0",
            "FloatPointsFrame, timestamps: [1527018806000000000], values: \"404\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn predicate_unsupported_in_scan() {
    // These kinds of predicates can't be pushed down into chunks, but they can be evaluated by the
    // general purpose DataFusion plan
    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsMultiTagValue",
        request: GrpcRequestBuilder::new()
            .or_tag_predicates([("state", "CA"), ("city", "Boston")].into_iter()),
        expected_results: vec![
            // Note these results include data from both o2 and h2o
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"70.4\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=LA,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [200], values: \"90\"",
            "SeriesFrame, tags: _field=reading,_measurement=o2,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"50\"",
            "SeriesFrame, tags: _field=temp,_measurement=o2,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"50.4\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn multi_negation() {
    // reproducer for https://github.com/influxdata/influxdb_iox/issues/4800
    Arc::new(ReadFilterTest {
        setup_name: "EndToEndTest",
        request: GrpcRequestBuilder::new()
            .or_tag_predicates([("host", "server01"), ("host", "")].into_iter()),
        expected_results: vec![
            "SeriesFrame, tags: _field=color,_measurement=attributes, type: 4",
            "StringPointsFrame, timestamps: [8000], values: blue",
            "SeriesFrame, tags: _field=value,_measurement=cpu_load_short,host=server01, type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"27.99\"",
            "SeriesFrame, tags: \
            _field=value,_measurement=cpu_load_short,host=server01,region=us-east, type: 0",
            "FloatPointsFrame, timestamps: [3000], values: \"1234567.891011\"",
            "SeriesFrame, tags: \
            _field=value,_measurement=cpu_load_short,host=server01,region=us-west, type: 0",
            "FloatPointsFrame, timestamps: [0, 4000], values: \"0.64,0.000003\"",
            "SeriesFrame, tags: _field=active,_measurement=status, type: 3",
            "BooleanPointsFrame, timestamps: [7000], values: true",
            "SeriesFrame, tags: _field=in,_measurement=swap,host=server01,name=disk0, type: 0",
            "FloatPointsFrame, timestamps: [6000], values: \"3\"",
            "SeriesFrame, tags: _field=out,_measurement=swap,host=server01,name=disk0, type: 0",
            "FloatPointsFrame, timestamps: [6000], values: \"4\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn data_plan_order() {
    Arc::new(ReadFilterTest {
        setup_name: "MeasurementsSortableTags",
        request: GrpcRequestBuilder::new(),
        expected_results: vec![
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [250], values: \"70.3\"",
            "SeriesFrame, tags: _field=other,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [250], values: \"5\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA, type: 0",
            "FloatPointsFrame, timestamps: [250], values: \"70.5\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Boston,state=MA,zz_tag=A, \
            type: 0",
            "FloatPointsFrame, timestamps: [1000], values: \"70.4\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Kingston,state=MA,zz_tag=A, \
            type: 0",
            "FloatPointsFrame, timestamps: [800], values: \"70.1\"",
            "SeriesFrame, tags: _field=temp,_measurement=h2o,city=Kingston,state=MA,zz_tag=B, \
            type: 0",
            "FloatPointsFrame, timestamps: [100], values: \"70.2\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn filter_on_value() {
    Arc::new(ReadFilterTest {
        setup_name: "MeasurementsForDefect2845",
        request: GrpcRequestBuilder::new()
            .field_value_predicate(1.77)
            .field_predicate("load4"),
        expected_results: vec![
            "SeriesFrame, tags: _field=load4,_measurement=system,host=host.local, type: 0",
            "FloatPointsFrame, timestamps: [1527018806000000000, 1527018826000000000], \
            values: \"1.77,1.77\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
#[should_panic(expected = "Unsupported _field predicate")]
async fn unsupported_field_predicate() {
    // Tell the test to panic with the expected message if `TEST_INTEGRATION` isn't set so that
    // this still passes
    maybe_skip_integration!("Unsupported _field predicate");

    Arc::new(ReadFilterTest {
        setup_name: "TwoMeasurementsManyFields",
        request: GrpcRequestBuilder::new()
            .not_field_predicate("temp")
            .or_tag_predicates([("city", "Boston")].into_iter()),
        expected_results: vec![
            "SeriesFrame, tags: _field=temp,_measurement=o2,state=CA, type: 0",
            "FloatPointsFrame, timestamps: [300], values: \"79\"",
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn periods_in_names() {
    Arc::new(ReadFilterTest {
        setup_name: "PeriodsInNames",
        request: GrpcRequestBuilder::new().timestamp_range(0, 1_700_000_001_000_000_000),
        expected_results: vec![
            // Should return both series
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

#[tokio::test]
async fn periods_in_predicates() {
    Arc::new(ReadFilterTest {
        setup_name: "PeriodsInNames",
        request: GrpcRequestBuilder::new()
            .timestamp_range(0, 1_700_000_001_000_000_000)
            .tag_predicate("tag.one", "value"),
        expected_results: vec![
            // Should return both series
            "SeriesFrame, tags: \
            _field=field.one,_measurement=measurement.one,tag.one=value,tag.two=other, type: 0",
            "FloatPointsFrame, timestamps: [1609459201000000001], values: \"1\"",
            "SeriesFrame, tags: \
            _field=field.two,_measurement=measurement.one,tag.one=value,tag.two=other, type: 3",
            "BooleanPointsFrame, timestamps: [1609459201000000001], values: true",
        ],
    })
    .run()
    .await;
}

#[derive(Debug)]
struct ReadFilterTest {
    setup_name: &'static str,
    request: GrpcRequestBuilder,
    expected_results: Vec<&'static str>,
}

#[async_trait]
impl InfluxRpcTest for ReadFilterTest {
    fn setup_name(&self) -> &'static str {
        self.setup_name
    }

    async fn request_and_assert(&self, cluster: &MiniCluster) {
        let mut storage_client = cluster.querier_storage_client();

        let read_filter_request = self.request.clone().source(cluster).build_read_filter();

        let read_filter_response = storage_client
            .read_filter(read_filter_request)
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

        let results = dump_data_frames(&frames);

        assert_eq!(results, self.expected_results);
    }
}
