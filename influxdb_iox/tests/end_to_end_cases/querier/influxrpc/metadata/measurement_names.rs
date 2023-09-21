use super::{run_data_test, InfluxRpcTest};
use async_trait::async_trait;
use data_types::{MAX_NANO_TIME, MIN_NANO_TIME};
use futures::{prelude::*, FutureExt};
use std::sync::Arc;
use test_helpers_end_to_end::{DataGenerator, GrpcRequestBuilder, MiniCluster, StepTestState};

#[tokio::test]
async fn measurement_names() {
    let generator = Arc::new(DataGenerator::new());
    run_data_test(
        Arc::clone(&generator),
        Box::new(move |state: &mut StepTestState| {
            let generator = Arc::clone(&generator);
            async move {
                let mut storage_client = state.cluster().querier_storage_client();

                let measurement_names_request = GrpcRequestBuilder::new()
                    .source(state.cluster())
                    .timestamp_range(generator.min_time(), generator.max_time())
                    .build_measurement_names();

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
async fn no_predicate() {
    Arc::new(MeasurementNamesTest {
        setup_name: "TwoMeasurements",
        request: GrpcRequestBuilder::new(),
        expected_names: vec!["cpu", "disk"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn predicate_no_results() {
    Arc::new(MeasurementNamesTest {
        setup_name: "TwoMeasurementsManyFields",
        // no rows pass this predicate
        request: GrpcRequestBuilder::new().timestamp_range(10_000_000, 20_000_000),
        expected_names: vec![],
    })
    .run()
    .await;
}

#[tokio::test]
async fn predicate_no_non_null_results() {
    // only a single row with a null field passes this predicate (expect no table names)
    Arc::new(MeasurementNamesTest {
        setup_name: "TwoMeasurementsManyFields",
        // no rows pass this predicate
        request: GrpcRequestBuilder::new()
            // only get last row of o2 (timestamp = 300)
            .timestamp_range(200, 400)
            // model predicate like _field='reading' which last row does not have
            .field_predicate("reading")
            .measurement_predicate("o2"),
        expected_names: vec![],
    })
    .run()
    .await;
}

#[tokio::test]
async fn generic_plan_predicate_no_non_null_results() {
    // only a single row with a null field passes this predicate (expect no table names) -- has a
    // general purpose predicate to force a generic plan
    Arc::new(MeasurementNamesTest {
        setup_name: "TwoMeasurementsManyFields",
        request: GrpcRequestBuilder::new()
            // only get last row of o2 (timestamp = 300)
            .timestamp_range(200, 400)
            // model predicate like _field='reading' which last row does not have
            .field_predicate("reading")
            .measurement_predicate("o2")
            .tag_predicate("state", "CA"),
        expected_names: vec![],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_includes_all_measurements() {
    Arc::new(MeasurementNamesTest {
        setup_name: "TwoMeasurements",
        request: GrpcRequestBuilder::new().timestamp_range(0, 201),
        expected_names: vec!["cpu", "disk"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_includes_some_measurements() {
    Arc::new(MeasurementNamesTest {
        setup_name: "TwoMeasurements",
        request: GrpcRequestBuilder::new().timestamp_range(0, 200),
        expected_names: vec!["cpu"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_includes_no_measurements() {
    Arc::new(MeasurementNamesTest {
        setup_name: "TwoMeasurements",
        request: GrpcRequestBuilder::new().timestamp_range(250, 350),
        expected_names: vec![],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_max_time_included() {
    Arc::new(MeasurementNamesTest {
        setup_name: "MeasurementWithMaxTime",
        request: GrpcRequestBuilder::new().timestamp_range(MIN_NANO_TIME + 1, MAX_NANO_TIME + 1),
        expected_names: vec!["cpu"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_max_time_excluded() {
    Arc::new(MeasurementNamesTest {
        setup_name: "MeasurementWithMaxTime",
        request: GrpcRequestBuilder::new().timestamp_range(MIN_NANO_TIME + 1, MAX_NANO_TIME),
        expected_names: vec![],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_all_time() {
    Arc::new(MeasurementNamesTest {
        setup_name: "MeasurementWithMaxTime",
        request: GrpcRequestBuilder::new().timestamp_range(MIN_NANO_TIME, MAX_NANO_TIME + 1),
        expected_names: vec!["cpu"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn periods() {
    Arc::new(MeasurementNamesTest {
        setup_name: "PeriodsInNames",
        request: GrpcRequestBuilder::new().timestamp_range(MIN_NANO_TIME, MAX_NANO_TIME + 1),
        expected_names: vec!["measurement.one"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn generic_predicate() {
    Arc::new(MeasurementNamesTest {
        setup_name: "TwoMeasurements",
        request: GrpcRequestBuilder::new()
            .field_predicate("bytes")
            .field_value_predicate(99),
        expected_names: vec!["disk"],
    })
    .run()
    .await;
}

#[derive(Debug)]
struct MeasurementNamesTest {
    setup_name: &'static str,
    request: GrpcRequestBuilder,
    expected_names: Vec<&'static str>,
}

#[async_trait]
impl InfluxRpcTest for MeasurementNamesTest {
    fn setup_name(&self) -> &'static str {
        self.setup_name
    }

    async fn request_and_assert(&self, cluster: &MiniCluster) {
        let mut storage_client = cluster.querier_storage_client();

        let measurement_names_request = self
            .request
            .clone()
            .source(cluster)
            .build_measurement_names();

        let measurement_names_response = storage_client
            .measurement_names(measurement_names_request)
            .await
            .unwrap();
        let responses: Vec<_> = measurement_names_response
            .into_inner()
            .try_collect()
            .await
            .unwrap();

        let names = &responses[0].values;
        let names: Vec<_> = names
            .iter()
            .map(|s| std::str::from_utf8(s).unwrap())
            .collect();

        assert_eq!(names, self.expected_names);
    }
}
