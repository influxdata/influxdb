use super::{run_data_test, InfluxRpcTest};
use async_trait::async_trait;
use futures::{prelude::*, FutureExt};
use influxdb_storage_client::tag_key_bytes_to_strings;
use std::sync::Arc;
use test_helpers_end_to_end::{
    maybe_skip_integration, DataGenerator, GrpcRequestBuilder, MiniCluster, StepTestState,
};

#[tokio::test]
async fn tag_values() {
    let generator = Arc::new(DataGenerator::new());
    run_data_test(
        Arc::clone(&generator),
        Box::new(move |state: &mut StepTestState| {
            let generator = Arc::clone(&generator);
            async move {
                let mut storage_client = state.cluster().querier_storage_client();

                let tag_values_request = GrpcRequestBuilder::new()
                    .source(state.cluster())
                    .timestamp_range(generator.min_time(), generator.max_time())
                    .tag_predicate("host", "server01")
                    .build_tag_values("host");

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
async fn data_without_tags_no_predicate() {
    Arc::new(TagValuesTest {
        setup_name: "TwoMeasurementsManyNulls",
        tag_key: "tag_not_in_chunks",
        request: GrpcRequestBuilder::new(),
        // If the tag is not present, expect no values back (not error)
        expected_values: vec![],
    })
    .run()
    .await;
}

#[tokio::test]
async fn data_with_tags_no_predicate() {
    Arc::new(TagValuesTest {
        setup_name: "TwoMeasurementsManyNulls",
        tag_key: "state",
        request: GrpcRequestBuilder::new(),
        expected_values: vec!["CA", "MA", "NY"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_predicate() {
    Arc::new(TagValuesTest {
        setup_name: "TwoMeasurementsManyNulls",
        tag_key: "state",
        request: GrpcRequestBuilder::new().timestamp_range(50, 201),
        expected_values: vec!["CA", "MA"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn tag_predicates() {
    Arc::new(TagValuesTest {
        setup_name: "TwoMeasurementsManyNulls",
        tag_key: "city",
        request: GrpcRequestBuilder::new().tag_predicate("state", "MA"),
        expected_values: vec!["Boston"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn measurement_predicates() {
    Arc::new(TagValuesTest {
        setup_name: "TwoMeasurementsManyNulls",
        tag_key: "state",
        request: GrpcRequestBuilder::new().measurement_predicate("h2o"),
        expected_values: vec!["CA", "MA"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn not_measurement_predicates() {
    Arc::new(TagValuesTest {
        setup_name: "TwoMeasurementsManyNulls",
        tag_key: "state",
        request: GrpcRequestBuilder::new()
            .not_measurement_predicate("o2")
            // filters out the NY row
            .timestamp_range(1, 600),
        expected_values: vec!["CA", "MA"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_and_measurement_predicates() {
    Arc::new(TagValuesTest {
        setup_name: "TwoMeasurementsManyNulls",
        tag_key: "state",
        request: GrpcRequestBuilder::new()
            .timestamp_range(50, 201)
            .measurement_predicate("o2"),
        expected_values: vec!["MA"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_and_tag_predicates() {
    Arc::new(TagValuesTest {
        setup_name: "MeasurementsSortableTags",
        tag_key: "zz_tag",
        request: GrpcRequestBuilder::new()
            .timestamp_range(700, 900)
            .tag_predicate("state", "MA"),
        expected_values: vec!["A"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn measurement_and_tag_predicate() {
    Arc::new(TagValuesTest {
        setup_name: "TwoMeasurementsMultiTagValue",
        tag_key: "city",
        request: GrpcRequestBuilder::new()
            .measurement_predicate("h2o")
            .tag_predicate("state", "MA"),
        expected_values: vec!["Boston", "Lowell"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_measurement_and_tag_predicate() {
    Arc::new(TagValuesTest {
        setup_name: "TwoMeasurementsManyNulls",
        tag_key: "state",
        request: GrpcRequestBuilder::new()
            .measurement_predicate("o2")
            .tag_predicate("state", "NY")
            .timestamp_range(1, 550),
        expected_values: vec!["NY"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn predicate_no_results() {
    Arc::new(TagValuesTest {
        setup_name: "TwoMeasurementsManyNulls",
        tag_key: "state",
        request: GrpcRequestBuilder::new()
            .measurement_predicate("o2")
            .tag_predicate("state", "NY")
            // filters out the NY row
            .timestamp_range(1, 300),
        expected_values: vec![],
    })
    .run()
    .await;
}

#[tokio::test]
async fn or_predicate() {
    Arc::new(TagValuesTest {
        setup_name: "TwoMeasurementsManyNulls",
        tag_key: "city",
        request: GrpcRequestBuilder::new()
            .measurement_predicate("o2")
            // since there is an 'OR' in this predicate, can't answer
            // with metadata alone
            .or_field_value_predicates([70.0].into_iter())
            // filters out the Brooklyn row
            .timestamp_range(1, 600),
        expected_values: vec!["Boston", "LA", "NYC"],
    })
    .run()
    .await;
}

#[tokio::test]
#[should_panic(
    expected = "gRPC planner error: column \'temp\' is not a tag, it is Some(Field(Float))"
)]
async fn tag_values_on_field_is_invalid() {
    // Tell the test to panic with the expected message if `TEST_INTEGRATION` isn't set so that
    // this still passes
    maybe_skip_integration!(
        "gRPC planner error: column \'temp\' is not a tag, it is Some(Field(Float))"
    );

    Arc::new(TagValuesTest {
        setup_name: "TwoMeasurementsManyNulls",
        // temp is a field, not a tag
        tag_key: "temp",
        request: GrpcRequestBuilder::new(),
        expected_values: vec!["Boston", "LA", "NYC"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn nonexistent_field_predicates() {
    Arc::new(TagValuesTest {
        setup_name: "TwoMeasurementsManyNulls",
        tag_key: "state",
        request: GrpcRequestBuilder::new()
            // since this field doesn't exist this predicate should match no values
            .field_predicate("not_a_column"),
        expected_values: vec![],
    })
    .run()
    .await;
}

#[tokio::test]
async fn field_predicates() {
    Arc::new(TagValuesTest {
        setup_name: "TwoMeasurementsManyFields",
        tag_key: "state",
        request: GrpcRequestBuilder::new()
            // this field does exist, but only for rows MA (not CA)
            .field_predicate("moisture"),
        expected_values: vec!["MA"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn periods() {
    Arc::new(TagValuesTest {
        setup_name: "PeriodsInNames",
        tag_key: "tag.one",
        request: GrpcRequestBuilder::new().timestamp_range(0, 1_700_000_001_000_000_000),
        expected_values: vec!["value", "value2"],
    })
    .run()
    .await;
}

#[derive(Debug)]
struct TagValuesTest {
    setup_name: &'static str,
    tag_key: &'static str,
    request: GrpcRequestBuilder,
    expected_values: Vec<&'static str>,
}

#[async_trait]
impl InfluxRpcTest for TagValuesTest {
    fn setup_name(&self) -> &'static str {
        self.setup_name
    }

    async fn request_and_assert(&self, cluster: &MiniCluster) {
        let mut storage_client = cluster.querier_storage_client();

        let tag_values_request = self
            .request
            .clone()
            .source(cluster)
            .build_tag_values(self.tag_key);

        let tag_values_response = storage_client.tag_values(tag_values_request).await.unwrap();
        let responses: Vec<_> = tag_values_response
            .into_inner()
            .try_collect()
            .await
            .unwrap();

        let values = &responses[0].values;
        let values: Vec<_> = values
            .iter()
            .map(|s| tag_key_bytes_to_strings(s.to_vec()))
            .collect();

        assert_eq!(values, self.expected_values);
    }
}
