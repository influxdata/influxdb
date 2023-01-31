use super::{run_data_test, InfluxRpcTest};
use async_trait::async_trait;
use data_types::{MAX_NANO_TIME, MIN_NANO_TIME};
use futures::{prelude::*, FutureExt};
use influxdb_storage_client::tag_key_bytes_to_strings;
use std::sync::Arc;
use test_helpers_end_to_end::{DataGenerator, GrpcRequestBuilder, MiniCluster, StepTestState};

#[tokio::test]
async fn tag_keys() {
    let generator = Arc::new(DataGenerator::new());
    run_data_test(
        Arc::clone(&generator),
        Box::new(move |state: &mut StepTestState| {
            let generator = Arc::clone(&generator);
            async move {
                let mut storage_client = state.cluster().querier_storage_client();

                let tag_keys_request = GrpcRequestBuilder::new()
                    .source(state.cluster())
                    .timestamp_range(generator.min_time(), generator.max_time())
                    .tag_predicate("host", "server01")
                    .build_tag_keys();

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
async fn data_without_tags_no_predicate() {
    Arc::new(TagKeysTest {
        setup_name: "OneMeasurementNoTags",
        request: GrpcRequestBuilder::new(),
        expected_keys: vec!["_m(0x00)", "_f(0xff)"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn data_without_tags_timestamp_range() {
    Arc::new(TagKeysTest {
        setup_name: "OneMeasurementNoTags",
        request: GrpcRequestBuilder::new().timestamp_range(0, 1_000),
        expected_keys: vec!["_m(0x00)", "_f(0xff)"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn no_predicate() {
    Arc::new(TagKeysTest {
        setup_name: "TwoMeasurementsManyNulls",
        request: GrpcRequestBuilder::new(),
        expected_keys: vec!["_m(0x00)", "borough", "city", "county", "state", "_f(0xff)"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range() {
    Arc::new(TagKeysTest {
        setup_name: "TwoMeasurementsManyNulls",
        request: GrpcRequestBuilder::new().timestamp_range(150, 201),
        expected_keys: vec!["_m(0x00)", "city", "state", "_f(0xff)"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_max_time_included() {
    Arc::new(TagKeysTest {
        setup_name: "MeasurementWithMaxTime",
        request: GrpcRequestBuilder::new().timestamp_range(MIN_NANO_TIME + 1, MAX_NANO_TIME + 1),
        expected_keys: vec!["_m(0x00)", "host", "_f(0xff)"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_max_time_excluded() {
    Arc::new(TagKeysTest {
        setup_name: "MeasurementWithMaxTime",
        // exclusive end
        request: GrpcRequestBuilder::new().timestamp_range(MIN_NANO_TIME + 1, MAX_NANO_TIME),
        expected_keys: vec!["_m(0x00)", "_f(0xff)"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_all_time() {
    Arc::new(TagKeysTest {
        setup_name: "MeasurementWithMaxTime",
        request: GrpcRequestBuilder::new().timestamp_range(MIN_NANO_TIME, MAX_NANO_TIME + 1),
        expected_keys: vec!["_m(0x00)", "host", "_f(0xff)"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn tag_predicates() {
    Arc::new(TagKeysTest {
        setup_name: "TwoMeasurementsManyNulls",
        request: GrpcRequestBuilder::new().tag_predicate("state", "MA"),
        expected_keys: vec!["_m(0x00)", "city", "county", "state", "_f(0xff)"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn always_true_predicate() {
    Arc::new(TagKeysTest {
        setup_name: "TwoMeasurementsManyNulls",
        request: GrpcRequestBuilder::new()
            .tag_predicate("state", "MA")
            // nonexistent column with !=; always true
            .not_tag_predicate("host", "server01"),
        expected_keys: vec!["_m(0x00)", "city", "county", "state", "_f(0xff)"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn measurement_predicates() {
    Arc::new(TagKeysTest {
        setup_name: "TwoMeasurementsManyNulls",
        request: GrpcRequestBuilder::new().measurement_predicate("o2"),
        expected_keys: vec!["_m(0x00)", "borough", "city", "state", "_f(0xff)"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_and_measurement_predicates() {
    Arc::new(TagKeysTest {
        setup_name: "TwoMeasurementsManyNulls",
        request: GrpcRequestBuilder::new()
            .timestamp_range(450, 550)
            .measurement_predicate("o2"),
        expected_keys: vec!["_m(0x00)", "city", "state", "_f(0xff)"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_and_tag_predicate() {
    Arc::new(TagKeysTest {
        setup_name: "TwoMeasurementsManyNulls",
        request: GrpcRequestBuilder::new()
            .timestamp_range(150, 201)
            .tag_predicate("state", "MA"),
        expected_keys: vec!["_m(0x00)", "city", "state", "_f(0xff)"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn measurement_and_tag_predicate() {
    Arc::new(TagKeysTest {
        setup_name: "TwoMeasurementsManyNulls",
        request: GrpcRequestBuilder::new()
            .measurement_predicate("o2")
            .tag_predicate("state", "NY"),
        expected_keys: vec!["_m(0x00)", "borough", "city", "state", "_f(0xff)"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn timestamp_range_measurement_and_tag_predicate() {
    Arc::new(TagKeysTest {
        setup_name: "TwoMeasurementsManyNulls",
        request: GrpcRequestBuilder::new()
            .timestamp_range(1, 550)
            .tag_predicate("state", "NY")
            .measurement_predicate("o2"),
        expected_keys: vec!["_m(0x00)", "city", "state", "_f(0xff)"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn end_to_end() {
    Arc::new(TagKeysTest {
        setup_name: "EndToEndTest",
        request: GrpcRequestBuilder::new()
            .timestamp_range(0, 10000)
            .tag_predicate("host", "server01"),
        expected_keys: vec!["_m(0x00)", "host", "name", "region", "_f(0xff)"],
    })
    .run()
    .await;
}

#[tokio::test]
async fn periods() {
    Arc::new(TagKeysTest {
        setup_name: "PeriodsInNames",
        request: GrpcRequestBuilder::new().timestamp_range(0, 1_700_000_001_000_000_000),
        expected_keys: vec!["_m(0x00)", "tag.one", "tag.two", "_f(0xff)"],
    })
    .run()
    .await;
}

#[derive(Debug)]
struct TagKeysTest {
    setup_name: &'static str,
    request: GrpcRequestBuilder,
    expected_keys: Vec<&'static str>,
}

#[async_trait]
impl InfluxRpcTest for TagKeysTest {
    fn setup_name(&self) -> &'static str {
        self.setup_name
    }

    async fn request_and_assert(&self, cluster: &MiniCluster) {
        let mut storage_client = cluster.querier_storage_client();

        let tag_keys_request = self.request.clone().source(cluster).build_tag_keys();

        let tag_keys_response = storage_client.tag_keys(tag_keys_request).await.unwrap();
        let responses: Vec<_> = tag_keys_response.into_inner().try_collect().await.unwrap();

        let keys = &responses[0].values;
        let keys: Vec<_> = keys
            .iter()
            .map(|s| tag_key_bytes_to_strings(s.to_vec()))
            .collect();

        assert_eq!(keys, self.expected_keys);
    }
}
