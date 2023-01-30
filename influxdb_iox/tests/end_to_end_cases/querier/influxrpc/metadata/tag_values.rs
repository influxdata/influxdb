use super::run_data_test;
use futures::{prelude::*, FutureExt};

use influxdb_storage_client::tag_key_bytes_to_strings;
use std::sync::Arc;
use test_helpers_end_to_end::{DataGenerator, GrpcRequestBuilder, StepTestState};

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
