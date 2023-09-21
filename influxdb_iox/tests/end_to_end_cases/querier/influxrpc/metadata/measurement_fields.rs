use super::{run_data_test, InfluxRpcTest};
use async_trait::async_trait;
use data_types::{MAX_NANO_TIME, MIN_NANO_TIME};
use futures::{prelude::*, FutureExt};
use generated_types::measurement_fields_response::{FieldType, MessageField};
use std::sync::Arc;
use test_helpers_end_to_end::{DataGenerator, GrpcRequestBuilder, MiniCluster, StepTestState};

#[tokio::test]
async fn measurement_fields() {
    let generator = Arc::new(DataGenerator::new());
    run_data_test(
        Arc::clone(&generator),
        Box::new(move |state: &mut StepTestState| {
            let generator = Arc::clone(&generator);
            async move {
                let mut storage_client = state.cluster().querier_storage_client();

                let measurement_fields_request = GrpcRequestBuilder::new()
                    .source(state.cluster())
                    .timestamp_range(generator.min_time(), generator.max_time())
                    .tag_predicate("host", "server01")
                    .build_measurement_fields("cpu_load_short");

                let ns_since_epoch = generator.ns_since_epoch();
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

#[tokio::test]
async fn field_columns_nonexistent_table_with_predicate() {
    Arc::new(MeasurementFieldsTest {
        setup_name: "TwoMeasurementsManyFields",
        table_name: "NoSuchTable",
        request: GrpcRequestBuilder::new().tag_predicate("state", "MA"),
        expected_fields: vec![],
    })
    .run()
    .await;
}

#[tokio::test]
async fn field_columns_existing_table_with_predicate() {
    Arc::new(MeasurementFieldsTest {
        setup_name: "TwoMeasurementsManyFields",
        table_name: "h2o",
        request: GrpcRequestBuilder::new().tag_predicate("state", "MA"),
        expected_fields: vec![
            MessageField {
                key: "moisture".into(),
                r#type: FieldType::Float.into(),
                timestamp: 100_000,
            },
            MessageField {
                key: "other_temp".into(),
                r#type: FieldType::Float.into(),
                timestamp: 250,
            },
            MessageField {
                key: "temp".into(),
                r#type: FieldType::Float.into(),
                timestamp: 100_000,
            },
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn field_columns_timestamp_range_predicate() {
    Arc::new(MeasurementFieldsTest {
        setup_name: "TwoMeasurementsManyFields",
        table_name: "h2o",
        request: GrpcRequestBuilder::new().timestamp_range(i64::MIN, i64::MAX - 2),
        expected_fields: vec![
            MessageField {
                key: "moisture".into(),
                r#type: FieldType::Float.into(),
                timestamp: 100_000,
            },
            MessageField {
                key: "other_temp".into(),
                r#type: FieldType::Float.into(),
                timestamp: 350,
            },
            MessageField {
                key: "temp".into(),
                r#type: FieldType::Float.into(),
                timestamp: 100_000,
            },
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn field_columns_no_predicate_full_time_range_returns_zero_timestamp() {
    Arc::new(MeasurementFieldsTest {
        setup_name: "TwoMeasurementsManyFields",
        table_name: "h2o",
        request: GrpcRequestBuilder::new(),
        expected_fields: vec![
            MessageField {
                key: "moisture".into(),
                r#type: FieldType::Float.into(),
                timestamp: 0,
            },
            MessageField {
                key: "other_temp".into(),
                r#type: FieldType::Float.into(),
                timestamp: 0,
            },
            MessageField {
                key: "temp".into(),
                r#type: FieldType::Float.into(),
                timestamp: 0,
            },
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn field_columns_tag_and_timestamp_range_predicate() {
    Arc::new(MeasurementFieldsTest {
        setup_name: "TwoMeasurementsManyFields",
        table_name: "h2o",
        request: GrpcRequestBuilder::new()
            .tag_predicate("state", "MA")
            .timestamp_range(200, 300),
        expected_fields: vec![MessageField {
            key: "other_temp".into(),
            r#type: FieldType::Float.into(),
            timestamp: 250,
        }],
    })
    .run()
    .await;
}

#[tokio::test]
async fn field_names() {
    Arc::new(MeasurementFieldsTest {
        setup_name: "OneMeasurementManyFields",
        table_name: "h2o",
        request: GrpcRequestBuilder::new().timestamp_range(0, 2_000),
        expected_fields: vec![
            MessageField {
                key: "field1".into(),
                r#type: FieldType::Float.into(),
                timestamp: 1_000,
            },
            MessageField {
                key: "field2".into(),
                r#type: FieldType::String.into(),
                timestamp: 100,
            },
            MessageField {
                key: "field3".into(),
                r#type: FieldType::Float.into(),
                timestamp: 100,
            },
            MessageField {
                key: "field4".into(),
                r#type: FieldType::Boolean.into(),
                timestamp: 1_000,
            },
        ],
    })
    .run()
    .await;
}

#[tokio::test]
async fn list_field_columns_all_time() {
    Arc::new(MeasurementFieldsTest {
        setup_name: "MeasurementWithMaxTime",
        table_name: "cpu",
        request: GrpcRequestBuilder::new().timestamp_range(MIN_NANO_TIME, MAX_NANO_TIME),
        expected_fields: vec![MessageField {
            key: "value".into(),
            r#type: FieldType::Float.into(),
            timestamp: 0,
        }],
    })
    .run()
    .await;
}

#[tokio::test]
async fn list_field_columns_max_time_included() {
    Arc::new(MeasurementFieldsTest {
        setup_name: "MeasurementWithMaxTime",
        table_name: "cpu",
        // if the range started at i64:MIN, we would hit the optimized case for 'all time'
        // and get the 'wrong' timestamp, since in the optimized case we don't check what timestamps
        // exist
        request: GrpcRequestBuilder::new().timestamp_range(MIN_NANO_TIME + 1, MAX_NANO_TIME + 1),
        expected_fields: vec![MessageField {
            key: "value".into(),
            r#type: FieldType::Float.into(),
            timestamp: MAX_NANO_TIME,
        }],
    })
    .run()
    .await;
}

#[tokio::test]
async fn list_field_columns_max_time_excluded() {
    Arc::new(MeasurementFieldsTest {
        setup_name: "MeasurementWithMaxTime",
        table_name: "cpu",
        // one less than max timestamp
        request: GrpcRequestBuilder::new().timestamp_range(MIN_NANO_TIME + 1, MAX_NANO_TIME),
        expected_fields: vec![],
    })
    .run()
    .await;
}

#[tokio::test]
async fn list_field_columns_with_periods() {
    Arc::new(MeasurementFieldsTest {
        setup_name: "PeriodsInNames",
        table_name: "measurement.one",
        request: GrpcRequestBuilder::new().timestamp_range(0, 1_700_000_001_000_000_000),
        expected_fields: vec![
            MessageField {
                key: "field.one".into(),
                r#type: FieldType::Float.into(),
                timestamp: 1_609_459_201_000_000_002,
            },
            MessageField {
                key: "field.two".into(),
                r#type: FieldType::Boolean.into(),
                timestamp: 1_609_459_201_000_000_002,
            },
        ],
    })
    .run()
    .await;
}

#[derive(Debug)]
struct MeasurementFieldsTest {
    setup_name: &'static str,
    table_name: &'static str,
    request: GrpcRequestBuilder,
    expected_fields: Vec<MessageField>,
}

#[async_trait]
impl InfluxRpcTest for MeasurementFieldsTest {
    fn setup_name(&self) -> &'static str {
        self.setup_name
    }

    async fn request_and_assert(&self, cluster: &MiniCluster) {
        let mut storage_client = cluster.querier_storage_client();

        let measurement_fields_request = self
            .request
            .clone()
            .source(cluster)
            .build_measurement_fields(self.table_name);

        let measurement_fields_response = storage_client
            .measurement_fields(measurement_fields_request)
            .await
            .unwrap();
        let responses: Vec<_> = measurement_fields_response
            .into_inner()
            .try_collect()
            .await
            .unwrap();

        assert_eq!(responses[0].fields, self.expected_fields);
    }
}
