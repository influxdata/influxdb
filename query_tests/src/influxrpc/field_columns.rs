use arrow::datatypes::DataType;
use data_types::timestamp::{MAX_NANO_TIME, MIN_NANO_TIME};
use datafusion::logical_plan::{col, lit};
use predicate::rpc_predicate::InfluxRpcPredicate;
use predicate::PredicateBuilder;
use query::{
    exec::fieldlist::{Field, FieldList},
    frontend::influxrpc::InfluxRpcPlanner,
};

use crate::scenarios::*;

/// Creates and loads several database scenarios using the db_setup
/// function.
///
/// runs field_column_names(predicate) and compares it to the expected
/// output
async fn run_field_columns_test_case<D>(
    db_setup: D,
    predicate: InfluxRpcPredicate,
    expected_fields: FieldList,
) where
    D: DbSetup,
{
    test_helpers::maybe_start_logging();

    for scenario in db_setup.make().await {
        let DbScenario {
            scenario_name, db, ..
        } = scenario;
        println!("Running scenario '{}'", scenario_name);
        println!("Predicate: '{:#?}'", predicate);
        let planner = InfluxRpcPlanner::default();
        let ctx = db.new_query_context(None);

        let plan = planner
            .field_columns(db.as_query_database(), predicate.clone())
            .await
            .expect("built plan successfully");
        let fields = ctx
            .to_field_list(plan)
            .await
            .expect("converted plan to strings successfully");

        assert_eq!(
            fields, expected_fields,
            "Error in  scenario '{}'\n\nexpected:\n{:#?}\nactual:\n{:#?}",
            scenario_name, expected_fields, fields
        );
    }
}

#[tokio::test]
async fn test_field_columns_no_predicate() {
    let predicate = PredicateBuilder::default()
        .add_expr(col("state").eq(lit("MA"))) // state=MA
        .build();
    let predicate = InfluxRpcPredicate::new_table("NoSuchTable", predicate);
    let expected_fields = FieldList::default();
    run_field_columns_test_case(TwoMeasurementsManyFields {}, predicate, expected_fields).await;
}

// NGA todo: add delete tests when the TwoMeasurementsManyFieldsWithDelete available

#[tokio::test]
async fn test_field_columns_with_pred() {
    // get only fields from h20 (but both chunks)
    let predicate = PredicateBuilder::default()
        .add_expr(col("state").eq(lit("MA"))) // state=MA
        .build();
    let predicate = InfluxRpcPredicate::new_table("h2o", predicate);

    let expected_fields = FieldList {
        fields: vec![
            Field {
                name: "moisture".into(),
                data_type: DataType::Float64,
                last_timestamp: 100000,
            },
            Field {
                name: "other_temp".into(),
                data_type: DataType::Float64,
                last_timestamp: 250,
            },
            Field {
                name: "temp".into(),
                data_type: DataType::Float64,
                last_timestamp: 100000,
            },
        ],
    };

    run_field_columns_test_case(TwoMeasurementsManyFields {}, predicate, expected_fields).await;
}

#[tokio::test]
async fn test_field_columns_measurement_pred() {
    // get only fields from h2o using a _measurement predicate
    let predicate = PredicateBuilder::default()
        .add_expr(col("_measurement").eq(lit("h2o")))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_fields = FieldList {
        fields: vec![
            Field {
                name: "moisture".into(),
                data_type: DataType::Float64,
                last_timestamp: 100000,
            },
            Field {
                name: "other_temp".into(),
                data_type: DataType::Float64,
                last_timestamp: 350,
            },
            Field {
                name: "temp".into(),
                data_type: DataType::Float64,
                last_timestamp: 100000,
            },
        ],
    };

    run_field_columns_test_case(TwoMeasurementsManyFields {}, predicate, expected_fields).await;
}

#[tokio::test]
async fn test_field_columns_with_ts_pred() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(200, 300)
        .add_expr(col("state").eq(lit("MA"))) // state=MA
        .build();
    let predicate = InfluxRpcPredicate::new_table("h2o", predicate);

    let expected_fields = FieldList {
        fields: vec![Field {
            name: "other_temp".into(),
            data_type: DataType::Float64,
            last_timestamp: 250,
        }],
    };

    run_field_columns_test_case(TwoMeasurementsManyFields {}, predicate, expected_fields).await;
}

#[tokio::test]
async fn test_field_name_plan() {
    test_helpers::maybe_start_logging();

    let predicate = PredicateBuilder::default().timestamp_range(0, 2000).build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_fields = FieldList {
        fields: vec![
            Field {
                name: "field1".into(),
                data_type: DataType::Float64,
                last_timestamp: 1000,
            },
            Field {
                name: "field2".into(),
                data_type: DataType::Utf8,
                last_timestamp: 100,
            },
            Field {
                name: "field3".into(),
                data_type: DataType::Float64,
                last_timestamp: 100,
            },
            Field {
                name: "field4".into(),
                data_type: DataType::Boolean,
                last_timestamp: 1000,
            },
        ],
    };

    run_field_columns_test_case(OneMeasurementManyFields {}, predicate, expected_fields).await;
}

#[tokio::test]
async fn test_field_name_plan_with_delete() {
    test_helpers::maybe_start_logging();

    let predicate = PredicateBuilder::default().timestamp_range(0, 2000).build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_fields = FieldList {
        fields: vec![
            Field {
                name: "field1".into(),
                data_type: DataType::Float64,
                last_timestamp: 100,
            },
            Field {
                name: "field2".into(),
                data_type: DataType::Utf8,
                last_timestamp: 100,
            },
            Field {
                name: "field3".into(),
                data_type: DataType::Float64,
                last_timestamp: 100,
            },
        ],
    };

    run_field_columns_test_case(
        OneMeasurementManyFieldsWithDelete {},
        predicate,
        expected_fields,
    )
    .await;
}

#[tokio::test]
async fn list_field_columns_max_time() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(MIN_NANO_TIME, MAX_NANO_TIME)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_fields = FieldList {
        fields: vec![Field {
            name: "value".into(),
            data_type: DataType::Float64,
            last_timestamp: MAX_NANO_TIME,
        }],
    };

    run_field_columns_test_case(MeasurementWithMaxTime {}, predicate, expected_fields).await;
}

#[tokio::test]
async fn list_field_columns_max_i64() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(i64::MIN, i64::MAX)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_fields = FieldList {
        fields: vec![Field {
            name: "value".into(),
            data_type: DataType::Float64,
            last_timestamp: MAX_NANO_TIME,
        }],
    };

    run_field_columns_test_case(MeasurementWithMaxTime {}, predicate, expected_fields).await;
}

#[tokio::test]
async fn list_field_columns_max_time_less_one() {
    let predicate = PredicateBuilder::default()
        // one less than max timestamp
        .timestamp_range(MIN_NANO_TIME, MAX_NANO_TIME - 1)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_fields = FieldList { fields: vec![] };

    run_field_columns_test_case(MeasurementWithMaxTime {}, predicate, expected_fields).await;
}

#[tokio::test]
async fn list_field_columns_max_time_greater_one() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(MIN_NANO_TIME + 1, MAX_NANO_TIME)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_fields = FieldList { fields: vec![] };

    run_field_columns_test_case(MeasurementWithMaxTime {}, predicate, expected_fields).await;
}
