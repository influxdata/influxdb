use arrow::datatypes::DataType;
use datafusion::logical_plan::{col, lit};
use predicate::predicate::{Predicate, PredicateBuilder};
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
    predicate: Predicate,
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
        let planner = InfluxRpcPlanner::new();
        let ctx = db.executor().new_context(query::exec::ExecutorType::Query);

        let plan = planner
            .field_columns(db.as_ref(), predicate.clone())
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
async fn test_field_columns_empty_database() {
    let predicate = PredicateBuilder::default().build();
    let expected_fields = FieldList::default();
    run_field_columns_test_case(NoData {}, predicate, expected_fields).await;
}

#[tokio::test]
async fn test_field_columns_no_predicate() {
    let predicate = PredicateBuilder::default()
        .table("NoSuchTable")
        .add_expr(col("state").eq(lit("MA"))) // state=MA
        .build();
    let expected_fields = FieldList::default();
    run_field_columns_test_case(TwoMeasurementsManyFields {}, predicate, expected_fields).await;
}

// NGA todo: add delete tests when the TwoMeasurementsManyFieldsWithDelete available

#[tokio::test]
async fn test_field_columns_with_pred() {
    // get only fields from h20 (but both chunks)
    let predicate = PredicateBuilder::default()
        .table("h2o")
        .add_expr(col("state").eq(lit("MA"))) // state=MA
        .build();

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
        .table("h2o")
        .timestamp_range(200, 300)
        .add_expr(col("state").eq(lit("MA"))) // state=MA
        .build();

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
                last_timestamp: 1000, // Need to verify with alamb if 1000 is the right one. It looks to me it should be 100
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
