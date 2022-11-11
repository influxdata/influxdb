use crate::scenarios::*;
use arrow::datatypes::DataType;
use data_types::{MAX_NANO_TIME, MIN_NANO_TIME};
use datafusion::prelude::{col, lit};
use iox_query::{
    exec::fieldlist::{Field, FieldList},
    frontend::influxrpc::InfluxRpcPlanner,
};
use predicate::{rpc_predicate::InfluxRpcPredicate, Predicate};

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
        let ctx = db.new_query_context(None);
        let planner = InfluxRpcPlanner::new(ctx.child_ctx("planner"));

        let plan = planner
            .field_columns(db.as_query_namespace_arc(), predicate.clone())
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
    let predicate = Predicate::default().with_expr(col("state").eq(lit("MA"))); // state=MA
    let predicate = InfluxRpcPredicate::new_table("NoSuchTable", predicate);
    let expected_fields = FieldList::default();
    run_field_columns_test_case(TwoMeasurementsManyFields {}, predicate, expected_fields).await;
}

#[tokio::test]
async fn test_field_columns_with_pred() {
    // get only fields from h20 (but both chunks)
    let predicate = Predicate::default().with_expr(col("state").eq(lit("MA"))); // state=MA
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
    let predicate = Predicate::default()
        .with_expr(col("_measurement").eq(lit("h2o")))
        .with_range(i64::MIN, i64::MAX - 2);
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
async fn test_field_columns_measurement_pred_all_time() {
    // get only fields from h2o using a _measurement predicate
    let predicate = Predicate::default().with_expr(col("_measurement").eq(lit("h2o")));
    let predicate = InfluxRpcPredicate::new(None, predicate);

    // optimized all-time case returns zero last_timestamp
    let expected_fields = FieldList {
        fields: vec![
            Field {
                name: "moisture".into(),
                data_type: DataType::Float64,
                last_timestamp: 0,
            },
            Field {
                name: "other_temp".into(),
                data_type: DataType::Float64,
                last_timestamp: 0,
            },
            Field {
                name: "temp".into(),
                data_type: DataType::Float64,
                last_timestamp: 0,
            },
        ],
    };

    run_field_columns_test_case(TwoMeasurementsManyFields {}, predicate, expected_fields).await;
}

#[tokio::test]
async fn test_field_columns_with_ts_pred() {
    let predicate = Predicate::default()
        .with_range(200, 300)
        .with_expr(col("state").eq(lit("MA"))); // state=MA
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

    let predicate = Predicate::default().with_range(0, 2000);
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
async fn list_field_columns_all_time() {
    let predicate = Predicate::default().with_range(MIN_NANO_TIME, MAX_NANO_TIME);
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_fields = FieldList {
        fields: vec![Field {
            name: "value".into(),
            data_type: DataType::Float64,
            last_timestamp: 0, // we hit the optimized case that ignores timestamps
        }],
    };

    run_field_columns_test_case(MeasurementWithMaxTime {}, predicate, expected_fields).await;
}

#[tokio::test]
async fn list_field_columns_max_time_included() {
    // if the range started at i64:MIN, we would hit the optimized case for 'all time'
    // and get the 'wrong' timestamp, since in the optimized case we don't check what timestamps
    // exist
    let predicate = Predicate::default().with_range(MIN_NANO_TIME + 1, MAX_NANO_TIME + 1);
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
async fn list_field_columns_max_time_excluded() {
    let predicate = Predicate::default()
        // one less than max timestamp
        .with_range(MIN_NANO_TIME + 1, MAX_NANO_TIME);
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_fields = FieldList { fields: vec![] };

    run_field_columns_test_case(MeasurementWithMaxTime {}, predicate, expected_fields).await;
}

#[tokio::test]
async fn list_field_columns_with_periods() {
    let predicate = Predicate::default().with_range(0, 1700000001000000000);
    let predicate = InfluxRpcPredicate::new(None, predicate);

    let expected_fields = FieldList {
        fields: vec![
            Field {
                name: "field.one".into(),
                data_type: DataType::Float64,
                last_timestamp: 1609459201000000002,
            },
            Field {
                name: "field.two".into(),
                data_type: DataType::Boolean,
                last_timestamp: 1609459201000000002,
            },
        ],
    };

    run_field_columns_test_case(PeriodsInNames {}, predicate, expected_fields).await;
}
