//! Tests for the table_names implementation

use arrow::datatypes::DataType;
use internal_types::{
    schema::{builder::SchemaBuilder, sort::SortKey, TIME_COLUMN_NAME},
    selection::Selection,
};
use query::{QueryChunk, QueryChunkMeta, QueryDatabase};

use super::scenarios::*;
use query::predicate::PredicateBuilder;

/// Creates and loads several database scenarios using the db_setup
/// function.
///
/// runs table_schema(predicate) and compares it to the expected
/// output
macro_rules! run_table_schema_test_case {
    ($DB_SETUP:expr, $SELECTION:expr, $TABLE_NAME:expr, $EXPECTED_SCHEMA:expr) => {
        test_helpers::maybe_start_logging();
        let selection = $SELECTION;
        let table_name = $TABLE_NAME;
        let expected_schema = $EXPECTED_SCHEMA;

        for scenario in $DB_SETUP.make().await {
            let DbScenario {
                scenario_name, db, ..
            } = scenario;
            println!("Running scenario '{}'", scenario_name);
            println!(
                "Getting schema for table '{}', selection {:?}",
                table_name, selection
            );

            // Make sure at least one table has data
            let mut chunks_with_table = 0;
            let predicate = PredicateBuilder::new().table(table_name).build();

            for chunk in db.chunks(&predicate) {
                if chunk.table_name().as_ref() == table_name {
                    chunks_with_table += 1;
                    let actual_schema = chunk.schema().select(selection.clone()).unwrap();

                    assert_eq!(
                        expected_schema,
                        actual_schema,
                        "Mismatch in chunk {}\nExpected:\n{:#?}\nActual:\n{:#?}\n",
                        chunk.id(),
                        expected_schema,
                        actual_schema
                    );
                }
            }
            assert!(
                chunks_with_table > 0,
                "Expected at least one chunk to have data, but none did"
            );
        }
    };
}

#[tokio::test]
async fn list_schema_cpu_all_mub() {
    // we expect columns to come out in lexographic order by name
    let expected_schema = SchemaBuilder::new()
        .tag("region")
        .timestamp()
        .field("user", DataType::Float64)
        .build()
        .unwrap();

    run_table_schema_test_case!(
        TwoMeasurementsMubScenario {},
        Selection::All,
        "cpu",
        expected_schema
    );
}

#[tokio::test]
async fn list_schema_cpu_all_rub() {
    // we expect columns to come out in lexographic order by name
    // The schema of RUB includes sort key
    let mut sort_key = SortKey::with_capacity(2);
    sort_key.push("region", Default::default());
    sort_key.push(TIME_COLUMN_NAME, Default::default());

    let expected_schema = SchemaBuilder::new()
        .tag("region")
        .timestamp()
        .field("user", DataType::Float64)
        .build_with_sort_key(&sort_key)
        .unwrap();

    run_table_schema_test_case!(
        TwoMeasurementsRubScenario {},
        Selection::All,
        "cpu",
        expected_schema
    );
}

#[tokio::test]
async fn list_schema_cpu_all_rub_set_sort_key() {
    // The schema of RUB includes sort key
    let mut sort_key = SortKey::with_capacity(2);
    sort_key.push("region", Default::default());
    sort_key.push(TIME_COLUMN_NAME, Default::default());

    let expected_schema = SchemaBuilder::new()
        .tag("region")
        .timestamp()
        .field("user", DataType::Float64)
        .build_with_sort_key(&sort_key)
        .unwrap();

    run_table_schema_test_case!(
        TwoMeasurementsRubScenario {},
        Selection::All,
        "cpu",
        expected_schema
    );

    // Now set
}

#[tokio::test]
async fn list_schema_disk_all() {
    // we expect columns to come out in lexographic order by name
    let expected_schema = SchemaBuilder::new()
        .field("bytes", DataType::Int64)
        .tag("region")
        .timestamp()
        .build()
        .unwrap();

    run_table_schema_test_case!(
        TwoMeasurementsMubScenario {},
        Selection::All,
        "disk",
        expected_schema
    );
}

#[tokio::test]
async fn list_schema_cpu_selection() {
    let expected_schema = SchemaBuilder::new()
        .field("user", DataType::Float64)
        .tag("region")
        .build()
        .unwrap();

    // Pick an order that is not lexographic
    let selection = Selection::Some(&["user", "region"]);

    run_table_schema_test_case!(
        TwoMeasurementsMubScenario {},
        selection,
        "cpu",
        expected_schema
    );
}

#[tokio::test]
async fn list_schema_disk_selection() {
    // we expect columns to come out in lexographic order by name
    let expected_schema = SchemaBuilder::new()
        .timestamp()
        .field("bytes", DataType::Int64)
        .build()
        .unwrap();

    // Pick an order that is not lexographic
    let selection = Selection::Some(&["time", "bytes"]);

    run_table_schema_test_case!(
        TwoMeasurementsMubScenario {},
        selection,
        "disk",
        expected_schema
    );
}

#[tokio::test]
async fn list_schema_location_all() {
    // we expect columns to come out in lexographic order by name
    let expected_schema = SchemaBuilder::new()
        .field("count", DataType::UInt64)
        .timestamp()
        .tag("town")
        .build()
        .unwrap();

    run_table_schema_test_case!(
        TwoMeasurementsUnsignedTypeMubScenario {},
        Selection::All,
        "restaurant",
        expected_schema
    );
}

#[tokio::test]
async fn test_set_sort_key_valid_same_order() {
    // Build the expected schema with sort key
    let mut sort_key = SortKey::with_capacity(3);
    sort_key.push("tag1", Default::default());
    sort_key.push("time", Default::default());
    sort_key.push("tag2", Default::default());

    let expected_schema = SchemaBuilder::new()
        .tag("tag1")
        .timestamp()
        .tag("tag2")
        .field("field_int", DataType::Int64)
        .field("field_float", DataType::Float64)
        .build_with_sort_key(&sort_key)
        .unwrap();

    // The same schema without sort key
    let mut schema = SchemaBuilder::new()
        .tag("tag1")
        .timestamp()
        .tag("tag2")
        .field("field_int", DataType::Int64)
        .field("field_float", DataType::Float64)
        .build()
        .unwrap();

    schema.set_sort_key(&sort_key);

    assert_eq!(
        expected_schema, schema,
        "Schema mismatch \nExpected:\n{:#?}\nActual:\n{:#?}\n",
        expected_schema, schema
    );
}

#[tokio::test]
async fn test_set_sort_key_valid_different_order() {
    // Build the expected schema with sort key "time, tag2, tag1"
    let mut sort_key = SortKey::with_capacity(3);
    sort_key.push("time", Default::default());
    sort_key.push("tag2", Default::default());
    sort_key.push("tag1", Default::default());

    let expected_schema = SchemaBuilder::new()
        .tag("tag1")
        .timestamp()
        .tag("tag2")
        .field("field_int", DataType::Int64)
        .field("field_float", DataType::Float64)
        .build_with_sort_key(&sort_key)
        .unwrap();

    // The same schema without sort key
    let mut schema = SchemaBuilder::new()
        .tag("tag1")
        .timestamp()
        .tag("tag2")
        .field("field_int", DataType::Int64)
        .field("field_float", DataType::Float64)
        .build()
        .unwrap();

    schema.set_sort_key(&sort_key);

    assert_eq!(
        expected_schema, schema,
        "Schema mismatch \nExpected:\n{:#?}\nActual:\n{:#?}\n",
        expected_schema, schema
    );
}

#[tokio::test]
async fn test_set_sort_key_valid_subset() {
    // Build the expected schema with sort key "time, tag1"
    let mut sort_key = SortKey::with_capacity(2);
    sort_key.push("time", Default::default());
    sort_key.push("tag1", Default::default());

    let expected_schema = SchemaBuilder::new()
        .tag("tag1")
        .timestamp()
        .tag("tag2")
        .field("field_int", DataType::Int64)
        .field("field_float", DataType::Float64)
        .build_with_sort_key(&sort_key)
        .unwrap();

    // The same schema without sort key
    let mut schema = SchemaBuilder::new()
        .tag("tag1")
        .timestamp()
        .tag("tag2")
        .field("field_int", DataType::Int64)
        .field("field_float", DataType::Float64)
        .build()
        .unwrap();

    // set sort key for it
    schema.set_sort_key(&sort_key);

    assert_eq!(
        expected_schema, schema,
        "Schema mismatch \nExpected:\n{:#?}\nActual:\n{:#?}\n",
        expected_schema, schema
    );
}

#[tokio::test]
async fn test_set_sort_key_valid_subset_of_fully_set() {
    // Build sort key "tag1, time, tag2"
    let mut sort_key = SortKey::with_capacity(3);
    sort_key.push("tag1", Default::default());
    sort_key.push("time", Default::default());
    sort_key.push("tag2", Default::default());

    // The schema with sort key
    let mut schema = SchemaBuilder::new()
        .tag("tag1")
        .timestamp()
        .tag("tag2")
        .field("field_int", DataType::Int64)
        .field("field_float", DataType::Float64)
        .build_with_sort_key(&sort_key)
        .unwrap();

    // reset sort key to "tag2, time"
    let mut sort_key = SortKey::with_capacity(2);
    sort_key.push("tag2", Default::default());
    sort_key.push("time", Default::default());

    schema.set_sort_key(&sort_key);

    // Expected schema with "tag2, time" as sort key
    let expected_schema = SchemaBuilder::new()
        .tag("tag1")
        .timestamp()
        .tag("tag2")
        .field("field_int", DataType::Int64)
        .field("field_float", DataType::Float64)
        .build_with_sort_key(&sort_key)
        .unwrap();

    assert_eq!(
        expected_schema, schema,
        "Schema mismatch \nExpected:\n{:#?}\nActual:\n{:#?}\n",
        expected_schema, schema
    );
}

#[tokio::test]
async fn test_set_sort_key_invalid_not_exist() {
    // Build the expected schema with sort key "time"
    let mut sort_key = SortKey::with_capacity(1);
    sort_key.push("time", Default::default());

    let expected_schema = SchemaBuilder::new()
        .tag("tag1")
        .timestamp()
        .tag("tag2")
        .field("field_int", DataType::Int64)
        .field("field_float", DataType::Float64)
        .build_with_sort_key(&sort_key)
        .unwrap();

    // The same schema without sort key
    let mut schema = SchemaBuilder::new()
        .tag("tag1")
        .timestamp()
        .tag("tag2")
        .field("field_int", DataType::Int64)
        .field("field_float", DataType::Float64)
        .build()
        .unwrap();

    // Nuild sort key that include valid "time" and invalid "no_tag"
    let mut sort_key = SortKey::with_capacity(2);
    sort_key.push("time", Default::default());
    // invalid column
    sort_key.push("not_tag", Default::default());

    // The invalid key will be ignored in this function
    schema.set_sort_key(&sort_key);

    assert_eq!(
        expected_schema, schema,
        "Schema mismatch \nExpected:\n{:#?}\nActual:\n{:#?}\n",
        expected_schema, schema
    );
}
