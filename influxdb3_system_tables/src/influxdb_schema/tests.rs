use super::*;
use datafusion::assert_batches_eq;
use influxdb3_catalog::catalog::{Catalog, InfluxFieldType};
use influxdb3_catalog::log::FieldDataType;

#[test]
fn test_influxdb_schema_schema() {
    let schema = influxdb_schema_schema();
    assert_eq!(schema.fields().len(), 3);

    let fields = schema.fields();
    assert_eq!(fields[0].name(), "measurement");
    assert_eq!(fields[0].data_type(), &DataType::Utf8View);
    assert!(!fields[0].is_nullable());

    assert_eq!(fields[1].name(), "key");
    assert_eq!(fields[1].data_type(), &DataType::Utf8View);
    assert!(!fields[1].is_nullable());

    assert_eq!(fields[2].name(), "data_type");
    assert_eq!(fields[2].data_type(), &DataType::Utf8View);
    assert!(!fields[2].is_nullable());
}

#[test]
fn test_column_type_to_influxdb_type() {
    assert_eq!(
        column_type_to_influxdb_type(&InfluxColumnType::Timestamp),
        "time"
    );
    assert_eq!(column_type_to_influxdb_type(&InfluxColumnType::Tag), "tag");
    assert_eq!(
        column_type_to_influxdb_type(&InfluxColumnType::Field(InfluxFieldType::Boolean)),
        "boolean"
    );
    assert_eq!(
        column_type_to_influxdb_type(&InfluxColumnType::Field(InfluxFieldType::Integer)),
        "integer"
    );
    assert_eq!(
        column_type_to_influxdb_type(&InfluxColumnType::Field(InfluxFieldType::UInteger)),
        "uinteger"
    );
    assert_eq!(
        column_type_to_influxdb_type(&InfluxColumnType::Field(InfluxFieldType::Float)),
        "float"
    );
    assert_eq!(
        column_type_to_influxdb_type(&InfluxColumnType::Field(InfluxFieldType::String)),
        "string"
    );
}

#[tokio::test]
async fn test_influxdb_schema_table_empty_database() {
    let catalog = Arc::new(Catalog::new_in_memory("test").await.unwrap());
    catalog.create_database("test_db").await.unwrap();
    let table = InfluxdbSchemaTable::new(catalog.db_schema("test_db").unwrap());

    let batch = table.scan(None, None).await.unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[tokio::test]
async fn test_influxdb_schema_table_with_data() {
    let catalog = Arc::new(Catalog::new_in_memory("test").await.unwrap());

    // Create a database and table with various column types
    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test_measurement",
            &["tag1", "tag2"],
            &[
                ("field_string", FieldDataType::String),
                ("field_int", FieldDataType::Integer),
                ("field_uint", FieldDataType::UInteger),
                ("field_float", FieldDataType::Float),
                ("field_bool", FieldDataType::Boolean),
            ],
        )
        .await
        .unwrap();

    let table = InfluxdbSchemaTable::new(catalog.db_schema("test_db").unwrap());
    let batch = table.scan(None, None).await.unwrap();
    assert_batches_eq!(
        [
            "+------------------+--------------+-----------+",
            "| measurement      | key          | data_type |",
            "+------------------+--------------+-----------+",
            "| test_measurement | field_bool   | boolean   |",
            "| test_measurement | field_float  | float     |",
            "| test_measurement | field_int    | integer   |",
            "| test_measurement | field_string | string    |",
            "| test_measurement | field_uint   | uinteger  |",
            "| test_measurement | tag1         | tag       |",
            "| test_measurement | tag2         | tag       |",
            "| test_measurement | time         | time      |",
            "+------------------+--------------+-----------+",
        ],
        &[batch]
    );
}

#[tokio::test]
async fn test_influxdb_schema_table_multiple_databases() {
    let catalog = Arc::new(Catalog::new_in_memory("test").await.unwrap());

    // Create multiple databases with tables
    catalog.create_database("db1").await.unwrap();
    catalog.create_database("db2").await.unwrap();

    catalog
        .create_table(
            "db1",
            "measurement1",
            &["tag1"],
            &[("field1", FieldDataType::String)],
        )
        .await
        .unwrap();

    catalog
        .create_table(
            "db2",
            "measurement2",
            &["tag2"],
            &[("field2", FieldDataType::Integer)],
        )
        .await
        .unwrap();

    let table = InfluxdbSchemaTable::new(catalog.db_schema("db1").unwrap());
    let batch = table.scan(None, None).await.unwrap();

    assert_batches_eq!(
        [
            "+--------------+--------+-----------+",
            "| measurement  | key    | data_type |",
            "+--------------+--------+-----------+",
            "| measurement1 | field1 | string    |",
            "| measurement1 | tag1   | tag       |",
            "| measurement1 | time   | time      |",
            "+--------------+--------+-----------+",
        ],
        &[batch]
    );

    let table = InfluxdbSchemaTable::new(catalog.db_schema("db2").unwrap());
    let batch = table.scan(None, None).await.unwrap();

    assert_batches_eq!(
        [
            "+--------------+--------+-----------+",
            "| measurement  | key    | data_type |",
            "+--------------+--------+-----------+",
            "| measurement2 | field2 | integer   |",
            "| measurement2 | tag2   | tag       |",
            "| measurement2 | time   | time      |",
            "+--------------+--------+-----------+",
        ],
        &[batch]
    );
}
