use std::sync::Arc;

use super::WriteValidator;
use crate::{Precision, write_buffer::Error};

use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::TableId;
use influxdb3_types::DatabaseName;
use influxdb3_wal::Gen1Duration;
use iox_time::{MockProvider, Time};
use object_store::memory::InMemory;

#[tokio::test]
async fn rejected_over_limit_line_creates_no_schema() {
    let catalog = Catalog::new_in_memory_with_limits(
        "test",
        influxdb3_catalog::catalog::CatalogLimits::new(10, 100, 5),
    )
    .await
    .unwrap();
    let database_name = DatabaseName::new("test").unwrap();
    // 1 tag + 8 fields projects to 9 columns against a limit of 5.
    let lp = "cpu,host=a f1=1,f2=1,f3=1,f4=1,f5=1,f6=1,f7=1,f8=1 1000";
    let result = WriteValidator::initialize(database_name.clone(), Arc::clone(&catalog))
        .unwrap()
        .v1_parse_lines_and_catalog_updates(
            lp,
            true,
            Time::from_timestamp_nanos(0),
            Precision::Auto,
        )
        .unwrap()
        .commit_catalog_changes()
        .await
        .unwrap()
        .unwrap_success()
        .convert_lines_to_buffer(Gen1Duration::new_5m());
    assert_eq!(result.errors.len(), 1);
    assert!(
        result.errors[0].error_message.contains("9 columns"),
        "error should report the projected column count, got: {}",
        result.errors[0].error_message
    );
    // The rejected line must not have created the table or any columns.
    assert!(
        catalog
            .db_schema("test")
            .unwrap()
            .table_definition("cpu")
            .is_none()
    );

    // An existing table's schema stays unchanged when an over-limit line is
    // rejected.
    let result = WriteValidator::initialize(database_name.clone(), Arc::clone(&catalog))
        .unwrap()
        .v1_parse_lines_and_catalog_updates(
            "cpu,host=a f1=1 1000",
            true,
            Time::from_timestamp_nanos(0),
            Precision::Auto,
        )
        .unwrap()
        .commit_catalog_changes()
        .await
        .unwrap()
        .unwrap_success()
        .convert_lines_to_buffer(Gen1Duration::new_5m());
    assert!(result.errors.is_empty());
    let columns_before = catalog
        .db_schema("test")
        .unwrap()
        .table_definition("cpu")
        .unwrap()
        .num_columns();
    let result = WriteValidator::initialize(database_name, Arc::clone(&catalog))
        .unwrap()
        .v1_parse_lines_and_catalog_updates(
            lp,
            true,
            Time::from_timestamp_nanos(0),
            Precision::Auto,
        )
        .unwrap()
        .commit_catalog_changes()
        .await
        .unwrap()
        .unwrap_success()
        .convert_lines_to_buffer(Gen1Duration::new_5m());
    assert_eq!(result.errors.len(), 1);
    let columns_after = catalog
        .db_schema("test")
        .unwrap()
        .table_definition("cpu")
        .unwrap()
        .num_columns();
    assert_eq!(columns_before, columns_after);
}

#[tokio::test]
async fn rejected_type_conflict_line_creates_no_schema() {
    let catalog = Catalog::new_in_memory("test").await.unwrap();
    let database_name = DatabaseName::new("test").unwrap();
    let result = WriteValidator::initialize(database_name.clone(), Arc::clone(&catalog))
        .unwrap()
        .v1_parse_lines_and_catalog_updates(
            "cpu f1=1.0 1000",
            true,
            Time::from_timestamp_nanos(0),
            Precision::Second,
        )
        .unwrap()
        .commit_catalog_changes()
        .await
        .unwrap()
        .unwrap_success()
        .convert_lines_to_buffer(Gen1Duration::new_5m());
    assert!(result.errors.is_empty());

    // f1 exists as a float; the string value rejects the line, and the line's
    // other new columns must not be created.
    let result = WriteValidator::initialize(database_name, Arc::clone(&catalog))
        .unwrap()
        .v1_parse_lines_and_catalog_updates(
            "cpu,newtag=x f9=9,f1=\"s\" 1000",
            true,
            Time::from_timestamp_nanos(0),
            Precision::Second,
        )
        .unwrap()
        .commit_catalog_changes()
        .await
        .unwrap()
        .unwrap_success()
        .convert_lines_to_buffer(Gen1Duration::new_5m());
    assert_eq!(result.errors.len(), 1);
    let table = catalog
        .db_schema("test")
        .unwrap()
        .table_definition("cpu")
        .unwrap();
    assert!(
        !table.column_exists("newtag") && !table.column_exists("f9"),
        "rejected line must not create columns"
    );
}

#[tokio::test]
async fn rejected_bad_timestamp_line_creates_no_schema() {
    let catalog = Catalog::new_in_memory("test").await.unwrap();
    let database_name = DatabaseName::new("test").unwrap();
    // Seconds precision overflows to_nanos for this timestamp.
    let result = WriteValidator::initialize(database_name, Arc::clone(&catalog))
        .unwrap()
        .v1_parse_lines_and_catalog_updates(
            "cpu,t1=a f1=1 100000000000000",
            true,
            Time::from_timestamp_nanos(0),
            Precision::Second,
        )
        .unwrap()
        .commit_catalog_changes()
        .await
        .unwrap()
        .unwrap_success()
        .convert_lines_to_buffer(Gen1Duration::new_5m());
    assert_eq!(result.errors.len(), 1);
    assert!(
        catalog
            .db_schema("test")
            .unwrap()
            .table_definition("cpu")
            .is_none(),
        "rejected line must not create the table"
    );
}

#[tokio::test]
async fn write_validator_v1() -> Result<(), Error> {
    let node_id = Arc::from("sample-host-id");
    let obj_store = Arc::new(InMemory::new());
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let database_name = DatabaseName::new("test").unwrap();
    let catalog = Arc::new(
        Catalog::new(node_id, obj_store, time_provider, Default::default())
            .await
            .unwrap(),
    );
    let expected_sequence = catalog.sequence_number().next();
    let result = WriteValidator::initialize(database_name.clone(), Arc::clone(&catalog))?
        .v1_parse_lines_and_catalog_updates(
            "cpu,tag1=foo val1=\"bar\" 1234",
            false,
            Time::from_timestamp_nanos(0),
            Precision::Auto,
        )?
        .commit_catalog_changes()
        .await?
        .unwrap_success()
        .convert_lines_to_buffer(Gen1Duration::new_5m());

    println!("result: {result:?}");
    assert_eq!(result.line_count, 1);
    assert_eq!(result.field_count, 1);
    assert_eq!(result.index_count, 1);
    assert!(result.errors.is_empty());
    assert_eq!(expected_sequence, catalog.sequence_number());
    assert_eq!(
        result.valid_data.database_name.as_ref(),
        database_name.as_str()
    );
    // cpu table
    let batch = result
        .valid_data
        .table_chunks
        .get(&TableId::from(0))
        .unwrap();
    assert_eq!(batch.row_count(), 1);

    // Validate another write, the result should be very similar, but now the catalog
    // has the table/columns added, so it will excercise a different code path:
    let expected_sequence = catalog.sequence_number();
    let result = WriteValidator::initialize(database_name.clone(), Arc::clone(&catalog))?
        .v1_parse_lines_and_catalog_updates(
            "cpu,tag1=foo val1=\"bar\" 1235",
            false,
            Time::from_timestamp_nanos(0),
            Precision::Auto,
        )?
        .commit_catalog_changes()
        .await?
        .unwrap_success()
        .convert_lines_to_buffer(Gen1Duration::new_5m());

    println!("result: {result:?}");
    assert_eq!(result.line_count, 1);
    assert_eq!(result.field_count, 1);
    assert_eq!(result.index_count, 1);
    assert_eq!(expected_sequence, catalog.sequence_number());
    assert!(result.errors.is_empty());

    // Validate another write, this time adding a new field:
    let expected_sequence = catalog.sequence_number().next();
    let result = WriteValidator::initialize(database_name.clone(), Arc::clone(&catalog))?
        .v1_parse_lines_and_catalog_updates(
            "cpu,tag1=foo val1=\"bar\",val2=false 1236",
            false,
            Time::from_timestamp_nanos(0),
            Precision::Auto,
        )?
        .commit_catalog_changes()
        .await?
        .unwrap_success()
        .convert_lines_to_buffer(Gen1Duration::new_5m());

    println!("result: {result:?}");
    assert_eq!(result.line_count, 1);
    assert_eq!(result.field_count, 2);
    assert_eq!(result.index_count, 1);
    assert!(result.errors.is_empty());
    assert_eq!(expected_sequence, catalog.sequence_number());

    Ok(())
}

/// A point that repeats a tag key must be rejected, the same way a repeated field key
/// already is. Without this, the duplicate tag produces two columns with the same id,
/// which desyncs the table buffer and later panics when building the record batch
/// ("all columns in a record batch must have the same length"). See influxdb_pro#4375.
#[tokio::test]
async fn write_validator_rejects_duplicate_tag() -> Result<(), Error> {
    let node_id = Arc::from("sample-host-id");
    let obj_store = Arc::new(InMemory::new());
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let database_name = DatabaseName::new("test").unwrap();
    let catalog = Arc::new(
        Catalog::new(node_id, obj_store, time_provider, Default::default())
            .await
            .unwrap(),
    );

    // accept_partial = true: the duplicate-tag line is collected into `errors` and
    // dropped, so no row is buffered.
    let result = WriteValidator::initialize(database_name.clone(), Arc::clone(&catalog))?
        .v1_parse_lines_and_catalog_updates(
            "cpu,tag1=foo,tag1=bar val1=\"bar\" 1234",
            true,
            Time::from_timestamp_nanos(0),
            Precision::Auto,
        )?
        .commit_catalog_changes()
        .await?
        .unwrap_success()
        .convert_lines_to_buffer(Gen1Duration::new_5m());

    println!("result: {result:?}");
    assert_eq!(
        result.line_count, 0,
        "the duplicate-tag line must not be buffered"
    );
    assert_eq!(result.errors.len(), 1, "expected exactly one line error");
    assert!(
        result.errors[0]
            .error_message
            .contains("multiple instances of 'tag1' tag found"),
        "unexpected error message: {:?}",
        result.errors
    );

    // accept_partial = false: the whole write is rejected with a ParseError.
    let err = WriteValidator::initialize(database_name.clone(), Arc::clone(&catalog))?
        .v1_parse_lines_and_catalog_updates(
            "cpu,tag1=foo,tag1=bar val1=\"bar\" 1234",
            false,
            Time::from_timestamp_nanos(0),
            Precision::Auto,
        )
        .err();
    assert!(
        matches!(
            &err,
            Some(Error::ParseError(e)) if e.error_message.contains("multiple instances of 'tag1' tag found")
        ),
        "expected a ParseError for the duplicate tag, got: {err:?}"
    );

    Ok(())
}
