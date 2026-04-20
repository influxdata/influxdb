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
