use super::log::{DeleteBatch, DeleteOp, FieldDataType, LastCacheSize, MaxAge, MaxCardinality};

use super::*;
use crate::object_store::versions::v2::CatalogFilePath;
use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
use iox_time::MockProvider;
use object_store::{local::LocalFileSystem, memory::InMemory};
use pretty_assertions::assert_eq;
use rstest::rstest;
use test_helpers::assert_contains;

use crate::serialize::versions::v2::{
    serialize_catalog_file, verify_and_deserialize_catalog_checkpoint_file,
};

#[test_log::test(tokio::test)]
async fn catalog_serialization() {
    let catalog = Catalog::new_in_memory("sample-host-id").await.unwrap();
    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test_table_1",
            &["tag_1", "tag_2", "tag_3"],
            &[
                ("string_field", FieldDataType::String),
                ("bool_field", FieldDataType::Boolean),
                ("i64_field", FieldDataType::Integer),
                ("u64_field", FieldDataType::UInteger),
                ("float_field", FieldDataType::Float),
            ],
        )
        .await
        .unwrap();
    catalog
        .create_table(
            "test_db",
            "test_table_2",
            &["tag_1", "tag_2", "tag_3"],
            &[
                ("string_field", FieldDataType::String),
                ("bool_field", FieldDataType::Boolean),
                ("i64_field", FieldDataType::Integer),
                ("u64_field", FieldDataType::UInteger),
                ("float_field", FieldDataType::Float),
            ],
        )
        .await
        .unwrap();

    insta::allow_duplicates! {
        insta::with_settings!({
            sort_maps => true,
            description => "catalog serialization to help catch breaking changes"
        }, {
            let snapshot = catalog.snapshot();
            insta::assert_json_snapshot!(snapshot, {
                ".catalog_uuid" => "[uuid]"
            });
            // Serialize/deserialize to ensure roundtrip
            let serialized = serialize_catalog_file(&snapshot).unwrap();
            let snapshot = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap() ;
            insta::assert_json_snapshot!(snapshot, {
                ".catalog_uuid" => "[uuid]"
            });
            catalog.update_from_snapshot(snapshot);
            assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(1)));
        });
    }
}

#[test]
fn add_columns_updates_schema_and_column_map() {
    let mut database = DatabaseSchema {
        id: DbId::from(0),
        name: "test".into(),
        tables: Repository::new(),
        retention_period: RetentionPeriod::Indefinite,
        processing_engine_triggers: Default::default(),
        deleted: false,
        hard_delete_time: None,
    };
    database
        .tables
        .insert(
            TableId::from(0),
            Arc::new(
                TableDefinition::new(
                    TableId::from(0),
                    "test".into(),
                    vec![
                        ColumnDefinition::field((0, 0), 0, "test", InfluxFieldType::String),
                        ColumnDefinition::tag(1, 1, "test999"),
                    ],
                    vec![(FieldFamilyId::new(0), FieldFamilyName::User("test".into()))],
                    vec![TagId::new(1)],
                    FieldFamilyMode::Aware,
                )
                .unwrap(),
            ),
        )
        .unwrap();

    let mut table = database.tables.get_by_id(&TableId::from(0)).unwrap();
    assert_eq!(table.columns.len(), 2);
    assert_eq!(
        table
            .column_id_to_name(&ColumnIdentifier::field(0, 0))
            .unwrap(),
        "test".into()
    );
    assert_eq!(
        table.column_id_to_name(&TagId::new(1).into()).unwrap(),
        "test999".into()
    );
    assert_eq!(table.series_key.len(), 1);
    assert_eq!(table.series_key_names.len(), 1);
    assert_eq!(table.sort_key, SortKey::from_columns(vec!["test999"]));
    assert_eq!(table.schema.primary_key(), &["test999"]);

    // add time and verify key is updated
    Arc::make_mut(&mut table)
        .add_columns(vec![ColumnDefinition::timestamp(0)])
        .unwrap();
    assert_eq!(table.series_key.len(), 1);
    assert_eq!(table.series_key_names.len(), 1);
    assert_eq!(
        table.sort_key,
        SortKey::from_columns(vec!["test999", TIME_COLUMN_NAME])
    );
    assert_eq!(table.schema.primary_key(), &["test999", TIME_COLUMN_NAME]);

    Arc::make_mut(&mut table)
        .add_columns(vec![ColumnDefinition::tag(3, 1, "test2")])
        .unwrap();

    // Verify the series key, series key names and sort key are updated when a tag column is added,
    // and that the "time" column is still at the end.
    assert_eq!(table.series_key.len(), 2);
    assert_eq!(table.series_key_names, &["test999".into(), "test2".into()]);
    assert_eq!(
        table.sort_key,
        SortKey::from_columns(vec!["test999", "test2", TIME_COLUMN_NAME])
    );

    let schema = table.influx_schema();
    assert_eq!(
        schema.field(0).0,
        InfluxColumnType::Field(InfluxFieldType::String)
    );
    assert_eq!(schema.field(1).0, InfluxColumnType::Tag);
    assert_eq!(schema.field(2).0, InfluxColumnType::Tag);

    assert_eq!(table.columns.len(), 4);
    assert_eq!(
        table.columns.get_by_name("test2").unwrap().id().to_tag(),
        3.into()
    );

    // Verify the schema is updated.
    assert_eq!(table.schema.len(), 4);
    assert_eq!(table.schema.measurement(), Some(&"test".to_owned()));
    let pk = table.schema.primary_key();
    assert_eq!(pk, &["test999", "test2", TIME_COLUMN_NAME]);
}

#[tokio::test]
async fn serialize_series_keys() {
    let catalog = Catalog::new_in_memory("sample-host-id").await.unwrap();
    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test_table_1",
            &["tag_1", "tag_2", "tag_3"],
            &[("field", FieldDataType::String)],
        )
        .await
        .unwrap();

    insta::allow_duplicates! {
        insta::with_settings!({
            sort_maps => true,
            description => "catalog serialization to help catch breaking changes"
        }, {
            let snapshot = catalog.snapshot();
            insta::assert_json_snapshot!(snapshot, {
                ".catalog_uuid" => "[uuid]"
            });
            // Serialize/deserialize to ensure roundtrip
            let serialized = serialize_catalog_file(&snapshot).unwrap();
            let snapshot = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap() ;
            insta::assert_json_snapshot!(snapshot, {
                ".catalog_uuid" => "[uuid]"
            });
            catalog.update_from_snapshot(snapshot);
            assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(1)));
        });
    }
}

#[tokio::test]
async fn serialize_last_cache() {
    let catalog = Catalog::new_in_memory("sample-host-id").await.unwrap();
    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test",
            &["tag_1", "tag_2", "tag_3"],
            &[("field", FieldDataType::String)],
        )
        .await
        .unwrap();
    catalog
        .create_last_cache(
            "test_db",
            "test",
            Some("test_table_last_cache"),
            Some(&["tag_1", "tag_3"]),
            Some(&["field"]),
            LastCacheSize::new(1).unwrap(),
            LastCacheTtl::from_secs(600),
        )
        .await
        .unwrap();

    insta::allow_duplicates! {
        insta::with_settings!({
            sort_maps => true,
            description => "catalog serialization to help catch breaking changes"
        }, {
            let snapshot = catalog.snapshot();
            insta::assert_json_snapshot!(snapshot, {
                ".catalog_uuid" => "[uuid]"
            });
            // Serialize/deserialize to ensure roundtrip
            let serialized = serialize_catalog_file(&snapshot).unwrap();
            let snapshot = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap() ;
            insta::assert_json_snapshot!(snapshot, {
                ".catalog_uuid" => "[uuid]"
            });
            catalog.update_from_snapshot(snapshot);
            assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(1)));
        });
    }
}

#[test_log::test(tokio::test)]
async fn test_serialize_distinct_cache() {
    let catalog = Catalog::new_in_memory("sample-host-id").await.unwrap();
    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test_table",
            &["tag_1", "tag_2", "tag_3"],
            &[("field", FieldDataType::String)],
        )
        .await
        .unwrap();
    catalog
        .create_distinct_cache(
            "test_db",
            "test_table",
            Some("test_cache"),
            &["tag_1", "tag_2"],
            MaxCardinality::from_usize_unchecked(100),
            MaxAge::from_secs(10),
        )
        .await
        .unwrap();

    insta::allow_duplicates! {
        insta::with_settings!({
            sort_maps => true,
            description => "catalog serialization to help catch breaking changes"
        }, {
            let snapshot = catalog.snapshot();
            insta::assert_json_snapshot!(snapshot, {
                ".catalog_uuid" => "[uuid]"
            });
            // Serialize/deserialize to ensure roundtrip
            let serialized = serialize_catalog_file(&snapshot).unwrap();
            let snapshot = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap() ;
            insta::assert_json_snapshot!(snapshot, {
                ".catalog_uuid" => "[uuid]"
            });
            catalog.update_from_snapshot(snapshot);
            assert_eq!(catalog.db_name_to_id("test_db"), Some(DbId::from(1)));
        });
    }
}

#[tokio::test]
async fn test_catalog_id() {
    let catalog = Catalog::new_in_memory("sample-host-id").await.unwrap();
    assert_eq!("sample-host-id", catalog.catalog_id().as_ref());
}

/// See: https://github.com/influxdata/influxdb/issues/25524
#[test_log::test(tokio::test)]
async fn apply_catalog_batch_fails_for_add_columns_on_nonexist_table() {
    let catalog = Catalog::new_in_memory("host").await.unwrap();
    catalog.create_database("foo").await.unwrap();
    let db_id = catalog.db_name_to_id("foo").unwrap();
    let catalog_batch = create::catalog_batch(
        db_id,
        "foo",
        0,
        [create::add_columns_op(
            db_id,
            "foo",
            0,
            "banana",
            [create::tag_def(0, 0, "papaya")],
            [],
        )],
    );
    debug!("getting write lock");
    let mut inner = catalog.inner.write();
    let sequence = inner.sequence_number();
    let err = inner
        .apply_catalog_batch(&catalog_batch, sequence.next())
        .expect_err("should fail to apply AddColumns operation for non-existent table");
    assert_contains!(err.to_string(), "Table banana not in DB schema for foo");
}

#[tokio::test]
async fn test_check_and_mark_table_as_deleted() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
    catalog.create_database("test").await.unwrap();
    catalog
        .create_table(
            "test",
            "boo",
            &["tag_1", "tag_2", "tag_3"],
            &[("field", FieldDataType::String)],
        )
        .await
        .unwrap();

    assert!(
        !catalog
            .db_schema("test")
            .unwrap()
            .table_definition("boo")
            .unwrap()
            .deleted
    );

    catalog
        .soft_delete_table("test", "boo", HardDeletionTime::Never)
        .await
        .unwrap();

    assert!(
        catalog
            .db_schema("test")
            .unwrap()
            .table_definition("boo-19700101T000000")
            .unwrap()
            .deleted
    );
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_table() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

    // Create database and table
    catalog.create_database("test").await.unwrap();
    catalog
        .create_table(
            "test",
            "boo",
            &["tag_1", "tag_2"],
            &[("field", FieldDataType::String)],
        )
        .await
        .unwrap();

    // Get database and table IDs
    let db_id = catalog.db_name_to_id("test").unwrap();
    let table_id = catalog
        .db_schema("test")
        .unwrap()
        .table_definition("boo")
        .unwrap()
        .table_id;

    // Verify table exists
    assert!(
        catalog
            .db_schema("test")
            .unwrap()
            .table_definition("boo")
            .is_some()
    );

    // Hard delete the table
    catalog.hard_delete_table(&db_id, &table_id).await.unwrap();

    // Verify table is removed from the database schema
    assert!(
        catalog
            .db_schema("test")
            .unwrap()
            .table_definition("boo")
            .is_none(),
        "Table should be removed after hard deletion"
    );

    // Verify database still exists
    assert!(
        catalog.db_schema("test").is_some(),
        "Database should still exist after table hard deletion"
    );

    assert!(
        catalog
            .db_schema("test")
            .unwrap()
            .table_definition("boo")
            .is_none(),
        "Table boo should be hard deleted"
    );
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_multiple_tables() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

    // Create database and multiple tables
    catalog.create_database("test").await.unwrap();
    catalog
        .create_table(
            "test",
            "table1",
            &["tag"],
            &[("field", FieldDataType::Float)],
        )
        .await
        .unwrap();
    catalog
        .create_table(
            "test",
            "table2",
            &["tag"],
            &[("field", FieldDataType::Integer)],
        )
        .await
        .unwrap();
    catalog
        .create_table(
            "test",
            "table3",
            &["tag"],
            &[("field", FieldDataType::String)],
        )
        .await
        .unwrap();

    // Get database and table IDs
    let db_id = catalog.db_name_to_id("test").unwrap();
    let db_schema = catalog.db_schema("test").unwrap();
    let table_id_1 = db_schema.table_definition("table1").unwrap().table_id;
    let table_id_2 = db_schema.table_definition("table2").unwrap().table_id;
    let table_id_3 = db_schema.table_definition("table3").unwrap().table_id;

    // Hard delete all tables
    catalog
        .hard_delete_table(&db_id, &table_id_1)
        .await
        .unwrap();
    catalog
        .hard_delete_table(&db_id, &table_id_2)
        .await
        .unwrap();
    catalog
        .hard_delete_table(&db_id, &table_id_3)
        .await
        .unwrap();

    // Verify all tables have been hard deleted
    let db_schema_after = catalog.db_schema("test").unwrap();
    assert!(
        db_schema_after.table_definition("table1").is_none(),
        "Table table1 should be hard deleted"
    );
    assert!(
        db_schema_after.table_definition("table2").is_none(),
        "Table table2 should be hard deleted"
    );
    assert!(
        db_schema_after.table_definition("table3").is_none(),
        "Table table3 should be hard deleted"
    );

    // Verify database still exists
    assert!(
        catalog.db_schema("test").is_some(),
        "Database should still exist after all tables are hard deleted"
    );
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_nonexistent_table() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

    // Try to delete table from non-existent database
    let fake_db_id = DbId::from(999);
    let fake_table_id = TableId::from(123);
    let result = catalog.hard_delete_table(&fake_db_id, &fake_table_id).await;
    assert!(matches!(result, Err(CatalogError::NotFound(_))));

    // Create database but try to delete non-existent table
    catalog.create_database("test").await.unwrap();
    let db_id = catalog.db_name_to_id("test").unwrap();
    let result = catalog.hard_delete_table(&db_id, &fake_table_id).await;
    assert!(matches!(result, Err(CatalogError::NotFound(_))));
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_table_after_soft_delete() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

    // Create database and table
    catalog.create_database("test").await.unwrap();
    catalog
        .create_table("test", "boo", &["tag"], &[("field", FieldDataType::String)])
        .await
        .unwrap();

    // Get database and table IDs
    let db_id = catalog.db_name_to_id("test").unwrap();
    let table_id = catalog
        .db_schema("test")
        .unwrap()
        .table_definition("boo")
        .unwrap()
        .table_id;

    // First soft delete the table
    catalog
        .soft_delete_table("test", "boo", HardDeletionTime::Never)
        .await
        .unwrap();

    // Verify table is soft deleted
    assert!(
        catalog
            .db_schema("test")
            .unwrap()
            .table_definition("boo-19700101T000000")
            .unwrap()
            .deleted
    );

    // Now hard delete the table
    catalog.hard_delete_table(&db_id, &table_id).await.unwrap();

    // Verify the soft-deleted table is now completely removed
    assert!(
        catalog
            .db_schema("test")
            .unwrap()
            .table_definition("boo-19700101T000000")
            .is_none(),
        "Soft-deleted table should be removed after hard deletion"
    );

    // Verify database still exists
    assert!(
        catalog.db_schema("test").is_some(),
        "Database should still exist after table hard deletion"
    );
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_table_with_snapshot() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

    // Create database and tables
    catalog.create_database("test").await.unwrap();
    catalog
        .create_table(
            "test",
            "table1",
            &["tag"],
            &[("field", FieldDataType::Float)],
        )
        .await
        .unwrap();
    catalog
        .create_table(
            "test",
            "table2",
            &["tag"],
            &[("field", FieldDataType::Integer)],
        )
        .await
        .unwrap();

    // Get database and table IDs
    let db_id = catalog.db_name_to_id("test").unwrap();
    let db_schema = catalog.db_schema("test").unwrap();
    let table_id_1 = db_schema.table_definition("table1").unwrap().table_id;

    // Hard delete one table
    catalog
        .hard_delete_table(&db_id, &table_id_1)
        .await
        .unwrap();

    // Take a snapshot
    let snapshot = catalog.snapshot();

    // Serialize and deserialize the snapshot
    let serialized = serialize_catalog_file(&snapshot).unwrap();
    let deserialized = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap();

    // Create a new catalog from the snapshot
    let new_catalog = Catalog::new_in_memory("test-host-2").await.unwrap();
    new_catalog.update_from_snapshot(deserialized);

    // Verify the new catalog has the same state as the original after hard deletion
    let new_db_schema = new_catalog.db_schema("test").unwrap();
    assert!(
        new_db_schema.table_definition("table1").is_none(),
        "Table1 should remain deleted in the new catalog"
    );
    assert!(
        new_db_schema.table_definition("table2").is_some(),
        "Table2 should still exist in the new catalog"
    );

    // Verify the database still exists
    assert!(
        new_catalog.db_schema("test").is_some(),
        "Database should exist in the new catalog"
    );
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_database() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

    // Create databases
    catalog.create_database("test").await.unwrap();
    catalog.create_database("test2").await.unwrap();

    // Create tables in test database
    catalog
        .create_table(
            "test",
            "table1",
            &["tag"],
            &[("field", FieldDataType::String)],
        )
        .await
        .unwrap();
    catalog
        .create_table(
            "test",
            "table2",
            &["tag"],
            &[("field", FieldDataType::Float)],
        )
        .await
        .unwrap();

    // Get database ID
    let db_id = catalog.db_name_to_id("test").unwrap();

    // Hard delete the database
    catalog.hard_delete_database(&db_id).await.unwrap();

    // Verify database is completely removed
    assert!(
        catalog.db_schema("test").is_none(),
        "Database 'test' should be removed after hard deletion"
    );

    // Verify test2 database still exists
    assert!(
        catalog.db_schema("test2").is_some(),
        "Database 'test2' should still exist"
    );

    // Verify we can't look up the deleted database by name
    assert!(
        catalog.db_name_to_id("test").is_none(),
        "Should not be able to look up deleted database by name"
    );
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_nonexistent_database() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

    // Try to delete non-existent database
    let fake_db_id = DbId::from(999);
    let result = catalog.hard_delete_database(&fake_db_id).await;
    assert!(matches!(result, Err(CatalogError::NotFound(_))));
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_internal_database() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

    // Get internal database ID
    let internal_db_id = catalog.db_name_to_id("_internal").unwrap();

    // Try to hard delete internal database
    let result = catalog.hard_delete_database(&internal_db_id).await;
    assert!(matches!(
        result,
        Err(CatalogError::CannotDeleteInternalDatabase)
    ));

    // Verify internal database still exists
    assert!(
        catalog.db_schema("_internal").is_some(),
        "Internal database should still exist after failed deletion attempt"
    );
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_database_overrides_table_deletions() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

    // Create database and tables
    catalog.create_database("test").await.unwrap();
    catalog
        .create_table(
            "test",
            "table1",
            &["tag"],
            &[("field", FieldDataType::String)],
        )
        .await
        .unwrap();
    catalog
        .create_table(
            "test",
            "table2",
            &["tag"],
            &[("field", FieldDataType::Float)],
        )
        .await
        .unwrap();

    // Get IDs
    let db_id = catalog.db_name_to_id("test").unwrap();
    let db_schema = catalog.db_schema("test").unwrap();
    let table1_id = db_schema.table_definition("table1").unwrap().table_id;
    let _table2_id = db_schema.table_definition("table2").unwrap().table_id;

    // First hard delete a table
    catalog.hard_delete_table(&db_id, &table1_id).await.unwrap();

    // Verify table1 is deleted but database still exists
    assert!(
        catalog
            .db_schema("test")
            .unwrap()
            .table_definition("table1")
            .is_none(),
        "Table1 should be deleted"
    );
    assert!(
        catalog.db_schema("test").is_some(),
        "Database should still exist after table deletion"
    );

    // Now hard delete the database
    catalog.hard_delete_database(&db_id).await.unwrap();

    // Verify the entire database is now gone
    assert!(
        catalog.db_schema("test").is_none(),
        "Database should be removed after hard deletion"
    );
    assert!(
        catalog.db_name_to_id("test").is_none(),
        "Database should not be found by name after hard deletion"
    );
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_database_with_snapshot() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

    // Create databases
    catalog.create_database("test1").await.unwrap();
    catalog.create_database("test2").await.unwrap();

    // Create tables
    catalog
        .create_table(
            "test1",
            "table1",
            &["tag"],
            &[("field", FieldDataType::Float)],
        )
        .await
        .unwrap();
    catalog
        .create_table(
            "test2",
            "table1",
            &["tag"],
            &[("field", FieldDataType::Integer)],
        )
        .await
        .unwrap();

    // Get database IDs
    let db1_id = catalog.db_name_to_id("test1").unwrap();
    let _db2_id = catalog.db_name_to_id("test2").unwrap();

    // Hard delete one database
    catalog.hard_delete_database(&db1_id).await.unwrap();

    // Verify test1 is deleted but test2 still exists before snapshot
    assert!(
        catalog.db_schema("test1").is_none(),
        "test1 database should be deleted"
    );
    assert!(
        catalog.db_schema("test2").is_some(),
        "test2 database should still exist"
    );

    // Take a snapshot
    let snapshot = catalog.snapshot();

    // Serialize and deserialize the snapshot
    let serialized = serialize_catalog_file(&snapshot).unwrap();
    let deserialized = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap();

    // Create a new catalog from the snapshot
    let new_catalog = Catalog::new_in_memory("test-host-2").await.unwrap();
    new_catalog.update_from_snapshot(deserialized);

    // Verify the new catalog has the same state
    assert!(
        new_catalog.db_schema("test1").is_none(),
        "test1 database should remain deleted in new catalog"
    );
    assert!(
        new_catalog.db_schema("test2").is_some(),
        "test2 database should still exist in new catalog"
    );

    // Verify test2's table still exists
    assert!(
        new_catalog
            .db_schema("test2")
            .unwrap()
            .table_definition("table1")
            .is_some(),
        "test2's table should still exist in new catalog"
    );
}

#[test_log::test(tokio::test)]
async fn test_hard_delete_database_after_soft_delete() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

    // Create database
    catalog.create_database("test").await.unwrap();
    catalog
        .create_table(
            "test",
            "table1",
            &["tag"],
            &[("field", FieldDataType::String)],
        )
        .await
        .unwrap();

    // Get database ID before soft delete
    let db_id = catalog.db_name_to_id("test").unwrap();

    // Soft delete the database
    catalog
        .soft_delete_database("test", HardDeletionTime::Never)
        .await
        .unwrap();

    // Find the soft-deleted database by iterating databases
    let db_schema = catalog
        .inner
        .read()
        .databases
        .get_by_id(&db_id)
        .expect("database should exist");
    assert!(db_schema.deleted);

    // Hard delete the database
    catalog.hard_delete_database(&db_id).await.unwrap();

    // Verify the database is completely removed
    assert!(
        catalog.inner.read().databases.get_by_id(&db_id).is_none(),
        "Database should be completely removed after hard deletion"
    );

    // Verify we can't find it by name either
    assert!(
        catalog.db_name_to_id("test").is_none(),
        "Should not be able to find hard deleted database by name"
    );
    assert!(
        catalog.db_name_to_id("test-19700101T000000").is_none(),
        "Should not be able to find soft-deleted name after hard deletion"
    );
}

#[test_log::test(tokio::test)]
async fn deleted_dbs_dont_count() {
    const NUM_DBS_LIMIT: usize = 3;

    let catalog = Catalog::new_in_memory_with_limits(
        "test",
        CatalogLimits {
            num_dbs: NUM_DBS_LIMIT,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    for i in 0..NUM_DBS_LIMIT {
        let db_name = format!("test-db-{i}");
        catalog.create_database(&db_name).await.unwrap();
    }

    // check the count of databases:
    assert_eq!(NUM_DBS_LIMIT, catalog.inner.read().database_count());

    // now create another database, this should NOT be allowed:
    let db_name = "a-database-too-far";
    catalog
        .create_database(db_name)
        .await
        .expect_err("should not be able to create more than permitted number of databases");

    // now delete a database:
    let db_name = format!("test-db-{}", NUM_DBS_LIMIT - 1);
    catalog
        .soft_delete_database(&db_name, HardDeletionTime::Never)
        .await
        .unwrap();

    // check again, count should have gone down:
    assert_eq!(NUM_DBS_LIMIT - 1, catalog.inner.read().database_count());

    // now create another database (using same name as the deleted one), this should be allowed:
    catalog
        .create_database(&db_name)
        .await
        .expect("can create a database again");

    // check new count:
    assert_eq!(NUM_DBS_LIMIT, catalog.inner.read().database_count());
}

#[test_log::test(tokio::test)]
async fn deleted_tables_dont_count() {
    const NUM_TABLES_LIMIT: usize = 3;

    let catalog = Catalog::new_in_memory_with_limits(
        "test",
        CatalogLimits {
            num_tables: NUM_TABLES_LIMIT,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let mut txn = catalog.begin("test-db").unwrap();

    // create as many tables as are allowed:
    for i in 0..NUM_TABLES_LIMIT {
        let table_name = format!("test-table-{i}");
        txn.table_or_create(&table_name).unwrap();
        txn.column_or_create(
            &table_name,
            "field",
            InfluxColumnType::Field(InfluxFieldType::String),
        )
        .unwrap();
        txn.column_or_create(&table_name, "time", InfluxColumnType::Timestamp)
            .unwrap();
    }
    catalog.commit(txn).await.unwrap();

    assert_eq!(NUM_TABLES_LIMIT, catalog.inner.read().table_count());

    // should not be able to create another table:
    let table_name = "a-table-too-far";
    catalog
        .create_table(
            "test-db",
            table_name,
            &["tag"],
            &[("field", FieldDataType::String)],
        )
        .await
        .expect_err("should not be able to exceed table limit");

    catalog
        .soft_delete_table(
            "test-db",
            format!("test-table-{}", NUM_TABLES_LIMIT - 1).as_str(),
            HardDeletionTime::Never,
        )
        .await
        .unwrap();

    assert_eq!(NUM_TABLES_LIMIT - 1, catalog.inner.read().table_count());

    // now create it again, this should be allowed:
    catalog
        .create_table(
            "test-db",
            table_name,
            &["tag"],
            &[("field", FieldDataType::String)],
        )
        .await
        .expect("should be created");

    assert_eq!(NUM_TABLES_LIMIT, catalog.inner.read().table_count());
}

#[test_log::test(tokio::test)]
async fn retention_period_cutoff_map() {
    use iox_time::MockProvider;
    let now = Time::from_timestamp(60 * 60 * 24, 0).unwrap();
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog =
        Catalog::new_in_memory_with_args("test", time_provider as _, CatalogArgs::default())
            .await
            .unwrap();

    let testdb1 = "test-db";
    let mut txn = catalog.begin(testdb1).unwrap();

    for i in 0..4 {
        let table_name = format!("test-table-{i}");
        txn.table_or_create(&table_name).unwrap();
        txn.column_or_create(
            &table_name,
            "field",
            InfluxColumnType::Field(InfluxFieldType::String),
        )
        .unwrap();
        txn.column_or_create(&table_name, "time", InfluxColumnType::Timestamp)
            .unwrap();
    }
    catalog.commit(txn).await.unwrap();

    let testdb2 = "test-db-2";
    let mut txn = catalog.begin(testdb2).unwrap();

    for i in 0..4 {
        let table_name = format!("test-table-{i}");
        txn.table_or_create(&table_name).unwrap();
        txn.column_or_create(
            &table_name,
            "field",
            InfluxColumnType::Field(InfluxFieldType::String),
        )
        .unwrap();
        txn.column_or_create(&table_name, "time", InfluxColumnType::Timestamp)
            .unwrap();
    }
    catalog.commit(txn).await.unwrap();

    let database_retention = Duration::from_secs(15);
    let database_cutoff = now - database_retention;

    // set per-table and database-level retention periods on table 2
    catalog
        .set_retention_period_for_database(testdb2, database_retention)
        .await
        .expect("must be able to set retention for database");

    let map = catalog.get_retention_period_cutoff_map();
    assert_eq!(map.len(), 4, "expect 4 entries in resulting map");

    // validate tables where there is either a table or a database retention set
    for (db_name, table_name, expected_cutoff) in [
        (testdb2, "test-table-0", database_cutoff.timestamp_nanos()),
        (testdb2, "test-table-1", database_cutoff.timestamp_nanos()),
        (testdb2, "test-table-2", database_cutoff.timestamp_nanos()),
        (testdb2, "test-table-3", database_cutoff.timestamp_nanos()),
    ] {
        let db_schema = catalog
            .db_schema(db_name)
            .expect("must be able to get expected database schema");
        let table_def = db_schema
            .table_definition(table_name)
            .expect("must be able to get expected table definition");
        let cutoff = map
            .get(&(db_schema.id(), table_def.id()))
            .expect("expected retention period must exist");
        assert_eq!(
            *cutoff, expected_cutoff,
            "expected cutoff must match actual"
        );
    }

    // validate tables with no retention set
    for (db_name, table_name) in [
        (testdb1, "test-table-0"),
        (testdb1, "test-table-1"),
        (testdb1, "test-table-2"),
        (testdb1, "test-table-3"),
    ] {
        let db_schema = catalog
            .db_schema(db_name)
            .expect("must be able to get expected database schema");
        let table_def = db_schema
            .table_definition(table_name)
            .expect("must be able to get expected table definition");
        let v = map.get(&(db_schema.id(), table_def.id()));
        assert!(
            v.is_none(),
            "no retention period cutoff expected for {db_name}/{table_name}"
        );
    }
}

#[test_log::test(tokio::test)]
async fn retention_period_set_and_clear_after_soft_delete() {
    // Setting or clearing retention periods on soft-deleted tables or databases should fail.

    let catalog = Catalog::new_in_memory("test").await.unwrap();
    let db_name = "table_db";
    let deleted_table = "deleted_table";
    let new_table = "new_table";

    // Create database and table
    catalog.create_database(db_name).await.unwrap();
    let db_id = catalog.db_name_to_id(db_name).unwrap();
    let deleted_table_id = {
        catalog
            .create_table(
                db_name,
                deleted_table,
                &["tag"],
                &[("field", FieldDataType::String)],
            )
            .await
            .unwrap();
        let db_schema = catalog.db_schema(db_name).unwrap();
        db_schema.table_name_to_id(deleted_table).unwrap()
    };

    // Delete the table
    catalog
        .soft_delete_table(db_name, deleted_table, HardDeletionTime::Default)
        .await
        .unwrap();

    // Get the deleted table name as the original table is renamed on delete
    let deleted_table_name = catalog
        .db_schema_by_id(&db_id)
        .and_then(|db_schema| db_schema.table_definition_by_id(&deleted_table_id))
        .map(|def| Arc::clone(&def.table_name))
        .expect("soft-deleted table should be addressable by id");
    assert_ne!(deleted_table_name.as_ref(), deleted_table);

    // Create a new table, delete the database, and ensure
    // database-level retention operations fail.
    catalog
        .create_table(
            db_name,
            new_table,
            &["tag"],
            &[("field", FieldDataType::String)],
        )
        .await
        .unwrap();

    catalog
        .soft_delete_database(db_name, HardDeletionTime::Never)
        .await
        .unwrap();

    // Get the deleted database name as the original database is renamed on delete
    let deleted_db_name = catalog
        .db_id_to_name(&db_id)
        .expect("soft-deleted database should be addressable by id");
    assert_ne!(deleted_db_name.as_ref(), db_name);

    // Database-level retention operations should fail on the deleted database.
    {
        let clear_db_result = catalog
            .clear_retention_period_for_database(&deleted_db_name)
            .await;
        assert!(
            matches!(clear_db_result, Err(CatalogError::AlreadyDeleted(_))),
            "clearing retention on a deleted database should fail"
        );
        let set_db_result = catalog
            .set_retention_period_for_database(&deleted_db_name, Duration::from_secs(600))
            .await;
        assert!(
            matches!(set_db_result, Err(CatalogError::AlreadyDeleted(_))),
            "setting retention on a deleted database should fail"
        );
    }
}

#[test_log::test(tokio::test)]
async fn test_catalog_file_ordering() {
    let local_disk =
        Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

    let init = async || {
        Catalog::new(
            "test",
            Arc::clone(&local_disk) as _,
            Arc::clone(&time_provider) as _,
            Default::default(),
        )
        .await
        .unwrap()
    };

    let catalog = init().await;

    // create a database, then a table, then add fields to that table
    // on reload, the add fields would fail if it was applied before the creation of the
    // table...
    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test_tbl",
            &["t1"],
            &[("f1", FieldDataType::String)],
        )
        .await
        .unwrap();
    let mut txn = catalog.begin("test_db").unwrap();
    txn.column_or_create(
        "test_tbl",
        "f2",
        InfluxColumnType::Field(InfluxFieldType::Integer),
    )
    .unwrap();
    catalog.commit(txn).await.unwrap();

    drop(catalog);

    let catalog = init().await;

    insta::assert_json_snapshot!(catalog.snapshot(), {
        ".catalog_uuid" => "[uuid]"
    });
}

// NOTE(tjh): this test was reported as flaky but has since been re-enabled:
// https://github.com/influxdata/influxdb_pro/issues/1827
#[test_log::test(tokio::test(start_paused = true))]
async fn test_load_from_catalog_checkpoint() {
    let obj_store = Arc::new(InMemory::new());
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

    let init = async || {
        // create a catalog that checkpoints every 10 sequences
        Catalog::new_with_checkpoint_interval(
            "test",
            Arc::clone(&obj_store) as _,
            Arc::clone(&time_provider) as _,
            Default::default(),
            10,
        )
        .await
        .unwrap()
    };

    let catalog = init().await;

    // make changes to create catalog operations that get persisted to the log:
    catalog.create_database("test_db").await.unwrap();
    for i in 0..10 {
        catalog
            .create_table(
                "test_db",
                format!("table_{i}").as_str(),
                &["t1"],
                &[("f1", FieldDataType::String)],
            )
            .await
            .unwrap();
    }
    tokio::time::advance(Duration::from_secs(2)).await;

    let prefix = catalog.object_store_prefix();
    drop(catalog);

    // delete up to the 10th catalog file so that when we re-init, we know it is loading
    // from the checkpoint:
    for i in 1..=10 {
        obj_store
            .delete(CatalogFilePath::log(prefix.as_ref(), CatalogSequenceNumber::new(i)).as_ref())
            .await
            .unwrap();
    }

    // catalog should load successfully:
    let catalog = init().await;

    // we created 10 tables so the catalog should have 10:
    assert_eq!(10, catalog.db_schema("test_db").unwrap().tables.len());
}

#[test_log::test(tokio::test)]
async fn test_load_many_files_with_default_checkpoint_interval() {
    let obj_store =
        Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());
    let obj_store = Arc::new(RequestCountedObjectStore::new(obj_store as _));
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

    let init = async || {
        // create a catalog that checkpoints every 10 sequences
        Catalog::new(
            "test",
            Arc::clone(&obj_store) as _,
            Arc::clone(&time_provider) as _,
            Default::default(),
        )
        .await
        .unwrap()
    };

    let catalog = init().await;
    catalog.create_database("foo").await.unwrap();
    for i in 0..100 {
        let table_name = format!("table_{i}");
        catalog
            .create_table(
                "foo",
                &table_name,
                &["t1"],
                &[("f1", FieldDataType::String)],
            )
            .await
            .unwrap();
        let mut txn = catalog.begin("foo").unwrap();
        txn.column_or_create(
            &table_name,
            "f2",
            InfluxColumnType::Field(InfluxFieldType::String),
        )
        .unwrap();
        catalog.commit(txn).await.unwrap();
    }

    let checkpoint_read_count = obj_store.total_read_request_count(
        CatalogFilePath::checkpoint(catalog.object_store_prefix().as_ref()).as_ref(),
    );
    // Reads on initialization:
    // - Migration check
    // - Regular load check
    assert_eq!(2, checkpoint_read_count);

    let first_log_read_count = obj_store.total_read_request_count(
        CatalogFilePath::log(
            catalog.object_store_prefix().as_ref(),
            CatalogSequenceNumber::new(1),
        )
        .as_ref(),
    );
    // this file should never have been read:
    assert_eq!(0, first_log_read_count);

    let last_log_read_count = obj_store.total_read_request_count(
        CatalogFilePath::log(
            catalog.object_store_prefix().as_ref(),
            catalog.sequence_number(),
        )
        .as_ref(),
    );
    // this file should never have been read:
    assert_eq!(0, last_log_read_count);

    // drop the catalog and re-initialize:
    drop(catalog);
    let catalog = init().await;

    let checkpoint_read_count = obj_store.total_read_request_count(
        CatalogFilePath::checkpoint(catalog.object_store_prefix().as_ref()).as_ref(),
    );
    // Reads:
    // - Migration check (does v2 exist?)
    // - Regular load
    assert_eq!(4, checkpoint_read_count);

    let first_log_read_count = obj_store.total_read_request_count(
        CatalogFilePath::log(
            catalog.object_store_prefix().as_ref(),
            CatalogSequenceNumber::new(1),
        )
        .as_ref(),
    );
    // this file should still not have been read, since it would have been covered by a
    // recent checkpoint:
    assert_eq!(0, first_log_read_count);

    let last_log_read_count = obj_store.total_read_request_count(
        CatalogFilePath::log(
            catalog.object_store_prefix().as_ref(),
            catalog.sequence_number(),
        )
        .as_ref(),
    );
    // this file should have been read on re-init, as it would not be covered by a
    // checkpoint:
    assert_eq!(1, last_log_read_count);
}

#[test_log::test(tokio::test)]
async fn apply_catalog_batch_fails_for_add_fields_past_tag_limit() {
    let catalog = Catalog::new_in_memory("host").await.unwrap();
    catalog.create_database("foo").await.unwrap();
    let tags = (0..NUM_TAG_COLUMNS_LIMIT)
        .map(|i| format!("tag_{i}"))
        .collect::<Vec<_>>();
    catalog
        .create_table("foo", "bar", &tags, &[("f1", FieldDataType::String)])
        .await
        .unwrap();

    let mut txn = catalog.begin("foo").unwrap();
    let err = txn
        .column_or_create("bar", "tag_too_much", InfluxColumnType::Tag)
        .unwrap_err();
    assert_contains!(
        err.to_string(),
        format!(
            "Update to schema would exceed number of tag columns per table limit of {NUM_TAG_COLUMNS_LIMIT} columns"
        )
    );
}

#[test_log::test(tokio::test)]
async fn apply_catalog_batch_fails_to_create_table_with_too_many_tags() {
    let catalog = Catalog::new_in_memory("host").await.unwrap();
    catalog.create_database("foo").await.unwrap();
    let tags = (0..NUM_TAG_COLUMNS_LIMIT + 1)
        .map(|i| format!("tag_{i}"))
        .collect::<Vec<_>>();
    let err = catalog
        .create_table("foo", "bar", &tags, &[("f1", FieldDataType::String)])
        .await;
    assert_contains!(
        err.unwrap_err().to_string(),
        format!(
            "Update to schema would exceed number of tag columns per table limit of {NUM_TAG_COLUMNS_LIMIT} columns"
        )
    );
}

#[tokio::test]
async fn test_catalog_gen1_duration_can_only_set_once() {
    // setup:
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let time: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let create_catalog = async || {
        Catalog::new(
            "test-node",
            Arc::clone(&store),
            Arc::clone(&time),
            Default::default(),
        )
        .await
        .unwrap()
    };
    let catalog = create_catalog().await;
    let duration = Duration::from_secs(10);
    // setting the first time succeeds:
    catalog.set_gen1_duration(duration).await.unwrap();
    assert_eq!(catalog.get_generation_duration(1), Some(duration));
    // setting again with the same duration is an AlreadyExists error:
    let err = catalog.set_gen1_duration(duration).await.unwrap_err();
    assert!(matches!(err, CatalogError::AlreadyExists));
    // setting again with a different duraiton is a different error case:
    let other_duration = Duration::from_secs(20);
    let err = catalog.set_gen1_duration(other_duration).await.unwrap_err();
    assert!(matches!(
        err,
        CatalogError::CannotChangeGenerationDuration {
            level: 1,
            existing,
            ..
        } if existing == duration.into()
    ));
    // drop and recreate the catalog:
    drop(catalog);
    let catalog = create_catalog().await;
    // the gen1 duration should still be set:
    assert_eq!(catalog.get_generation_duration(1), Some(duration));
}

#[tokio::test]
async fn test_catalog_with_empty_gen_durations_can_be_set() {
    // setup:
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let time: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let create_catalog = async || {
        Catalog::new(
            "test-node",
            Arc::clone(&store),
            Arc::clone(&time),
            Default::default(),
        )
        .await
        .unwrap()
    };
    // only initialize the catalog so it is persisted to object store with an empty generation
    // configuration
    let catalog = create_catalog().await;
    let expected_catalog_uuid = catalog.catalog_uuid();

    // drop the catalog and re-initialize from object store:
    drop(catalog);
    let catalog = create_catalog().await;
    let actual_catalog_uuid = catalog.catalog_uuid();
    assert_eq!(expected_catalog_uuid, actual_catalog_uuid);
    assert!(catalog.get_generation_duration(1).is_none());

    // set the gen1 duration, which should work:
    let duration = Duration::from_secs(10);
    catalog.set_gen1_duration(duration).await.unwrap();
    assert_eq!(catalog.get_generation_duration(1), Some(duration));
}

#[test]
fn test_deleted_objects_initialization() {
    let catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
    // Test that catalog initializes successfully
    assert_eq!(catalog.catalog_id.as_ref(), "test-catalog");
}

#[test]
fn test_apply_delete_batch_delete_database() {
    let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
    let db_id = DbId::from(1);

    // Create a database first
    let db_schema = DatabaseSchema::new(db_id, "test_db".into());
    catalog.databases.insert(db_id, db_schema).unwrap();

    let delete_batch = DeleteBatch {
        time_ns: 1000,
        ops: vec![DeleteOp::DeleteDatabase(db_id)],
    };

    let result = catalog.apply_delete_batch(&delete_batch).unwrap();
    assert!(result);
}

#[test]
fn test_apply_delete_batch_delete_table() {
    let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
    let db_id = DbId::from(1);
    let table_id = TableId::from(1);

    // Create a database and table first
    let mut db_schema = DatabaseSchema::new(db_id, "test_db".into());
    let table_def =
        TableDefinition::new_empty(table_id, "test_table".into(), FieldFamilyMode::Aware);
    db_schema.tables.insert(table_id, table_def).unwrap();
    catalog.databases.insert(db_id, db_schema).unwrap();

    let delete_batch = DeleteBatch {
        time_ns: 1000,
        ops: vec![DeleteOp::DeleteTable(db_id, table_id)],
    };

    let result = catalog.apply_delete_batch(&delete_batch).unwrap();
    assert!(result);
}

#[test]
fn test_apply_delete_batch_multiple_tables() {
    let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
    let db_id = DbId::from(1);
    let table_id_1 = TableId::from(1);
    let table_id_2 = TableId::from(2);
    let table_id_3 = TableId::from(3);

    // Create a database and tables first
    let mut db_schema = DatabaseSchema::new(db_id, "test_db".into());
    db_schema
        .tables
        .insert(
            table_id_1,
            TableDefinition::new_empty(table_id_1, "table1".into(), FieldFamilyMode::Aware),
        )
        .unwrap();
    db_schema
        .tables
        .insert(
            table_id_2,
            TableDefinition::new_empty(table_id_2, "table2".into(), FieldFamilyMode::Aware),
        )
        .unwrap();
    db_schema
        .tables
        .insert(
            table_id_3,
            TableDefinition::new_empty(table_id_3, "table3".into(), FieldFamilyMode::Aware),
        )
        .unwrap();
    catalog.databases.insert(db_id, db_schema).unwrap();

    let delete_batch = DeleteBatch {
        time_ns: 1000,
        ops: vec![
            DeleteOp::DeleteTable(db_id, table_id_1),
            DeleteOp::DeleteTable(db_id, table_id_2),
            DeleteOp::DeleteTable(db_id, table_id_3),
        ],
    };

    let result = catalog.apply_delete_batch(&delete_batch).unwrap();
    assert!(result);
}

#[test]
fn test_apply_delete_batch_mixed_operations() {
    let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
    let db_id_1 = DbId::from(1);
    let db_id_2 = DbId::from(2);
    let table_id_1 = TableId::from(1);
    let table_id_2 = TableId::from(2);

    // Create databases and tables first
    let mut db_schema_1 = DatabaseSchema::new(db_id_1, "test_db1".into());
    db_schema_1
        .tables
        .insert(
            table_id_1,
            TableDefinition::new_empty(table_id_1, "table1".into(), FieldFamilyMode::Aware),
        )
        .unwrap();
    db_schema_1
        .tables
        .insert(
            table_id_2,
            TableDefinition::new_empty(table_id_2, "table2".into(), FieldFamilyMode::Aware),
        )
        .unwrap();
    catalog.databases.insert(db_id_1, db_schema_1).unwrap();

    let db_schema_2 = DatabaseSchema::new(db_id_2, "test_db2".into());
    catalog.databases.insert(db_id_2, db_schema_2).unwrap();

    let delete_batch = DeleteBatch {
        time_ns: 1000,
        ops: vec![
            DeleteOp::DeleteTable(db_id_1, table_id_1),
            DeleteOp::DeleteTable(db_id_1, table_id_2),
            DeleteOp::DeleteDatabase(db_id_2),
        ],
    };

    let result = catalog.apply_delete_batch(&delete_batch).unwrap();
    assert!(result);
}

#[test]
fn test_apply_delete_batch_database_overrides_tables() {
    let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
    let db_id = DbId::from(1);
    let table_id = TableId::from(1);

    // Create a database and table first
    let mut db_schema = DatabaseSchema::new(db_id, "test_db".into());
    db_schema
        .tables
        .insert(
            table_id,
            TableDefinition::new_empty(table_id, "test_table".into(), FieldFamilyMode::Aware),
        )
        .unwrap();
    catalog.databases.insert(db_id, db_schema).unwrap();

    // First delete a table
    let delete_batch_1 = DeleteBatch {
        time_ns: 1000,
        ops: vec![DeleteOp::DeleteTable(db_id, table_id)],
    };
    catalog.apply_delete_batch(&delete_batch_1).unwrap();

    // Then delete the database
    let delete_batch_2 = DeleteBatch {
        time_ns: 2000,
        ops: vec![DeleteOp::DeleteDatabase(db_id)],
    };
    catalog.apply_delete_batch(&delete_batch_2).unwrap();
}

#[test_log::test(tokio::test)]
async fn test_catalog_batch_delete_serialization() {
    let db_id = DbId::from(1);
    let table_id = TableId::from(1);

    let delete_batch = CatalogBatch::delete(
        1000,
        vec![
            DeleteOp::DeleteDatabase(db_id),
            DeleteOp::DeleteTable(DbId::from(2), table_id),
        ],
    );

    // Test basic properties
    assert_eq!(delete_batch.n_ops(), 2);
    assert!(delete_batch.as_delete().is_some());

    // Test serialization roundtrip
    let serialized = serde_json::to_string(&delete_batch).unwrap();
    let deserialized: CatalogBatch = serde_json::from_str(&serialized).unwrap();

    if let CatalogBatch::Delete(batch) = deserialized {
        assert_eq!(batch.time_ns, 1000);
        assert_eq!(batch.ops.len(), 2);
        match &batch.ops[0] {
            DeleteOp::DeleteDatabase(id) => assert_eq!(*id, db_id),
            _ => panic!("Expected DeleteDatabase operation"),
        }
        match &batch.ops[1] {
            DeleteOp::DeleteTable(db, tbl) => {
                assert_eq!(*db, DbId::from(2));
                assert_eq!(*tbl, table_id);
            }
            _ => panic!("Expected DeleteTable operation"),
        }
    } else {
        panic!("Expected Delete variant");
    }
}

#[test_log::test(tokio::test)]
async fn test_catalog_with_deleted_objects_snapshot() {
    let catalog = Catalog::new_in_memory("test-host").await.unwrap();

    // Create a database and table
    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test_table",
            &["tag1"],
            &[("field1", FieldDataType::Float)],
        )
        .await
        .unwrap();

    // Get the IDs
    let db_id = catalog.db_name_to_id("test_db").unwrap();
    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_def = db_schema.table_definition("test_table").unwrap();
    let table_id = table_def.table_id;

    // Apply delete operations directly to inner catalog for testing
    let delete_batch = DeleteBatch {
        time_ns: 1000,
        ops: vec![DeleteOp::DeleteTable(db_id, table_id)],
    };

    catalog
        .inner
        .write()
        .apply_delete_batch(&delete_batch)
        .unwrap();

    // Verify table is deleted from database schema
    let db_schema_after = catalog.db_schema("test_db").unwrap();
    assert!(
        db_schema_after.table_definition("test_table").is_none(),
        "Table should be deleted from schema"
    );

    // Create a snapshot
    let snapshot = catalog.snapshot();

    // Test serialization/deserialization roundtrip
    let serialized = serialize_catalog_file(&snapshot).unwrap();
    let deserialized = verify_and_deserialize_catalog_checkpoint_file(serialized).unwrap();

    // Create a new catalog from the snapshot
    let new_catalog = Catalog::new_in_memory("test-host-2").await.unwrap();
    new_catalog.update_from_snapshot(deserialized);

    // Verify the new catalog has the same state - table is deleted
    let new_db_schema = new_catalog.db_schema("test_db").unwrap();
    assert!(
        new_db_schema.table_definition("test_table").is_none(),
        "Table should remain deleted in new catalog"
    );
    assert!(
        new_catalog.db_schema("test_db").is_some(),
        "Database should still exist in new catalog"
    );
}

#[test]
fn test_database_deletion_removes_from_deleted_tables() {
    let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
    let db_id_1 = DbId::from(1);
    let db_id_2 = DbId::from(2);
    let table_id_1 = TableId::from(1);
    let table_id_2 = TableId::from(2);
    let table_id_3 = TableId::from(3);

    // First, delete some tables from both databases
    let delete_batch_1 = DeleteBatch {
        time_ns: 1000,
        ops: vec![
            DeleteOp::DeleteTable(db_id_1, table_id_1),
            DeleteOp::DeleteTable(db_id_1, table_id_2),
            DeleteOp::DeleteTable(db_id_2, table_id_3),
        ],
    };
    catalog.apply_delete_batch(&delete_batch_1).unwrap();

    // Now delete database 1
    let delete_batch_2 = DeleteBatch {
        time_ns: 2000,
        ops: vec![DeleteOp::DeleteDatabase(db_id_1)],
    };
    catalog.apply_delete_batch(&delete_batch_2).unwrap();
}

#[test]
fn test_apply_delete_batch_removes_database_from_schema() {
    let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());

    // Create a database
    let db_id = DbId::from(1);
    let db_name = Arc::from("test_db");
    let db_schema = DatabaseSchema::new(db_id, Arc::clone(&db_name));
    catalog
        .databases
        .insert(db_id, Arc::new(db_schema))
        .unwrap();

    // Verify database exists
    assert!(catalog.databases.get_by_id(&db_id).is_some());

    // Delete the database
    let delete_batch = DeleteBatch {
        time_ns: 1000,
        ops: vec![DeleteOp::DeleteDatabase(db_id)],
    };

    let result = catalog.apply_delete_batch(&delete_batch).unwrap();
    assert!(result);

    // Verify database is removed from schema
    assert!(catalog.databases.get_by_id(&db_id).is_none());
}

#[test]
fn test_apply_delete_batch_removes_table_from_schema() {
    let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());

    // Create a database with a table
    let db_id = DbId::from(1);
    let db_name = Arc::from("test_db");
    let mut db_schema = DatabaseSchema::new(db_id, Arc::clone(&db_name));

    let table_id = TableId::from(1);
    let table_name = Arc::from("test_table");
    let table_def =
        TableDefinition::new_empty(table_id, Arc::clone(&table_name), FieldFamilyMode::Aware);
    db_schema
        .tables
        .insert(table_id, Arc::new(table_def))
        .unwrap();

    catalog
        .databases
        .insert(db_id, Arc::new(db_schema))
        .unwrap();

    // Verify table exists
    let db = catalog.databases.get_by_id(&db_id).unwrap();
    assert!(db.tables.get_by_id(&table_id).is_some());

    // Delete the table
    let delete_batch = DeleteBatch {
        time_ns: 1000,
        ops: vec![DeleteOp::DeleteTable(db_id, table_id)],
    };

    let result = catalog.apply_delete_batch(&delete_batch).unwrap();
    assert!(result);

    // Verify table is removed from schema
    let db = catalog.databases.get_by_id(&db_id).unwrap();
    assert!(db.tables.get_by_id(&table_id).is_none());
}

/// Tests that deleting a table from a database schema that has multiple Arc references
/// is correctly handled.
#[test]
fn test_apply_delete_batch_database_delete_table_correctness_with_multiple_schema_references() {
    let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());

    let db_id = DbId::from(1);

    // Database 1 with 2 tables
    let mut db_schema_1 = DatabaseSchema::new(db_id, Arc::from("db1"));
    let table_id_1 = TableId::from(1);
    let table_id_2 = TableId::from(2);
    db_schema_1
        .tables
        .insert(
            table_id_1,
            Arc::new(TableDefinition::new_empty(
                table_id_1,
                Arc::from("table1"),
                FieldFamilyMode::Aware,
            )),
        )
        .unwrap();
    db_schema_1
        .tables
        .insert(
            table_id_2,
            Arc::new(TableDefinition::new_empty(
                table_id_2,
                Arc::from("table2"),
                FieldFamilyMode::Aware,
            )),
        )
        .unwrap();
    catalog
        .databases
        .insert(db_id, Arc::new(db_schema_1))
        .unwrap();

    // Create an additional reference to the database schema
    let _db_schema = catalog.databases.get_by_id(&db_id).unwrap();

    // Delete table from db1 and entire db2
    let delete_batch = DeleteBatch {
        time_ns: 1000,
        ops: vec![DeleteOp::DeleteTable(db_id, table_id_1)],
    };

    let result = catalog.apply_delete_batch(&delete_batch).unwrap();
    assert!(result);

    let new_db_schema = catalog.databases.get_by_id(&db_id).unwrap();
    assert!(new_db_schema.tables.get_by_id(&table_id_1).is_none());
    assert!(new_db_schema.tables.get_by_id(&table_id_2).is_some());
}

#[test]
fn test_apply_delete_batch_database_deletion_removes_all_tables() {
    let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());

    // Create a database with multiple tables
    let db_id = DbId::from(1);
    let db_name = Arc::from("test_db");
    let mut db_schema = DatabaseSchema::new(db_id, Arc::clone(&db_name));

    let table_ids = vec![TableId::from(1), TableId::from(2), TableId::from(3)];
    for (i, table_id) in table_ids.iter().enumerate() {
        let table_name = Arc::from(format!("table_{i}"));
        let table_def = TableDefinition::new_empty(*table_id, table_name, FieldFamilyMode::Aware);
        db_schema
            .tables
            .insert(*table_id, Arc::new(table_def))
            .unwrap();
    }

    catalog
        .databases
        .insert(db_id, Arc::new(db_schema))
        .unwrap();

    // Verify all tables exist
    let db = catalog.databases.get_by_id(&db_id).unwrap();
    for table_id in &table_ids {
        assert!(db.tables.get_by_id(table_id).is_some());
    }

    // Delete the database
    let delete_batch = DeleteBatch {
        time_ns: 1000,
        ops: vec![DeleteOp::DeleteDatabase(db_id)],
    };

    let result = catalog.apply_delete_batch(&delete_batch).unwrap();
    assert!(result);

    // Verify database and all its tables are removed from schema
    assert!(catalog.databases.get_by_id(&db_id).is_none());
}

#[test]
fn test_apply_delete_batch_mixed_operations_with_schema_removal() {
    let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());

    // Create two databases with tables
    let db_id_1 = DbId::from(1);
    let db_id_2 = DbId::from(2);

    // Database 1 with 2 tables
    let mut db_schema_1 = DatabaseSchema::new(db_id_1, Arc::from("db1"));
    let table_id_1 = TableId::from(1);
    let table_id_2 = TableId::from(2);
    db_schema_1
        .tables
        .insert(
            table_id_1,
            Arc::new(TableDefinition::new_empty(
                table_id_1,
                Arc::from("table1"),
                FieldFamilyMode::Aware,
            )),
        )
        .unwrap();
    db_schema_1
        .tables
        .insert(
            table_id_2,
            Arc::new(TableDefinition::new_empty(
                table_id_2,
                Arc::from("table2"),
                FieldFamilyMode::Aware,
            )),
        )
        .unwrap();

    // Database 2 with 1 table
    let mut db_schema_2 = DatabaseSchema::new(db_id_2, Arc::from("db2"));
    let table_id_3 = TableId::from(3);
    db_schema_2
        .tables
        .insert(
            table_id_3,
            Arc::new(TableDefinition::new_empty(
                table_id_3,
                Arc::from("table3"),
                FieldFamilyMode::Aware,
            )),
        )
        .unwrap();

    catalog
        .databases
        .insert(db_id_1, Arc::new(db_schema_1))
        .unwrap();
    catalog
        .databases
        .insert(db_id_2, Arc::new(db_schema_2))
        .unwrap();

    // Delete table from db1 and entire db2
    let delete_batch = DeleteBatch {
        time_ns: 1000,
        ops: vec![
            DeleteOp::DeleteTable(db_id_1, table_id_1),
            DeleteOp::DeleteDatabase(db_id_2),
        ],
    };

    let result = catalog.apply_delete_batch(&delete_batch).unwrap();
    assert!(result);

    // Verify db1 still exists with only table2
    let db1 = catalog.databases.get_by_id(&db_id_1).unwrap();
    assert!(db1.tables.get_by_id(&table_id_1).is_none());
    assert!(db1.tables.get_by_id(&table_id_2).is_some());

    // Verify db2 is completely removed
    assert!(catalog.databases.get_by_id(&db_id_2).is_none());
}

#[test]
fn test_apply_delete_batch_table_deletion_after_database_deletion() {
    let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());

    let db_id = DbId::from(1);
    let table_id = TableId::from(1);

    // First delete the database
    let delete_batch_1 = DeleteBatch {
        time_ns: 1000,
        ops: vec![DeleteOp::DeleteDatabase(db_id)],
    };
    catalog.apply_delete_batch(&delete_batch_1).unwrap();

    // Then try to delete a table from the deleted database
    let delete_batch_2 = DeleteBatch {
        time_ns: 2000,
        ops: vec![DeleteOp::DeleteTable(db_id, table_id)],
    };
    let result = catalog.apply_delete_batch(&delete_batch_2).unwrap();

    // Should return false since no changes were made (database already deleted)
    assert!(!result);
}

#[test]
fn test_serialization_format() {
    let mut catalog = InnerCatalog::new("test-catalog".into(), Uuid::new_v4());
    let db_id_1 = DbId::from(2);
    let db_id_2 = DbId::from(5);
    let db_id_3 = DbId::from(3);
    let db_id_4 = DbId::from(9);
    let table_id_1 = TableId::from(1);
    let table_id_2 = TableId::from(6);
    let table_id_3 = TableId::from(7);
    let table_id_4 = TableId::from(2);

    // Create the exact scenario from the user's example
    let delete_batch = DeleteBatch {
        time_ns: 1000,
        ops: vec![
            DeleteOp::DeleteDatabase(db_id_1),          // 2
            DeleteOp::DeleteDatabase(db_id_2),          // 5
            DeleteOp::DeleteTable(db_id_3, table_id_1), // 3: [1, 6, 7]
            DeleteOp::DeleteTable(db_id_3, table_id_2),
            DeleteOp::DeleteTable(db_id_3, table_id_3),
            DeleteOp::DeleteTable(db_id_4, table_id_4), // 9: [2]
        ],
    };
    catalog.apply_delete_batch(&delete_batch).unwrap();

    insta::allow_duplicates! {
        insta::with_settings!({
            sort_maps => true,
            description => "Catalog snapshot with deleted objects"
        }, {
            let snapshot = catalog.snapshot();

            insta::assert_json_snapshot!(snapshot, {
                ".catalog_uuid" => "[uuid]"
            });
        })
    }
}

#[test_log::test(tokio::test)]
async fn test_database_hard_delete_time_never() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
    catalog.create_database("test_db").await.unwrap();

    // Get database ID before soft delete
    let db_id = catalog.db_name_to_id("test_db").unwrap();

    // Soft delete with Never
    catalog
        .soft_delete_database("test_db", HardDeletionTime::Never)
        .await
        .unwrap();

    // Verify hard_delete_time is None
    let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
    assert!(db_schema.deleted);
    assert!(db_schema.hard_delete_time.is_none());
}

#[test_log::test(tokio::test)]
async fn test_database_hard_delete_time_default() {
    use iox_time::MockProvider;
    let now = Time::from_timestamp_nanos(1000000000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();

    // Get database ID before soft delete
    let db_id = catalog.db_name_to_id("test_db").unwrap();

    // Soft delete with Default
    catalog
        .soft_delete_database("test_db", HardDeletionTime::Default)
        .await
        .unwrap();

    // Verify hard_delete_time is set to now + default duration
    let expected_time = now + Catalog::DEFAULT_HARD_DELETE_DURATION;
    let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
    assert!(db_schema.deleted);
    assert_eq!(db_schema.hard_delete_time, Some(expected_time));
}

#[test_log::test(tokio::test)]
async fn test_database_hard_delete_time_specific_timestamp() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
    catalog.create_database("test_db").await.unwrap();

    // Get database ID before soft delete
    let db_id = catalog.db_name_to_id("test_db").unwrap();

    let specific_time = Time::from_timestamp_nanos(5000000000);

    // Soft delete with specific timestamp
    catalog
        .soft_delete_database("test_db", HardDeletionTime::Timestamp(specific_time))
        .await
        .unwrap();

    // Verify hard_delete_time is set to the specific time
    let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
    assert!(db_schema.deleted);
    assert_eq!(db_schema.hard_delete_time, Some(specific_time));
}

#[test_log::test(tokio::test)]
async fn test_database_hard_delete_time_now() {
    use iox_time::MockProvider;
    let now = Time::from_timestamp_nanos(2000000000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();

    // Get database ID before soft delete
    let db_id = catalog.db_name_to_id("test_db").unwrap();

    // Soft delete with Now
    catalog
        .soft_delete_database("test_db", HardDeletionTime::Now)
        .await
        .unwrap();

    // Verify hard_delete_time is set to current time
    let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
    assert!(db_schema.deleted);
    assert_eq!(db_schema.hard_delete_time, Some(now));
}

#[test_log::test(tokio::test)]
async fn test_database_hard_delete_time_serialization() {
    use iox_time::MockProvider;
    let now = Time::from_timestamp_nanos(3000000000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();

    // Get database ID before soft delete
    let db_id = catalog.db_name_to_id("test_db").unwrap();

    // Soft delete with Default hard delete time
    catalog
        .soft_delete_database("test_db", HardDeletionTime::Default)
        .await
        .unwrap();

    // Take a snapshot
    let snapshot = catalog.snapshot();

    // Verify hard_delete_time is in the snapshot
    let expected_time = now + Catalog::DEFAULT_HARD_DELETE_DURATION;
    let db_snapshot = snapshot.databases.repo.get(&db_id).unwrap();
    assert_eq!(
        db_snapshot.hard_delete_time,
        Some(expected_time.timestamp_nanos())
    );

    // Test deserialization
    let new_catalog = Catalog::new_in_memory("test-catalog-2").await.unwrap();
    new_catalog.update_from_snapshot(snapshot);

    let restored_db_schema = new_catalog.db_schema_by_id(&db_id).unwrap();
    assert!(restored_db_schema.deleted);
    assert_eq!(restored_db_schema.hard_delete_time, Some(expected_time));
}

#[test_log::test(tokio::test)]
async fn test_database_deletion_status_existing_not_deleted() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
    catalog.create_database("test_db").await.unwrap();

    let db_id = catalog.db_name_to_id("test_db").unwrap();

    // Database exists and is not deleted - should return None
    assert_eq!(catalog.database_deletion_status(db_id), None);
}

#[test_log::test(tokio::test)]
async fn test_database_deletion_status_soft_deleted() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
    catalog.create_database("test_db").await.unwrap();

    let db_id = catalog.db_name_to_id("test_db").unwrap();

    // Soft delete the database
    catalog
        .soft_delete_database("test_db", HardDeletionTime::Never)
        .await
        .unwrap();

    // Should return Soft status
    assert_eq!(
        catalog.database_deletion_status(db_id),
        Some(DeletionStatus::Soft)
    );
}

#[test_log::test(tokio::test)]
async fn test_database_deletion_status_hard_deleted() {
    use iox_time::MockProvider;
    use std::time::Duration;

    let now = Time::from_timestamp_nanos(1_000_000_000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();
    let db_id = catalog.db_name_to_id("test_db").unwrap();

    // Soft delete the database with immediate hard deletion
    catalog
        .soft_delete_database("test_db", HardDeletionTime::Now)
        .await
        .unwrap();

    // Advance time to simulate hard deletion has occurred
    let future_time = now + Duration::from_secs(3600); // 1 hour later
    time_provider.set(future_time);

    // Should return Hard status with duration
    match catalog.database_deletion_status(db_id) {
        Some(DeletionStatus::Hard(duration)) => {
            // Duration should be approximately 1 hour
            assert!(duration >= Duration::from_secs(3599));
            assert!(duration <= Duration::from_secs(3601));
        }
        other => panic!("Expected Hard deletion status, got {other:?}"),
    }
}

#[test_log::test(tokio::test)]
async fn test_database_deletion_status_scheduled_for_hard_deletion() {
    use iox_time::MockProvider;
    use std::time::Duration;

    let now = Time::from_timestamp_nanos(1_000_000_000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();
    let db_id = catalog.db_name_to_id("test_db").unwrap();

    // Soft delete with future hard deletion time
    let future_deletion_time = now + Duration::from_secs(7200); // 2 hours from now
    catalog
        .soft_delete_database("test_db", HardDeletionTime::Timestamp(future_deletion_time))
        .await
        .unwrap();

    // Should still return Soft status since hard deletion time hasn't arrived
    assert_eq!(
        catalog.database_deletion_status(db_id),
        Some(DeletionStatus::Soft)
    );

    // Advance time past the hard deletion time
    let past_deletion_time = future_deletion_time + Duration::from_secs(600); // 10 minutes after
    time_provider.set(past_deletion_time);

    // Now should return Hard status
    match catalog.database_deletion_status(db_id) {
        Some(DeletionStatus::Hard(duration)) => {
            // Duration should be approximately 10 minutes
            assert!(duration >= Duration::from_secs(599));
            assert!(duration <= Duration::from_secs(601));
        }
        other => panic!("Expected Hard deletion status, got {other:?}"),
    }
}

#[test_log::test(tokio::test)]
async fn test_database_deletion_status_not_found() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();

    // Non-existent database ID
    let non_existent_id = DbId::from(999);

    // Should return NotFound status
    assert_eq!(
        catalog.database_deletion_status(non_existent_id),
        Some(DeletionStatus::NotFound)
    );
}

#[test_log::test(tokio::test)]
async fn test_database_deletion_status_in_deleted_set() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
    catalog.create_database("test_db").await.unwrap();

    let db_id = catalog.db_name_to_id("test_db").unwrap();

    // Manually remove database to simulate hard deletion cleanup
    {
        let mut inner = catalog.inner.write();
        inner.databases.remove(&db_id);
    }

    // Should return NotFound status
    assert_eq!(
        catalog.database_deletion_status(db_id),
        Some(DeletionStatus::NotFound)
    );
}

#[test_log::test(tokio::test)]
async fn test_delete_table_when_database_deleted() {
    // Testing: deleting a table from a database that has already been soft deleted
    // should not succeed.

    let now = Time::from_timestamp_nanos(1_000_000_000);
    let db_name_1 = "test_db_1";
    let db_name_2 = "test_db_2";
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    // Create databases and tables
    catalog.create_database(db_name_1).await.unwrap();
    catalog
        .create_table(
            db_name_1,
            "test_table",
            &["tag1"],
            &[("field1", FieldDataType::String)],
        )
        .await
        .unwrap();

    catalog.create_database(db_name_2).await.unwrap();
    catalog
        .create_table(
            db_name_2,
            "test_table",
            &["tag1"],
            &[("field1", FieldDataType::String)],
        )
        .await
        .unwrap();

    let db_id_1 = catalog.db_name_to_id(db_name_1).unwrap();
    let db_id_2 = catalog.db_name_to_id(db_name_2).unwrap();

    // Soft delete the database, but never hard delete
    {
        catalog
            .soft_delete_database(db_name_1, HardDeletionTime::Never)
            .await
            .unwrap();

        let deleted_db_name = catalog
            .db_schema_by_id(&db_id_1)
            .expect("deleted db schema should exist")
            .name();

        // Soft delete the table from the deleted database -- should fail
        let result = catalog
            .soft_delete_table(&deleted_db_name, "test_table", HardDeletionTime::Default)
            .await;
        assert!(
            matches!(result, Err(CatalogError::AlreadyDeleted(_))),
            "expected table delete to fail for deleted database, got {result:?}"
        );
    }

    // Soft delete the database with a default deletion timestamp
    {
        catalog
            .soft_delete_database(db_name_2, HardDeletionTime::Default)
            .await
            .unwrap();

        let deleted_db_name = catalog
            .db_schema_by_id(&db_id_2)
            .expect("deleted db schema should exist")
            .name();

        // Soft delete the table from the deleted database -- should fail
        let result = catalog
            .soft_delete_table(&deleted_db_name, "test_table", HardDeletionTime::Default)
            .await;
        assert!(
            matches!(result, Err(CatalogError::AlreadyDeleted(_))),
            "expected table delete to fail for deleted database, got {result:?}"
        );
    }
}

#[test_log::test(tokio::test)]
async fn test_table_deletion_status_existing_not_deleted() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test_table",
            &["tag1"],
            &[("field1", FieldDataType::String)],
        )
        .await
        .unwrap();

    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_id = db_schema.table_name_to_id("test_table").unwrap();

    // Table exists and is not deleted - should return None
    assert_eq!(
        db_schema.table_deletion_status(table_id, catalog.time_provider()),
        None
    );
}

#[test_log::test(tokio::test)]
async fn test_table_deletion_status_soft_deleted() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test_table",
            &["tag1"],
            &[("field1", FieldDataType::String)],
        )
        .await
        .unwrap();

    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_id = db_schema.table_name_to_id("test_table").unwrap();

    // Soft delete the table
    catalog
        .soft_delete_table("test_db", "test_table", HardDeletionTime::Never)
        .await
        .unwrap();

    // Get updated schema after deletion
    let updated_db_schema = catalog.db_schema("test_db").unwrap();

    // Should return Soft status
    assert_eq!(
        updated_db_schema.table_deletion_status(table_id, catalog.time_provider()),
        Some(DeletionStatus::Soft)
    );
}

#[test_log::test(tokio::test)]
async fn test_table_deletion_status_hard_deleted() {
    use iox_time::MockProvider;
    use std::time::Duration;

    let now = Time::from_timestamp_nanos(1_000_000_000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test_table",
            &["tag1"],
            &[("field1", FieldDataType::String)],
        )
        .await
        .unwrap();

    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_id = db_schema.table_name_to_id("test_table").unwrap();

    // Soft delete the table with immediate hard deletion
    catalog
        .soft_delete_table("test_db", "test_table", HardDeletionTime::Now)
        .await
        .unwrap();

    // Advance time to simulate hard deletion has occurred
    let future_time = now + Duration::from_secs(3600); // 1 hour later
    time_provider.set(future_time);

    // Get updated schema after deletion
    let updated_db_schema = catalog.db_schema("test_db").unwrap();

    // Should return Hard status with duration
    match updated_db_schema.table_deletion_status(table_id, Arc::clone(&time_provider) as _) {
        Some(DeletionStatus::Hard(duration)) => {
            // Duration should be approximately 1 hour
            assert!(duration >= Duration::from_secs(3599));
            assert!(duration <= Duration::from_secs(3601));
        }
        other => panic!("Expected Hard deletion status, got {other:?}"),
    }
}

#[test_log::test(tokio::test)]
async fn test_table_deletion_status_scheduled_for_hard_deletion() {
    use iox_time::MockProvider;
    use std::time::Duration;

    let now = Time::from_timestamp_nanos(1_000_000_000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test_table",
            &["tag1"],
            &[("field1", FieldDataType::String)],
        )
        .await
        .unwrap();

    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_id = db_schema.table_name_to_id("test_table").unwrap();

    // Soft delete with future hard deletion time
    let future_deletion_time = now + Duration::from_secs(7200); // 2 hours from now
    catalog
        .soft_delete_table(
            "test_db",
            "test_table",
            HardDeletionTime::Timestamp(future_deletion_time),
        )
        .await
        .unwrap();

    // Get updated schema after deletion
    let updated_db_schema = catalog.db_schema("test_db").unwrap();

    // Should still return Soft status since hard deletion time hasn't arrived
    assert_eq!(
        updated_db_schema.table_deletion_status(table_id, Arc::clone(&time_provider) as _),
        Some(DeletionStatus::Soft)
    );

    // Advance time past the hard deletion time
    let past_deletion_time = future_deletion_time + Duration::from_secs(600); // 10 minutes after
    time_provider.set(past_deletion_time);

    // Get updated schema (should be the same instance since it's time-based)
    let final_db_schema = catalog.db_schema("test_db").unwrap();

    // Now should return Hard status
    match final_db_schema.table_deletion_status(table_id, Arc::clone(&time_provider) as _) {
        Some(DeletionStatus::Hard(duration)) => {
            // Duration should be approximately 10 minutes
            assert!(duration >= Duration::from_secs(599));
            assert!(duration <= Duration::from_secs(601));
        }
        other => panic!("Expected Hard deletion status, got {other:?}"),
    }
}

#[test_log::test(tokio::test)]
async fn test_table_deletion_status_not_found() {
    let catalog = Catalog::new_in_memory("test-catalog").await.unwrap();
    catalog.create_database("test_db").await.unwrap();

    let db_schema = catalog.db_schema("test_db").unwrap();
    // Non-existent table ID
    let non_existent_id = TableId::from(999);

    // Should return NotFound status
    assert_eq!(
        db_schema.table_deletion_status(non_existent_id, catalog.time_provider()),
        Some(DeletionStatus::NotFound)
    );
}

#[test_log::test(tokio::test)]
async fn test_table_deletion_status_multiple_tables() {
    use iox_time::MockProvider;
    use std::time::Duration;

    let now = Time::from_timestamp_nanos(1_000_000_000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();

    // Create multiple tables
    catalog
        .create_table(
            "test_db",
            "table1",
            &["tag1"],
            &[("field1", FieldDataType::String)],
        )
        .await
        .unwrap();
    catalog
        .create_table(
            "test_db",
            "table2",
            &["tag1"],
            &[("field1", FieldDataType::String)],
        )
        .await
        .unwrap();
    catalog
        .create_table(
            "test_db",
            "table3",
            &["tag1"],
            &[("field1", FieldDataType::String)],
        )
        .await
        .unwrap();

    let db_schema = catalog.db_schema("test_db").unwrap();
    let table1_id = db_schema.table_name_to_id("table1").unwrap();
    let table2_id = db_schema.table_name_to_id("table2").unwrap();
    let table3_id = db_schema.table_name_to_id("table3").unwrap();

    // Leave table1 as is (not deleted)
    // Soft delete table2
    catalog
        .soft_delete_table("test_db", "table2", HardDeletionTime::Never)
        .await
        .unwrap();
    // Hard delete table3
    catalog
        .soft_delete_table("test_db", "table3", HardDeletionTime::Now)
        .await
        .unwrap();

    // Advance time for table3 hard deletion
    time_provider.set(now + Duration::from_secs(1800)); // 30 minutes later

    // Get updated schema
    let updated_db_schema = catalog.db_schema("test_db").unwrap();

    // Test all three tables
    assert_eq!(
        updated_db_schema.table_deletion_status(table1_id, Arc::clone(&time_provider) as _),
        None
    ); // Not deleted
    assert_eq!(
        updated_db_schema.table_deletion_status(table2_id, Arc::clone(&time_provider) as _),
        Some(DeletionStatus::Soft)
    ); // Soft deleted
    match updated_db_schema.table_deletion_status(table3_id, Arc::clone(&time_provider) as _) {
        Some(DeletionStatus::Hard(duration)) => {
            // Should be around 30 minutes
            assert!(duration >= Duration::from_secs(1799));
            assert!(duration <= Duration::from_secs(1801));
        }
        other => panic!("Expected Hard deletion status for table3, got {other:?}"),
    }
}

// Tests for idempotent default hard deletion behavior

#[test_log::test(tokio::test)]
async fn test_database_soft_delete_default_preserves_existing_hard_delete_time() {
    // Test that soft deleting a database with Default preserves existing hard_delete_time
    use iox_time::MockProvider;
    let now = Time::from_timestamp_nanos(1000000000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();

    // Get database ID before soft delete
    let db_id = catalog.db_name_to_id("test_db").unwrap();

    // First soft delete with a specific timestamp
    let specific_time = Time::from_timestamp_nanos(5000000000);
    catalog
        .soft_delete_database("test_db", HardDeletionTime::Timestamp(specific_time))
        .await
        .unwrap();

    // Verify the database is soft deleted with the specific hard_delete_time
    let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
    assert!(db_schema.deleted);
    assert_eq!(db_schema.hard_delete_time, Some(specific_time));

    // Get the renamed database name using the ID
    let renamed_db_name = catalog
        .db_schema_by_id(&db_id)
        .expect("soft-deleted database should exist")
        .name();

    // Now soft delete again with Default using the renamed name
    // This should return AlreadyDeleted since nothing changes
    let result = catalog
        .soft_delete_database(&renamed_db_name, HardDeletionTime::Default)
        .await;

    // Should get AlreadyDeleted error since hard_delete_time doesn't change
    assert!(
        matches!(result, Err(CatalogError::AlreadyDeleted(_))),
        "Expected AlreadyDeleted error, got {result:?}"
    );

    // Verify hard_delete_time is unchanged
    let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
    assert!(db_schema.deleted);
    assert_eq!(db_schema.hard_delete_time, Some(specific_time));
}

#[test_log::test(tokio::test)]
async fn test_database_soft_delete_default_sets_new_when_none_exists() {
    // Test that soft deleting a database with Default sets new hard_delete_time when none exists
    use iox_time::MockProvider;
    let now = Time::from_timestamp_nanos(1000000000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();

    // Get database ID before soft delete
    let db_id = catalog.db_name_to_id("test_db").unwrap();

    // Soft delete with Default - should set new hard_delete_time
    catalog
        .soft_delete_database("test_db", HardDeletionTime::Default)
        .await
        .unwrap();

    // Verify hard_delete_time is set to now + default duration
    let expected_time = now + Catalog::DEFAULT_HARD_DELETE_DURATION;
    let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
    assert!(db_schema.deleted);
    assert_eq!(db_schema.hard_delete_time, Some(expected_time));
}

#[test_log::test(tokio::test)]
async fn test_database_soft_delete_default_multiple_calls_idempotent() {
    // Test that multiple soft delete calls with Default are idempotent
    use iox_time::MockProvider;
    let now = Time::from_timestamp_nanos(1000000000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();

    // Get database ID before soft delete
    let db_id = catalog.db_name_to_id("test_db").unwrap();

    // First soft delete with a specific timestamp
    let specific_time = Time::from_timestamp_nanos(5000000000);
    catalog
        .soft_delete_database("test_db", HardDeletionTime::Timestamp(specific_time))
        .await
        .unwrap();

    // Verify initial state
    let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
    assert!(db_schema.deleted);
    assert_eq!(db_schema.hard_delete_time, Some(specific_time));

    // Get the renamed database name using the ID
    let renamed_db_name = catalog
        .db_schema_by_id(&db_id)
        .expect("soft-deleted database should exist")
        .name();

    // Call soft delete with Default multiple times - all should be idempotent
    for i in 1..=3 {
        let result = catalog
            .soft_delete_database(&renamed_db_name, HardDeletionTime::Default)
            .await;

        // Should always get AlreadyDeleted since nothing changes
        assert!(
            matches!(result, Err(CatalogError::AlreadyDeleted(_))),
            "Call {i} expected AlreadyDeleted error, got {result:?}"
        );

        // Verify hard_delete_time remains unchanged
        let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
        assert!(db_schema.deleted);
        assert_eq!(
            db_schema.hard_delete_time,
            Some(specific_time),
            "hard_delete_time should remain unchanged after call {}",
            i
        );
    }
}

#[test_log::test(tokio::test)]
async fn test_database_soft_delete_override_existing_with_specific_time() {
    // Test that soft deleting with specific time overrides existing hard_delete_time
    use iox_time::MockProvider;
    let now = Time::from_timestamp_nanos(1000000000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();

    // Get database ID before soft delete
    let db_id = catalog.db_name_to_id("test_db").unwrap();

    // First soft delete with Default
    catalog
        .soft_delete_database("test_db", HardDeletionTime::Default)
        .await
        .unwrap();

    // Verify initial state with default hard_delete_time
    let expected_default_time = now + Catalog::DEFAULT_HARD_DELETE_DURATION;
    let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
    assert!(db_schema.deleted);
    assert_eq!(db_schema.hard_delete_time, Some(expected_default_time));

    // Get the renamed database name using the ID
    let renamed_db_name = catalog
        .db_schema_by_id(&db_id)
        .expect("soft-deleted database should exist")
        .name();

    // Now soft delete again with a specific timestamp - should update the hard_delete_time
    let new_specific_time = Time::from_timestamp_nanos(7000000000);
    catalog
        .soft_delete_database(
            &renamed_db_name,
            HardDeletionTime::Timestamp(new_specific_time),
        )
        .await
        .unwrap();

    // Verify hard_delete_time was updated to the new specific time
    let db_schema = catalog.db_schema_by_id(&db_id).unwrap();
    assert!(db_schema.deleted);
    assert_eq!(db_schema.hard_delete_time, Some(new_specific_time));
}

#[test_log::test(tokio::test)]
async fn test_table_soft_delete_default_preserves_existing_hard_delete_time() {
    // Test that soft deleting a table with Default preserves existing hard_delete_time
    use iox_time::MockProvider;
    let now = Time::from_timestamp_nanos(1000000000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test_table",
            &["tag1", "tag2"],
            &[("field1", FieldDataType::String)],
        )
        .await
        .unwrap();

    // Get the table ID before soft delete
    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_id = db_schema.table_name_to_id("test_table").unwrap();

    // First soft delete with a specific timestamp
    let specific_time = Time::from_timestamp_nanos(5000000000);
    catalog
        .soft_delete_table(
            "test_db",
            "test_table",
            HardDeletionTime::Timestamp(specific_time),
        )
        .await
        .unwrap();

    // Get the renamed table using the table ID
    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_def = db_schema
        .table_definition_by_id(&table_id)
        .expect("soft-deleted table should exist");
    let renamed_table_name = Arc::<str>::clone(&table_def.table_name);

    // Verify the table is soft deleted with the specific hard_delete_time
    assert!(table_def.deleted);
    assert_eq!(table_def.hard_delete_time, Some(specific_time));

    // Now soft delete again with Default using the renamed name
    // This should return AlreadyDeleted since nothing changes
    let result = catalog
        .soft_delete_table("test_db", &renamed_table_name, HardDeletionTime::Default)
        .await;

    // Should get AlreadyDeleted error since hard_delete_time doesn't change
    assert!(
        matches!(result, Err(CatalogError::AlreadyDeleted(_))),
        "Expected AlreadyDeleted error, got {result:?}"
    );

    // Verify hard_delete_time is unchanged
    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_def = db_schema.table_definition(&renamed_table_name).unwrap();
    assert!(table_def.deleted);
    assert_eq!(table_def.hard_delete_time, Some(specific_time));
}

#[test_log::test(tokio::test)]
async fn test_table_soft_delete_default_sets_new_when_none_exists() {
    // Test that soft deleting a table with Default sets new hard_delete_time when none exists
    use iox_time::MockProvider;
    let now = Time::from_timestamp_nanos(1000000000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test_table",
            &["tag1"],
            &[("field1", FieldDataType::Float)],
        )
        .await
        .unwrap();

    // Get the table ID before soft delete
    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_id = db_schema.table_name_to_id("test_table").unwrap();

    // Soft delete with Default - should set new hard_delete_time
    catalog
        .soft_delete_table("test_db", "test_table", HardDeletionTime::Default)
        .await
        .unwrap();

    // Get the table using the table ID
    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_def = db_schema
        .table_definition_by_id(&table_id)
        .expect("soft-deleted table should exist");

    // Verify hard_delete_time is set to now + default duration
    let expected_time = now + Catalog::DEFAULT_HARD_DELETE_DURATION;
    assert!(table_def.deleted);
    assert_eq!(table_def.hard_delete_time, Some(expected_time));
}

#[test_log::test(tokio::test)]
async fn test_table_soft_delete_default_multiple_calls_idempotent() {
    // Test that multiple soft delete calls with Default are idempotent
    use iox_time::MockProvider;
    let now = Time::from_timestamp_nanos(1000000000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test_table",
            &["tag1", "tag2"],
            &[("field1", FieldDataType::Integer)],
        )
        .await
        .unwrap();

    // Get the table ID before soft delete
    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_id = db_schema.table_name_to_id("test_table").unwrap();

    // First soft delete with a specific timestamp
    let specific_time = Time::from_timestamp_nanos(5000000000);
    catalog
        .soft_delete_table(
            "test_db",
            "test_table",
            HardDeletionTime::Timestamp(specific_time),
        )
        .await
        .unwrap();

    // Get the renamed table name using the table ID
    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_def = db_schema
        .table_definition_by_id(&table_id)
        .expect("soft-deleted table should exist");
    let renamed_table_name = Arc::<str>::clone(&table_def.table_name);

    // Call soft delete with Default multiple times - all should be idempotent
    for i in 1..=3 {
        let result = catalog
            .soft_delete_table("test_db", &renamed_table_name, HardDeletionTime::Default)
            .await;

        // Should always get AlreadyDeleted since nothing changes
        assert!(
            matches!(result, Err(CatalogError::AlreadyDeleted(_))),
            "Call {i} expected AlreadyDeleted error, got {result:?}"
        );

        // Verify hard_delete_time remains unchanged
        let db_schema = catalog.db_schema("test_db").unwrap();
        let table_def = db_schema.table_definition_by_id(&table_id).unwrap();
        assert!(table_def.deleted);
        assert_eq!(
            table_def.hard_delete_time,
            Some(specific_time),
            "hard_delete_time should remain unchanged after call {}",
            i
        );
    }
}

#[test_log::test(tokio::test)]
async fn test_table_soft_delete_override_existing_with_specific_time() {
    // Test that soft deleting with specific time overrides existing hard_delete_time
    use iox_time::MockProvider;
    let now = Time::from_timestamp_nanos(1000000000);
    let time_provider = Arc::new(MockProvider::new(now));
    let catalog = Catalog::new_in_memory_with_args(
        "test-catalog",
        Arc::clone(&time_provider) as _,
        CatalogArgs::default(),
    )
    .await
    .unwrap();

    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test_table",
            &["tag1"],
            &[("field1", FieldDataType::UInteger)],
        )
        .await
        .unwrap();

    // Get the table ID before soft delete
    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_id = db_schema.table_name_to_id("test_table").unwrap();

    // First soft delete with Default
    catalog
        .soft_delete_table("test_db", "test_table", HardDeletionTime::Default)
        .await
        .unwrap();

    // Get the renamed table and verify initial state
    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_def = db_schema
        .table_definition_by_id(&table_id)
        .expect("soft-deleted table should exist");
    let renamed_table_name = Arc::<str>::clone(&table_def.table_name);

    // Verify initial state with default hard_delete_time
    let expected_default_time = now + Catalog::DEFAULT_HARD_DELETE_DURATION;
    assert!(table_def.deleted);
    assert_eq!(table_def.hard_delete_time, Some(expected_default_time));

    // Now soft delete again with a specific timestamp - should update the hard_delete_time
    let new_specific_time = Time::from_timestamp_nanos(7000000000);
    catalog
        .soft_delete_table(
            "test_db",
            &renamed_table_name,
            HardDeletionTime::Timestamp(new_specific_time),
        )
        .await
        .unwrap();

    // Verify hard_delete_time was updated to the new specific time
    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_def = db_schema.table_definition_by_id(&table_id).unwrap();
    assert!(table_def.deleted);
    assert_eq!(table_def.hard_delete_time, Some(new_specific_time));
}

#[rstest]
#[case::empty_tag_name(&[""], &[("field1", FieldDataType::String)], "tag key cannot be empty")]
#[case::empty_field_name(&["tag1"], &[("", FieldDataType::String)], "field key cannot be empty")]
#[case::multiple_with_empty_tag(&["tag1", "", "tag3"], &[("field1", FieldDataType::String)], "tag key cannot be empty")]
#[case::multiple_with_empty_field(&["tag1"], &[("field1", FieldDataType::String), ("", FieldDataType::Integer)], "field key cannot be empty")]
#[case::tag_with_newline(&["tag\nname"], &[("field1", FieldDataType::String)], "tag key cannot contain control characters")]
#[case::field_with_newline(&["tag1"], &[("field\nname", FieldDataType::String)], "field key cannot contain control characters")]
#[case::tag_with_tab(&["tag\tname"], &[("field1", FieldDataType::String)], "tag key cannot contain control characters")]
#[case::field_with_tab(&["tag1"], &[("field\tname", FieldDataType::String)], "field key cannot contain control characters")]
#[case::tag_with_carriage_return(&["tag\rname"], &[("field1", FieldDataType::String)], "tag key cannot contain control characters")]
#[case::field_with_carriage_return(&["tag1"], &[("field\rname", FieldDataType::String)], "field key cannot contain control characters")]
#[case::tag_with_null(&["tag\0name"], &[("field1", FieldDataType::String)], "tag key cannot contain control characters")]
#[case::field_with_null(&["tag1"], &[("field\0name", FieldDataType::String)], "field key cannot contain control characters")]
#[case::tag_with_form_feed(&["tag\x0Cname"], &[("field1", FieldDataType::String)], "tag key cannot contain control characters")]
#[case::field_with_form_feed(&["tag1"], &[("field\x0Cname", FieldDataType::String)], "field key cannot contain control characters")]
#[case::tag_with_del(&["tag\x7Fname"], &[("field1", FieldDataType::String)], "tag key cannot contain control characters")]
#[case::field_with_del(&["tag1"], &[("field\x7Fname", FieldDataType::String)], "field key cannot contain control characters")]
#[test_log::test(tokio::test)]
async fn test_create_table_validates_tag_and_field_names(
    #[case] tags: &[&str],
    #[case] fields: &[(&str, FieldDataType)],
    #[case] expected_error: &str,
) {
    let catalog = Catalog::new_in_memory("test-host").await.unwrap();
    catalog.create_database("test_db").await.unwrap();

    let result = catalog
        .create_table("test_db", "test_table", tags, fields)
        .await;

    assert!(result.is_err());
    let err_string = result.unwrap_err().to_string();
    assert_contains!(err_string, expected_error);
}

// Test valid tag and field names, including those that require escaping in line protocol.
#[rstest]
// Simple valid names
#[case::valid_simple(&["tag1"], &[("field1", FieldDataType::String)], &["tag1"], &["field1"])]
#[case::valid_multiple_tags(&["tag1", "tag2", "tag3"], &[("field1", FieldDataType::String)], &["tag1", "tag2", "tag3"], &["field1"])]
#[case::valid_multiple_fields(&["tag1"], &[("field1", FieldDataType::String), ("field2", FieldDataType::Integer), ("field3", FieldDataType::Float)], &["tag1"], &["field1", "field2", "field3"])]
#[case::valid_underscore(&["tag_1"], &[("field_1", FieldDataType::String)], &["tag_1"], &["field_1"])]
#[case::valid_numbers(&["tag123"], &[("field456", FieldDataType::String)], &["tag123"], &["field456"])]
#[case::valid_mixed(&["tag_123"], &[("field_456", FieldDataType::String)], &["tag_123"], &["field_456"])]
// Special characters that don't require escaping
#[case::valid_camelcase(&["tagName"], &[("fieldName", FieldDataType::String)], &["tagName"], &["fieldName"])]
#[case::valid_dots(&["tag.name"], &[("field.name", FieldDataType::String)], &["tag.name"], &["field.name"])]
#[case::valid_hyphens(&["tag-name"], &[("field-name", FieldDataType::String)], &["tag-name"], &["field-name"])]
#[case::valid_colons(&["tag:name"], &[("field:name", FieldDataType::String)], &["tag:name"], &["field:name"])]
#[case::valid_slashes(&["tag/name"], &[("field/name", FieldDataType::String)], &["tag/name"], &["field/name"])]
#[case::valid_brackets(&["tag[0]"], &[("field[0]", FieldDataType::String)], &["tag[0]"], &["field[0]"])]
#[case::valid_parentheses(&["tag(1)"], &[("field(1)", FieldDataType::String)], &["tag(1)"], &["field(1)"])]
#[case::valid_special_chars(&["tag@host"], &[("field#1", FieldDataType::String)], &["tag@host"], &["field#1"])]
#[case::valid_unicode(&["tag_名前"], &[("field_值", FieldDataType::String)], &["tag_名前"], &["field_值"])]
#[case::valid_long_names(&["this_is_a_very_long_tag_name_with_many_characters"], &[("this_is_a_very_long_field_name_with_many_characters", FieldDataType::String)], &["this_is_a_very_long_tag_name_with_many_characters"], &["this_is_a_very_long_field_name_with_many_characters"])]
// Names that require escaping in line protocol; the catalog stores the unescaped form of names:
// - "tag\ with\ space" in line protocol -> "tag with space" in catalog
// - "tag\,comma" in line protocol -> "tag,comma" in catalog
// - "tag\=equals" in line protocol -> "tag=equals" in catalog
// - "tag\\backslash" in line protocol -> "tag\backslash" in catalog
#[case::escaped_space(&["tag with space"], &[("field with space", FieldDataType::String)], &["tag with space"], &["field with space"])]
#[case::escaped_comma(&["tag,comma"], &[("field,comma", FieldDataType::String)], &["tag,comma"], &["field,comma"])]
#[case::escaped_equals(&["tag=equals"], &[("field=equals", FieldDataType::String)], &["tag=equals"], &["field=equals"])]
#[case::literal_backslash(&["tag\\backslash"], &[("field\\backslash", FieldDataType::String)], &["tag\\backslash"], &["field\\backslash"])]
#[case::multiple_special(&["tag with,comma=equals"], &[("field=with space,and=comma", FieldDataType::String)], &["tag with,comma=equals"], &["field=with space,and=comma"])]
#[test_log::test(tokio::test)]
async fn test_create_table_with_valid_names(
    #[case] tags: &[&str],
    #[case] fields: &[(&str, FieldDataType)],
    #[case] expected_tag_names: &[&str],
    #[case] expected_field_names: &[&str],
) {
    let catalog = Catalog::new_in_memory("test-host").await.unwrap();
    catalog.create_database("test_db").await.unwrap();

    let result = catalog
        .create_table("test_db", "test_table", tags, fields)
        .await;

    assert!(
        result.is_ok(),
        "Failed to create table with names that are valid in escaped line protocol: {result:?}"
    );

    // Verify the table was created correctly
    let db_schema = catalog.db_schema("test_db").unwrap();
    let table_def = db_schema.table_definition("test_table").unwrap();

    // Verify tag names match expectations
    let tag_columns: Vec<_> = table_def
        .columns
        .resource_iter()
        .filter(|col| col.column_type() == InfluxColumnType::Tag)
        .collect();

    assert_eq!(tag_columns.len(), expected_tag_names.len());
    for (i, expected_name) in expected_tag_names.iter().enumerate() {
        assert_eq!(
            tag_columns[i].name().as_ref(),
            *expected_name,
            "Tag name mismatch at index {i}"
        );
    }

    // Verify field names match expectations
    let field_columns: Vec<_> = table_def
        .columns
        .resource_iter()
        .filter(|col| {
            !matches!(
                col.column_type(),
                InfluxColumnType::Tag | InfluxColumnType::Timestamp
            )
        })
        .collect();

    assert_eq!(field_columns.len(), expected_field_names.len());
    for (i, expected_name) in expected_field_names.iter().enumerate() {
        assert_eq!(
            field_columns[i].name().as_ref(),
            *expected_name,
            "Field name mismatch at index {i}"
        );
    }
}
