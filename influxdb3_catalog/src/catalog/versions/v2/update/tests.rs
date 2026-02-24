use crate::CatalogError;
use crate::catalog::versions::v2::{Catalog, CreateTableOptions};
use crate::log::versions::v4::FieldDataType;
use iox_time::{MockProvider, Time};
use schema::{InfluxColumnType, InfluxFieldType};
use std::sync::Arc;

/// Tests which focus on testing the expected behaviour when creating a new table.
mod create_table {
    use super::*;
    use crate::catalog::versions::v2::{
        CatalogLimits, FieldFamilyMode, NUM_FIELDS_PER_FAMILY_LIMIT,
    };
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn all_auto_defined_field_families() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
        let store = Arc::new(InMemory::new());

        {
            let catalog = Arc::new(
                Catalog::new_with_store(
                    "test",
                    Arc::clone(&store) as _,
                    Arc::clone(&time_provider) as _,
                )
                .await
                .unwrap(),
            );

            catalog.create_database("foo").await.unwrap();

            catalog
                .create_table_opts(
                    "foo",
                    "bar",
                    &["tag"],
                    &[
                        ("str", FieldDataType::String),
                        ("f64", FieldDataType::Float),
                        ("i64", FieldDataType::Integer),
                        ("u64", FieldDataType::UInteger),
                        ("bool", FieldDataType::Boolean),
                    ],
                    CreateTableOptions {
                        ..Default::default()
                    },
                )
                .await
                .unwrap();

            let foo_db = catalog.db_schema("foo").expect("foo should exist");
            let bar = foo_db.tables.get_by_name("bar").expect("bar should exist");
            assert_eq!(bar.num_columns(), 7);
            assert_eq!(bar.num_tag_columns(), 1);
            assert_eq!(bar.num_field_columns(), 5);
            assert_eq!(bar.num_field_families(), 1);

            insta::assert_json_snapshot!("all_auto_defined_field_families__catalog_snapshot", catalog.snapshot(), {
                ".catalog_uuid" => "[uuid]",
                ".nodes.repo[][1].instance_id" => "[uuid]",
                ".nodes.repo[][1].process_uuids[]" => "[uuid]"
            });
        }

        // Reload the catalog from the object store logs and verify it matches the existing
        // insta snapshot
        {
            let catalog = Arc::new(
                Catalog::new_with_store(
                    "test",
                    Arc::clone(&store) as _,
                    Arc::clone(&time_provider) as _,
                )
                .await
                .unwrap(),
            );

            let foo_db = catalog.db_schema("foo").expect("foo should exist");
            let bar = foo_db.tables.get_by_name("bar").expect("bar should exist");
            assert_eq!(bar.num_columns(), 7);
            assert_eq!(bar.num_tag_columns(), 1);
            assert_eq!(bar.num_field_columns(), 5);
            assert_eq!(bar.num_field_families(), 1);

            insta::assert_json_snapshot!("all_auto_defined_field_families__catalog_snapshot", catalog.snapshot(), {
                ".catalog_uuid" => "[uuid]",
                ".nodes.repo[][1].instance_id" => "[uuid]",
                ".nodes.repo[][1].process_uuids[]" => "[uuid]"
            });
        }
    }

    /// Ensure that when the current auto field family reaches its field limit, a new auto field family is created,
    /// and any additional fields are added to this new family.
    #[tokio::test]
    async fn auto_family_rolls_to_next() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
        let store = Arc::new(InMemory::new());

        {
            let catalog = Arc::new(
                Catalog::new_with_store(
                    "test",
                    Arc::clone(&store) as _,
                    Arc::clone(&time_provider) as _,
                )
                .await
                .unwrap(),
            );

            catalog.create_database("foo").await.unwrap();

            let fields = (0..NUM_FIELDS_PER_FAMILY_LIMIT + 2)
                .map(|v| (format!("field_{v}"), FieldDataType::Float))
                .collect::<Vec<_>>();

            catalog
                .create_table_opts(
                    "foo",
                    "bar",
                    &["tag"],
                    &fields,
                    CreateTableOptions {
                        ..Default::default()
                    },
                )
                .await
                .unwrap();

            let foo_db = catalog.db_schema("foo").expect("foo should exist");
            let bar = foo_db.tables.get_by_name("bar").expect("bar should exist");
            assert!(bar.field_families.contains_name("__0"));
            assert!(bar.field_families.contains_name("__1"));
            assert_eq!(bar.num_columns(), 2 + NUM_FIELDS_PER_FAMILY_LIMIT + 2);
            assert_eq!(bar.num_tag_columns(), 1);
            assert_eq!(bar.num_field_columns(), NUM_FIELDS_PER_FAMILY_LIMIT + 2);
            assert_eq!(bar.num_field_families(), 2);
        }

        // Reload the catalog from the object store logs and verify it again
        {
            let catalog = Arc::new(
                Catalog::new_with_store(
                    "test",
                    Arc::clone(&store) as _,
                    Arc::clone(&time_provider) as _,
                )
                .await
                .unwrap(),
            );

            let foo_db = catalog.db_schema("foo").expect("foo should exist");
            let bar = foo_db.tables.get_by_name("bar").expect("bar should exist");
            assert!(bar.field_families.contains_name("__0"));
            assert!(bar.field_families.contains_name("__1"));
            assert_eq!(bar.num_columns(), 2 + NUM_FIELDS_PER_FAMILY_LIMIT + 2);
            assert_eq!(bar.num_tag_columns(), 1);
            assert_eq!(bar.num_field_columns(), NUM_FIELDS_PER_FAMILY_LIMIT + 2);
            assert_eq!(bar.num_field_families(), 2);
        }
    }

    #[tokio::test]
    async fn verify_tags_fields_are_valid() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
        let store = Arc::new(InMemory::new());
        let catalog = Arc::new(
            Catalog::new_with_store("test", store, time_provider)
                .await
                .unwrap(),
        );

        catalog.create_database("foo").await.unwrap();

        let result = catalog
            .create_table_opts(
                "foo",
                "bar",
                &["foo", ""], // Invalid tag name
                &[("field1", FieldDataType::Float)],
                CreateTableOptions::default(),
            )
            .await;

        assert!(
            matches!(result, Err(CatalogError::InvalidConfiguration { message }) if message.as_ref() == "tag key cannot be empty")
        );

        let result = catalog
            .create_table_opts(
                "foo",
                "bar",
                &["foo"], // Invalid tag name
                &[("field1", FieldDataType::Float), ("", FieldDataType::Float)],
                CreateTableOptions::default(),
            )
            .await;

        assert!(
            matches!(result, Err(CatalogError::InvalidConfiguration { message }) if message.as_ref() == "field key cannot be empty")
        );

        let result = catalog
            .create_table_opts(
                "foo",
                "bar",
                &["foo", "foo\t"], // Invalid tag name
                &[("field1", FieldDataType::Float)],
                CreateTableOptions::default(),
            )
            .await;

        assert!(
            matches!(result, Err(CatalogError::InvalidConfiguration { message }) if message.as_ref() == "tag key cannot contain control characters")
        );

        let result = catalog
            .create_table_opts(
                "foo",
                "bar",
                &["foo"], // Invalid tag name
                &[
                    ("field1", FieldDataType::Float),
                    ("foo\t", FieldDataType::Float),
                ],
                CreateTableOptions::default(),
            )
            .await;

        assert!(
            matches!(result, Err(CatalogError::InvalidConfiguration { message }) if message.as_ref() == "field key cannot contain control characters")
        );
    }

    #[tokio::test]
    async fn column_or_create_enforces_column_limit() {
        let catalog = Catalog::new_in_memory_with_limits(
            "test",
            CatalogLimits {
                num_columns_per_table: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let mut txn = catalog.begin("test_db").unwrap();
        txn.table_or_create("test_table").unwrap();

        // Add columns up to the limit (3 total)
        txn.column_or_create("test_table", "tag1", InfluxColumnType::Tag)
            .unwrap();
        txn.column_or_create("test_table", "time", InfluxColumnType::Timestamp)
            .unwrap();
        txn.column_or_create(
            "test_table",
            "field1",
            InfluxColumnType::Field(InfluxFieldType::Float),
        )
        .unwrap();

        // This should fail - we're at the limit now
        let result = txn.column_or_create(
            "test_table",
            "field2",
            InfluxColumnType::Field(InfluxFieldType::Integer),
        );
        assert!(matches!(result, Err(CatalogError::TooManyColumns(3))));
    }

    #[tokio::test]
    async fn verify_tags_fields_cannot_be_named_reserved_columns() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
        let store = Arc::new(InMemory::new());
        let catalog = Arc::new(
            Catalog::new_with_store("test", store, time_provider)
                .await
                .unwrap(),
        );

        catalog.create_database("foo").await.unwrap();

        // Test tag named "time"
        let result = catalog
            .create_table_opts(
                "foo",
                "bar",
                &["time"], // Invalid tag name
                &[("field1", FieldDataType::Float)],
                CreateTableOptions::default(),
            )
            .await;

        assert!(
            matches!(result, Err(CatalogError::ReservedColumn(name)) if name.as_ref() == "time")
        );

        // Test field named "time"
        let result = catalog
            .create_table_opts(
                "foo",
                "bar",
                &["tag1"],
                &[("time", FieldDataType::Float)], // Invalid field name
                CreateTableOptions::default(),
            )
            .await;

        assert!(
            matches!(result, Err(CatalogError::ReservedColumn(name)) if name.as_ref() == "time")
        );

        // Test tag named "__chunk_order"
        let result = catalog
            .create_table_opts(
                "foo",
                "bar2",
                &["__chunk_order"], // Invalid tag name
                &[("field1", FieldDataType::Float)],
                CreateTableOptions::default(),
            )
            .await;

        assert!(
            matches!(result, Err(CatalogError::ReservedColumn(name)) if name.as_ref() == "__chunk_order")
        );

        // Test field named "__chunk_order"
        let result = catalog
            .create_table_opts(
                "foo",
                "bar3",
                &["tag1"],
                &[("__chunk_order", FieldDataType::Integer)], // Invalid field name
                CreateTableOptions::default(),
            )
            .await;

        assert!(
            matches!(result, Err(CatalogError::ReservedColumn(name)) if name.as_ref() == "__chunk_order")
        );
    }

    #[tokio::test]
    async fn verify_duplicate_tag_or_field_returns_error() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
        let store = Arc::new(InMemory::new());
        let catalog = Arc::new(
            Catalog::new_with_store("test", store, time_provider)
                .await
                .unwrap(),
        );

        catalog.create_database("foo").await.unwrap();

        // Test duplicate tag names
        let result = catalog
            .create_table_opts(
                "foo",
                "bar",
                &["tag1", "tag2", "tag1"], // Duplicate tag1
                &[("field1", FieldDataType::Float)],
                CreateTableOptions::default(),
            )
            .await;

        assert!(matches!(
            result,
            Err(CatalogError::DuplicateColumn { name, existing })
                if name.as_ref() == "tag1" && existing == InfluxColumnType::Tag
        ));

        // Test duplicate field names
        let result = catalog
            .create_table_opts(
                "foo",
                "bar2",
                &["tag1"],
                &[
                    ("field1", FieldDataType::Float),
                    ("field2", FieldDataType::Integer),
                    ("field1", FieldDataType::String), // Duplicate field1
                ],
                CreateTableOptions::default(),
            )
            .await;

        assert!(matches!(
            result,
            Err(CatalogError::DuplicateColumn { name, existing })
                if name.as_ref() == "field1" && existing == InfluxColumnType::Field(InfluxFieldType::Float)
        ));

        // Test tag/field name collision
        let result = catalog
            .create_table_opts(
                "foo",
                "bar3",
                &["samename"],
                &[("samename", FieldDataType::Float)], // Same name as tag
                CreateTableOptions::default(),
            )
            .await;

        assert!(matches!(
            result,
            Err(CatalogError::DuplicateColumn { name, existing })
                if name.as_ref() == "samename" && existing == InfluxColumnType::Tag
        ));
    }

    #[tokio::test]
    async fn all_user_defined_field_families() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
        let store = Arc::new(InMemory::new());

        // Create catalog and schema
        {
            let catalog = Arc::new(
                Catalog::new_with_store(
                    "test",
                    Arc::clone(&store) as _,
                    Arc::clone(&time_provider) as _,
                )
                .await
                .unwrap(),
            );

            catalog.create_database("foo").await.unwrap();

            catalog
                .create_table_opts(
                    "foo",
                    "bar",
                    &["tag02", "tag01", "tag03"],
                    &[
                        ("ff1::str", FieldDataType::String),
                        ("ff1::f64", FieldDataType::Float),
                        ("ff2::f64", FieldDataType::Float),
                        ("ff2::i64", FieldDataType::Integer),
                        ("ff3::u64", FieldDataType::UInteger),
                        ("ff3::bool", FieldDataType::Boolean),
                    ],
                    CreateTableOptions {
                        ..Default::default()
                    },
                )
                .await
                .unwrap();

            let foo_db = catalog.db_schema("foo").expect("foo should exist");
            let bar = foo_db.tables.get_by_name("bar").expect("bar should exist");
            assert_eq!(bar.num_field_families(), 3);

            insta::assert_json_snapshot!("all_user_defined_field_families__catalog_snapshot", catalog.snapshot(), {
                ".catalog_uuid" => "[uuid]",
                ".nodes.repo[][1].instance_id" => "[uuid]",
                ".nodes.repo[][1].process_uuids[]" => "[uuid]"
            });
        }

        // Reload the catalog from the object store logs and verify it matches the existing
        // insta snapshot
        {
            let catalog = Arc::new(
                Catalog::new_with_store(
                    "test",
                    Arc::clone(&store) as _,
                    Arc::clone(&time_provider) as _,
                )
                .await
                .unwrap(),
            );

            let foo_db = catalog.db_schema("foo").expect("foo should exist");
            let bar = foo_db.tables.get_by_name("bar").expect("bar should exist");
            assert_eq!(bar.num_field_families(), 3);

            insta::assert_json_snapshot!("all_user_defined_field_families__catalog_snapshot", catalog.snapshot(), {
                ".catalog_uuid" => "[uuid]",
                ".nodes.repo[][1].instance_id" => "[uuid]",
                ".nodes.repo[][1].process_uuids[]" => "[uuid]"
            });
        }
    }

    /// This test ensures that when the table is [FieldFamilyMode::Auto], it ignores
    /// the `<field family>::` prefix and all fields are auto-assigned to a field family.
    #[tokio::test]
    async fn table_is_auto_field_family() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
        let store = Arc::new(InMemory::new());

        // Create catalog and schema
        {
            let catalog = Arc::new(
                Catalog::new_with_store(
                    "test",
                    Arc::clone(&store) as _,
                    Arc::clone(&time_provider) as _,
                )
                .await
                .unwrap(),
            );

            catalog.create_database("foo").await.unwrap();

            catalog
                .create_table_opts(
                    "foo",
                    "bar",
                    &["tag02", "tag01", "tag03"],
                    &[
                        ("ff1::str", FieldDataType::String),
                        ("ff1::f64", FieldDataType::Float),
                        ("ff2::f64", FieldDataType::Float),
                        ("ff2::i64", FieldDataType::Integer),
                        ("ff3::u64", FieldDataType::UInteger),
                        ("ff3::bool", FieldDataType::Boolean),
                    ],
                    CreateTableOptions {
                        field_family_mode: FieldFamilyMode::Auto,
                    },
                )
                .await
                .unwrap();

            let foo_db = catalog.db_schema("foo").expect("foo should exist");
            foo_db.tables.get_by_name("bar").expect("bar should exist");

            insta::assert_json_snapshot!("table_is_auto_field_family", catalog.snapshot(), {
                ".catalog_uuid" => "[uuid]",
                ".nodes.repo[][1].instance_id" => "[uuid]",
                ".nodes.repo[][1].process_uuids[]" => "[uuid]"
            });
        }

        // Reload the catalog from the object store logs and verify it matches the existing
        // insta snapshot
        {
            let catalog = Arc::new(
                Catalog::new_with_store(
                    "test",
                    Arc::clone(&store) as _,
                    Arc::clone(&time_provider) as _,
                )
                .await
                .unwrap(),
            );

            let foo_db = catalog.db_schema("foo").expect("foo should exist");
            foo_db.tables.get_by_name("bar").expect("bar should exist");

            insta::assert_json_snapshot!("table_is_auto_field_family", catalog.snapshot(), {
                ".catalog_uuid" => "[uuid]",
                ".nodes.repo[][1].instance_id" => "[uuid]",
                ".nodes.repo[][1].process_uuids[]" => "[uuid]"
            });
        }
    }
}

/// Focus on testing the expected behaviour when updating an existing table.
mod update_table {
    use super::*;
    use crate::catalog::versions::v2::update::create_table_columns;
    use crate::catalog::versions::v2::{CatalogArgs, CatalogLimits, FieldFamilyMode};
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn test_add_tags_to_existing_table() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
        let store = Arc::new(InMemory::new());

        {
            let catalog = Arc::new(
                Catalog::new_with_store(
                    "test",
                    Arc::clone(&store) as _,
                    Arc::clone(&time_provider) as _,
                )
                .await
                .unwrap(),
            );

            // Create database and table with initial tags and fields
            catalog.create_database("foo").await.unwrap();
            catalog
                .create_table(
                    "foo",
                    "bar",
                    &["tag1", "tag2"],
                    &[("field1", FieldDataType::Float)],
                )
                .await
                .unwrap();

            // Verify initial state
            let foo_db = catalog.db_schema("foo").expect("foo should exist");
            let bar_table = foo_db.tables.get_by_name("bar").expect("bar should exist");
            assert_eq!(bar_table.tag_columns.len(), 2);

            // Add new tags to the existing table
            let mut txn = catalog.begin("foo").unwrap();
            txn.column_or_create("bar", "tag3", InfluxColumnType::Tag)
                .unwrap();
            txn.column_or_create("bar", "tag4", InfluxColumnType::Tag)
                .unwrap();
            catalog.commit(txn).await.unwrap();

            // Verify tags were added
            let foo_db = catalog.db_schema("foo").expect("foo should exist");
            let bar_table = foo_db.tables.get_by_name("bar").expect("bar should exist");
            assert_eq!(bar_table.tag_columns.len(), 4);
            assert!(bar_table.column_definition("tag3").is_some());
            assert!(bar_table.column_definition("tag4").is_some());
        }

        // Reload catalog from object store to verify persistence
        {
            let catalog = Arc::new(
                Catalog::new_with_store(
                    "test",
                    Arc::clone(&store) as _,
                    Arc::clone(&time_provider) as _,
                )
                .await
                .unwrap(),
            );

            let foo_db = catalog.db_schema("foo").expect("foo should exist");
            let bar_table = foo_db.tables.get_by_name("bar").expect("bar should exist");
            assert_eq!(bar_table.tag_columns.len(), 4);
            assert!(bar_table.column_definition("tag3").is_some());
            assert!(bar_table.column_definition("tag4").is_some());
        }
    }

    #[tokio::test]
    async fn test_add_fields_to_existing_table() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
        let store = Arc::new(InMemory::new());

        {
            let catalog = Arc::new(
                Catalog::new_with_store(
                    "test",
                    Arc::clone(&store) as _,
                    Arc::clone(&time_provider) as _,
                )
                .await
                .unwrap(),
            );

            // Create database and table with initial fields
            catalog.create_database("foo").await.unwrap();
            catalog
                .create_table(
                    "foo",
                    "measurements",
                    &["location"],
                    &[("temperature", FieldDataType::Float)],
                )
                .await
                .unwrap();

            // Add various field types to the existing table
            let mut txn = catalog.begin("foo").unwrap();
            txn.column_or_create(
                "measurements",
                "humidity",
                InfluxColumnType::Field(InfluxFieldType::Float),
            )
            .unwrap();
            txn.column_or_create(
                "measurements",
                "pressure",
                InfluxColumnType::Field(InfluxFieldType::Integer),
            )
            .unwrap();
            txn.column_or_create(
                "measurements",
                "sensor_id",
                InfluxColumnType::Field(InfluxFieldType::UInteger),
            )
            .unwrap();
            txn.column_or_create(
                "measurements",
                "is_active",
                InfluxColumnType::Field(InfluxFieldType::Boolean),
            )
            .unwrap();
            txn.column_or_create(
                "measurements",
                "status",
                InfluxColumnType::Field(InfluxFieldType::String),
            )
            .unwrap();
            catalog.commit(txn).await.unwrap();

            // Verify fields were added
            let foo_db = catalog.db_schema("foo").expect("foo should exist");
            let table = foo_db
                .tables
                .get_by_name("measurements")
                .expect("measurements should exist");

            // Check all fields exist with correct types
            let humidity = table
                .column_definition("humidity")
                .expect("humidity should exist");
            assert_eq!(
                humidity.column_type(),
                InfluxColumnType::Field(InfluxFieldType::Float)
            );

            let pressure = table
                .column_definition("pressure")
                .expect("pressure should exist");
            assert_eq!(
                pressure.column_type(),
                InfluxColumnType::Field(InfluxFieldType::Integer)
            );

            let sensor_id = table
                .column_definition("sensor_id")
                .expect("sensor_id should exist");
            assert_eq!(
                sensor_id.column_type(),
                InfluxColumnType::Field(InfluxFieldType::UInteger)
            );

            let is_active = table
                .column_definition("is_active")
                .expect("is_active should exist");
            assert_eq!(
                is_active.column_type(),
                InfluxColumnType::Field(InfluxFieldType::Boolean)
            );

            let status = table
                .column_definition("status")
                .expect("status should exist");
            assert_eq!(
                status.column_type(),
                InfluxColumnType::Field(InfluxFieldType::String)
            );

            // Add fields with user-defined field families
            let mut txn = catalog.begin("foo").unwrap();
            txn.column_or_create(
                "measurements",
                "metrics::cpu_usage",
                InfluxColumnType::Field(InfluxFieldType::Float),
            )
            .unwrap();
            txn.column_or_create(
                "measurements",
                "metrics::memory_usage",
                InfluxColumnType::Field(InfluxFieldType::Float),
            )
            .unwrap();
            txn.column_or_create(
                "measurements",
                "metadata::version",
                InfluxColumnType::Field(InfluxFieldType::String),
            )
            .unwrap();
            catalog.commit(txn).await.unwrap();

            // Verify user-defined field families were created
            let foo_db = catalog.db_schema("foo").expect("foo should exist");
            let table = foo_db
                .tables
                .get_by_name("measurements")
                .expect("measurements should exist");
            assert!(table.field_families.contains_name("metrics"));
            assert!(table.field_families.contains_name("metadata"));
        }

        // Reload catalog from object store to verify persistence
        {
            let catalog = Arc::new(
                Catalog::new_with_store(
                    "test",
                    Arc::clone(&store) as _,
                    Arc::clone(&time_provider) as _,
                )
                .await
                .unwrap(),
            );

            let foo_db = catalog.db_schema("foo").expect("foo should exist");
            let table = foo_db
                .tables
                .get_by_name("measurements")
                .expect("measurements should exist");

            // Verify all fields persist after reload
            assert!(table.column_definition("humidity").is_some());
            assert!(table.column_definition("pressure").is_some());
            assert!(table.column_definition("sensor_id").is_some());
            assert!(table.column_definition("is_active").is_some());
            assert!(table.column_definition("status").is_some());
            assert!(table.column_definition("metrics::cpu_usage").is_some());
            assert!(table.column_definition("metrics::memory_usage").is_some());
            assert!(table.column_definition("metadata::version").is_some());

            // Verify field families persist
            assert!(table.field_families.contains_name("metrics"));
            assert!(table.field_families.contains_name("metadata"));
        }
    }

    #[tokio::test]
    async fn test_reserved_column_time_error() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
        let store = Arc::new(InMemory::new());
        let catalog = Arc::new(
            Catalog::new_with_store("test", store, time_provider)
                .await
                .unwrap(),
        );

        // Create database and table
        catalog.create_database("foo").await.unwrap();
        catalog
            .create_table(
                "foo",
                "data",
                &["tag1"],
                &[("field1", FieldDataType::Float)],
            )
            .await
            .unwrap();

        // The table already has a "time" column as the timestamp column.
        // Trying to add a tag or field named "time" should result in InvalidColumnType
        // because the column already exists with type Timestamp.

        // Try to add a tag named "time"
        let mut txn = catalog.begin("foo").unwrap();
        let result = txn.column_or_create("data", "time", InfluxColumnType::Tag);
        assert!(matches!(
            result,
            Err(CatalogError::InvalidColumnType { column_name, expected, got })
                if column_name.as_ref() == "time"
                && expected == InfluxColumnType::Timestamp
                && got == InfluxColumnType::Tag
        ));

        // Try to add a field named "time"
        let mut txn = catalog.begin("foo").unwrap();
        let result = txn.column_or_create(
            "data",
            "time",
            InfluxColumnType::Field(InfluxFieldType::Float),
        );
        assert!(matches!(
            result,
            Err(CatalogError::InvalidColumnType { column_name, expected, got })
                if column_name.as_ref() == "time"
                && expected == InfluxColumnType::Timestamp
                && got == InfluxColumnType::Field(InfluxFieldType::Float)
        ));

        // Trying to add "time" as Timestamp should succeed because it already exists
        let mut txn = catalog.begin("foo").unwrap();
        let result = txn.column_or_create("data", "time", InfluxColumnType::Timestamp);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_duplicate_column_errors() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
        let store = Arc::new(InMemory::new());
        let catalog = Arc::new(
            Catalog::new_with_store("test", store, time_provider)
                .await
                .unwrap(),
        );

        // Create database and table with existing columns
        catalog.create_database("foo").await.unwrap();
        catalog
            .create_table(
                "foo",
                "metrics",
                &["host", "region"],
                &[
                    ("cpu", FieldDataType::Float),
                    ("memory", FieldDataType::Integer),
                ],
            )
            .await
            .unwrap();

        // Test 1: Try to add duplicate tag
        let mut txn = catalog.begin("foo").unwrap();
        let result = txn.column_or_create("metrics", "host", InfluxColumnType::Tag);
        // Should succeed because it already exists with same type
        assert!(result.is_ok());

        // Test 2: Try to add tag with same name as existing field
        let mut txn = catalog.begin("foo").unwrap();
        let result = txn.column_or_create("metrics", "cpu", InfluxColumnType::Tag);
        assert!(matches!(
            result,
            Err(CatalogError::InvalidColumnType { column_name, expected, got })
                if column_name.as_ref() == "cpu"
                && expected == InfluxColumnType::Field(InfluxFieldType::Float)
                && got == InfluxColumnType::Tag
        ));

        // Test 3: Try to add field with same name as existing tag
        let mut txn = catalog.begin("foo").unwrap();
        let result = txn.column_or_create(
            "metrics",
            "host",
            InfluxColumnType::Field(InfluxFieldType::String),
        );
        assert!(matches!(
            result,
            Err(CatalogError::InvalidColumnType { column_name, expected, got })
                if column_name.as_ref() == "host"
                && expected == InfluxColumnType::Tag
                && got == InfluxColumnType::Field(InfluxFieldType::String)
        ));

        // Test 4: Try to add field with different type than existing field
        let mut txn = catalog.begin("foo").unwrap();
        let result = txn.column_or_create(
            "metrics",
            "cpu",
            InfluxColumnType::Field(InfluxFieldType::String),
        );
        assert!(matches!(
            result,
            Err(CatalogError::InvalidColumnType { column_name, expected, got })
                if column_name.as_ref() == "cpu"
                && expected == InfluxColumnType::Field(InfluxFieldType::Float)
                && got == InfluxColumnType::Field(InfluxFieldType::String)
        ));

        // Test 5: Adding duplicate within same transaction
        let mut txn = catalog.begin("foo").unwrap();
        txn.column_or_create("metrics", "new_tag", InfluxColumnType::Tag)
            .unwrap();
        let result = txn.column_or_create(
            "metrics",
            "new_tag",
            InfluxColumnType::Field(InfluxFieldType::Float),
        );
        assert!(matches!(
            result,
            Err(CatalogError::InvalidColumnType { column_name, expected, got })
                if column_name.as_ref() == "new_tag"
                && expected == InfluxColumnType::Tag
                && got == InfluxColumnType::Field(InfluxFieldType::Float)
        ));
    }

    #[tokio::test]
    async fn test_update_table_comprehensive_snapshot() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
        let store = Arc::new(InMemory::new());

        {
            let catalog = Arc::new(
                Catalog::new_with_store(
                    "test",
                    Arc::clone(&store) as _,
                    Arc::clone(&time_provider) as _,
                )
                .await
                .unwrap(),
            );

            // Create initial database and table
            catalog.create_database("test_db").await.unwrap();
            catalog
                .create_table_opts(
                    "test_db",
                    "sensors",
                    &["location", "sensor_type"],
                    &[
                        ("temperature", FieldDataType::Float),
                        ("humidity", FieldDataType::Float),
                    ],
                    CreateTableOptions {
                        ..Default::default()
                    },
                )
                .await
                .unwrap();

            // First update: Add more tags
            let mut txn = catalog.begin("test_db").unwrap();
            txn.column_or_create("sensors", "building", InfluxColumnType::Tag)
                .unwrap();
            txn.column_or_create("sensors", "floor", InfluxColumnType::Tag)
                .unwrap();
            catalog.commit(txn).await.unwrap();

            // Second update: Add fields with auto field families
            let mut txn = catalog.begin("test_db").unwrap();
            txn.column_or_create(
                "sensors",
                "pressure",
                InfluxColumnType::Field(InfluxFieldType::Float),
            )
            .unwrap();
            txn.column_or_create(
                "sensors",
                "light_level",
                InfluxColumnType::Field(InfluxFieldType::Integer),
            )
            .unwrap();
            txn.column_or_create(
                "sensors",
                "motion_detected",
                InfluxColumnType::Field(InfluxFieldType::Boolean),
            )
            .unwrap();
            catalog.commit(txn).await.unwrap();

            // Third update: Add fields with user-defined field families
            let mut txn = catalog.begin("test_db").unwrap();
            txn.column_or_create(
                "sensors",
                "environmental::co2_level",
                InfluxColumnType::Field(InfluxFieldType::Float),
            )
            .unwrap();
            txn.column_or_create(
                "sensors",
                "environmental::air_quality",
                InfluxColumnType::Field(InfluxFieldType::UInteger),
            )
            .unwrap();
            txn.column_or_create(
                "sensors",
                "metadata::firmware_version",
                InfluxColumnType::Field(InfluxFieldType::String),
            )
            .unwrap();
            txn.column_or_create(
                "sensors",
                "metadata::last_calibration",
                InfluxColumnType::Field(InfluxFieldType::String),
            )
            .unwrap();
            catalog.commit(txn).await.unwrap();

            // Create a second table to make the snapshot more comprehensive
            catalog
                .create_table(
                    "test_db",
                    "events",
                    &["event_type"],
                    &[("value", FieldDataType::Float)],
                )
                .await
                .unwrap();

            // Update the second table
            let mut txn = catalog.begin("test_db").unwrap();
            txn.column_or_create("events", "severity", InfluxColumnType::Tag)
                .unwrap();
            txn.column_or_create(
                "events",
                "message",
                InfluxColumnType::Field(InfluxFieldType::String),
            )
            .unwrap();
            catalog.commit(txn).await.unwrap();

            // Take snapshot
            insta::assert_json_snapshot!("update_table_comprehensive__catalog_snapshot", catalog.snapshot(), {
                ".catalog_uuid" => "[uuid]",
                ".nodes.repo[][1].instance_id" => "[uuid]",
                ".nodes.repo[][1].process_uuids[]" => "[uuid]"
            });
        }

        // Reload catalog from object store and verify snapshot matches
        {
            let catalog = Arc::new(
                Catalog::new_with_store(
                    "test",
                    Arc::clone(&store) as _,
                    Arc::clone(&time_provider) as _,
                )
                .await
                .unwrap(),
            );

            // Verify all tables and columns exist
            let test_db = catalog.db_schema("test_db").expect("test_db should exist");

            let sensors = test_db
                .tables
                .get_by_name("sensors")
                .expect("sensors should exist");
            assert_eq!(sensors.tag_columns.len(), 4); // location, sensor_type, building, floor
            assert!(sensors.field_families.contains_name("__0")); // auto field family
            assert!(sensors.field_families.contains_name("environmental"));
            assert!(sensors.field_families.contains_name("metadata"));

            let events = test_db
                .tables
                .get_by_name("events")
                .expect("events should exist");
            assert_eq!(events.tag_columns.len(), 2); // event_type, severity

            // Verify snapshot still matches after reload
            insta::assert_json_snapshot!("update_table_comprehensive__catalog_snapshot", catalog.snapshot(), {
                ".catalog_uuid" => "[uuid]",
                ".nodes.repo[][1].instance_id" => "[uuid]",
                ".nodes.repo[][1].process_uuids[]" => "[uuid]"
            });
        }
    }

    #[tokio::test]
    async fn column_or_create_enforces_column_limit() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(10000)));
        let limits = CatalogLimits {
            num_columns_per_table: 3,
            ..Default::default()
        };
        let catalog = Arc::new(
            Catalog::new_in_memory_with_args_limits(
                "test",
                time_provider,
                CatalogArgs::default(),
                limits,
            )
            .await
            .unwrap(),
        );

        // Create the table at the max column count
        {
            let mut txn = catalog.begin("test_db").unwrap();
            txn.create_table(
                "test_table",
                create_table_columns::none(),
                FieldFamilyMode::Auto,
            )
            .unwrap();

            // Add columns up to the limit (3 total)
            txn.column_or_create("test_table", "tag1", InfluxColumnType::Tag)
                .unwrap();
            txn.column_or_create("test_table", "time", InfluxColumnType::Timestamp)
                .unwrap();
            txn.column_or_create(
                "test_table",
                "field1",
                InfluxColumnType::Field(InfluxFieldType::Float),
            )
            .unwrap();

            catalog.commit(txn).await.unwrap();
        }

        let mut txn = catalog.begin("test_db").unwrap();
        txn.table_or_create("test_table").unwrap();

        // This should fail - we're at the limit now
        let result = txn.column_or_create(
            "test_table",
            "field2",
            InfluxColumnType::Field(InfluxFieldType::Integer),
        );
        assert!(matches!(result, Err(CatalogError::TooManyColumns(3))));
    }
}

/// Reproducer for issue <https://github.com/influxdata/influxdb/issues/26776>
#[tokio::test]
async fn test_field_family_arc_ref_count_bug() {
    use super::CreateTableColumns;
    use crate::catalog::versions::v2::FieldFamilyMode;

    let catalog = Arc::new(Catalog::new_in_memory("test-catalog").await.unwrap());
    {
        let mut txn = catalog.begin("test_db").unwrap();
        txn.create_table(
            "test_table",
            Some(CreateTableColumns {
                tags: &["tag"],
                fields: &[("ff::a", FieldDataType::Integer)],
            }),
            FieldFamilyMode::Aware,
        )
        .unwrap();
        catalog.commit(txn).await.unwrap();
    }
    {
        let mut txn = catalog.begin("test_db").unwrap();
        let ttx = txn.table_transaction("test_table").unwrap();
        // This should not panic.
        let _col_def = ttx
            .add_field("ff::b", InfluxFieldType::Float)
            .expect("add field");
    }
}
