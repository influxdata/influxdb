//! Migration from [v1::InnerCatalog] to [v2::InnerCatalog].

use crate::catalog::versions::v2::update::TableTransaction;
use crate::catalog::versions::v2::{
    DatabaseSchema, FieldFamilyMode, InnerCatalog, NUM_FIELDS_PER_FAMILY_LIMIT, Snapshot,
    TableDefinition, TableUpdate, field,
};
use crate::catalog::versions::{v1, v2};
use crate::log::versions::v3 as logprev;
use crate::log::versions::v4 as lognext;
use crate::log::versions::v4::{AddColumnsLog, DistinctCacheDefinition, LastCacheDefinition};
use hashbrown::HashMap;
use hashbrown::hash_map::Entry;
use itertools::Itertools;
use observability_deps::tracing::info;
use schema::InfluxColumnType;
use std::borrow::Cow;
use std::sync::Arc;

#[allow(dead_code)]
pub(super) fn migrate(from: &v1::InnerCatalog) -> crate::Result<v2::InnerCatalog> {
    info!("Migrating Catalog from v1 → v2");

    // Take the inner catalog from a newly constructed v2::Catalog, which ensures it is in a
    // consistent state, including invariants such as creating the internal database.
    let mut v2_inner = InnerCatalog::new(Arc::clone(&from.catalog_id), from.catalog_uuid);

    // 1. Migrate tokens and token permissions
    v2_inner.tokens = from.tokens.clone();

    // 2. Migrate databases
    for (db_id, v1_db) in from.databases.iter() {
        let db_schema = DatabaseSchema {
            id: v1_db.id,
            name: Arc::clone(&v1_db.name),
            tables: Default::default(),
            retention_period: v1_db.retention_period.into(),
            processing_engine_triggers: Default::default(),
            deleted: v1_db.deleted,
            hard_delete_time: v1_db.hard_delete_time,
        };

        v2_inner
            .databases
            .insert(*db_id, db_schema)
            .expect("database ID and name should not exist");
        let db_schema = v2_inner
            .databases
            .get_by_id(db_id)
            .expect("database ID should exist");

        let mut new_tables = Vec::with_capacity(v1_db.tables.len());
        for v1_table in v1_db.tables.resource_iter() {
            let mut table_def = migrate_table(&db_schema, v1_table)?;

            migrate_last_caches(v1_table, &mut table_def);
            migrate_distinct_caches(v1_table, &mut table_def);

            new_tables.push(table_def);
        }

        drop(db_schema);

        let db_arc = v2_inner
            .databases
            .get_mut_by_id(db_id)
            .expect("database ID should exist");
        let db_schema = Arc::get_mut(db_arc).expect("no other references");

        // Add tables to database
        for table_def in new_tables {
            db_schema
                .tables
                .insert(table_def.table_id, table_def)
                .expect("table ID and name should not exist");
        }
        db_schema.tables.next_id = v1_db.tables.next_id;

        migrate_processing_engine_triggers(v1_db, db_schema);
    }

    v2_inner.databases.next_id = from.databases.next_id;

    migrate_nodes(from, &mut v2_inner);

    v2_inner.sequence = from.sequence;

    Ok(InnerCatalog::from_snapshot(v2_inner.snapshot()))
}

fn migrate_nodes(from: &v1::InnerCatalog, v2_inner: &mut InnerCatalog) {
    for v1_node in from.nodes.resource_iter() {
        let v2_node = v2::NodeDefinition {
            node_id: Arc::clone(&v1_node.node_id),
            node_catalog_id: v1_node.node_catalog_id,
            instance_id: Arc::clone(&v1_node.instance_id),
            mode: v1_node
                .mode
                .iter()
                .cloned()
                .map(lognext::NodeMode::from)
                .collect(),
            core_count: v1_node.core_count,
            state: match v1_node.state {
                v1::NodeState::Running { registered_time_ns } => {
                    v2::NodeState::Running { registered_time_ns }
                }
                v1::NodeState::Stopped { stopped_time_ns } => {
                    v2::NodeState::Stopped { stopped_time_ns }
                }
            },
            cli_params: None, // v1 catalog doesn't have cli_params
        };
        v2_inner
            .nodes
            .insert(v1_node.node_catalog_id, Arc::new(v2_node))
            .expect("node shouldn't exist by ID or node_id");
    }
    v2_inner.nodes.next_id = from.nodes.next_id;
}

fn migrate_processing_engine_triggers(
    v1_db: &Arc<v1::DatabaseSchema>,
    db_schema: &mut DatabaseSchema,
) {
    for v1_trigger in v1_db.processing_engine_triggers.resource_iter() {
        let v2_trigger: lognext::TriggerDefinition = v1_trigger.as_ref().clone().into();
        db_schema
            .processing_engine_triggers
            .insert(v2_trigger.trigger_id, Arc::new(v2_trigger))
            .expect("trigger definition shouldn't exist by ID or name");
    }
    db_schema.processing_engine_triggers.next_id = v1_db.processing_engine_triggers.next_id;
}

fn migrate_last_caches(v1_table: &Arc<v1::TableDefinition>, table_def: &mut TableDefinition) {
    for v1_def in v1_table.last_caches.resource_iter() {
        let key_columns = conv::from_column_ids(v1_table, table_def, v1_def.key_columns.iter());

        let value_columns = {
            match &v1_def.value_columns {
                logprev::LastCacheValueColumnsDef::Explicit { columns } => {
                    lognext::LastCacheValueColumnsDef::Explicit {
                        columns: conv::from_column_ids(v1_table, table_def, columns.iter()),
                    }
                }
                logprev::LastCacheValueColumnsDef::AllNonKeyColumns => {
                    lognext::LastCacheValueColumnsDef::AllNonKeyColumns
                }
            }
        };

        let lcd = LastCacheDefinition {
            table_id: table_def.table_id,
            table: Arc::clone(&table_def.table_name),
            id: v1_def.id,
            name: Arc::clone(&v1_def.name),
            key_columns,
            value_columns,
            count: v1_def.count.into(),
            ttl: v1_def.ttl.into(),
        };

        table_def
            .last_caches
            .insert(lcd.id, lcd)
            .expect("last cache definition shouldn't exist by ID or name");
    }
    table_def.last_caches.next_id = v1_table.last_caches.next_id;
}

fn migrate_distinct_caches(v1_table: &Arc<v1::TableDefinition>, table_def: &mut TableDefinition) {
    for v1_def in v1_table.distinct_caches.resource_iter() {
        let column_ids = conv::from_column_ids(v1_table, table_def, v1_def.column_ids.iter());

        let dcd = DistinctCacheDefinition {
            table_id: table_def.table_id,
            table_name: Arc::clone(&table_def.table_name),
            cache_id: v1_def.cache_id,
            cache_name: Arc::clone(&v1_def.cache_name),
            column_ids,
            max_cardinality: v1_def.max_cardinality.into(),
            max_age_seconds: v1_def.max_age_seconds.into(),
        };

        table_def
            .distinct_caches
            .insert(dcd.cache_id, dcd)
            .expect("distinct cache definition shouldn't exist by ID or name");
    }
    table_def.distinct_caches.next_id = v1_table.distinct_caches.next_id;
}

fn migrate_table(
    db_schema: &Arc<DatabaseSchema>,
    v1_table: &Arc<v1::TableDefinition>,
) -> crate::Result<TableDefinition> {
    let field_family_mode = determine_field_family_mode(v1_table);
    // Create a TableTransaction to leverage existing APIs for
    // adding columns and ensuring field family invariants.
    let mut tx = TableTransaction::new(
        TableDefinition::new_empty(
            v1_table.table_id,
            Arc::clone(&v1_table.table_name),
            field_family_mode,
        ),
        Arc::clone(db_schema),
        usize::MAX,
    );

    // We can expect all columns can be added without failure:
    //
    // - Time: The table is new, so the time column can be added
    //
    // - Tags: v1 schema can have a maximum of 250 tags, which is less than 256 in v2 schema
    //
    // - Fields: The field family mode has been determined by evaluating the source table, so all
    //   fields can be either added to their respective field families or assigned to field
    //   families automatically
    v1_table
        .columns
        .resource_iter()
        // We must preserve the original creation order by sorting by the id.
        .sorted_by(|a, b| a.id.cmp(&b.id))
        .for_each(|column| match column.data_type {
            InfluxColumnType::Timestamp => {
                tx.add_time().expect("add time");
            }
            InfluxColumnType::Tag => {
                tx.add_tag(&column.name).expect("add tag");
            }
            InfluxColumnType::Field(ft) => {
                tx.add_field(&column.name, ft).expect("add field");
            }
        });

    let mut table_def = TableDefinition::new(
        v1_table.table_id,
        Arc::clone(&v1_table.table_name),
        vec![],
        vec![],
        vec![],
        field_family_mode,
    )?;
    table_def.deleted = v1_table.deleted;
    table_def.hard_delete_time = v1_table.hard_delete_time;

    // Update the new table from the transaction, which enforces other
    // TableDefinition invariants, such as setting the series_key.
    Ok(AddColumnsLog::from(tx)
        .update_table(Cow::Owned(table_def))
        .expect("table should update as it is empty")
        .into_owned())
}

/// Determines the field family mode by analyzing the field columns in the table definition.
///
/// # NOTE
///
/// A table is expected to be [FieldFamilyMode::Aware]. However, if field column names use the
/// [field::FIELD_FAMILY_DELIMITER], the migration will attempt to assign those fields to the
/// specified field family. Should the number of fields in a single family exceed the
/// [NUM_FIELDS_PER_FAMILY_LIMIT], the table will be [FieldFamilyMode::Auto], and always ignore
/// the field family prefix.
fn determine_field_family_mode(table: &v1::TableDefinition) -> FieldFamilyMode {
    let mut field_families: HashMap<&str, usize> = HashMap::new();

    for column in table
        .columns
        .resource_iter()
        .filter(|col| matches!(col.data_type, InfluxColumnType::Field(_)))
    {
        if let Some(prefix) = column.name.split(field::FIELD_FAMILY_DELIMITER).next() {
            match field_families.entry(prefix) {
                Entry::Occupied(mut oe) => {
                    *oe.get_mut() += 1;
                    if *oe.get() > NUM_FIELDS_PER_FAMILY_LIMIT {
                        return FieldFamilyMode::Auto;
                    }
                }
                Entry::Vacant(ve) => {
                    ve.insert(1);
                }
            }
        }
    }

    FieldFamilyMode::Aware
}

mod conv {
    use crate::catalog::versions::{v1, v2};
    use influxdb3_id::ColumnId;
    use influxdb3_id::ColumnIdentifier;
    use std::sync::Arc;

    pub(super) fn from_column_ids<'a>(
        source: &Arc<v1::TableDefinition>,
        target: &v2::TableDefinition,
        ids: impl Iterator<Item = &'a ColumnId>,
    ) -> Vec<ColumnIdentifier> {
        ids.map(|col_id| {
            target
                .column_definition(
                    source
                        .column_definition_by_id(col_id)
                        // We expect the column to exist by ID, as the IDs were derived
                        // from the source table.
                        .expect("source column exists")
                        .name
                        .as_ref(),
                )
                // We expect the column to exist by name, as it the target was just
                // created from the source table.
                .expect("target column exists by name")
                .id()
        })
        .collect::<Vec<_>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::versions::v1::{self};
    use crate::catalog::versions::v2::FieldFamilyName;
    use crate::log::versions::v3::FieldDataType;
    use iox_time::Time;

    /// Helper macro to simplify table creation in tests
    macro_rules! create_test_table {
        // Variant for empty tags and fields
        ($catalog:expr, $db:expr, $table:expr) => {{
            let empty_tags: &[&str] = &[];
            let empty_fields: &[(&str, FieldDataType)] = &[];
            $catalog
                .create_table($db, $table, empty_tags, empty_fields)
                .await
                .unwrap()
        }};
        // Normal variant with tags and/or fields
        ($catalog:expr, $db:expr, $table:expr, tags: [$($tag:expr),*], fields: [$(($field_name:expr, $field_type:ident)),* $(,)?]) => {{
            $catalog
                .create_table(
                    $db,
                    $table,
                    &[$($tag),*],
                    &[$(($field_name, FieldDataType::$field_type)),*],
                )
                .await
                .unwrap()
        }};
    }

    async fn create_v1_catalog() -> v1::Catalog {
        let time_provider = Arc::new(iox_time::MockProvider::new(Time::from_timestamp_nanos(
            1000,
        )));
        v1::Catalog::new_in_memory_with_args(
            "test-catalog",
            time_provider,
            v1::CatalogArgs::default(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_migrate_empty_catalog() {
        let v1_catalog = create_v1_catalog().await;

        let v2_catalog = migrate(&v1_catalog.inner.read()).unwrap();

        // Verify basic catalog properties
        assert_eq!(v2_catalog.catalog_id, v1_catalog.catalog_id());
        assert_eq!(v2_catalog.sequence_number(), v1_catalog.sequence_number());

        // Verify only the internal database exists
        assert_eq!(v2_catalog.databases.len(), 1); // Only _internal database
    }

    #[tokio::test]
    async fn test_migrate_preserves_catalog_uuid() {
        let v1_catalog = create_v1_catalog().await;
        let v1_uuid = v1_catalog.catalog_uuid();

        let v2_catalog = migrate(&v1_catalog.inner.read()).unwrap();

        // Verify catalog UUID is preserved during migration
        assert_eq!(
            v2_catalog.catalog_uuid, v1_uuid,
            "catalog UUID should be preserved during v1 to v2 migration"
        );
    }

    mod tokens {
        use super::*;
        use crate::catalog::DEFAULT_OPERATOR_TOKEN_NAME;

        #[tokio::test]
        async fn all_token_types() {
            let v1_catalog = create_v1_catalog().await;

            v1_catalog.create_database("foo").await.unwrap();

            v1_catalog.create_admin_token(false).await.unwrap();

            v1_catalog
                .create_named_admin_token_with_permission("my_admin".into(), Some(500))
                .await
                .unwrap();

            let v2_catalog = migrate(&v1_catalog.inner.read()).unwrap();

            // Verify default operator token
            {
                let v1_token = v1_catalog
                    .inner
                    .read()
                    .tokens
                    .repo()
                    .get_by_name(DEFAULT_OPERATOR_TOKEN_NAME)
                    .unwrap();
                let v2_token = v2_catalog.tokens.repo().get_by_id(&v1_token.id).unwrap();
                pretty_assertions::assert_eq!(v1_token, v2_token);
            }

            // Verify my_admin
            {
                let v1_token = v1_catalog
                    .inner
                    .read()
                    .tokens
                    .repo()
                    .get_by_name("my_admin")
                    .unwrap();
                let v2_token = v2_catalog.tokens.repo().get_by_id(&v1_token.id).unwrap();
                pretty_assertions::assert_eq!(v1_token, v2_token);
            }
        }
    }

    /// Tests related to database migration
    mod database {
        use super::*;

        #[tokio::test]
        async fn with_retention() {
            let v1_catalog = create_v1_catalog().await;

            // Create a database with retention period
            v1_catalog.create_database("test_db").await.unwrap();
            let retention_duration = std::time::Duration::from_secs(86400); // 1 day
            v1_catalog
                .set_retention_period_for_database("test_db", retention_duration)
                .await
                .unwrap();

            // Get database ID
            let db_id = v1_catalog.db_name_to_id("test_db").unwrap();

            let v2_catalog = migrate(&v1_catalog.inner.read()).unwrap();

            // Verify database was migrated
            assert_eq!(v2_catalog.databases.len(), 2); // test_db + _internal

            let v2_db = v2_catalog.databases.get_by_id(&db_id).unwrap();
            assert_eq!(v2_db.name.as_ref(), "test_db");
            assert_eq!(
                v2_db.retention_period,
                crate::log::versions::v4::RetentionPeriod::Duration(retention_duration)
            );
            assert!(!v2_db.deleted);
            assert_eq!(v2_db.hard_delete_time, None);
        }

        /// Verify soft and hard deleted values are migrated.
        #[tokio::test]
        async fn deleted() {
            let v1_catalog = create_v1_catalog().await;

            // Create and soft delete a database
            v1_catalog.create_database("test_db").await.unwrap();
            let db_id = v1_catalog.db_name_to_id("test_db").unwrap();

            let hard_delete_time = Time::from_timestamp_nanos(1234567890);
            v1_catalog
                .soft_delete_database(
                    "test_db",
                    v1::update::HardDeletionTime::Timestamp(hard_delete_time),
                )
                .await
                .unwrap();

            let v2_catalog = migrate(&v1_catalog.inner.read()).unwrap();

            let v2_db = v2_catalog.databases.get_by_id(&db_id).unwrap();
            assert!(v2_db.deleted);
            assert_eq!(v2_db.hard_delete_time, Some(hard_delete_time));
        }

        #[tokio::test]
        async fn multiple_databases() {
            let v1_catalog = create_v1_catalog().await;

            v1_catalog.create_database("db1").await.unwrap();
            v1_catalog.create_database("db2").await.unwrap();

            let v2_catalog = migrate(&v1_catalog.inner.read()).unwrap();

            // Verify all databases and tables were migrated
            assert_eq!(v2_catalog.databases.len(), 3); // db1, db2 + _internal

            let db1_id = v1_catalog.db_name_to_id("db1").unwrap();
            assert!(v2_catalog.databases.get_by_id(&db1_id).is_some());

            let db2_id = v1_catalog.db_name_to_id("db2").unwrap();
            assert!(v2_catalog.databases.get_by_id(&db2_id).is_some());
        }
    }

    mod table {
        use super::*;

        /// Verify columns are migrated and field families are created
        mod columns_and_families {
            use super::*;
            use pretty_assertions::{assert_eq, assert_matches};
            use schema::InfluxFieldType;

            #[tokio::test]
            async fn with_field_families() {
                let v1_catalog = create_v1_catalog().await;

                // Create database and table with fields that have field family prefixes
                v1_catalog.create_database("test_db").await.unwrap();
                create_test_table!(v1_catalog, "test_db", "test_table",
                    tags: ["tag1"],
                    fields: [
                        ("metrics::cpu", Float),
                        ("metrics::memory", Float),
                        ("status::active", Boolean),
                        ("plain_field", String),
                    ]
                );

                let db_id = v1_catalog.db_name_to_id("test_db").unwrap();
                let v1_db_schema = v1_catalog.db_schema_by_id(&db_id).unwrap();
                let table_id = v1_db_schema.table_name_to_id("test_table").unwrap();

                let v2_inner = migrate(&v1_catalog.inner.read()).unwrap();

                // Verify table was migrated with Aware mode
                let v2_db = v2_inner.databases.get_by_id(&db_id).unwrap();
                let v2_table = v2_db.tables.get_by_id(&table_id).unwrap();

                assert_eq!(v2_table.field_family_mode, FieldFamilyMode::Aware);

                // Verify field families
                assert_eq!(v2_table.field_families.len(), 3); // metrics, status, __0
                assert!(v2_table.field_families.get_by_name("metrics").is_some());
                assert!(v2_table.field_families.get_by_name("status").is_some());
                assert!(v2_table.field_families.get_by_name("__0").is_some());

                assert!(v2_table.column_definition("metrics::cpu").is_some());
                assert!(v2_table.column_definition("plain_field").is_some(),);
            }

            /// Verify auto field family mode is selected when a table has
            /// more than 100 fields in a single field family.
            #[tokio::test]
            async fn auto_mode() {
                let v1_catalog = create_v1_catalog().await;

                // Create database and table with many fields to trigger Auto mode
                v1_catalog.create_database("test_db").await.unwrap();

                // Create fields that exceed the limit for a single family
                let fields = (0..=NUM_FIELDS_PER_FAMILY_LIMIT + 5)
                    .map(|i| (format!("family::field_{i}"), FieldDataType::Float))
                    .collect::<Vec<_>>();

                v1_catalog
                    .create_table("test_db", "test_table", &["tag1"], &fields)
                    .await
                    .unwrap();

                let db_id = v1_catalog.db_name_to_id("test_db").unwrap();
                let v1_db_schema = v1_catalog.db_schema_by_id(&db_id).unwrap();
                let table_id = v1_db_schema.table_name_to_id("test_table").unwrap();

                let v2_inner = migrate(&v1_catalog.inner.read()).unwrap();

                // Verify table was migrated with Auto mode
                let v2_db = v2_inner.databases.get_by_id(&db_id).unwrap();
                let v2_table = v2_db.tables.get_by_id(&table_id).unwrap();

                assert_eq!(v2_table.field_family_mode, FieldFamilyMode::Auto);

                // Verify auto-generated field families
                assert_eq!(v2_table.field_families.len(), 2);

                // All field families are Auto
                assert!(
                    v2_table
                        .field_families
                        .resource_iter()
                        .all(|ff| matches!(ff.name, FieldFamilyName::Auto(_)))
                )
            }

            #[tokio::test]
            async fn with_all_column_types() {
                let v1_catalog = create_v1_catalog().await;

                // Create database and table with all column types
                v1_catalog.create_database("test_db").await.unwrap();
                create_test_table!(v1_catalog, "test_db", "test_table",
                    tags: ["tag1", "tag2", "tag3"],
                    fields: [
                        ("field_string", String),
                        ("field_bool", Boolean),
                        ("field_int", Integer),
                        ("field_uint", UInteger),
                        ("field_float", Float),
                    ]
                );

                let db_id = v1_catalog.db_name_to_id("test_db").unwrap();
                let v1_db_schema = v1_catalog.db_schema_by_id(&db_id).unwrap();
                let table_id = v1_db_schema.table_name_to_id("test_table").unwrap();

                let v2_inner = migrate(&v1_catalog.inner.read()).unwrap();

                // Verify all columns were migrated correctly
                let v2_db = v2_inner.databases.get_by_id(&db_id).unwrap();
                let v2_table = v2_db.tables.get_by_id(&table_id).unwrap();

                // Check timestamp column exists
                let v2_time_col = v2_table.column_definition("time");
                assert!(v2_time_col.is_some(), "Timestamp column should exist");

                // Check tag columns
                assert_eq!(v2_table.num_tag_columns(), 3);
                // Verify each tag exists
                assert!(v2_table.column_definition("tag1").is_some());
                assert!(v2_table.column_definition("tag2").is_some());
                assert!(v2_table.column_definition("tag3").is_some());

                // Check field columns
                assert_eq!(v2_table.num_field_columns(), 5);

                use InfluxColumnType::*;
                use InfluxFieldType::*;

                let col_def = v2_table.column_definition("field_string").unwrap();
                assert_matches!(col_def.column_type(), Field(String));

                let col_def = v2_table.column_definition("field_bool").unwrap();
                assert_matches!(col_def.column_type(), Field(Boolean));

                let col_def = v2_table.column_definition("field_int").unwrap();
                assert_matches!(col_def.column_type(), Field(Integer));

                let col_def = v2_table.column_definition("field_uint").unwrap();
                assert_matches!(col_def.column_type(), Field(UInteger));

                let col_def = v2_table.column_definition("field_float").unwrap();
                assert_matches!(col_def.column_type(), Field(Float));
            }

            #[tokio::test]
            async fn deleted() {
                let v1_catalog = create_v1_catalog().await;

                // Create database and table, then soft delete the table
                v1_catalog.create_database("test_db").await.unwrap();
                create_test_table!(v1_catalog, "test_db", "test_table",
                    tags: ["tag1"],
                    fields: [("field1", String)]
                );

                let db_id = v1_catalog.db_name_to_id("test_db").unwrap();
                let v1_db_schema = v1_catalog.db_schema_by_id(&db_id).unwrap();
                let table_id = v1_db_schema.table_name_to_id("test_table").unwrap();

                let hard_delete_time = Time::from_timestamp_nanos(9876543210);
                v1_catalog
                    .soft_delete_table(
                        "test_db",
                        "test_table",
                        v1::update::HardDeletionTime::Timestamp(hard_delete_time),
                    )
                    .await
                    .unwrap();

                let v2_inner = migrate(&v1_catalog.inner.read()).unwrap();

                // Verify soft deleted table was migrated
                let v2_db = v2_inner.databases.get_by_id(&db_id).unwrap();
                let v2_table = v2_db.tables.get_by_id(&table_id).unwrap();

                assert!(v2_table.deleted);
                assert_eq!(v2_table.hard_delete_time, Some(hard_delete_time));
            }

            #[tokio::test]
            async fn preserves_column_order() {
                let v1_catalog = create_v1_catalog().await;

                // Create database and table
                v1_catalog.create_database("test_db").await.unwrap();
                create_test_table!(v1_catalog, "test_db", "test_table");

                // Add columns in a specific order to test preservation
                // Each column in a separate transaction
                let cols = vec![
                    ("tag_b", FieldDataType::Tag),
                    ("field_2", FieldDataType::Float),
                    ("tag_a", FieldDataType::Tag),
                    ("field_1", FieldDataType::Integer),
                    ("tag_c", FieldDataType::Tag),
                    ("field_3", FieldDataType::String),
                ];
                for (name, data_type) in cols {
                    let mut tx = v1_catalog.begin("test_db").unwrap();
                    tx.column_or_create("test_table", name, data_type).unwrap();
                    v1_catalog.commit(tx).await.unwrap();
                }

                // Get v1 column order
                let db_id = v1_catalog.db_name_to_id("test_db").unwrap();
                let v1_db_schema = v1_catalog.db_schema_by_id(&db_id).unwrap();
                let v1_table_def = v1_db_schema.table_definition("test_table").unwrap();
                let v1_column_names: Vec<_> = v1_table_def
                    .columns
                    .resource_iter()
                    // Return the columns in their original creation order.
                    .sorted_by_key(|a| a.id)
                    .map(|col| col.name.to_string())
                    .collect();

                let v2_inner = migrate(&v1_catalog.inner.read()).unwrap();

                // Get v2 column order and verify it matches v1
                let v2_db = v2_inner.databases.get_by_id(&db_id).unwrap();
                let table_id = v1_db_schema.table_name_to_id("test_table").unwrap();
                let v2_table_def = v2_db.tables.get_by_id(&table_id).unwrap();

                // Verify series key name order has been preserved
                assert_eq!(v1_table_def.series_key_names, v2_table_def.series_key_names);

                // Verify column order is preserved
                let v2_column_names: Vec<_> = v2_table_def
                    .columns
                    .resource_iter()
                    // Return the columns in their original creation order.
                    .sorted_by_key(|a| a.ord_id())
                    .map(|col| col.name().to_string())
                    .collect();

                assert_eq!(
                    v1_column_names, v2_column_names,
                    "Column order should be preserved during migration"
                );
            }
        }

        /// Verify table last caches
        mod last_cache {
            use super::*;
            use crate::log::versions::v3::{FieldDataType, LastCacheSize, LastCacheTtl};
            use pretty_assertions::assert_matches;

            #[tokio::test]
            async fn with_explicit_columns() {
                let v1_catalog = create_v1_catalog().await;

                // Create database and table
                v1_catalog.create_database("test_db").await.unwrap();
                create_test_table!(v1_catalog, "test_db", "test_table",
                    tags: ["tag1", "tag2"],
                    fields: [
                        ("field1", String),
                        ("field2", Integer),
                        ("field3", Float),
                    ]
                );

                // Create a last cache with explicit columns
                v1_catalog
                    .create_last_cache(
                        "test_db",
                        "test_table",
                        Some("my_cache"),
                        Some(&["tag1"]),             // key columns
                        Some(&["field1", "field3"]), // value columns
                        LastCacheSize::new(10).unwrap(),
                        LastCacheTtl::from_secs(3600),
                    )
                    .await
                    .unwrap();

                let v1_db_id = v1_catalog.db_name_to_id("test_db").unwrap();
                let v1_db_schema = v1_catalog.db_schema_by_id(&v1_db_id).unwrap();
                let v1_table_def = v1_db_schema.table_definition("test_table").unwrap();
                let v1_lc_def = v1_table_def.last_caches.get_by_name("my_cache").unwrap();
                let v1_table_id = v1_table_def.table_id;

                let v2_inner = migrate(&v1_catalog.inner.read()).unwrap();

                // Verify last cache was migrated
                let v2_db = v2_inner.databases.get_by_id(&v1_db_id).unwrap();
                let v2_table = v2_db.tables.get_by_id(&v1_table_id).unwrap();

                assert_eq!(v2_table.last_caches.len(), 1);
                let v2_cache = v2_table.last_caches.get_by_id(&v1_lc_def.id).unwrap();

                // Verify cache properties
                assert_eq!(v2_cache.name.as_ref(), "my_cache");
                assert_eq!(v2_cache.table_id, v1_table_id);
                assert_eq!(v2_cache.table.as_ref(), "test_table");
                assert_eq!(v2_cache.count, lognext::LastCacheSize::new(10).unwrap());
                assert_eq!(v2_cache.ttl, lognext::LastCacheTtl::from_secs(3600));

                // Verify key columns
                assert_eq!(v2_cache.key_columns.len(), 1);
                let v2_tag1_col = v2_table.column_definition("tag1").unwrap();
                assert_eq!(v2_cache.key_columns[0], v2_tag1_col.id());

                // Verify value columns
                {
                    let col1_id = v2_table
                        .column_definition("field1")
                        .map(|c| c.id())
                        .unwrap();
                    let col2_id = v2_table
                        .column_definition("field3")
                        .map(|c| c.id())
                        .unwrap();
                    assert_matches!(&v2_cache.value_columns, lognext::LastCacheValueColumnsDef::Explicit { columns } if columns == &vec![col1_id, col2_id]);
                }
            }

            #[tokio::test]
            async fn with_all_non_key_columns() {
                let v1_catalog = create_v1_catalog().await;

                // Create database and table
                v1_catalog.create_database("test_db").await.unwrap();
                create_test_table!(v1_catalog, "test_db", "test_table",
                    tags: ["tag1", "tag2", "tag3"],
                    fields: [
                        ("field1", String),
                        ("field2", Integer),
                        ("field3", Float),
                        ("field4", Boolean),
                    ]
                );

                // Create a last cache with AllNonKeyColumns mode
                v1_catalog
                    .create_last_cache(
                        "test_db",
                        "test_table",
                        Some("all_fields_cache"),
                        Some(&["tag1", "tag2"]), // key columns
                        None as Option<&[&str]>, // None means AllNonKeyColumns
                        LastCacheSize::new(5).unwrap(),
                        LastCacheTtl::from_secs(1800),
                    )
                    .await
                    .unwrap();

                let db_id = v1_catalog.db_name_to_id("test_db").unwrap();
                let v1_db_schema = v1_catalog.db_schema_by_id(&db_id).unwrap();
                let table_id = v1_db_schema.table_name_to_id("test_table").unwrap();

                let v2_inner = migrate(&v1_catalog.inner.read()).unwrap();

                // Verify last cache was migrated
                let v2_db = v2_inner.databases.get_by_id(&db_id).unwrap();
                let v2_table = v2_db.tables.get_by_id(&table_id).unwrap();

                assert_eq!(v2_table.last_caches.len(), 1);
                let v2_cache = v2_table.last_caches.resource_iter().next().unwrap();

                // Verify cache properties
                assert_eq!(v2_cache.name.as_ref(), "all_fields_cache");
                assert_eq!(v2_cache.count, lognext::LastCacheSize::new(5).unwrap());
                assert_eq!(v2_cache.ttl, lognext::LastCacheTtl::from_secs(1800));

                // Verify key columns
                assert_eq!(v2_cache.key_columns.len(), 2);
                let v2_tag1_col = v2_table.column_definition("tag1").unwrap();
                let v2_tag2_col = v2_table.column_definition("tag2").unwrap();
                assert!(v2_cache.key_columns.contains(&v2_tag1_col.id()));
                assert!(v2_cache.key_columns.contains(&v2_tag2_col.id()));

                assert_matches!(
                    v2_cache.value_columns,
                    lognext::LastCacheValueColumnsDef::AllNonKeyColumns
                );
            }

            #[tokio::test]
            async fn multiple_last_caches_per_table() {
                let v1_catalog = create_v1_catalog().await;

                // Create database and table
                v1_catalog.create_database("test_db").await.unwrap();
                create_test_table!(v1_catalog, "test_db", "test_table",
                    tags: ["tag1", "tag2", "tag3"],
                    fields: [
                        ("field1", Integer),
                        ("field2", Float),
                        ("field3", String),
                        ("field4", Boolean),
                    ]
                );

                // Create multiple last caches with different configurations
                v1_catalog
                    .create_last_cache(
                        "test_db",
                        "test_table",
                        Some("cache1"),
                        Some(&["tag1"]),
                        Some(&["field1", "field2"]),
                        LastCacheSize::new(100).unwrap(),
                        LastCacheTtl::from_secs(7200),
                    )
                    .await
                    .unwrap();

                v1_catalog
                    .create_last_cache(
                        "test_db",
                        "test_table",
                        Some("cache2"),
                        Some(&["tag2", "tag3"]),
                        None as Option<&[&str]>, // AllNonKeyColumns
                        LastCacheSize::new(50).unwrap(),
                        LastCacheTtl::from_secs(3600),
                    )
                    .await
                    .unwrap();

                v1_catalog
                    .create_last_cache(
                        "test_db",
                        "test_table",
                        Some("cache3"),
                        Some(&["tag1", "tag2", "tag3"]),
                        Some(&["field4"]),
                        LastCacheSize::new(1).unwrap(),
                        LastCacheTtl::from_secs(3600),
                    )
                    .await
                    .unwrap();

                let db_id = v1_catalog.db_name_to_id("test_db").unwrap();
                let v1_db_schema = v1_catalog.db_schema_by_id(&db_id).unwrap();
                let table_id = v1_db_schema.table_name_to_id("test_table").unwrap();

                let v2_inner = migrate(&v1_catalog.inner.read()).unwrap();

                // Verify all last caches were migrated
                let v2_db = v2_inner.databases.get_by_id(&db_id).unwrap();
                let v2_table = v2_db.tables.get_by_id(&table_id).unwrap();

                assert_eq!(v2_table.last_caches.len(), 3);

                let v2_cache1 = v2_table.last_caches.get_by_name("cache1").unwrap();
                assert_eq!(v2_cache1.count, lognext::LastCacheSize::new(100).unwrap());
                assert_eq!(v2_cache1.ttl, lognext::LastCacheTtl::from_secs(7200));
                assert_eq!(v2_cache1.key_columns.len(), 1);
                match &v2_cache1.value_columns {
                    lognext::LastCacheValueColumnsDef::Explicit { columns } => {
                        assert_eq!(columns.len(), 2);
                    }
                    _ => panic!("Expected explicit columns for cache1"),
                }

                let v2_cache2 = v2_table.last_caches.get_by_name("cache2").unwrap();
                assert_eq!(v2_cache2.count, lognext::LastCacheSize::new(50).unwrap());
                assert_eq!(v2_cache2.ttl, lognext::LastCacheTtl::from_secs(3600));
                assert_eq!(v2_cache2.key_columns.len(), 2);
                assert_matches!(
                    v2_cache2.value_columns,
                    lognext::LastCacheValueColumnsDef::AllNonKeyColumns
                );

                let v2_cache3 = v2_table.last_caches.get_by_name("cache3").unwrap();
                assert_eq!(v2_cache3.count, lognext::LastCacheSize::new(1).unwrap());
                assert_eq!(v2_cache3.ttl, lognext::LastCacheTtl::from_secs(3600));
                assert_eq!(v2_cache3.key_columns.len(), 3);
                assert_matches!(&v2_cache3.value_columns, lognext::LastCacheValueColumnsDef::Explicit { columns } if columns.len() == 1);
            }
        }

        /// Verify table distinct caches are migrated
        mod distinct_cache {
            use super::*;
            use crate::log::versions::{v3, v4};

            #[tokio::test]
            async fn basic_distinct_cache() {
                let v1_catalog = create_v1_catalog().await;

                // Create database and table
                v1_catalog.create_database("test_db").await.unwrap();
                create_test_table!(v1_catalog, "test_db", "test_table",
                    tags: ["tag1", "tag2", "tag3"],
                    fields: [
                        ("field1", String),
                        ("field2", Integer),
                    ]
                );

                // Create a distinct cache
                v1_catalog
                    .create_distinct_cache(
                        "test_db",
                        "test_table",
                        Some("my_distinct_cache"),
                        &["tag1", "tag2"],
                        v3::MaxCardinality::from_usize_unchecked(1000),
                        v3::MaxAge::from_secs(3600),
                    )
                    .await
                    .unwrap();

                let db_id = v1_catalog.db_name_to_id("test_db").unwrap();
                let v1_db_schema = v1_catalog.db_schema_by_id(&db_id).unwrap();
                let table_id = v1_db_schema.table_name_to_id("test_table").unwrap();
                let v1_table_def = v1_db_schema.table_definition("test_table").unwrap();
                let v1_dc_def = v1_table_def
                    .distinct_caches
                    .get_by_name("my_distinct_cache")
                    .unwrap();

                let v2_inner = migrate(&v1_catalog.inner.read()).unwrap();

                // Verify distinct cache was migrated
                let v2_db = v2_inner.databases.get_by_id(&db_id).unwrap();
                let v2_table = v2_db.tables.get_by_id(&table_id).unwrap();

                assert_eq!(v2_table.distinct_caches.len(), 1);
                let v2_cache = v2_table
                    .distinct_caches
                    .get_by_id(&v1_dc_def.cache_id)
                    .unwrap();

                // Verify cache properties
                assert_eq!(v2_cache.cache_name.as_ref(), "my_distinct_cache");
                assert_eq!(v2_cache.table_id, table_id);
                assert_eq!(v2_cache.table_name.as_ref(), "test_table");
                assert_eq!(
                    v2_cache.max_cardinality,
                    v4::MaxCardinality::from_usize_unchecked(1000)
                );
                assert_eq!(v2_cache.max_age_seconds, v4::MaxAge::from_secs(3600));

                // Verify columns
                let v2_tag1_id = v2_table.column_definition("tag1").map(|t| t.id()).unwrap();
                let v2_tag2_id = v2_table.column_definition("tag2").map(|t| t.id()).unwrap();
                assert_eq!(v2_cache.column_ids, vec![v2_tag1_id, v2_tag2_id]);
            }

            #[tokio::test]
            async fn multiple_distinct_caches() {
                let v1_catalog = create_v1_catalog().await;

                // Create database and table
                v1_catalog.create_database("test_db").await.unwrap();
                create_test_table!(v1_catalog, "test_db", "test_table",
                    tags: ["tag1", "tag2", "tag3", "tag4"],
                    fields: [
                        ("field1", String),
                        ("field2", Integer),
                        ("field3", Float),
                    ]
                );

                // Create multiple distinct caches with different configurations
                v1_catalog
                    .create_distinct_cache(
                        "test_db",
                        "test_table",
                        Some("cache1"),
                        &["tag1"],
                        v3::MaxCardinality::from_usize_unchecked(100),
                        v3::MaxAge::from_secs(3600),
                    )
                    .await
                    .unwrap();

                v1_catalog
                    .create_distinct_cache(
                        "test_db",
                        "test_table",
                        Some("cache2"),
                        &["tag2", "tag3"],
                        v3::MaxCardinality::from_usize_unchecked(5000),
                        v3::MaxAge::from_secs(7200),
                    )
                    .await
                    .unwrap();

                v1_catalog
                    .create_distinct_cache(
                        "test_db",
                        "test_table",
                        Some("cache3"),
                        &["tag1", "tag2", "tag3", "tag4"],
                        v3::MaxCardinality::from_usize_unchecked(10000),
                        v3::MaxAge::from_secs(86400), // 1 day
                    )
                    .await
                    .unwrap();

                let db_id = v1_catalog.db_name_to_id("test_db").unwrap();
                let v1_db_schema = v1_catalog.db_schema_by_id(&db_id).unwrap();
                let table_id = v1_db_schema.table_name_to_id("test_table").unwrap();

                let v2_inner = migrate(&v1_catalog.inner.read()).unwrap();

                // Verify all distinct caches were migrated
                let v2_db = v2_inner.databases.get_by_id(&db_id).unwrap();
                let v2_table = v2_db.tables.get_by_id(&table_id).unwrap();

                assert_eq!(v2_table.distinct_caches.len(), 3);

                // Find and verify each cache by name
                let v2_cache1 = v2_table.distinct_caches.get_by_name("cache1").unwrap();
                assert_eq!(
                    v2_cache1.max_cardinality,
                    v4::MaxCardinality::from_usize_unchecked(100)
                );
                assert_eq!(v2_cache1.max_age_seconds, v4::MaxAge::from_secs(3600));
                assert_eq!(v2_cache1.column_ids.len(), 1);

                let v2_cache2 = v2_table.distinct_caches.get_by_name("cache2").unwrap();
                assert_eq!(
                    v2_cache2.max_cardinality,
                    v4::MaxCardinality::from_usize_unchecked(5000)
                );
                assert_eq!(v2_cache2.max_age_seconds, v4::MaxAge::from_secs(7200));
                assert_eq!(v2_cache2.column_ids.len(), 2);

                let v2_cache3 = v2_table.distinct_caches.get_by_name("cache3").unwrap();
                assert_eq!(
                    v2_cache3.max_cardinality,
                    v4::MaxCardinality::from_usize_unchecked(10000)
                );
                assert_eq!(v2_cache3.max_age_seconds, v4::MaxAge::from_secs(86400));
                assert_eq!(v2_cache3.column_ids.len(), 4);
            }

            #[tokio::test]
            async fn with_complex_column_mapping() {
                let v1_catalog = create_v1_catalog().await;

                // Create database and table
                v1_catalog.create_database("test_db").await.unwrap();
                create_test_table!(v1_catalog, "test_db", "test_table",
                    tags: ["tag1", "tag2", "tag3"],
                    fields: [
                        ("field1", Integer),
                        ("field2", Float),
                        ("field3", String),
                    ]
                );

                // Add more columns in non-sequential order to create gaps in column IDs
                {
                    let mut txn = v1_catalog.begin("test_db").unwrap();
                    txn.column_or_create("test_table", "tag5", FieldDataType::Tag)
                        .unwrap();
                    txn.column_or_create("test_table", "tag4", FieldDataType::Tag)
                        .unwrap();
                    v1_catalog.commit(txn).await.unwrap();
                }

                // Create distinct cache with columns that have non-sequential IDs
                v1_catalog
                    .create_distinct_cache(
                        "test_db",
                        "test_table",
                        Some("complex_mapping_cache"),
                        &["tag3", "tag1", "tag5"], // Non-sequential tag order
                        v3::MaxCardinality::from_usize_unchecked(50000),
                        v3::MaxAge::from_secs(10800),
                    )
                    .await
                    .unwrap();

                let db_id = v1_catalog.db_name_to_id("test_db").unwrap();
                let v1_db_schema = v1_catalog.db_schema_by_id(&db_id).unwrap();
                let table_id = v1_db_schema.table_name_to_id("test_table").unwrap();

                let v2_inner = migrate(&v1_catalog.inner.read()).unwrap();

                // Verify distinct cache was migrated with correct column mappings
                let v2_db = v2_inner.databases.get_by_id(&db_id).unwrap();
                let v2_table = v2_db.tables.get_by_id(&table_id).unwrap();

                assert_eq!(v2_table.distinct_caches.len(), 1);

                let v2_cache = v2_table
                    .distinct_caches
                    .get_by_name("complex_mapping_cache")
                    .unwrap();

                // Verify column mapping
                assert_eq!(v2_cache.column_ids.len(), 3);
                let v2_tag3_col = v2_table.column_definition("tag3").unwrap();
                let v2_tag1_col = v2_table.column_definition("tag1").unwrap();
                let v2_tag5_col = v2_table.column_definition("tag5").unwrap();
                assert_eq!(v2_cache.column_ids[0], v2_tag3_col.id());
                assert_eq!(v2_cache.column_ids[1], v2_tag1_col.id());
                assert_eq!(v2_cache.column_ids[2], v2_tag5_col.id());
            }
        }
    }

    /// Tests related to processing engine triggers migration
    mod processing_engine_triggers {
        use super::*;
        use crate::log::versions::v3::{TriggerSettings, ValidPluginFilename};
        use pretty_assertions::assert_matches;

        #[tokio::test]
        async fn multiple_trigger_types() {
            let v1_catalog = create_v1_catalog().await;

            // Create database
            v1_catalog.create_database("test_db").await.unwrap();

            // Add various trigger types
            let triggers = vec![
                ("single_wal", "table:my_table"),
                ("all_wal", "all_tables"),
                ("schedule", "cron:0 * * * * *"),
                ("request", "request:/api/trigger"),
            ];

            for (name, spec) in triggers {
                v1_catalog
                    .create_processing_engine_trigger(
                        "test_db",
                        name,
                        ValidPluginFilename::from_validated_name(&format!("{name}.wasm")),
                        "test-node".into(),
                        spec,
                        TriggerSettings::default(),
                        &None,
                        false,
                    )
                    .await
                    .expect("trigger was created");
            }

            let db_id = v1_catalog.db_name_to_id("test_db").unwrap();
            let v2_inner = migrate(&v1_catalog.inner.read()).unwrap();

            // Verify all triggers were migrated
            let v2_db = v2_inner.databases.get_by_id(&db_id).unwrap();
            assert_eq!(v2_db.processing_engine_triggers.len(), 4);

            // Verify trigger types
            let v2_single_wal = v2_db
                .processing_engine_triggers
                .get_by_name("single_wal")
                .unwrap();
            assert_matches!(&v2_single_wal.trigger, lognext::TriggerSpecificationDefinition::SingleTableWalWrite { table_name } if table_name == "my_table");

            let v2_all_wal = v2_db
                .processing_engine_triggers
                .get_by_name("all_wal")
                .unwrap();
            assert_matches!(
                &v2_all_wal.trigger,
                lognext::TriggerSpecificationDefinition::AllTablesWalWrite
            );

            let v2_schedule = v2_db
                .processing_engine_triggers
                .get_by_name("schedule")
                .unwrap();
            assert_matches!(&v2_schedule.trigger, lognext::TriggerSpecificationDefinition::Schedule { schedule } if schedule == "0 * * * * *");

            let v2_request = v2_db
                .processing_engine_triggers
                .get_by_name("request")
                .unwrap();
            assert_matches!(&v2_request.trigger, lognext::TriggerSpecificationDefinition::RequestPath { path } if path == "/api/trigger");
        }

        #[tokio::test]
        async fn trigger_with_settings() {
            let v1_catalog = create_v1_catalog().await;

            // Create database
            v1_catalog.create_database("test_db").await.unwrap();

            // Create trigger with custom settings
            v1_catalog
                .create_processing_engine_trigger(
                    "test_db",
                    "configured_trigger",
                    ValidPluginFilename::from_validated_name("configured.wasm"),
                    "test-node".into(),
                    "all_tables",
                    TriggerSettings {
                        run_async: true,
                        error_behavior: logprev::ErrorBehavior::Retry,
                    },
                    &None,
                    false,
                )
                .await
                .unwrap();

            let db_id = v1_catalog.db_name_to_id("test_db").unwrap();
            let v2_inner = migrate(&v1_catalog.inner.read()).unwrap();

            // Verify trigger was migrated with settings
            let v2_db = v2_inner.databases.get_by_id(&db_id).unwrap();
            let v2_trigger = v2_db
                .processing_engine_triggers
                .get_by_name("configured_trigger")
                .unwrap();

            assert!(v2_trigger.trigger_settings.run_async);
            assert_matches!(
                v2_trigger.trigger_settings.error_behavior,
                lognext::ErrorBehavior::Retry
            );
        }
    }

    mod catalog_snapshot {
        use super::*;
        use crate::catalog::versions::v1::HardDeletionTime;
        use crate::log::versions::v3;
        use crate::log::versions::v3::{LastCacheSize, LastCacheTtl, NodeMode};

        #[tokio::test]
        async fn verify_snapshot() {
            let v1_catalog = create_v1_catalog().await;

            v1_catalog.create_database("db_1").await.unwrap();

            {
                v1_catalog.create_admin_token(false).await.unwrap();

                v1_catalog
                    .create_named_admin_token_with_permission("my_admin".into(), Some(500))
                    .await
                    .unwrap();
            }

            {
                let process_uuid_getter: Arc<dyn influxdb3_process::ProcessUuidGetter> =
                    Arc::new(influxdb3_process::ProcessUuidWrapper::new());

                // Register nodes
                v1_catalog
                    .register_node(
                        "node-1",
                        1,
                        vec![NodeMode::Core],
                        Arc::clone(&process_uuid_getter),
                        Arc::from("test-instance-node-1"),
                    )
                    .await
                    .unwrap();
            }

            {
                // Create table
                create_test_table!(v1_catalog, "db_1", "test_table",
                    tags: ["tag1", "tag2", "tag3"],
                    fields: [
                        ("ff1::field1", String),
                        ("ff1::field2", Integer),
                        ("field3", Float),
                        ("field4", Boolean),
                    ]
                );

                // Create a last cache with AllNonKeyColumns mode
                v1_catalog
                    .create_last_cache(
                        "db_1",
                        "test_table",
                        Some("all_fields_cache"),
                        Some(&["tag1", "tag2"]), // key columns
                        None as Option<&[&str]>, // None means AllNonKeyColumns
                        LastCacheSize::new(5).unwrap(),
                        LastCacheTtl::from_secs(1800),
                    )
                    .await
                    .unwrap();

                // Create a distinct cache
                v1_catalog
                    .create_distinct_cache(
                        "db_1",
                        "test_table",
                        Some("my_distinct_cache"),
                        &["tag1", "tag2"],
                        v3::MaxCardinality::from_usize_unchecked(1000),
                        v3::MaxAge::from_secs(3600),
                    )
                    .await
                    .unwrap();
            }

            // Create a table with arbitrary column order, to produce a
            // series_key of [tag_b,tag_a,tag_c]
            {
                create_test_table!(v1_catalog, "db_1", "arbitrary");

                // Add columns in a specific order to test preservation
                // Each column in a separate transaction
                let cols = vec![
                    ("tag_b", FieldDataType::Tag),
                    ("field_2", FieldDataType::Float),
                    ("tag_a", FieldDataType::Tag),
                    ("field_1", FieldDataType::Integer),
                    ("tag_c", FieldDataType::Tag),
                    ("field_3", FieldDataType::String),
                ];
                for (name, data_type) in cols {
                    let mut tx = v1_catalog.begin("db_1").unwrap();
                    tx.column_or_create("arbitrary", name, data_type).unwrap();
                    v1_catalog.commit(tx).await.unwrap();
                }
            }

            {
                v1_catalog.create_database("db_2").await.unwrap();

                create_test_table!(v1_catalog, "db_2", "table_1",
                    tags: ["tag1", "tag3"],
                    fields: [
                        ("ff1::field1", String),
                        ("ff1::field2", Integer),
                        ("field4", Boolean),
                    ]
                );

                create_test_table!(v1_catalog, "db_2", "table_2",
                    tags: ["tag1", "tag3"],
                    fields: [
                        ("ff1::field1", String),
                        ("ff1::field2", Integer),
                        ("field4", Boolean),
                    ]
                );

                create_test_table!(v1_catalog, "db_2", "table_3",
                    tags: ["tag1", "tag3"],
                    fields: [
                        ("ff1::field1", String),
                        ("ff1::field2", Integer),
                        ("field4", Boolean),
                    ]
                );

                v1_catalog
                    .soft_delete_table("db_2", "table_1", HardDeletionTime::Now)
                    .await
                    .unwrap();

                // Hard delete a table, so that we can verify the next_id is still correctly set.
                let db = v1_catalog.db_schema("db_2").unwrap();
                let tb_id = db.table_name_to_id("table_3").unwrap();
                v1_catalog.hard_delete_table(&db.id, &tb_id).await.unwrap();
            }

            // Ensure all state is committed
            v1_catalog.update_from_snapshot(v1_catalog.snapshot());

            let v2_catalog = migrate(&v1_catalog.inner.read()).unwrap();

            // Verify snapshot still matches after reload
            insta::assert_json_snapshot!("verify snapshot", v2_catalog.snapshot(), {
                ".catalog_uuid" => "[uuid]",
                ".nodes.repo[][1].instance_id" => "[uuid]",
                ".nodes.repo[][1].process_uuids[]" => "[uuid]",
                ".tokens.repo[][1].hash" => "[hash]",
            });
        }
    }
}
