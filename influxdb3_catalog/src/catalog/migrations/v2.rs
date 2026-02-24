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
mod tests;
