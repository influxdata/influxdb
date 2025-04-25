use uuid::Uuid;

use crate::log::versions::v2;

impl From<super::OrderedCatalogBatch> for v2::OrderedCatalogBatch {
    fn from(value: super::OrderedCatalogBatch) -> Self {
        Self {
            catalog_batch: value.catalog_batch.into(),
            sequence_number: value.sequence_number,
        }
    }
}

impl From<super::CatalogBatch> for v2::CatalogBatch {
    fn from(value: super::CatalogBatch) -> Self {
        match value {
            super::CatalogBatch::Node(node_batch) => v2::CatalogBatch::Node(node_batch.into()),
            super::CatalogBatch::Database(database_batch) => {
                v2::CatalogBatch::Database(database_batch.into())
            }
        }
    }
}

impl From<super::NodeBatch> for v2::NodeBatch {
    fn from(value: super::NodeBatch) -> Self {
        Self {
            time_ns: value.time_ns,
            node_catalog_id: value.node_catalog_id,
            node_id: value.node_id,
            ops: value.ops.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<super::NodeCatalogOp> for v2::NodeCatalogOp {
    fn from(value: super::NodeCatalogOp) -> Self {
        match value {
            super::NodeCatalogOp::RegisterNode(register_node_log) => {
                v2::NodeCatalogOp::RegisterNode(register_node_log.into())
            }
        }
    }
}

impl From<super::RegisterNodeLog> for v2::RegisterNodeLog {
    fn from(value: super::RegisterNodeLog) -> Self {
        Self {
            node_id: value.node_id,
            instance_id: value.instance_id,
            registered_time_ns: value.registered_time_ns,
            core_count: value.core_count,
            mode: value.mode.into_iter().map(Into::into).collect(),
            // NB: just default to a new random UUID, this is no different than if an actual
            // process's UUID was used, which is also random.
            process_uuid: Uuid::new_v4(),
        }
    }
}

impl From<super::NodeMode> for v2::NodeMode {
    fn from(value: super::NodeMode) -> Self {
        match value {
            super::NodeMode::Core => v2::NodeMode::Core,
        }
    }
}

impl From<super::DatabaseBatch> for v2::DatabaseBatch {
    fn from(value: super::DatabaseBatch) -> Self {
        Self {
            time_ns: value.time_ns,
            database_id: value.database_id,
            database_name: value.database_name,
            ops: value.ops.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<super::DatabaseCatalogOp> for v2::DatabaseCatalogOp {
    fn from(value: super::DatabaseCatalogOp) -> Self {
        match value {
            super::DatabaseCatalogOp::CreateDatabase(create_database_log) => {
                v2::DatabaseCatalogOp::CreateDatabase(create_database_log.into())
            }
            super::DatabaseCatalogOp::SoftDeleteDatabase(soft_delete_database_log) => {
                v2::DatabaseCatalogOp::SoftDeleteDatabase(soft_delete_database_log.into())
            }
            super::DatabaseCatalogOp::CreateTable(create_table_log) => {
                v2::DatabaseCatalogOp::CreateTable(create_table_log.into())
            }
            super::DatabaseCatalogOp::SoftDeleteTable(soft_delete_table_log) => {
                v2::DatabaseCatalogOp::SoftDeleteTable(soft_delete_table_log.into())
            }
            super::DatabaseCatalogOp::AddFields(add_fields_log) => {
                v2::DatabaseCatalogOp::AddFields(add_fields_log.into())
            }
            super::DatabaseCatalogOp::CreateDistinctCache(distinct_cache_definition) => {
                v2::DatabaseCatalogOp::CreateDistinctCache(distinct_cache_definition.into())
            }
            super::DatabaseCatalogOp::DeleteDistinctCache(delete_distinct_cache_log) => {
                v2::DatabaseCatalogOp::DeleteDistinctCache(delete_distinct_cache_log.into())
            }
            super::DatabaseCatalogOp::CreateLastCache(last_cache_definition) => {
                v2::DatabaseCatalogOp::CreateLastCache(last_cache_definition.into())
            }
            super::DatabaseCatalogOp::DeleteLastCache(delete_last_cache_log) => {
                v2::DatabaseCatalogOp::DeleteLastCache(delete_last_cache_log.into())
            }
            super::DatabaseCatalogOp::CreateTrigger(trigger_definition) => {
                v2::DatabaseCatalogOp::CreateTrigger(trigger_definition.into())
            }
            super::DatabaseCatalogOp::DeleteTrigger(delete_trigger_log) => {
                v2::DatabaseCatalogOp::DeleteTrigger(delete_trigger_log.into())
            }
            super::DatabaseCatalogOp::EnableTrigger(trigger_identifier) => {
                v2::DatabaseCatalogOp::EnableTrigger(trigger_identifier.into())
            }
            super::DatabaseCatalogOp::DisableTrigger(trigger_identifier) => {
                v2::DatabaseCatalogOp::DisableTrigger(trigger_identifier.into())
            }
        }
    }
}

impl From<super::CreateDatabaseLog> for v2::CreateDatabaseLog {
    fn from(value: super::CreateDatabaseLog) -> Self {
        Self {
            database_id: value.database_id,
            database_name: value.database_name,
        }
    }
}

impl From<super::SoftDeleteDatabaseLog> for v2::SoftDeleteDatabaseLog {
    fn from(value: super::SoftDeleteDatabaseLog) -> Self {
        Self {
            database_id: value.database_id,
            database_name: value.database_name,
            deletion_time: value.deletion_time,
        }
    }
}

impl From<super::CreateTableLog> for v2::CreateTableLog {
    fn from(value: super::CreateTableLog) -> Self {
        Self {
            database_id: value.database_id,
            database_name: value.database_name,
            table_name: value.table_name,
            table_id: value.table_id,
            field_definitions: value
                .field_definitions
                .into_iter()
                .map(Into::into)
                .collect(),
            key: value.key,
        }
    }
}

impl From<super::FieldDefinition> for v2::FieldDefinition {
    fn from(value: super::FieldDefinition) -> Self {
        Self {
            name: value.name,
            id: value.id,
            data_type: value.data_type.into(),
        }
    }
}

impl From<super::FieldDataType> for v2::FieldDataType {
    fn from(value: super::FieldDataType) -> Self {
        match value {
            super::FieldDataType::String => v2::FieldDataType::String,
            super::FieldDataType::Integer => v2::FieldDataType::Integer,
            super::FieldDataType::UInteger => v2::FieldDataType::UInteger,
            super::FieldDataType::Float => v2::FieldDataType::Float,
            super::FieldDataType::Boolean => v2::FieldDataType::Boolean,
            super::FieldDataType::Timestamp => v2::FieldDataType::Timestamp,
            super::FieldDataType::Tag => v2::FieldDataType::Tag,
        }
    }
}

impl From<super::SoftDeleteTableLog> for v2::SoftDeleteTableLog {
    fn from(value: super::SoftDeleteTableLog) -> Self {
        Self {
            database_id: value.database_id,
            database_name: value.database_name,
            table_id: value.table_id,
            table_name: value.table_name,
            deletion_time: value.deletion_time,
        }
    }
}

impl From<super::AddFieldsLog> for v2::AddFieldsLog {
    fn from(value: super::AddFieldsLog) -> Self {
        Self {
            database_name: value.database_name,
            database_id: value.database_id,
            table_name: value.table_name,
            table_id: value.table_id,
            field_definitions: value
                .field_definitions
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl From<super::DistinctCacheDefinition> for v2::DistinctCacheDefinition {
    fn from(value: super::DistinctCacheDefinition) -> Self {
        Self {
            table_id: value.table_id,
            table_name: value.table_name,
            cache_id: value.cache_id,
            cache_name: value.cache_name,
            column_ids: value.column_ids,
            max_cardinality: value.max_cardinality.into(),
            max_age_seconds: value.max_age_seconds.into(),
        }
    }
}

impl From<super::MaxCardinality> for v2::MaxCardinality {
    fn from(value: super::MaxCardinality) -> Self {
        Self::from(value.0)
    }
}

impl From<super::MaxAge> for v2::MaxAge {
    fn from(value: super::MaxAge) -> Self {
        Self(value.0)
    }
}

impl From<super::DeleteDistinctCacheLog> for v2::DeleteDistinctCacheLog {
    fn from(value: super::DeleteDistinctCacheLog) -> Self {
        Self {
            table_id: value.table_id,
            table_name: value.table_name,
            cache_id: value.cache_id,
            cache_name: value.cache_name,
        }
    }
}

impl From<super::LastCacheDefinition> for v2::LastCacheDefinition {
    fn from(value: super::LastCacheDefinition) -> Self {
        Self {
            table_id: value.table_id,
            table: value.table,
            id: value.id,
            name: value.name,
            key_columns: value.key_columns,
            value_columns: value.value_columns.into(),
            count: value.count.into(),
            ttl: value.ttl.into(),
        }
    }
}

impl From<super::LastCacheValueColumnsDef> for v2::LastCacheValueColumnsDef {
    fn from(value: super::LastCacheValueColumnsDef) -> Self {
        match value {
            super::LastCacheValueColumnsDef::Explicit { columns } => {
                v2::LastCacheValueColumnsDef::Explicit { columns }
            }
            super::LastCacheValueColumnsDef::AllNonKeyColumns => {
                v2::LastCacheValueColumnsDef::AllNonKeyColumns
            }
        }
    }
}

impl From<super::LastCacheSize> for v2::LastCacheSize {
    fn from(value: super::LastCacheSize) -> Self {
        Self(value.0)
    }
}

impl From<super::LastCacheTtl> for v2::LastCacheTtl {
    fn from(value: super::LastCacheTtl) -> Self {
        Self(value.0)
    }
}

impl From<super::DeleteLastCacheLog> for v2::DeleteLastCacheLog {
    fn from(value: super::DeleteLastCacheLog) -> Self {
        Self {
            table_id: value.table_id,
            table_name: value.table_name,
            id: value.id,
            name: value.name,
        }
    }
}

impl From<super::TriggerDefinition> for v2::TriggerDefinition {
    fn from(value: super::TriggerDefinition) -> Self {
        Self {
            trigger_id: value.trigger_id,
            trigger_name: value.trigger_name,
            plugin_filename: value.plugin_filename,
            database_name: value.database_name,
            node_id: value.node_id,
            trigger: value.trigger.into(),
            trigger_settings: value.trigger_settings.into(),
            trigger_arguments: value.trigger_arguments,
            disabled: value.disabled,
        }
    }
}

impl From<super::TriggerSpecificationDefinition> for v2::TriggerSpecificationDefinition {
    fn from(value: super::TriggerSpecificationDefinition) -> Self {
        match value {
            super::TriggerSpecificationDefinition::SingleTableWalWrite { table_name } => {
                v2::TriggerSpecificationDefinition::SingleTableWalWrite { table_name }
            }
            super::TriggerSpecificationDefinition::AllTablesWalWrite => {
                v2::TriggerSpecificationDefinition::AllTablesWalWrite
            }
            super::TriggerSpecificationDefinition::Schedule { schedule } => {
                v2::TriggerSpecificationDefinition::Schedule { schedule }
            }
            super::TriggerSpecificationDefinition::RequestPath { path } => {
                v2::TriggerSpecificationDefinition::RequestPath { path }
            }
            super::TriggerSpecificationDefinition::Every { duration } => {
                v2::TriggerSpecificationDefinition::Every { duration }
            }
        }
    }
}

impl From<super::TriggerSettings> for v2::TriggerSettings {
    fn from(value: super::TriggerSettings) -> Self {
        Self {
            run_async: value.run_async,
            error_behavior: value.error_behavior.into(),
        }
    }
}

impl From<super::ErrorBehavior> for v2::ErrorBehavior {
    fn from(value: super::ErrorBehavior) -> Self {
        match value {
            super::ErrorBehavior::Log => v2::ErrorBehavior::Log,
            super::ErrorBehavior::Retry => v2::ErrorBehavior::Retry,
            super::ErrorBehavior::Disable => v2::ErrorBehavior::Disable,
        }
    }
}

impl From<super::DeleteTriggerLog> for v2::DeleteTriggerLog {
    fn from(value: super::DeleteTriggerLog) -> Self {
        Self {
            trigger_id: value.trigger_id,
            trigger_name: value.trigger_name,
            force: value.force,
        }
    }
}

impl From<super::TriggerIdentifier> for v2::TriggerIdentifier {
    fn from(value: super::TriggerIdentifier) -> Self {
        Self {
            db_id: value.db_id,
            db_name: value.db_name,
            trigger_id: value.trigger_id,
            trigger_name: value.trigger_name,
        }
    }
}
