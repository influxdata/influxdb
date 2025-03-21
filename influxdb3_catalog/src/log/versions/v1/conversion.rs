use crate::log::versions::latest;

impl From<super::OrderedCatalogBatch> for latest::OrderedCatalogBatch {
    fn from(value: super::OrderedCatalogBatch) -> Self {
        Self {
            catalog_batch: value.catalog_batch.into(),
            sequence_number: value.sequence_number,
        }
    }
}

impl From<super::CatalogBatch> for latest::CatalogBatch {
    fn from(value: super::CatalogBatch) -> Self {
        match value {
            super::CatalogBatch::Node(node_batch) => latest::CatalogBatch::Node(node_batch.into()),
            super::CatalogBatch::Database(database_batch) => {
                latest::CatalogBatch::Database(database_batch.into())
            }
        }
    }
}

impl From<super::NodeBatch> for latest::NodeBatch {
    fn from(value: super::NodeBatch) -> Self {
        Self {
            time_ns: value.time_ns,
            node_catalog_id: value.node_catalog_id,
            node_id: value.node_id,
            ops: value.ops.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<super::NodeCatalogOp> for latest::NodeCatalogOp {
    fn from(value: super::NodeCatalogOp) -> Self {
        match value {
            super::NodeCatalogOp::RegisterNode(register_node_log) => {
                latest::NodeCatalogOp::RegisterNode(register_node_log.into())
            }
        }
    }
}

impl From<super::RegisterNodeLog> for latest::RegisterNodeLog {
    fn from(value: super::RegisterNodeLog) -> Self {
        Self {
            node_id: value.node_id,
            instance_id: value.instance_id,
            registered_time_ns: value.registered_time_ns,
            core_count: value.core_count,
            mode: value.mode.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<super::NodeMode> for latest::NodeMode {
    fn from(value: super::NodeMode) -> Self {
        match value {
            super::NodeMode::Core => latest::NodeMode::Core,
        }
    }
}

impl From<super::DatabaseBatch> for latest::DatabaseBatch {
    fn from(value: super::DatabaseBatch) -> Self {
        Self {
            time_ns: value.time_ns,
            database_id: value.database_id,
            database_name: value.database_name,
            ops: value.ops.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<super::DatabaseCatalogOp> for latest::DatabaseCatalogOp {
    fn from(value: super::DatabaseCatalogOp) -> Self {
        match value {
            super::DatabaseCatalogOp::CreateDatabase(create_database_log) => {
                latest::DatabaseCatalogOp::CreateDatabase(create_database_log.into())
            }
            super::DatabaseCatalogOp::SoftDeleteDatabase(soft_delete_database_log) => {
                latest::DatabaseCatalogOp::SoftDeleteDatabase(soft_delete_database_log.into())
            }
            super::DatabaseCatalogOp::CreateTable(create_table_log) => {
                latest::DatabaseCatalogOp::CreateTable(create_table_log.into())
            }
            super::DatabaseCatalogOp::SoftDeleteTable(soft_delete_table_log) => {
                latest::DatabaseCatalogOp::SoftDeleteTable(soft_delete_table_log.into())
            }
            super::DatabaseCatalogOp::AddFields(add_fields_log) => {
                latest::DatabaseCatalogOp::AddFields(add_fields_log.into())
            }
            super::DatabaseCatalogOp::CreateDistinctCache(distinct_cache_definition) => {
                latest::DatabaseCatalogOp::CreateDistinctCache(distinct_cache_definition.into())
            }
            super::DatabaseCatalogOp::DeleteDistinctCache(delete_distinct_cache_log) => {
                latest::DatabaseCatalogOp::DeleteDistinctCache(delete_distinct_cache_log.into())
            }
            super::DatabaseCatalogOp::CreateLastCache(last_cache_definition) => {
                latest::DatabaseCatalogOp::CreateLastCache(last_cache_definition.into())
            }
            super::DatabaseCatalogOp::DeleteLastCache(delete_last_cache_log) => {
                latest::DatabaseCatalogOp::DeleteLastCache(delete_last_cache_log.into())
            }
            super::DatabaseCatalogOp::CreateTrigger(trigger_definition) => {
                latest::DatabaseCatalogOp::CreateTrigger(trigger_definition.into())
            }
            super::DatabaseCatalogOp::DeleteTrigger(delete_trigger_log) => {
                latest::DatabaseCatalogOp::DeleteTrigger(delete_trigger_log.into())
            }
            super::DatabaseCatalogOp::EnableTrigger(trigger_identifier) => {
                latest::DatabaseCatalogOp::EnableTrigger(trigger_identifier.into())
            }
            super::DatabaseCatalogOp::DisableTrigger(trigger_identifier) => {
                latest::DatabaseCatalogOp::DisableTrigger(trigger_identifier.into())
            }
        }
    }
}

impl From<super::CreateDatabaseLog> for latest::CreateDatabaseLog {
    fn from(value: super::CreateDatabaseLog) -> Self {
        Self {
            database_id: value.database_id,
            database_name: value.database_name,
        }
    }
}

impl From<super::SoftDeleteDatabaseLog> for latest::SoftDeleteDatabaseLog {
    fn from(value: super::SoftDeleteDatabaseLog) -> Self {
        Self {
            database_id: value.database_id,
            database_name: value.database_name,
            deletion_time: value.deletion_time,
        }
    }
}

impl From<super::CreateTableLog> for latest::CreateTableLog {
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

impl From<super::FieldDefinition> for latest::FieldDefinition {
    fn from(value: super::FieldDefinition) -> Self {
        Self {
            name: value.name,
            id: value.id,
            data_type: value.data_type.into(),
        }
    }
}

impl From<super::FieldDataType> for latest::FieldDataType {
    fn from(value: super::FieldDataType) -> Self {
        match value {
            super::FieldDataType::String => latest::FieldDataType::String,
            super::FieldDataType::Integer => latest::FieldDataType::Integer,
            super::FieldDataType::UInteger => latest::FieldDataType::UInteger,
            super::FieldDataType::Float => latest::FieldDataType::Float,
            super::FieldDataType::Boolean => latest::FieldDataType::Boolean,
            super::FieldDataType::Timestamp => latest::FieldDataType::Timestamp,
            super::FieldDataType::Tag => latest::FieldDataType::Tag,
        }
    }
}

impl From<super::SoftDeleteTableLog> for latest::SoftDeleteTableLog {
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

impl From<super::AddFieldsLog> for latest::AddFieldsLog {
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

impl From<super::DistinctCacheDefinition> for latest::DistinctCacheDefinition {
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

impl From<super::MaxCardinality> for latest::MaxCardinality {
    fn from(value: super::MaxCardinality) -> Self {
        Self::from(value.0)
    }
}

impl From<super::MaxAge> for latest::MaxAge {
    fn from(value: super::MaxAge) -> Self {
        Self(value.0)
    }
}

impl From<super::DeleteDistinctCacheLog> for latest::DeleteDistinctCacheLog {
    fn from(value: super::DeleteDistinctCacheLog) -> Self {
        Self {
            table_id: value.table_id,
            table_name: value.table_name,
            cache_id: value.cache_id,
            cache_name: value.cache_name,
        }
    }
}

impl From<super::LastCacheDefinition> for latest::LastCacheDefinition {
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

impl From<super::LastCacheValueColumnsDef> for latest::LastCacheValueColumnsDef {
    fn from(value: super::LastCacheValueColumnsDef) -> Self {
        match value {
            super::LastCacheValueColumnsDef::Explicit { columns } => {
                latest::LastCacheValueColumnsDef::Explicit { columns }
            }
            super::LastCacheValueColumnsDef::AllNonKeyColumns => {
                latest::LastCacheValueColumnsDef::AllNonKeyColumns
            }
        }
    }
}

impl From<super::LastCacheSize> for latest::LastCacheSize {
    fn from(value: super::LastCacheSize) -> Self {
        Self(value.0)
    }
}

impl From<super::LastCacheTtl> for latest::LastCacheTtl {
    fn from(value: super::LastCacheTtl) -> Self {
        Self(value.0)
    }
}

impl From<super::DeleteLastCacheLog> for latest::DeleteLastCacheLog {
    fn from(value: super::DeleteLastCacheLog) -> Self {
        Self {
            table_id: value.table_id,
            table_name: value.table_name,
            id: value.id,
            name: value.name,
        }
    }
}

impl From<super::TriggerDefinition> for latest::TriggerDefinition {
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

impl From<super::TriggerSpecificationDefinition> for latest::TriggerSpecificationDefinition {
    fn from(value: super::TriggerSpecificationDefinition) -> Self {
        match value {
            super::TriggerSpecificationDefinition::SingleTableWalWrite { table_name } => {
                latest::TriggerSpecificationDefinition::SingleTableWalWrite { table_name }
            }
            super::TriggerSpecificationDefinition::AllTablesWalWrite => {
                latest::TriggerSpecificationDefinition::AllTablesWalWrite
            }
            super::TriggerSpecificationDefinition::Schedule { schedule } => {
                latest::TriggerSpecificationDefinition::Schedule { schedule }
            }
            super::TriggerSpecificationDefinition::RequestPath { path } => {
                latest::TriggerSpecificationDefinition::RequestPath { path }
            }
            super::TriggerSpecificationDefinition::Every { duration } => {
                latest::TriggerSpecificationDefinition::Every { duration }
            }
        }
    }
}

impl From<super::TriggerSettings> for latest::TriggerSettings {
    fn from(value: super::TriggerSettings) -> Self {
        Self {
            run_async: value.run_async,
            error_behavior: value.error_behavior.into(),
        }
    }
}

impl From<super::ErrorBehavior> for latest::ErrorBehavior {
    fn from(value: super::ErrorBehavior) -> Self {
        match value {
            super::ErrorBehavior::Log => latest::ErrorBehavior::Log,
            super::ErrorBehavior::Retry => latest::ErrorBehavior::Retry,
            super::ErrorBehavior::Disable => latest::ErrorBehavior::Disable,
        }
    }
}

impl From<super::DeleteTriggerLog> for latest::DeleteTriggerLog {
    fn from(value: super::DeleteTriggerLog) -> Self {
        Self {
            trigger_id: value.trigger_id,
            trigger_name: value.trigger_name,
            force: value.force,
        }
    }
}

impl From<super::TriggerIdentifier> for latest::TriggerIdentifier {
    fn from(value: super::TriggerIdentifier) -> Self {
        Self {
            db_id: value.db_id,
            db_name: value.db_name,
            trigger_id: value.trigger_id,
            trigger_name: value.trigger_name,
        }
    }
}
