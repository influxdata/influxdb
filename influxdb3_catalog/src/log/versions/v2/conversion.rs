use crate::log::versions::{v2, v3};

impl From<v2::OrderedCatalogBatch> for v3::OrderedCatalogBatch {
    fn from(value: v2::OrderedCatalogBatch) -> Self {
        Self {
            catalog_batch: value.catalog_batch.into(),
            sequence_number: value.sequence_number,
        }
    }
}

impl From<v2::CatalogBatch> for v3::CatalogBatch {
    fn from(value: v2::CatalogBatch) -> Self {
        match value {
            v2::CatalogBatch::Node(node_batch) => v3::CatalogBatch::Node(node_batch.into()),
            v2::CatalogBatch::Database(database_batch) => {
                v3::CatalogBatch::Database(database_batch.into())
            }
            v2::CatalogBatch::Token(token_batch) => v3::CatalogBatch::Token(token_batch.into()),
        }
    }
}

impl From<v2::TokenBatch> for v3::TokenBatch {
    fn from(value: v2::TokenBatch) -> Self {
        Self {
            time_ns: value.time_ns,
            ops: value.ops.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<v2::TokenCatalogOp> for v3::TokenCatalogOp {
    fn from(value: v2::TokenCatalogOp) -> Self {
        match value {
            v2::TokenCatalogOp::CreateAdminToken(create_admin_token_details) => {
                v3::TokenCatalogOp::CreateAdminToken(create_admin_token_details.into())
            }
            v2::TokenCatalogOp::RegenerateAdminToken(regenerate_admin_token_details) => {
                v3::TokenCatalogOp::RegenerateAdminToken(regenerate_admin_token_details.into())
            }
            v2::TokenCatalogOp::DeleteToken(delete_token_details) => {
                v3::TokenCatalogOp::DeleteToken(delete_token_details.into())
            }
        }
    }
}

impl From<v2::CreateAdminTokenDetails> for v3::CreateAdminTokenDetails {
    fn from(value: v2::CreateAdminTokenDetails) -> Self {
        Self {
            token_id: value.token_id,
            name: value.name,
            hash: value.hash,
            created_at: value.created_at,
            updated_at: value.updated_at,
            expiry: value.expiry,
        }
    }
}

impl From<v2::RegenerateAdminTokenDetails> for v3::RegenerateAdminTokenDetails {
    fn from(value: v2::RegenerateAdminTokenDetails) -> Self {
        Self {
            token_id: value.token_id,
            hash: value.hash,
            updated_at: value.updated_at,
        }
    }
}

impl From<v2::DeleteTokenDetails> for v3::DeleteTokenDetails {
    fn from(value: v2::DeleteTokenDetails) -> Self {
        Self {
            token_name: value.token_name,
        }
    }
}

impl From<v2::NodeBatch> for v3::NodeBatch {
    fn from(value: v2::NodeBatch) -> Self {
        Self {
            time_ns: value.time_ns,
            node_catalog_id: value.node_catalog_id,
            node_id: value.node_id,
            ops: value.ops.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<v2::NodeCatalogOp> for v3::NodeCatalogOp {
    fn from(value: v2::NodeCatalogOp) -> Self {
        match value {
            v2::NodeCatalogOp::RegisterNode(register_node_log) => {
                v3::NodeCatalogOp::RegisterNode(register_node_log.into())
            }
            v2::NodeCatalogOp::StopNode(stop_node_log) => {
                v3::NodeCatalogOp::StopNode(stop_node_log.into())
            }
        }
    }
}

impl From<v2::RegisterNodeLog> for v3::RegisterNodeLog {
    fn from(value: v2::RegisterNodeLog) -> Self {
        Self {
            node_id: value.node_id,
            instance_id: value.instance_id,
            registered_time_ns: value.registered_time_ns,
            core_count: value.core_count,
            mode: value.mode.into_iter().map(Into::into).collect(),
            process_uuid: value.process_uuid,
        }
    }
}

impl From<v2::StopNodeLog> for v3::StopNodeLog {
    fn from(value: v2::StopNodeLog) -> Self {
        Self {
            node_id: value.node_id,
            stopped_time_ns: value.stopped_time_ns,
            process_uuid: value.process_uuid,
        }
    }
}

impl From<v2::NodeMode> for v3::NodeMode {
    fn from(value: v2::NodeMode) -> Self {
        match value {
            v2::NodeMode::Core => v3::NodeMode::Core,
        }
    }
}

impl From<v2::DatabaseBatch> for v3::DatabaseBatch {
    fn from(value: v2::DatabaseBatch) -> Self {
        Self {
            time_ns: value.time_ns,
            database_id: value.database_id,
            database_name: value.database_name,
            ops: value.ops.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<v2::DatabaseCatalogOp> for v3::DatabaseCatalogOp {
    fn from(value: v2::DatabaseCatalogOp) -> Self {
        match value {
            v2::DatabaseCatalogOp::CreateDatabase(create_database_log) => {
                v3::DatabaseCatalogOp::CreateDatabase(create_database_log.into())
            }
            v2::DatabaseCatalogOp::SoftDeleteDatabase(soft_delete_database_log) => {
                v3::DatabaseCatalogOp::SoftDeleteDatabase(soft_delete_database_log.into())
            }
            v2::DatabaseCatalogOp::CreateTable(create_table_log) => {
                v3::DatabaseCatalogOp::CreateTable(create_table_log.into())
            }
            v2::DatabaseCatalogOp::SoftDeleteTable(soft_delete_table_log) => {
                v3::DatabaseCatalogOp::SoftDeleteTable(soft_delete_table_log.into())
            }
            v2::DatabaseCatalogOp::AddFields(add_fields_log) => {
                v3::DatabaseCatalogOp::AddFields(add_fields_log.into())
            }
            v2::DatabaseCatalogOp::CreateDistinctCache(distinct_cache_definition) => {
                v3::DatabaseCatalogOp::CreateDistinctCache(distinct_cache_definition.into())
            }
            v2::DatabaseCatalogOp::DeleteDistinctCache(delete_distinct_cache_log) => {
                v3::DatabaseCatalogOp::DeleteDistinctCache(delete_distinct_cache_log.into())
            }
            v2::DatabaseCatalogOp::CreateLastCache(last_cache_definition) => {
                v3::DatabaseCatalogOp::CreateLastCache(last_cache_definition.into())
            }
            v2::DatabaseCatalogOp::DeleteLastCache(delete_last_cache_log) => {
                v3::DatabaseCatalogOp::DeleteLastCache(delete_last_cache_log.into())
            }
            v2::DatabaseCatalogOp::CreateTrigger(trigger_definition) => {
                v3::DatabaseCatalogOp::CreateTrigger(trigger_definition.into())
            }
            v2::DatabaseCatalogOp::DeleteTrigger(delete_trigger_log) => {
                v3::DatabaseCatalogOp::DeleteTrigger(delete_trigger_log.into())
            }
            v2::DatabaseCatalogOp::EnableTrigger(trigger_identifier) => {
                v3::DatabaseCatalogOp::EnableTrigger(trigger_identifier.into())
            }
            v2::DatabaseCatalogOp::DisableTrigger(trigger_identifier) => {
                v3::DatabaseCatalogOp::DisableTrigger(trigger_identifier.into())
            }
        }
    }
}

impl From<v2::CreateDatabaseLog> for v3::CreateDatabaseLog {
    fn from(value: v2::CreateDatabaseLog) -> Self {
        Self {
            database_id: value.database_id,
            database_name: value.database_name,
            retention_period: None,
        }
    }
}

impl From<v2::SoftDeleteDatabaseLog> for v3::SoftDeleteDatabaseLog {
    fn from(value: v2::SoftDeleteDatabaseLog) -> Self {
        Self {
            database_id: value.database_id,
            database_name: value.database_name,
            deletion_time: value.deletion_time,
        }
    }
}

impl From<v2::CreateTableLog> for v3::CreateTableLog {
    fn from(value: v2::CreateTableLog) -> Self {
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

impl From<v2::FieldDefinition> for v3::FieldDefinition {
    fn from(value: v2::FieldDefinition) -> Self {
        Self {
            name: value.name,
            id: value.id,
            data_type: value.data_type.into(),
        }
    }
}

impl From<v2::FieldDataType> for v3::FieldDataType {
    fn from(value: v2::FieldDataType) -> Self {
        match value {
            v2::FieldDataType::String => v3::FieldDataType::String,
            v2::FieldDataType::Integer => v3::FieldDataType::Integer,
            v2::FieldDataType::UInteger => v3::FieldDataType::UInteger,
            v2::FieldDataType::Float => v3::FieldDataType::Float,
            v2::FieldDataType::Boolean => v3::FieldDataType::Boolean,
            v2::FieldDataType::Timestamp => v3::FieldDataType::Timestamp,
            v2::FieldDataType::Tag => v3::FieldDataType::Tag,
        }
    }
}

impl From<v2::SoftDeleteTableLog> for v3::SoftDeleteTableLog {
    fn from(value: v2::SoftDeleteTableLog) -> Self {
        Self {
            database_id: value.database_id,
            database_name: value.database_name,
            table_id: value.table_id,
            table_name: value.table_name,
            deletion_time: value.deletion_time,
            hard_deletion_time: None,
        }
    }
}

impl From<v2::AddFieldsLog> for v3::AddFieldsLog {
    fn from(value: v2::AddFieldsLog) -> Self {
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

impl From<v2::DistinctCacheDefinition> for v3::DistinctCacheDefinition {
    fn from(value: v2::DistinctCacheDefinition) -> Self {
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

impl From<v2::MaxCardinality> for v3::MaxCardinality {
    fn from(value: v2::MaxCardinality) -> Self {
        Self::from(value.0)
    }
}

impl From<v2::MaxAge> for v3::MaxAge {
    fn from(value: v2::MaxAge) -> Self {
        Self(value.0)
    }
}

impl From<v2::DeleteDistinctCacheLog> for v3::DeleteDistinctCacheLog {
    fn from(value: v2::DeleteDistinctCacheLog) -> Self {
        Self {
            table_id: value.table_id,
            table_name: value.table_name,
            cache_id: value.cache_id,
            cache_name: value.cache_name,
        }
    }
}

impl From<v2::LastCacheDefinition> for v3::LastCacheDefinition {
    fn from(value: v2::LastCacheDefinition) -> Self {
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

impl From<v2::LastCacheValueColumnsDef> for v3::LastCacheValueColumnsDef {
    fn from(value: v2::LastCacheValueColumnsDef) -> Self {
        match value {
            v2::LastCacheValueColumnsDef::Explicit { columns } => {
                v3::LastCacheValueColumnsDef::Explicit { columns }
            }
            v2::LastCacheValueColumnsDef::AllNonKeyColumns => {
                v3::LastCacheValueColumnsDef::AllNonKeyColumns
            }
        }
    }
}

impl From<v2::LastCacheSize> for v3::LastCacheSize {
    fn from(value: v2::LastCacheSize) -> Self {
        Self(value.0)
    }
}

impl From<v2::LastCacheTtl> for v3::LastCacheTtl {
    fn from(value: v2::LastCacheTtl) -> Self {
        Self(value.0)
    }
}

impl From<v2::DeleteLastCacheLog> for v3::DeleteLastCacheLog {
    fn from(value: v2::DeleteLastCacheLog) -> Self {
        Self {
            table_id: value.table_id,
            table_name: value.table_name,
            id: value.id,
            name: value.name,
        }
    }
}

impl From<v2::TriggerDefinition> for v3::TriggerDefinition {
    fn from(value: v2::TriggerDefinition) -> Self {
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

impl From<v2::TriggerSpecificationDefinition> for v3::TriggerSpecificationDefinition {
    fn from(value: v2::TriggerSpecificationDefinition) -> Self {
        match value {
            v2::TriggerSpecificationDefinition::SingleTableWalWrite { table_name } => {
                v3::TriggerSpecificationDefinition::SingleTableWalWrite { table_name }
            }
            v2::TriggerSpecificationDefinition::AllTablesWalWrite => {
                v3::TriggerSpecificationDefinition::AllTablesWalWrite
            }
            v2::TriggerSpecificationDefinition::Schedule { schedule } => {
                v3::TriggerSpecificationDefinition::Schedule { schedule }
            }
            v2::TriggerSpecificationDefinition::RequestPath { path } => {
                v3::TriggerSpecificationDefinition::RequestPath { path }
            }
            v2::TriggerSpecificationDefinition::Every { duration } => {
                v3::TriggerSpecificationDefinition::Every { duration }
            }
        }
    }
}

impl From<v2::TriggerSettings> for v3::TriggerSettings {
    fn from(value: v2::TriggerSettings) -> Self {
        Self {
            run_async: value.run_async,
            error_behavior: value.error_behavior.into(),
        }
    }
}

impl From<v2::ErrorBehavior> for v3::ErrorBehavior {
    fn from(value: v2::ErrorBehavior) -> Self {
        match value {
            v2::ErrorBehavior::Log => v3::ErrorBehavior::Log,
            v2::ErrorBehavior::Retry => v3::ErrorBehavior::Retry,
            v2::ErrorBehavior::Disable => v3::ErrorBehavior::Disable,
        }
    }
}

impl From<v2::DeleteTriggerLog> for v3::DeleteTriggerLog {
    fn from(value: v2::DeleteTriggerLog) -> Self {
        Self {
            trigger_id: value.trigger_id,
            trigger_name: value.trigger_name,
            force: value.force,
        }
    }
}

impl From<v2::TriggerIdentifier> for v3::TriggerIdentifier {
    fn from(value: v2::TriggerIdentifier) -> Self {
        Self {
            db_id: value.db_id,
            db_name: value.db_name,
            trigger_id: value.trigger_id,
            trigger_name: value.trigger_name,
        }
    }
}
