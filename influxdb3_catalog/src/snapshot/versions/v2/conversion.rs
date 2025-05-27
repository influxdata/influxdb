use influxdb3_id::CatalogId;

use crate::snapshot::versions::v3;

impl From<super::CatalogSnapshot> for v3::CatalogSnapshot {
    fn from(value: super::CatalogSnapshot) -> Self {
        Self {
            nodes: value.nodes.into(),
            databases: value.databases.into(),
            sequence: value.sequence,
            catalog_id: value.catalog_id,
            catalog_uuid: value.catalog_uuid,
            tokens: value.tokens.into(),
        }
    }
}

impl<I, RS, RL> From<super::RepositorySnapshot<I, RS>> for v3::RepositorySnapshot<I, RL>
where
    I: CatalogId,
    RL: From<RS>,
{
    fn from(value: super::RepositorySnapshot<I, RS>) -> Self {
        Self {
            repo: value.repo.into_iter().map(|(i, r)| (i, r.into())).collect(),
            next_id: value.next_id,
        }
    }
}

impl From<super::NodeSnapshot> for v3::NodeSnapshot {
    fn from(value: super::NodeSnapshot) -> Self {
        Self {
            node_id: value.node_id,
            node_catalog_id: value.node_catalog_id,
            instance_id: value.instance_id,
            mode: value.mode.into_iter().map(Into::into).collect(),
            state: value.state.into(),
            core_count: value.core_count,
        }
    }
}

impl From<super::TokenInfoSnapshot> for v3::TokenInfoSnapshot {
    fn from(value: super::TokenInfoSnapshot) -> Self {
        Self {
            id: value.id,
            name: value.name,
            hash: value.hash,
            created_at: value.created_at,
            description: value.description,
            created_by: value.created_by,
            expiry: value.expiry,
            updated_by: value.updated_by,
            updated_at: value.updated_at,
            permissions: value.permissions.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<super::PermissionSnapshot> for v3::PermissionSnapshot {
    fn from(value: super::PermissionSnapshot) -> Self {
        Self {
            resource_type: value.resource_type.into(),
            resource_identifier: value.resource_identifier.into(),
            actions: value.actions.into(),
        }
    }
}

impl From<super::ResourceTypeSnapshot> for v3::ResourceTypeSnapshot {
    fn from(value: super::ResourceTypeSnapshot) -> Self {
        match value {
            super::ResourceTypeSnapshot::Database => v3::ResourceTypeSnapshot::Database,
            super::ResourceTypeSnapshot::Token => v3::ResourceTypeSnapshot::Token,
            super::ResourceTypeSnapshot::Wildcard => v3::ResourceTypeSnapshot::Wildcard,
        }
    }
}

impl From<super::ResourceIdentifierSnapshot> for v3::ResourceIdentifierSnapshot {
    fn from(value: super::ResourceIdentifierSnapshot) -> Self {
        match value {
            super::ResourceIdentifierSnapshot::Database(db_ids) => {
                v3::ResourceIdentifierSnapshot::Database(db_ids)
            }
            super::ResourceIdentifierSnapshot::Token(token_ids) => {
                v3::ResourceIdentifierSnapshot::Token(token_ids)
            }
            super::ResourceIdentifierSnapshot::Wildcard => v3::ResourceIdentifierSnapshot::Wildcard,
        }
    }
}

impl From<super::ActionsSnapshot> for v3::ActionsSnapshot {
    fn from(value: super::ActionsSnapshot) -> Self {
        match value {
            super::ActionsSnapshot::Database(database_actions_snapshot) => {
                v3::ActionsSnapshot::Database(database_actions_snapshot.into())
            }
            super::ActionsSnapshot::Token(crud_actions_snapshot) => {
                v3::ActionsSnapshot::Token(crud_actions_snapshot.into())
            }
            super::ActionsSnapshot::Wildcard => v3::ActionsSnapshot::Wildcard,
        }
    }
}

impl From<super::DatabaseActionsSnapshot> for v3::DatabaseActionsSnapshot {
    fn from(value: super::DatabaseActionsSnapshot) -> Self {
        v3::DatabaseActionsSnapshot(value.0)
    }
}

impl From<super::CrudActionsSnapshot> for v3::CrudActionsSnapshot {
    fn from(value: super::CrudActionsSnapshot) -> Self {
        v3::CrudActionsSnapshot(value.0)
    }
}

impl From<super::NodeState> for v3::NodeStateSnapshot {
    fn from(value: super::NodeState) -> Self {
        match value {
            super::NodeState::Running { registered_time_ns } => {
                v3::NodeStateSnapshot::Running { registered_time_ns }
            }
            super::NodeState::Stopped { stopped_time_ns } => {
                v3::NodeStateSnapshot::Stopped { stopped_time_ns }
            }
        }
    }
}

impl From<super::DatabaseSnapshot> for v3::DatabaseSnapshot {
    fn from(value: super::DatabaseSnapshot) -> Self {
        Self {
            id: value.id,
            name: value.name,
            tables: value.tables.into(),
            processing_engine_triggers: value.processing_engine_triggers.into(),
            deleted: value.deleted,
        }
    }
}

impl From<super::TableSnapshot> for v3::TableSnapshot {
    fn from(value: super::TableSnapshot) -> Self {
        Self {
            table_id: value.table_id,
            table_name: value.table_name,
            key: value.key,
            columns: value.columns.into(),
            last_caches: value.last_caches.into(),
            distinct_caches: value.distinct_caches.into(),
            deleted: value.deleted,
        }
    }
}

impl From<super::ColumnDefinitionSnapshot> for v3::ColumnDefinitionSnapshot {
    fn from(value: super::ColumnDefinitionSnapshot) -> Self {
        Self {
            name: value.name,
            id: value.id,
            r#type: value.r#type.into(),
            influx_type: value.influx_type.into(),
            nullable: value.nullable,
        }
    }
}

impl From<super::DataType> for v3::DataType {
    fn from(value: super::DataType) -> Self {
        match value {
            super::DataType::Null => v3::DataType::Null,
            super::DataType::Bool => v3::DataType::Bool,
            super::DataType::I8 => v3::DataType::I8,
            super::DataType::I16 => v3::DataType::I16,
            super::DataType::I32 => v3::DataType::I32,
            super::DataType::I64 => v3::DataType::I64,
            super::DataType::U8 => v3::DataType::U8,
            super::DataType::U16 => v3::DataType::U16,
            super::DataType::U32 => v3::DataType::U32,
            super::DataType::U64 => v3::DataType::U64,
            super::DataType::F16 => v3::DataType::F16,
            super::DataType::F32 => v3::DataType::F32,
            super::DataType::F64 => v3::DataType::F64,
            super::DataType::Str => v3::DataType::Str,
            super::DataType::BigStr => v3::DataType::BigStr,
            super::DataType::StrView => v3::DataType::StrView,
            super::DataType::Bin => v3::DataType::Bin,
            super::DataType::BigBin => v3::DataType::BigBin,
            super::DataType::BinView => v3::DataType::BinView,
            super::DataType::Dict(a, b) => v3::DataType::Dict(
                Box::new(a.as_ref().clone().into()),
                Box::new(b.as_ref().clone().into()),
            ),
            super::DataType::Time(tu, tz) => v3::DataType::Time(tu.into(), tz),
        }
    }
}

impl From<super::TimeUnit> for v3::TimeUnit {
    fn from(value: super::TimeUnit) -> Self {
        match value {
            super::TimeUnit::Second => v3::TimeUnit::Second,
            super::TimeUnit::Millisecond => v3::TimeUnit::Millisecond,
            super::TimeUnit::Microsecond => v3::TimeUnit::Microsecond,
            super::TimeUnit::Nanosecond => v3::TimeUnit::Nanosecond,
        }
    }
}

impl From<super::InfluxType> for v3::InfluxType {
    fn from(value: super::InfluxType) -> Self {
        match value {
            super::InfluxType::Tag => v3::InfluxType::Tag,
            super::InfluxType::Field => v3::InfluxType::Field,
            super::InfluxType::Time => v3::InfluxType::Time,
        }
    }
}

impl From<super::LastCacheSnapshot> for v3::LastCacheSnapshot {
    fn from(value: super::LastCacheSnapshot) -> Self {
        Self {
            table_id: value.table_id,
            table: value.table,
            id: value.id,
            name: value.name,
            keys: value.keys,
            vals: value.vals,
            n: value.n,
            ttl: value.ttl,
        }
    }
}

impl From<super::DistinctCacheSnapshot> for v3::DistinctCacheSnapshot {
    fn from(value: super::DistinctCacheSnapshot) -> Self {
        Self {
            table_id: value.table_id,
            table: value.table,
            id: value.id,
            name: value.name,
            cols: value.cols,
            max_cardinality: value.max_cardinality.into(),
            max_age_seconds: value.max_age_seconds.into(),
        }
    }
}

impl From<super::ProcessingEngineTriggerSnapshot> for v3::ProcessingEngineTriggerSnapshot {
    fn from(value: super::ProcessingEngineTriggerSnapshot) -> Self {
        Self {
            trigger_id: value.trigger_id,
            trigger_name: value.trigger_name,
            plugin_filename: value.plugin_filename,
            database_name: value.database_name,
            node_id: value.node_id,
            trigger_specification: value.trigger_specification.into(),
            trigger_settings: value.trigger_settings.into(),
            trigger_arguments: value.trigger_arguments,
            disabled: value.disabled,
        }
    }
}
