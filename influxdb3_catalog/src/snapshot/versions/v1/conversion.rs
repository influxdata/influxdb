use influxdb3_id::CatalogId;

use crate::{catalog::NodeState, snapshot::versions::latest};

impl From<super::CatalogSnapshot> for latest::CatalogSnapshot {
    fn from(value: super::CatalogSnapshot) -> Self {
        Self {
            nodes: value.nodes.into(),
            databases: value.databases.into(),
            sequence: value.sequence,
            catalog_id: value.catalog_id,
            catalog_uuid: value.catalog_uuid,
        }
    }
}

impl<I, RS, RL> From<super::RepositorySnapshot<I, RS>> for latest::RepositorySnapshot<I, RL>
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

impl From<super::NodeSnapshot> for latest::NodeSnapshot {
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

impl From<super::NodeState> for NodeState {
    fn from(value: super::NodeState) -> Self {
        match value {
            super::NodeState::Running { registered_time_ns } => {
                NodeState::Running { registered_time_ns }
            }
            super::NodeState::Stopped { stopped_time_ns } => NodeState::Stopped { stopped_time_ns },
        }
    }
}

impl From<super::DatabaseSnapshot> for latest::DatabaseSnapshot {
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

impl From<super::TableSnapshot> for latest::TableSnapshot {
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

impl From<super::ColumnDefinitionSnapshot> for latest::ColumnDefinitionSnapshot {
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

impl From<super::DataType> for latest::DataType {
    fn from(value: super::DataType) -> Self {
        match value {
            super::DataType::Null => latest::DataType::Null,
            super::DataType::Bool => latest::DataType::Bool,
            super::DataType::I8 => latest::DataType::I8,
            super::DataType::I16 => latest::DataType::I16,
            super::DataType::I32 => latest::DataType::I32,
            super::DataType::I64 => latest::DataType::I64,
            super::DataType::U8 => latest::DataType::U8,
            super::DataType::U16 => latest::DataType::U16,
            super::DataType::U32 => latest::DataType::U32,
            super::DataType::U64 => latest::DataType::U64,
            super::DataType::F16 => latest::DataType::F16,
            super::DataType::F32 => latest::DataType::F32,
            super::DataType::F64 => latest::DataType::F64,
            super::DataType::Str => latest::DataType::Str,
            super::DataType::BigStr => latest::DataType::BigStr,
            super::DataType::StrView => latest::DataType::StrView,
            super::DataType::Bin => latest::DataType::Bin,
            super::DataType::BigBin => latest::DataType::BigBin,
            super::DataType::BinView => latest::DataType::BinView,
            super::DataType::Dict(a, b) => latest::DataType::Dict(
                Box::new(a.as_ref().clone().into()),
                Box::new(b.as_ref().clone().into()),
            ),
            super::DataType::Time(tu, tz) => latest::DataType::Time(tu.into(), tz.into()),
        }
    }
}

impl From<super::TimeUnit> for latest::TimeUnit {
    fn from(value: super::TimeUnit) -> Self {
        match value {
            super::TimeUnit::Second => latest::TimeUnit::Second,
            super::TimeUnit::Millisecond => latest::TimeUnit::Millisecond,
            super::TimeUnit::Microsecond => latest::TimeUnit::Microsecond,
            super::TimeUnit::Nanosecond => latest::TimeUnit::Nanosecond,
        }
    }
}

impl From<super::InfluxType> for latest::InfluxType {
    fn from(value: super::InfluxType) -> Self {
        match value {
            super::InfluxType::Tag => latest::InfluxType::Tag,
            super::InfluxType::Field => latest::InfluxType::Field,
            super::InfluxType::Time => latest::InfluxType::Time,
        }
    }
}

impl From<super::LastCacheSnapshot> for latest::LastCacheSnapshot {
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

impl From<super::DistinctCacheSnapshot> for latest::DistinctCacheSnapshot {
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

impl From<super::ProcessingEngineTriggerSnapshot> for latest::ProcessingEngineTriggerSnapshot {
    fn from(value: super::ProcessingEngineTriggerSnapshot) -> Self {
        Self {
            trigger_id: value.trigger_id,
            trigger_name: value.trigger_name,
            node_id: value.node_id,
            plugin_filename: value.plugin_filename,
            database_name: value.database_name,
            trigger_specification: value.trigger_specification.into(),
            trigger_settings: value.trigger_settings.into(),
            trigger_arguments: value.trigger_arguments,
            disabled: value.disabled,
        }
    }
}
