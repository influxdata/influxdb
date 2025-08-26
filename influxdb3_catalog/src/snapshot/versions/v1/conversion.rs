use influxdb3_id::CatalogId;

use crate::snapshot::versions::v2;

impl From<super::CatalogSnapshot> for v2::CatalogSnapshot {
    fn from(value: super::CatalogSnapshot) -> Self {
        Self {
            nodes: value.nodes.into(),
            databases: value.databases.into(),
            sequence: value.sequence,
            catalog_id: value.catalog_id,
            catalog_uuid: value.catalog_uuid,
            tokens: v2::RepositorySnapshot::default(),
        }
    }
}

impl<I, RS, RL> From<super::RepositorySnapshot<I, RS>> for v2::RepositorySnapshot<I, RL>
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

impl From<super::NodeSnapshot> for v2::NodeSnapshot {
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

impl From<super::NodeState> for v2::NodeState {
    fn from(value: super::NodeState) -> Self {
        match value {
            super::NodeState::Running { registered_time_ns } => {
                v2::NodeState::Running { registered_time_ns }
            }
            super::NodeState::Stopped { stopped_time_ns } => {
                v2::NodeState::Stopped { stopped_time_ns }
            }
        }
    }
}

impl From<super::DatabaseSnapshot> for v2::DatabaseSnapshot {
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

impl From<super::TableSnapshot> for v2::TableSnapshot {
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

impl From<super::ColumnDefinitionSnapshot> for v2::ColumnDefinitionSnapshot {
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

impl From<super::DataType> for v2::DataType {
    fn from(value: super::DataType) -> Self {
        match value {
            super::DataType::Null => v2::DataType::Null,
            super::DataType::Bool => v2::DataType::Bool,
            super::DataType::I8 => v2::DataType::I8,
            super::DataType::I16 => v2::DataType::I16,
            super::DataType::I32 => v2::DataType::I32,
            super::DataType::I64 => v2::DataType::I64,
            super::DataType::U8 => v2::DataType::U8,
            super::DataType::U16 => v2::DataType::U16,
            super::DataType::U32 => v2::DataType::U32,
            super::DataType::U64 => v2::DataType::U64,
            super::DataType::F16 => v2::DataType::F16,
            super::DataType::F32 => v2::DataType::F32,
            super::DataType::F64 => v2::DataType::F64,
            super::DataType::Str => v2::DataType::Str,
            super::DataType::BigStr => v2::DataType::BigStr,
            super::DataType::StrView => v2::DataType::StrView,
            super::DataType::Bin => v2::DataType::Bin,
            super::DataType::BigBin => v2::DataType::BigBin,
            super::DataType::BinView => v2::DataType::BinView,
            super::DataType::Dict(a, b) => v2::DataType::Dict(
                Box::new(a.as_ref().clone().into()),
                Box::new(b.as_ref().clone().into()),
            ),
            super::DataType::Time(tu, tz) => v2::DataType::Time(tu.into(), tz),
        }
    }
}

impl From<super::TimeUnit> for v2::TimeUnit {
    fn from(value: super::TimeUnit) -> Self {
        match value {
            super::TimeUnit::Second => v2::TimeUnit::Second,
            super::TimeUnit::Millisecond => v2::TimeUnit::Millisecond,
            super::TimeUnit::Microsecond => v2::TimeUnit::Microsecond,
            super::TimeUnit::Nanosecond => v2::TimeUnit::Nanosecond,
        }
    }
}

impl From<super::InfluxType> for v2::InfluxType {
    fn from(value: super::InfluxType) -> Self {
        match value {
            super::InfluxType::Tag => v2::InfluxType::Tag,
            super::InfluxType::Field => v2::InfluxType::Field,
            super::InfluxType::Time => v2::InfluxType::Time,
        }
    }
}

impl From<super::LastCacheSnapshot> for v2::LastCacheSnapshot {
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

impl From<super::DistinctCacheSnapshot> for v2::DistinctCacheSnapshot {
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

impl From<super::ProcessingEngineTriggerSnapshot> for v2::ProcessingEngineTriggerSnapshot {
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
