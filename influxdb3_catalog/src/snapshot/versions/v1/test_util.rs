use std::{
    sync::atomic::{AtomicU8, Ordering},
    time::Duration,
};

use influxdb3_id::{
    CatalogId, ColumnId, DbId, DistinctCacheId, LastCacheId, NodeId, SerdeVecMap, TableId,
    TriggerId,
};
use uuid::Uuid;

use crate::{
    catalog::CatalogSequenceNumber,
    log::versions::v1::{ErrorBehavior, MaxAge, MaxCardinality},
};

use super::{
    CatalogSnapshot, ColumnDefinitionSnapshot, DataType, DatabaseSnapshot, DistinctCacheSnapshot,
    InfluxType, LastCacheSnapshot, NodeMode, NodeSnapshot, ProcessingEngineTriggerSnapshot,
    RepositorySnapshot, TableSnapshot, TriggerSpecificationDefinition,
};

pub(crate) trait Generate {
    fn generate() -> Self;
}

impl<T> Generate for Vec<T>
where
    T: Generate,
{
    fn generate() -> Self {
        let mut v = vec![];
        for _ in 0..10 {
            v.push(T::generate());
        }
        v
    }
}

impl<I, R> Generate for RepositorySnapshot<I, R>
where
    I: CatalogId,
    R: Generate,
{
    fn generate() -> Self {
        let mut next_id = I::default();
        let mut repo = SerdeVecMap::new();
        for _ in 0..10 {
            repo.insert(next_id, R::generate());
            next_id = next_id.next();
        }
        Self { repo, next_id }
    }
}

impl Generate for CatalogSnapshot {
    fn generate() -> Self {
        Self {
            nodes: Generate::generate(),
            databases: Generate::generate(),
            sequence: CatalogSequenceNumber::new(42),
            catalog_id: "test-catalog".into(),
            catalog_uuid: Uuid::new_v4(),
        }
    }
}

impl Generate for NodeSnapshot {
    fn generate() -> Self {
        Self {
            node_id: "test-node".into(),
            node_catalog_id: NodeId::new(0),
            instance_id: "uuid".into(),
            mode: Generate::generate(),
            state: super::NodeState::Running {
                registered_time_ns: 0,
            },
            core_count: 2,
        }
    }
}

impl Generate for NodeMode {
    fn generate() -> Self {
        Self::Core
    }
}

impl Generate for DatabaseSnapshot {
    fn generate() -> Self {
        Self {
            id: DbId::new(0),
            name: "test-db".into(),
            tables: Generate::generate(),
            processing_engine_triggers: Generate::generate(),
            deleted: false,
        }
    }
}

impl Generate for TableSnapshot {
    fn generate() -> Self {
        Self {
            table_id: TableId::new(0),
            table_name: "test-table".into(),
            key: vec![ColumnId::new(0), ColumnId::new(1), ColumnId::new(1)],
            columns: Generate::generate(),
            last_caches: Generate::generate(),
            distinct_caches: Generate::generate(),
            deleted: false,
        }
    }
}

impl Generate for ProcessingEngineTriggerSnapshot {
    fn generate() -> Self {
        Self {
            trigger_id: TriggerId::new(0),
            trigger_name: "test-trigger".into(),
            node_id: "test-node".into(),
            plugin_filename: "plugin.py".into(),
            database_name: "test-db".into(),
            trigger_specification: Generate::generate(),
            trigger_settings: super::TriggerSettings {
                run_async: false,
                error_behavior: Generate::generate(),
            },
            trigger_arguments: Some(Default::default()),
            disabled: false,
        }
    }
}

static TRIGGER_SPECIFICATION_GENERATOR: AtomicU8 = AtomicU8::new(0);

impl Generate for TriggerSpecificationDefinition {
    fn generate() -> Self {
        let g = TRIGGER_SPECIFICATION_GENERATOR.fetch_add(1, Ordering::AcqRel);
        match g % 5 {
            1 => TriggerSpecificationDefinition::SingleTableWalWrite {
                table_name: "test-table".into(),
            },
            2 => TriggerSpecificationDefinition::Schedule {
                schedule: "* * * 1 *".into(),
            },
            3 => TriggerSpecificationDefinition::RequestPath {
                path: "/test/path".into(),
            },
            4 => TriggerSpecificationDefinition::Every {
                duration: Duration::from_secs(1),
            },
            _ => TriggerSpecificationDefinition::AllTablesWalWrite,
        }
    }
}

static ERROR_BEHAVIOUR_GENERATOR: AtomicU8 = AtomicU8::new(0);

impl Generate for ErrorBehavior {
    fn generate() -> Self {
        let g = ERROR_BEHAVIOUR_GENERATOR.fetch_add(1, Ordering::AcqRel);
        match g % 3 {
            1 => ErrorBehavior::Log,
            2 => ErrorBehavior::Retry,
            _ => ErrorBehavior::Disable,
        }
    }
}

impl Generate for ColumnDefinitionSnapshot {
    fn generate() -> Self {
        let (t, it) = Generate::generate();
        Self {
            name: "test-col".into(),
            id: ColumnId::new(0),
            r#type: t,
            influx_type: it,
            nullable: false,
        }
    }
}

static COLUMN_TYPE_GENERATOR: AtomicU8 = AtomicU8::new(0);

impl Generate for (DataType, InfluxType) {
    fn generate() -> Self {
        let g = COLUMN_TYPE_GENERATOR.fetch_add(1, Ordering::AcqRel);
        match g % 3 {
            1 => (DataType::Bool, InfluxType::Field),
            2 => (DataType::I64, InfluxType::Field),
            3 => (DataType::U64, InfluxType::Field),
            4 => (DataType::Str, InfluxType::Field),
            5 => (DataType::F64, InfluxType::Field),
            6 => (
                DataType::Dict(Box::new(DataType::I32), Box::new(DataType::Str)),
                InfluxType::Tag,
            ),
            _ => (
                DataType::Time(super::TimeUnit::Nanosecond, None),
                InfluxType::Time,
            ),
        }
    }
}

impl Generate for LastCacheSnapshot {
    fn generate() -> Self {
        Self {
            table_id: TableId::new(0),
            table: "test-table".into(),
            id: LastCacheId::new(0),
            name: "test-cache".into(),
            keys: vec![ColumnId::new(0), ColumnId::new(1)],
            vals: Some(vec![ColumnId::new(2), ColumnId::new(3)]),
            n: 2,
            ttl: 3600,
        }
    }
}

impl Generate for DistinctCacheSnapshot {
    fn generate() -> Self {
        Self {
            table_id: TableId::new(0),
            table: "test-table".into(),
            id: DistinctCacheId::new(0),
            name: "test-cache".into(),
            cols: vec![ColumnId::new(0), ColumnId::new(1)],
            max_cardinality: MaxCardinality::default(),
            max_age_seconds: MaxAge::default(),
        }
    }
}
