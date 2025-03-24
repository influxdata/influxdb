//! Version 1 (v1) Catalog Snapshot definitions
//!
//! v1 catalog serialization relied on [`bitcode`][bitcode] for serialization/deserialization of
//! catalog snapshot files.
//!
//! The type definitions in this module should not be modified. They exist for deserializing
//! files that were written with the v1 format. `bitcode` is not self-describing, and does not
//! handle the addition of new fields to types like other self-describing formats.
//!
//! See also [#26180][issue].
//!
//! [bitcode]: https://docs.rs/bitcode/latest/bitcode/
//! [issue]: https://github.com/influxdata/influxdb/issues/26180
use crate::{
    catalog::CatalogSequenceNumber,
    log::versions::v1::{
        MaxAge, MaxCardinality, NodeMode, TriggerSettings, TriggerSpecificationDefinition,
    },
};
use arrow::datatypes::DataType as ArrowDataType;
use hashbrown::HashMap;
use influxdb3_id::{
    CatalogId, ColumnId, DbId, DistinctCacheId, LastCacheId, NodeId, SerdeVecMap, TableId,
    TriggerId,
};
use schema::{InfluxColumnType, InfluxFieldType, TIME_DATA_TIMEZONE};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

mod conversion;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CatalogSnapshot {
    nodes: RepositorySnapshot<NodeId, NodeSnapshot>,
    databases: RepositorySnapshot<DbId, DatabaseSnapshot>,
    sequence: CatalogSequenceNumber,
    catalog_id: Arc<str>,
    catalog_uuid: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct NodeSnapshot {
    node_id: Arc<str>,
    node_catalog_id: NodeId,
    instance_id: Arc<str>,
    mode: Vec<NodeMode>,
    state: NodeState,
    core_count: u64,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum NodeState {
    Running { registered_time_ns: i64 },
    Stopped { stopped_time_ns: i64 },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DatabaseSnapshot {
    id: DbId,
    name: Arc<str>,
    tables: RepositorySnapshot<TableId, TableSnapshot>,
    processing_engine_triggers: RepositorySnapshot<TriggerId, ProcessingEngineTriggerSnapshot>,
    deleted: bool,
}

/// A snapshot of a `TableDefinition` used for serialization of table information from the
/// catalog.
///
/// This is used over serde's `Serialize`/`Deserialize` implementations on the inner `Schema` type
/// due to them being considered unstable. This type intends to mimic the structure of the Arrow
/// `Schema`, and will help guard against potential breaking changes to the Arrow Schema types.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TableSnapshot {
    table_id: TableId,
    table_name: Arc<str>,
    key: Vec<ColumnId>,
    columns: RepositorySnapshot<ColumnId, ColumnDefinitionSnapshot>,
    last_caches: RepositorySnapshot<LastCacheId, LastCacheSnapshot>,
    distinct_caches: RepositorySnapshot<DistinctCacheId, DistinctCacheSnapshot>,
    deleted: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ProcessingEngineTriggerSnapshot {
    trigger_id: TriggerId,
    trigger_name: Arc<str>,
    node_id: Arc<str>,
    plugin_filename: String,
    database_name: Arc<str>,
    trigger_specification: TriggerSpecificationDefinition,
    trigger_settings: TriggerSettings,
    trigger_arguments: Option<HashMap<String, String>>,
    disabled: bool,
}

/// The inner column definition for a [`TableSnapshot`]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ColumnDefinitionSnapshot {
    name: Arc<str>,
    /// The id of the column
    id: ColumnId,
    /// The column's data type
    r#type: DataType,
    /// The columns Influx type
    influx_type: InfluxType,
    /// Whether the column can hold NULL values
    nullable: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LastCacheSnapshot {
    table_id: TableId,
    table: Arc<str>,
    id: LastCacheId,
    name: Arc<str>,
    keys: Vec<ColumnId>,
    vals: Option<Vec<ColumnId>>,
    n: usize,
    ttl: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DistinctCacheSnapshot {
    table_id: TableId,
    table: Arc<str>,
    id: DistinctCacheId,
    name: Arc<str>,
    cols: Vec<ColumnId>,
    max_cardinality: MaxCardinality,
    max_age_seconds: MaxAge,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RepositorySnapshot<I, R>
where
    I: CatalogId,
{
    repo: SerdeVecMap<I, R>,
    next_id: I,
}

/// Representation of Arrow's `DataType` for table snapshots.
///
/// Uses `#[non_exhaustive]` with the assumption that variants will be added as we support
/// more Arrow data types.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub(crate) enum DataType {
    Null,
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F16,
    F32,
    F64,
    Str,
    BigStr,
    StrView,
    Bin,
    BigBin,
    BinView,
    Dict(Box<DataType>, Box<DataType>),
    Time(TimeUnit, Option<Arc<str>>),
}

/// Representation of Arrow's `TimeUnit` for table snapshots.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub(crate) enum TimeUnit {
    #[serde(rename = "s")]
    Second,
    #[serde(rename = "ms")]
    Millisecond,
    #[serde(rename = "us")]
    Microsecond,
    #[serde(rename = "ns")]
    Nanosecond,
}

impl From<arrow::datatypes::TimeUnit> for TimeUnit {
    fn from(arrow_unit: arrow::datatypes::TimeUnit) -> Self {
        match arrow_unit {
            arrow::datatypes::TimeUnit::Second => Self::Second,
            arrow::datatypes::TimeUnit::Millisecond => Self::Millisecond,
            arrow::datatypes::TimeUnit::Microsecond => Self::Microsecond,
            arrow::datatypes::TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}

/// Used to annotate columns in a Schema by their respective type in the Influx Data Model
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum InfluxType {
    Tag,
    Field,
    Time,
}

impl From<InfluxColumnType> for InfluxType {
    fn from(col_type: InfluxColumnType) -> Self {
        match col_type {
            InfluxColumnType::Tag => Self::Tag,
            InfluxColumnType::Field(_) => Self::Field,
            InfluxColumnType::Timestamp => Self::Time,
        }
    }
}

impl From<InfluxColumnType> for DataType {
    fn from(value: InfluxColumnType) -> Self {
        match value {
            InfluxColumnType::Tag => Self::Dict(Box::new(Self::I32), Box::new(Self::Str)),
            InfluxColumnType::Field(field) => match field {
                InfluxFieldType::Float => Self::F64,
                InfluxFieldType::Integer => Self::I64,
                InfluxFieldType::UInteger => Self::U64,
                InfluxFieldType::String => Self::Str,
                InfluxFieldType::Boolean => Self::Bool,
            },
            InfluxColumnType::Timestamp => Self::Time(TimeUnit::Nanosecond, TIME_DATA_TIMEZONE()),
        }
    }
}

impl From<&ArrowDataType> for DataType {
    fn from(arrow_type: &ArrowDataType) -> Self {
        match arrow_type {
            ArrowDataType::Null => Self::Null,
            ArrowDataType::Boolean => Self::Bool,
            ArrowDataType::Int8 => Self::I8,
            ArrowDataType::Int16 => Self::I16,
            ArrowDataType::Int32 => Self::I32,
            ArrowDataType::Int64 => Self::I64,
            ArrowDataType::UInt8 => Self::U8,
            ArrowDataType::UInt16 => Self::U16,
            ArrowDataType::UInt32 => Self::U32,
            ArrowDataType::UInt64 => Self::U64,
            ArrowDataType::Float16 => Self::F16,
            ArrowDataType::Float32 => Self::F32,
            ArrowDataType::Float64 => Self::F64,
            ArrowDataType::Timestamp(unit, tz) => Self::Time((*unit).into(), tz.clone()),
            ArrowDataType::Date32 => unimplemented!(),
            ArrowDataType::Date64 => unimplemented!(),
            ArrowDataType::Time32(_) => unimplemented!(),
            ArrowDataType::Time64(_) => unimplemented!(),
            ArrowDataType::Duration(_) => unimplemented!(),
            ArrowDataType::Interval(_) => unimplemented!(),
            ArrowDataType::Binary => Self::Bin,
            ArrowDataType::FixedSizeBinary(_) => unimplemented!(),
            ArrowDataType::LargeBinary => Self::BigBin,
            ArrowDataType::BinaryView => Self::BinView,
            ArrowDataType::Utf8 => Self::Str,
            ArrowDataType::LargeUtf8 => Self::BigStr,
            ArrowDataType::Utf8View => Self::StrView,
            ArrowDataType::List(_) => unimplemented!(),
            ArrowDataType::ListView(_) => unimplemented!(),
            ArrowDataType::FixedSizeList(_, _) => unimplemented!(),
            ArrowDataType::LargeList(_) => unimplemented!(),
            ArrowDataType::LargeListView(_) => unimplemented!(),
            ArrowDataType::Struct(_) => unimplemented!(),
            ArrowDataType::Union(_, _) => unimplemented!(),
            ArrowDataType::Dictionary(key_type, val_type) => Self::Dict(
                Box::new(key_type.as_ref().into()),
                Box::new(val_type.as_ref().into()),
            ),
            ArrowDataType::Decimal128(_, _) => unimplemented!(),
            ArrowDataType::Decimal256(_, _) => unimplemented!(),
            ArrowDataType::Map(_, _) => unimplemented!(),
            ArrowDataType::RunEndEncoded(_, _) => unimplemented!(),
        }
    }
}

// NOTE: Ideally, we will remove the need for the InfluxFieldType, and be able
// to use Arrow's DataType directly. If that happens, this conversion will need
// to support the entirety of Arrow's DataType enum, which is why [`DataType`]
// has been defined to mimic the Arrow type.
//
// See <https://github.com/influxdata/influxdb_iox/issues/11111>
impl From<DataType> for InfluxFieldType {
    fn from(data_type: DataType) -> Self {
        match data_type {
            DataType::Bool => Self::Boolean,
            DataType::I64 => Self::Integer,
            DataType::U64 => Self::UInteger,
            DataType::F64 => Self::Float,
            DataType::Str => Self::String,
            other => unimplemented!("unsupported data type in catalog {other:?}"),
        }
    }
}

impl From<&DataType> for InfluxFieldType {
    fn from(data_type: &DataType) -> Self {
        match data_type {
            DataType::Bool => Self::Boolean,
            DataType::I64 => Self::Integer,
            DataType::U64 => Self::UInteger,
            DataType::F64 => Self::Float,
            DataType::Str => Self::String,
            other => unimplemented!("unsupported data type in catalog {other:?}"),
        }
    }
}

#[cfg(test)]
pub(crate) mod test_util {
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
        CatalogSnapshot, ColumnDefinitionSnapshot, DataType, DatabaseSnapshot,
        DistinctCacheSnapshot, InfluxType, LastCacheSnapshot, NodeMode, NodeSnapshot,
        ProcessingEngineTriggerSnapshot, RepositorySnapshot, TableSnapshot,
        TriggerSpecificationDefinition,
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
}
