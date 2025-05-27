/// The v3 changes are _only_ done for token permission mapping `v2`s `db:*:write[,read]` is mapped
/// to `v3`s `db:*:write,create[,read]` permission. There are no structural changes in the catalog
/// log files.
use crate::catalog::CatalogSequenceNumber;
use crate::log::{
    MaxAge, MaxCardinality, NodeMode, TriggerSettings, TriggerSpecificationDefinition,
};
use arrow::datatypes::DataType as ArrowDataType;
use hashbrown::HashMap;
use influxdb3_id::{
    CatalogId, ColumnId, DbId, DistinctCacheId, LastCacheId, NodeId, SerdeVecMap, TableId, TokenId,
    TriggerId,
};
use schema::{InfluxColumnType, InfluxFieldType, TIME_DATA_TIMEZONE};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub(crate) nodes: RepositorySnapshot<NodeId, NodeSnapshot>,
    pub(crate) databases: RepositorySnapshot<DbId, DatabaseSnapshot>,
    pub(crate) sequence: CatalogSequenceNumber,
    #[serde(default)]
    pub(crate) tokens: RepositorySnapshot<TokenId, TokenInfoSnapshot>,
    pub(crate) catalog_id: Arc<str>,
    pub(crate) catalog_uuid: Uuid,
}

impl CatalogSnapshot {
    pub(crate) fn sequence_number(&self) -> CatalogSequenceNumber {
        self.sequence
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct TokenInfoSnapshot {
    pub id: TokenId,
    pub name: Arc<str>,
    pub hash: Vec<u8>,
    pub created_at: i64,
    pub description: Option<String>,
    pub created_by: Option<TokenId>,
    pub expiry: i64,
    pub updated_by: Option<TokenId>,
    pub updated_at: Option<i64>,
    pub permissions: Vec<PermissionSnapshot>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PermissionSnapshot {
    pub resource_type: ResourceTypeSnapshot,
    pub resource_identifier: ResourceIdentifierSnapshot,
    pub actions: ActionsSnapshot,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub(crate) enum ResourceTypeSnapshot {
    Database,
    Token,
    Wildcard,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum ResourceIdentifierSnapshot {
    Database(Vec<DbId>),
    Token(Vec<TokenId>),
    Wildcard,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum ActionsSnapshot {
    Database(DatabaseActionsSnapshot),
    Token(CrudActionsSnapshot),
    Wildcard,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct DatabaseActionsSnapshot(pub u16);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct CrudActionsSnapshot(pub u16);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct SystemActionsSnapshot(pub u16);

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct NodeSnapshot {
    pub(crate) node_id: Arc<str>,
    pub(crate) node_catalog_id: NodeId,
    pub(crate) instance_id: Arc<str>,
    pub(crate) mode: Vec<NodeMode>,
    pub(crate) state: NodeStateSnapshot,
    pub(crate) core_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DatabaseSnapshot {
    pub(crate) id: DbId,
    pub(crate) name: Arc<str>,
    pub(crate) tables: RepositorySnapshot<TableId, TableSnapshot>,
    pub(crate) processing_engine_triggers:
        RepositorySnapshot<TriggerId, ProcessingEngineTriggerSnapshot>,
    pub(crate) deleted: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TableSnapshot {
    pub(crate) table_id: TableId,
    pub(crate) table_name: Arc<str>,
    pub(crate) key: Vec<ColumnId>,
    pub(crate) columns: RepositorySnapshot<ColumnId, ColumnDefinitionSnapshot>,
    pub(crate) last_caches: RepositorySnapshot<LastCacheId, LastCacheSnapshot>,
    pub(crate) distinct_caches: RepositorySnapshot<DistinctCacheId, DistinctCacheSnapshot>,
    pub(crate) deleted: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ProcessingEngineTriggerSnapshot {
    pub trigger_id: TriggerId,
    pub trigger_name: Arc<str>,
    pub node_id: Arc<str>,
    pub plugin_filename: String,
    pub database_name: Arc<str>,
    pub trigger_specification: TriggerSpecificationDefinition,
    pub trigger_settings: TriggerSettings,
    pub trigger_arguments: Option<HashMap<String, String>>,
    pub disabled: bool,
}

/// The inner column definition for a [`TableSnapshot`]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ColumnDefinitionSnapshot {
    pub(crate) name: Arc<str>,
    /// The id of the column
    pub(crate) id: ColumnId,
    /// The column's data type
    pub(crate) r#type: DataType,
    /// The columns Influx type
    pub(crate) influx_type: InfluxType,
    /// Whether the column can hold NULL values
    pub(crate) nullable: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LastCacheSnapshot {
    pub(crate) table_id: TableId,
    pub(crate) table: Arc<str>,
    pub(crate) id: LastCacheId,
    pub(crate) name: Arc<str>,
    pub(crate) keys: Vec<ColumnId>,
    pub(crate) vals: Option<Vec<ColumnId>>,
    pub(crate) n: usize,
    pub(crate) ttl: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DistinctCacheSnapshot {
    pub(crate) table_id: TableId,
    pub(crate) table: Arc<str>,
    pub(crate) id: DistinctCacheId,
    pub(crate) name: Arc<str>,
    pub(crate) cols: Vec<ColumnId>,
    pub(crate) max_cardinality: MaxCardinality,
    pub(crate) max_age_seconds: MaxAge,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct RepositorySnapshot<I, R>
where
    I: CatalogId,
{
    pub(crate) repo: SerdeVecMap<I, R>,
    pub(crate) next_id: I,
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

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum NodeStateSnapshot {
    Running { registered_time_ns: i64 },
    Stopped { stopped_time_ns: i64 },
}
