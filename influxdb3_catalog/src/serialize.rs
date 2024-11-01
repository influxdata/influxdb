use crate::catalog::ColumnDefinition;
use crate::catalog::DatabaseSchema;
use crate::catalog::TableDefinition;
use arrow::datatypes::DataType as ArrowDataType;
use bimap::BiHashMap;
use influxdb3_id::ColumnId;
use influxdb3_id::DbId;
use influxdb3_id::SerdeVecMap;
use influxdb3_id::TableId;
use influxdb3_wal::{LastCacheDefinition, LastCacheValueColumnsDef};
use schema::InfluxColumnType;
use schema::InfluxFieldType;
use schema::TIME_DATA_TIMEZONE;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

impl Serialize for DatabaseSchema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let snapshot = DatabaseSnapshot::from(self);
        snapshot.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DatabaseSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        DatabaseSnapshot::deserialize(deserializer).map(Into::into)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DatabaseSnapshot {
    id: DbId,
    name: Arc<str>,
    tables: SerdeVecMap<TableId, TableSnapshot>,
}

impl From<&DatabaseSchema> for DatabaseSnapshot {
    fn from(db: &DatabaseSchema) -> Self {
        Self {
            id: db.id,
            name: Arc::clone(&db.name),
            tables: db
                .tables
                .iter()
                .map(|(table_id, table_def)| (*table_id, table_def.as_ref().into()))
                .collect(),
        }
    }
}

impl From<DatabaseSnapshot> for DatabaseSchema {
    fn from(snap: DatabaseSnapshot) -> Self {
        let mut table_map = BiHashMap::with_capacity(snap.tables.len());
        let tables = snap
            .tables
            .into_iter()
            .map(|(id, table)| {
                table_map.insert(id, Arc::clone(&table.table_name));
                (id, Arc::new(table.into()))
            })
            .collect();
        Self {
            id: snap.id,
            name: snap.name,
            tables,
            table_map,
        }
    }
}

impl Serialize for TableDefinition {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let snapshot = TableSnapshot::from(self);
        snapshot.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TableDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        TableSnapshot::deserialize(deserializer).map(Into::into)
    }
}

/// A snapshot of a [`TableDefinition`] used for serialization of table information from the
/// catalog.
///
/// This is used over serde's `Serialize`/`Deserialize` implementations on the inner `Schema` type
/// due to them being considered unstable. This type intends to mimic the structure of the Arrow
/// `Schema`, and will help guard against potential breaking changes to the Arrow Schema types.
#[derive(Debug, Serialize, Deserialize)]
struct TableSnapshot {
    table_id: TableId,
    table_name: Arc<str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key: Option<Vec<ColumnId>>,
    cols: SerdeVecMap<ColumnId, ColumnDefinitionSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    last_caches: Vec<LastCacheSnapshot>,
}

/// Representation of Arrow's `DataType` for table snapshots.
///
/// Uses `#[non_exhaustive]` with the assumption that variants will be added as we support
/// more Arrow data types.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
enum DataType {
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
enum TimeUnit {
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
enum InfluxType {
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

/// The inner column definition for a [`TableSnapshot`]
#[derive(Debug, Serialize, Deserialize)]
struct ColumnDefinitionSnapshot {
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

impl From<ColumnDefinitionSnapshot> for ColumnDefinition {
    fn from(snap: ColumnDefinitionSnapshot) -> Self {
        Self {
            id: snap.id,
            name: Arc::clone(&snap.name),
            data_type: match snap.influx_type {
                InfluxType::Tag => InfluxColumnType::Tag,
                InfluxType::Field => InfluxColumnType::Field(InfluxFieldType::from(&snap.r#type)),
                InfluxType::Time => InfluxColumnType::Timestamp,
            },
            nullable: snap.nullable,
        }
    }
}

impl From<&TableDefinition> for TableSnapshot {
    fn from(def: &TableDefinition) -> Self {
        Self {
            table_id: def.table_id,
            table_name: Arc::clone(&def.table_name),
            key: def.series_key.clone(),
            cols: def
                .columns
                .iter()
                .map(|(col_id, col_def)| {
                    (
                        *col_id,
                        ColumnDefinitionSnapshot {
                            name: Arc::clone(&col_def.name),
                            id: *col_id,
                            r#type: col_def.data_type.into(),
                            influx_type: col_def.data_type.into(),
                            nullable: col_def.nullable,
                        },
                    )
                })
                .collect(),
            last_caches: def.last_caches.values().map(Into::into).collect(),
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

impl From<TableSnapshot> for TableDefinition {
    fn from(snap: TableSnapshot) -> Self {
        let table_id = snap.table_id;
        let table_def = Self::new(
            table_id,
            snap.table_name,
            snap.cols
                .into_iter()
                .map(|(id, def)| {
                    (
                        id,
                        def.name,
                        match def.influx_type {
                            InfluxType::Tag => InfluxColumnType::Tag,
                            InfluxType::Field => {
                                InfluxColumnType::Field(InfluxFieldType::from(def.r#type))
                            }
                            InfluxType::Time => InfluxColumnType::Timestamp,
                        },
                    )
                })
                .collect(),
            snap.key,
        )
        .expect("serialized catalog should be valid");
        Self {
            last_caches: snap
                .last_caches
                .into_iter()
                .map(|lc_snap| (Arc::clone(&lc_snap.name), lc_snap.into()))
                .collect(),
            ..table_def
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
struct LastCacheSnapshot {
    table_id: TableId,
    table: Arc<str>,
    name: Arc<str>,
    keys: Vec<ColumnId>,
    vals: Option<Vec<ColumnId>>,
    n: usize,
    ttl: u64,
}

impl From<&LastCacheDefinition> for LastCacheSnapshot {
    fn from(lcd: &LastCacheDefinition) -> Self {
        Self {
            table_id: lcd.table_id,
            table: Arc::clone(&lcd.table),
            name: Arc::clone(&lcd.name),
            keys: lcd.key_columns.to_vec(),
            vals: match &lcd.value_columns {
                LastCacheValueColumnsDef::Explicit { columns } => Some(columns.to_vec()),
                LastCacheValueColumnsDef::AllNonKeyColumns => None,
            },
            n: lcd.count.into(),
            ttl: lcd.ttl,
        }
    }
}

impl From<LastCacheSnapshot> for LastCacheDefinition {
    fn from(snap: LastCacheSnapshot) -> Self {
        Self {
            table_id: snap.table_id,
            table: snap.table,
            name: snap.name,
            key_columns: snap.keys,
            value_columns: match snap.vals {
                Some(columns) => LastCacheValueColumnsDef::Explicit { columns },
                None => LastCacheValueColumnsDef::AllNonKeyColumns,
            },
            count: snap
                .n
                .try_into()
                .expect("catalog contains invalid last cache size"),
            ttl: snap.ttl,
        }
    }
}
