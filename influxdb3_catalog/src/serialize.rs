use crate::catalog::TableDefinition;
use arrow::datatypes::DataType as ArrowDataType;
use influxdb3_id::TableId;
use influxdb3_wal::{LastCacheDefinition, LastCacheValueColumnsDef};
use schema::{InfluxColumnType, SchemaBuilder};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
        TableSnapshot::<'de>::deserialize(deserializer).map(Into::into)
    }
}

/// A snapshot of a [`TableDefinition`] used for serialization of table information from the
/// catalog.
///
/// This is used over serde's `Serialize`/`Deserialize` implementations on the inner `Schema` type
/// due to them being considered unstable. This type intends to mimic the structure of the Arrow
/// `Schema`, and will help guard against potential breaking changes to the Arrow Schema types.
#[serde_with::serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct TableSnapshot<'a> {
    table_id: TableId,
    name: &'a str,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key: Option<Vec<&'a str>>,
    #[serde_as(as = "serde_with::MapPreventDuplicates<_, _>")]
    cols: BTreeMap<&'a str, ColumnDefinition<'a>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    last_caches: Vec<LastCacheSnapshot<'a>>,
}

/// Representation of Arrow's `DataType` for table snapshots.
///
/// Uses `#[non_exhaustive]` with the assumption that variants will be added as we support
/// more Arrow data types.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
enum DataType<'a> {
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
    Dict(Box<DataType<'a>>, Box<DataType<'a>>),
    Time(TimeUnit, Option<&'a str>),
}

/// Representation of Arrow's `TimeUnit` for table snapshots.
#[derive(Debug, Serialize, Deserialize)]
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

/// The inner column definition for a [`TableSnapshot`]
#[derive(Debug, Serialize, Deserialize)]
struct ColumnDefinition<'a> {
    /// The column's data type
    #[serde(borrow)]
    r#type: DataType<'a>,
    /// The columns Influx type
    influx_type: InfluxType,
    /// Whether the column can hold NULL values
    nullable: bool,
}

impl<'a> From<&'a TableDefinition> for TableSnapshot<'a> {
    fn from(def: &'a TableDefinition) -> Self {
        let cols = def
            .schema()
            .iter()
            .map(|(col_type, f)| {
                (
                    f.name().as_str(),
                    ColumnDefinition {
                        r#type: f.data_type().into(),
                        influx_type: col_type.into(),
                        nullable: f.is_nullable(),
                    },
                )
            })
            .collect();
        let keys = def.schema().series_key();
        let last_caches = def.last_caches.values().map(Into::into).collect();
        Self {
            table_id: def.table_id,
            name: def.name.as_ref(),
            cols,
            key: keys,
            last_caches,
        }
    }
}

impl<'a> From<&'a ArrowDataType> for DataType<'a> {
    fn from(arrow_type: &'a ArrowDataType) -> Self {
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
            ArrowDataType::Timestamp(unit, tz) => Self::Time((*unit).into(), tz.as_deref()),
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

impl<'a> From<TableSnapshot<'a>> for TableDefinition {
    fn from(snap: TableSnapshot<'a>) -> Self {
        let name = snap.name.into();
        let table_id = snap.table_id;
        let mut b = SchemaBuilder::new();
        b.measurement(snap.name.to_string());
        if let Some(keys) = snap.key {
            b.with_series_key(keys);
        }
        for (name, col) in snap.cols {
            match col.influx_type {
                InfluxType::Tag => {
                    b.influx_column(name, schema::InfluxColumnType::Tag);
                }
                InfluxType::Field => {
                    b.influx_field(name, col.r#type.into());
                }
                InfluxType::Time => {
                    b.timestamp();
                }
            }
        }

        let schema = b.build().expect("valid schema from snapshot");
        let last_caches = snap
            .last_caches
            .into_iter()
            .map(|lc_snap| (lc_snap.name.to_string(), lc_snap.into()))
            .collect();

        Self {
            table_id,
            name,
            schema,
            last_caches,
        }
    }
}

// NOTE: Ideally, we will remove the need for the InfluxFieldType, and be able
// to use Arrow's DataType directly. If that happens, this conversion will need
// to support the entirety of Arrow's DataType enum, which is why [`DataType`]
// has been defined to mimic the Arrow type.
//
// See <https://github.com/influxdata/influxdb_iox/issues/11111>
impl<'a> From<DataType<'a>> for schema::InfluxFieldType {
    fn from(data_type: DataType<'a>) -> Self {
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
struct LastCacheSnapshot<'a> {
    table: &'a str,
    name: &'a str,
    keys: Vec<&'a str>,
    vals: Option<Vec<&'a str>>,
    n: usize,
    ttl: u64,
}

impl<'a> From<&'a LastCacheDefinition> for LastCacheSnapshot<'a> {
    fn from(lcd: &'a LastCacheDefinition) -> Self {
        Self {
            table: &lcd.table,
            name: &lcd.name,
            keys: lcd.key_columns.iter().map(|v| v.as_str()).collect(),
            vals: match &lcd.value_columns {
                LastCacheValueColumnsDef::Explicit { columns } => {
                    Some(columns.iter().map(|v| v.as_str()).collect())
                }
                LastCacheValueColumnsDef::AllNonKeyColumns => None,
            },
            n: lcd.count.into(),
            ttl: lcd.ttl,
        }
    }
}

impl<'a> From<LastCacheSnapshot<'a>> for LastCacheDefinition {
    fn from(snap: LastCacheSnapshot<'a>) -> Self {
        Self {
            table: snap.table.to_string(),
            name: snap.name.to_string(),
            key_columns: snap.keys.iter().map(|s| s.to_string()).collect(),
            value_columns: match snap.vals {
                Some(cols) => LastCacheValueColumnsDef::Explicit {
                    columns: cols.iter().map(|s| s.to_string()).collect(),
                },
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
