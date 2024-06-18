use std::collections::BTreeMap;

use arrow::datatypes::DataType as ArrowDataType;
use schema::{InfluxColumnType, SchemaBuilder};
use serde::{Deserialize, Serialize};

use super::TableDefinition;

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
    name: &'a str,
    #[serde_as(as = "serde_with::MapPreventDuplicates<_, _>")]
    cols: BTreeMap<&'a str, ColumnDefinition<'a>>,
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
        let name = def.name.as_str();
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
        Self { name, cols }
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
            // Arrow's TimeUnit does not impl Copy, so we cheaply clone it:
            // See <https://github.com/apache/arrow-rs/issues/5839>
            ArrowDataType::Timestamp(unit, tz) => Self::Time(unit.clone().into(), tz.as_deref()),
            ArrowDataType::Date32 => todo!(),
            ArrowDataType::Date64 => todo!(),
            ArrowDataType::Time32(_) => todo!(),
            ArrowDataType::Time64(_) => todo!(),
            ArrowDataType::Duration(_) => todo!(),
            ArrowDataType::Interval(_) => todo!(),
            ArrowDataType::Binary => Self::Bin,
            ArrowDataType::FixedSizeBinary(_) => todo!(),
            ArrowDataType::LargeBinary => Self::BigBin,
            ArrowDataType::BinaryView => Self::BinView,
            ArrowDataType::Utf8 => Self::Str,
            ArrowDataType::LargeUtf8 => Self::BigStr,
            ArrowDataType::Utf8View => Self::StrView,
            ArrowDataType::List(_) => todo!(),
            ArrowDataType::ListView(_) => todo!(),
            ArrowDataType::FixedSizeList(_, _) => todo!(),
            ArrowDataType::LargeList(_) => todo!(),
            ArrowDataType::LargeListView(_) => todo!(),
            ArrowDataType::Struct(_) => todo!(),
            ArrowDataType::Union(_, _) => todo!(),
            ArrowDataType::Dictionary(key_type, val_type) => Self::Dict(
                Box::new(key_type.as_ref().into()),
                Box::new(val_type.as_ref().into()),
            ),
            ArrowDataType::Decimal128(_, _) => todo!(),
            ArrowDataType::Decimal256(_, _) => todo!(),
            ArrowDataType::Map(_, _) => todo!(),
            ArrowDataType::RunEndEncoded(_, _) => todo!(),
        }
    }
}

impl<'a> From<TableSnapshot<'a>> for TableDefinition {
    fn from(snap: TableSnapshot<'a>) -> Self {
        let name = snap.name.to_owned();
        let mut b = SchemaBuilder::new();
        b.measurement(&name);
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

        Self { name, schema }
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
