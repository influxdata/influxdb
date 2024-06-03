use arrow::datatypes::DataType as ArrowDataType;
use schema::SchemaBuilder;
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

#[derive(Debug, Serialize, Deserialize)]
struct TableSnapshot<'a> {
    name: &'a str,
    cols: Vec<ColumnDefinition<'a>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
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
    Time(TimeUnit, Option<&'a str>),
}

#[derive(Debug, Serialize, Deserialize)]
enum TimeUnit {
    #[serde(rename = "s")]
    Seconds,
    #[serde(rename = "ms")]
    Milliseconds,
    #[serde(rename = "us")]
    Microseconds,
    #[serde(rename = "ns")]
    Nanoseconds,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum InfluxType {
    Tag,
    Field,
    Time,
}

#[derive(Debug, Serialize, Deserialize)]
struct ColumnDefinition<'a> {
    name: &'a str,
    r#type: DataType<'a>,
    meta: InfluxType,
    nullable: bool,
}

impl<'a> From<&'a TableDefinition> for TableSnapshot<'a> {
    fn from(def: &'a TableDefinition) -> Self {
        let name = def.name.as_str();
        let mut cols: Vec<ColumnDefinition<'_>> = def
            .schema()
            .fields_iter()
            .map(|f| ColumnDefinition {
                name: f.name(),
                r#type: f.data_type().into(),
                meta: InfluxType::Field,
                nullable: f.is_nullable(),
            })
            .collect();
        cols.extend(def.schema().tags_iter().map(|f| ColumnDefinition {
            name: f.name(),
            r#type: f.data_type().into(),
            meta: InfluxType::Tag,
            nullable: true,
        }));
        cols.extend(def.schema().time_iter().map(|f| ColumnDefinition {
            name: f.name(),
            r#type: f.data_type().into(),
            meta: InfluxType::Time,
            nullable: false,
        }));
        Self { name, cols }
    }
}

impl<'a> From<&'a ArrowDataType> for DataType<'a> {
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
            ArrowDataType::Timestamp(_, _) => todo!(),
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
            ArrowDataType::Dictionary(_, _) => todo!(),
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
        for col in snap.cols {
            match col.meta {
                InfluxType::Tag => {
                    b.influx_column(col.name, schema::InfluxColumnType::Tag);
                }
                InfluxType::Field => {
                    b.influx_field(col.name, col.r#type.into());
                }
                InfluxType::Time => (),
            }
        }
        b.timestamp();

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
