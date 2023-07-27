//! Types having to do with columns.

use super::TableId;
use generated_types::influxdata::iox::{gossip, schema::v1 as proto};
use influxdb_line_protocol::FieldValue;
use schema::{builder::SchemaBuilder, InfluxColumnType, InfluxFieldType, Schema};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    convert::TryFrom,
    ops::Deref,
};

/// Unique ID for a `Column`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct ColumnId(i64);

#[allow(missing_docs)]
impl ColumnId {
    pub fn new(v: i64) -> Self {
        Self(v)
    }
    pub fn get(&self) -> i64 {
        self.0
    }
}

/// Column definitions for a table indexed by their name
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnsByName(BTreeMap<String, ColumnSchema>);

impl From<BTreeMap<String, ColumnSchema>> for ColumnsByName {
    fn from(value: BTreeMap<String, ColumnSchema>) -> Self {
        Self(value)
    }
}

impl ColumnsByName {
    /// Create a new instance holding the given [`Column`]s.
    pub fn new(columns: impl IntoIterator<Item = Column>) -> Self {
        Self(
            columns
                .into_iter()
                .map(|c| {
                    (
                        c.name,
                        ColumnSchema {
                            id: c.id,
                            column_type: c.column_type,
                        },
                    )
                })
                .collect(),
        )
    }

    /// Add the given column name and schema to this set of columns.
    ///
    /// # Panics
    ///
    /// This method panics if a column of the same name already exists in `self`.
    pub fn add_column(&mut self, column_name: String, column_schema: ColumnSchema) {
        let old = self.0.insert(column_name, column_schema);
        assert!(old.is_none());
    }

    /// Iterate over the names and columns.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &ColumnSchema)> {
        self.0.iter()
    }

    /// Whether a column with this name is in the set.
    pub fn contains_column_name(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    /// Return number of columns in the set.
    pub fn column_count(&self) -> usize {
        self.0.len()
    }

    /// Return the set of column names. Used in combination with a write operation's
    /// column names to determine whether a write would exceed the max allowed columns.
    pub fn names(&self) -> BTreeSet<&str> {
        self.0.keys().map(|name| name.as_str()).collect()
    }

    /// Return an iterator of the set of column IDs.
    pub fn ids(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.0.values().map(|c| c.id)
    }

    /// Get a column by its name.
    pub fn get(&self, name: &str) -> Option<&ColumnSchema> {
        self.0.get(name)
    }

    /// Create `ID->name` map for columns.
    pub fn id_map(&self) -> HashMap<ColumnId, &str> {
        self.0
            .iter()
            .map(|(name, c)| (c.id, name.as_str()))
            .collect()
    }
}

impl IntoIterator for ColumnsByName {
    type Item = (String, ColumnSchema);
    type IntoIter = std::collections::btree_map::IntoIter<String, ColumnSchema>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

// ColumnsByName is a newtype so that we can implement this `TryFrom` in this crate
impl TryFrom<ColumnsByName> for Schema {
    type Error = schema::builder::Error;

    fn try_from(value: ColumnsByName) -> Result<Self, Self::Error> {
        let mut builder = SchemaBuilder::new();

        for (column_name, column_schema) in value.into_iter() {
            let t = InfluxColumnType::from(column_schema.column_type);
            builder.influx_column(column_name, t);
        }

        builder.build()
    }
}

/// Data object for a column
#[derive(Debug, Clone, sqlx::FromRow, Eq, PartialEq)]
pub struct Column {
    /// the column id
    pub id: ColumnId,
    /// the table id the column is in
    pub table_id: TableId,
    /// the name of the column, which is unique in the table
    pub name: String,
    /// the logical type of the column
    pub column_type: ColumnType,
}

impl Column {
    /// returns true if the column type is a tag
    pub fn is_tag(&self) -> bool {
        self.column_type == ColumnType::Tag
    }

    /// returns true if the column type matches the line protocol field value type
    pub fn matches_field_type(&self, field_value: &FieldValue) -> bool {
        match field_value {
            FieldValue::I64(_) => self.column_type == ColumnType::I64,
            FieldValue::U64(_) => self.column_type == ColumnType::U64,
            FieldValue::F64(_) => self.column_type == ColumnType::F64,
            FieldValue::String(_) => self.column_type == ColumnType::String,
            FieldValue::Boolean(_) => self.column_type == ColumnType::Bool,
        }
    }
}

/// The column id and its type for a column
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ColumnSchema {
    /// the column id
    pub id: ColumnId,
    /// the column type
    pub column_type: ColumnType,
}

impl ColumnSchema {
    /// returns true if the column is a tag
    pub fn is_tag(&self) -> bool {
        self.column_type == ColumnType::Tag
    }

    /// returns true if the column matches the line protocol field value type
    pub fn matches_field_type(&self, field_value: &FieldValue) -> bool {
        matches!(
            (field_value, self.column_type),
            (FieldValue::I64(_), ColumnType::I64)
                | (FieldValue::U64(_), ColumnType::U64)
                | (FieldValue::F64(_), ColumnType::F64)
                | (FieldValue::String(_), ColumnType::String)
                | (FieldValue::Boolean(_), ColumnType::Bool)
        )
    }

    /// Returns true if `mb_column` is of the same type as `self`.
    pub fn matches_type(&self, mb_column_influx_type: InfluxColumnType) -> bool {
        self.column_type == mb_column_influx_type
    }
}

impl TryFrom<&gossip::v1::Column> for ColumnSchema {
    type Error = Box<dyn std::error::Error>;

    fn try_from(v: &gossip::v1::Column) -> Result<Self, Self::Error> {
        Ok(Self {
            id: ColumnId::new(v.column_id),
            column_type: ColumnType::try_from(v.column_type as i16)?,
        })
    }
}

/// The column data type
#[allow(missing_docs)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash, sqlx::Type)]
#[repr(i16)]
pub enum ColumnType {
    I64 = 1,
    U64 = 2,
    F64 = 3,
    Bool = 4,
    String = 5,
    Time = 6,
    Tag = 7,
}

impl ColumnType {
    /// the short string description of the type
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::I64 => "i64",
            Self::U64 => "u64",
            Self::F64 => "f64",
            Self::Bool => "bool",
            Self::String => "string",
            Self::Time => "time",
            Self::Tag => "tag",
        }
    }
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.as_str();

        write!(f, "{s}")
    }
}

impl TryFrom<i16> for ColumnType {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            x if x == Self::I64 as i16 => Ok(Self::I64),
            x if x == Self::U64 as i16 => Ok(Self::U64),
            x if x == Self::F64 as i16 => Ok(Self::F64),
            x if x == Self::Bool as i16 => Ok(Self::Bool),
            x if x == Self::String as i16 => Ok(Self::String),
            x if x == Self::Time as i16 => Ok(Self::Time),
            x if x == Self::Tag as i16 => Ok(Self::Tag),
            _ => Err("invalid column value".into()),
        }
    }
}

impl From<InfluxColumnType> for ColumnType {
    fn from(value: InfluxColumnType) -> Self {
        match value {
            InfluxColumnType::Tag => Self::Tag,
            InfluxColumnType::Field(InfluxFieldType::Float) => Self::F64,
            InfluxColumnType::Field(InfluxFieldType::Integer) => Self::I64,
            InfluxColumnType::Field(InfluxFieldType::UInteger) => Self::U64,
            InfluxColumnType::Field(InfluxFieldType::String) => Self::String,
            InfluxColumnType::Field(InfluxFieldType::Boolean) => Self::Bool,
            InfluxColumnType::Timestamp => Self::Time,
        }
    }
}

impl From<ColumnType> for InfluxColumnType {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::I64 => Self::Field(InfluxFieldType::Integer),
            ColumnType::U64 => Self::Field(InfluxFieldType::UInteger),
            ColumnType::F64 => Self::Field(InfluxFieldType::Float),
            ColumnType::Bool => Self::Field(InfluxFieldType::Boolean),
            ColumnType::String => Self::Field(InfluxFieldType::String),
            ColumnType::Time => Self::Timestamp,
            ColumnType::Tag => Self::Tag,
        }
    }
}

impl PartialEq<InfluxColumnType> for ColumnType {
    fn eq(&self, got: &InfluxColumnType) -> bool {
        match self {
            Self::I64 => matches!(got, InfluxColumnType::Field(InfluxFieldType::Integer)),
            Self::U64 => matches!(got, InfluxColumnType::Field(InfluxFieldType::UInteger)),
            Self::F64 => matches!(got, InfluxColumnType::Field(InfluxFieldType::Float)),
            Self::Bool => matches!(got, InfluxColumnType::Field(InfluxFieldType::Boolean)),
            Self::String => matches!(got, InfluxColumnType::Field(InfluxFieldType::String)),
            Self::Time => matches!(got, InfluxColumnType::Timestamp),
            Self::Tag => matches!(got, InfluxColumnType::Tag),
        }
    }
}

/// Returns the `ColumnType` for the passed in line protocol `FieldValue` type
pub fn column_type_from_field(field_value: &FieldValue) -> ColumnType {
    match field_value {
        FieldValue::I64(_) => ColumnType::I64,
        FieldValue::U64(_) => ColumnType::U64,
        FieldValue::F64(_) => ColumnType::F64,
        FieldValue::String(_) => ColumnType::String,
        FieldValue::Boolean(_) => ColumnType::Bool,
    }
}

impl TryFrom<proto::column_schema::ColumnType> for ColumnType {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: proto::column_schema::ColumnType) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::column_schema::ColumnType::I64 => ColumnType::I64,
            proto::column_schema::ColumnType::U64 => ColumnType::U64,
            proto::column_schema::ColumnType::F64 => ColumnType::F64,
            proto::column_schema::ColumnType::Bool => ColumnType::Bool,
            proto::column_schema::ColumnType::String => ColumnType::String,
            proto::column_schema::ColumnType::Time => ColumnType::Time,
            proto::column_schema::ColumnType::Tag => ColumnType::Tag,
            proto::column_schema::ColumnType::Unspecified => {
                return Err("unknown column type".into())
            }
        })
    }
}

/// Set of columns.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::Type)]
#[sqlx(transparent, no_pg_array)]
pub struct ColumnSet(Vec<ColumnId>);

impl ColumnSet {
    /// Create new column set.
    ///
    /// The order of the passed columns will NOT be preserved.
    ///
    /// # Panic
    /// Panics when the set of passed columns contains duplicates.
    pub fn new<I>(columns: I) -> Self
    where
        I: IntoIterator<Item = ColumnId>,
    {
        let mut columns: Vec<ColumnId> = columns.into_iter().collect();
        columns.sort();

        let len_pre_dedup = columns.len();
        columns.dedup();
        let len_post_dedup = columns.len();
        assert_eq!(len_pre_dedup, len_post_dedup, "set contains duplicates");

        columns.shrink_to_fit();

        Self(columns)
    }

    /// Estimate the memory consumption of this object and its contents
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self) + (std::mem::size_of::<ColumnId>() * self.0.capacity())
    }
}

impl From<ColumnSet> for Vec<ColumnId> {
    fn from(set: ColumnSet) -> Self {
        set.0
    }
}

impl Deref for ColumnSet {
    type Target = [ColumnId];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use super::*;

    #[test]
    #[should_panic = "set contains duplicates"]
    fn test_column_set_duplicates() {
        ColumnSet::new([ColumnId::new(1), ColumnId::new(2), ColumnId::new(1)]);
    }

    #[test]
    fn test_column_schema() {
        assert_eq!(
            ColumnType::try_from(proto::column_schema::ColumnType::I64).unwrap(),
            ColumnType::I64,
        );
        assert_eq!(
            ColumnType::try_from(proto::column_schema::ColumnType::U64).unwrap(),
            ColumnType::U64,
        );
        assert_eq!(
            ColumnType::try_from(proto::column_schema::ColumnType::F64).unwrap(),
            ColumnType::F64,
        );
        assert_eq!(
            ColumnType::try_from(proto::column_schema::ColumnType::Bool).unwrap(),
            ColumnType::Bool,
        );
        assert_eq!(
            ColumnType::try_from(proto::column_schema::ColumnType::String).unwrap(),
            ColumnType::String,
        );
        assert_eq!(
            ColumnType::try_from(proto::column_schema::ColumnType::Time).unwrap(),
            ColumnType::Time,
        );
        assert_eq!(
            ColumnType::try_from(proto::column_schema::ColumnType::Tag).unwrap(),
            ColumnType::Tag,
        );

        assert!(ColumnType::try_from(proto::column_schema::ColumnType::Unspecified).is_err());
    }

    #[test]
    fn test_gossip_proto_conversion() {
        let proto = gossip::v1::Column {
            name: "bananas".to_string(),
            column_id: 42,
            column_type: gossip::v1::column::ColumnType::String as _,
        };

        let got = ColumnSchema::try_from(&proto).expect("should succeed");
        assert_matches!(got, ColumnSchema{id, column_type} => {
            assert_eq!(id.get(), 42);
            assert_eq!(column_type, ColumnType::String);
        });
    }

    #[test]
    fn test_gossip_proto_conversion_invalid_type() {
        let proto = gossip::v1::Column {
            name: "bananas".to_string(),
            column_id: 42,
            column_type: 42,
        };

        ColumnSchema::try_from(&proto).expect_err("should succeed");
    }
}
