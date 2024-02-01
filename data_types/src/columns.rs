//! Types having to do with columns.

use super::TableId;
use generated_types::influxdata::iox::{column_type::v1 as proto, gossip};
use influxdb_line_protocol::FieldValue;
use schema::{builder::SchemaBuilder, sort::SortKey, InfluxColumnType, InfluxFieldType, Schema};
use snafu::Snafu;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    convert::TryFrom,
    ops::Deref,
    sync::Arc,
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
#[derive(Debug, Clone, Eq, PartialEq, Hash, Default)]
pub struct ColumnsByName(BTreeMap<Arc<str>, ColumnSchema>);

impl From<BTreeMap<Arc<str>, ColumnSchema>> for ColumnsByName {
    fn from(value: BTreeMap<Arc<str>, ColumnSchema>) -> Self {
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
                        Arc::from(c.name),
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
    pub fn add_column(&mut self, column_name: impl Into<Arc<str>>, column_schema: ColumnSchema) {
        let old = self.0.insert(column_name.into(), column_schema);
        assert!(old.is_none());
    }

    /// Iterate over the names and columns.
    pub fn iter(&self) -> impl Iterator<Item = (&Arc<str>, &ColumnSchema)> {
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
        self.0.keys().map(|name| name.as_ref()).collect()
    }

    /// Return an iterator of the set of column IDs.
    pub fn ids(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.0.values().map(|c| c.id)
    }

    /// Return column ids of the given column names
    ///
    /// # Panics
    ///
    /// Panics if any of the names are not found in this set.
    pub fn ids_for_names<T>(&self, names: impl IntoIterator<Item = T> + Send) -> SortKeyIds
    where
        T: AsRef<str>,
    {
        SortKeyIds::from(names.into_iter().map(|name| {
            let name = name.as_ref();
            self.get(name)
                .unwrap_or_else(|| panic!("column name not found: {}", name))
                .id
                .get()
        }))
    }

    /// Get a column by its name.
    pub fn get(&self, name: &str) -> Option<&ColumnSchema> {
        self.0.get(name)
    }

    /// Get the `ColumnId` for the time column, if present (a table created through
    /// `table_load_or_create` will always have a time column).
    pub fn time_column_id(&self) -> Option<ColumnId> {
        self.get(schema::TIME_COLUMN_NAME).map(|column| column.id)
    }

    /// Create `ID->name` map for columns.
    pub fn id_map(&self) -> HashMap<ColumnId, Arc<str>> {
        self.0
            .iter()
            .map(|(name, c)| (c.id, Arc::clone(name)))
            .collect()
    }
}

impl IntoIterator for ColumnsByName {
    type Item = (Arc<str>, ColumnSchema);
    type IntoIter = std::collections::btree_map::IntoIter<Arc<str>, ColumnSchema>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl FromIterator<(Arc<str>, ColumnSchema)> for ColumnsByName {
    fn from_iter<T: IntoIterator<Item = (Arc<str>, ColumnSchema)>>(iter: T) -> Self {
        Self(BTreeMap::from_iter(iter))
    }
}

// ColumnsByName is a newtype so that we can implement this `TryFrom` in this crate
impl TryFrom<ColumnsByName> for Schema {
    type Error = schema::builder::Error;

    fn try_from(value: ColumnsByName) -> Result<Self, Self::Error> {
        let mut builder = SchemaBuilder::new();

        for (column_name, column_schema) in value.into_iter() {
            let t = InfluxColumnType::from(column_schema.column_type);
            builder.influx_column(column_name.as_ref(), t);
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
    pub fn matches_field_type(&self, field_value: &FieldValue<'_>) -> bool {
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
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
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
    pub fn matches_field_type(&self, field_value: &FieldValue<'_>) -> bool {
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

/// Errors deserialising a protobuf serialised [`ColumnType`].
#[derive(Debug, Snafu)]
#[snafu(display("invalid column value"))]
#[allow(missing_copy_implementations)]
pub struct ColumnTypeProtoError {}

impl TryFrom<i16> for ColumnType {
    type Error = ColumnTypeProtoError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            x if x == Self::I64 as i16 => Ok(Self::I64),
            x if x == Self::U64 as i16 => Ok(Self::U64),
            x if x == Self::F64 as i16 => Ok(Self::F64),
            x if x == Self::Bool as i16 => Ok(Self::Bool),
            x if x == Self::String as i16 => Ok(Self::String),
            x if x == Self::Time as i16 => Ok(Self::Time),
            x if x == Self::Tag as i16 => Ok(Self::Tag),
            _ => Err(ColumnTypeProtoError {}),
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
pub fn column_type_from_field(field_value: &FieldValue<'_>) -> ColumnType {
    match field_value {
        FieldValue::I64(_) => ColumnType::I64,
        FieldValue::U64(_) => ColumnType::U64,
        FieldValue::F64(_) => ColumnType::F64,
        FieldValue::String(_) => ColumnType::String,
        FieldValue::Boolean(_) => ColumnType::Bool,
    }
}

impl TryFrom<proto::ColumnType> for ColumnType {
    type Error = &'static str;

    fn try_from(value: proto::ColumnType) -> Result<Self, Self::Error> {
        Ok(match value {
            proto::ColumnType::I64 => Self::I64,
            proto::ColumnType::U64 => Self::U64,
            proto::ColumnType::F64 => Self::F64,
            proto::ColumnType::Bool => Self::Bool,
            proto::ColumnType::String => Self::String,
            proto::ColumnType::Time => Self::Time,
            proto::ColumnType::Tag => Self::Tag,
            proto::ColumnType::Unspecified => return Err("unknown column type"),
        })
    }
}

impl From<ColumnType> for proto::ColumnType {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::I64 => Self::I64,
            ColumnType::U64 => Self::U64,
            ColumnType::F64 => Self::F64,
            ColumnType::Bool => Self::Bool,
            ColumnType::String => Self::String,
            ColumnType::Time => Self::Time,
            ColumnType::Tag => Self::Tag,
        }
    }
}

/// Set of columns and used as Set data type.
///
/// # Data Structure
/// This is internally implemented as a sorted vector. The sorting allows for fast [`PartialEq`]/[`Eq`]/[`Hash`] and
/// ensures that the PostgreSQL data is deterministic. Note that PostgreSQL does NOT have a set type at the moment, so
/// this is stored as an array.
#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::Type)]
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

        assert!(
            columns.windows(2).all(|w| w[0] != w[1]),
            "set contains duplicates"
        );

        columns.shrink_to_fit();

        Self(columns)
    }

    /// Create a new empty [`ColumnSet`]
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    /// Estimate the memory consumption of this object and its contents
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self) + (std::mem::size_of::<ColumnId>() * self.0.capacity())
    }

    /// The set is empty or not
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Computes the union of `self` and `other`
    pub fn union(&mut self, other: &Self) {
        let mut insert_idx = 0;
        let mut src_idx = 0;

        while insert_idx < self.0.len() && src_idx < other.0.len() {
            let s = self.0[insert_idx];
            let o = other.0[src_idx];

            match s.cmp(&o) {
                Ordering::Less => insert_idx += 1,
                Ordering::Equal => {
                    insert_idx += 1;
                    src_idx += 1;
                }
                Ordering::Greater => {
                    self.0.insert(insert_idx, o);
                    insert_idx += 1;
                    src_idx += 1;
                }
            }
        }
        self.0.extend_from_slice(&other.0[src_idx..]);
    }

    /// Returns the indices and ids in `self` that are present in both `self` and `other`
    ///
    /// ```
    /// # use data_types::{ColumnId, ColumnSet};
    /// let a = ColumnSet::new([1, 2, 4, 6, 7].into_iter().map(ColumnId::new));
    /// let b = ColumnSet::new([2, 4, 6].into_iter().map(ColumnId::new));
    ///
    /// assert_eq!(
    ///     a.intersect(&b).collect::<Vec<_>>(),
    ///     vec![(1, b[0]), (2, b[1]), (3, b[2])]
    /// )
    /// ```
    pub fn intersect<'a>(
        &'a self,
        other: &'a Self,
    ) -> impl Iterator<Item = (usize, ColumnId)> + 'a {
        let mut left_idx = 0;
        let mut right_idx = 0;
        std::iter::from_fn(move || loop {
            let s = self.0.get(left_idx)?;
            let o = other.get(right_idx)?;

            match s.cmp(o) {
                Ordering::Less => left_idx += 1,
                Ordering::Greater => right_idx += 1,
                Ordering::Equal => {
                    let t = left_idx;
                    left_idx += 1;
                    right_idx += 1;
                    return Some((t, *s));
                }
            }
        })
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

/// Set of sorted column IDs in a specific given order at creation time, to be used as a
/// [`SortKey`] by looking up the column names in the table's schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::Type, Default)]
#[sqlx(transparent, no_pg_array)]
pub struct SortKeyIds(Vec<ColumnId>);

impl SortKeyIds {
    /// Create new sorted column set.
    ///
    /// The order of the passed columns will be preserved.
    ///
    /// # Panic
    /// Panics when the set of passed columns contains duplicates.
    pub fn new<I>(columns: I) -> Self
    where
        I: IntoIterator<Item = ColumnId>,
    {
        let mut columns: Vec<ColumnId> = columns.into_iter().collect();

        // Validate the ID set contains no duplicates.
        //
        // This validates an invariant in debug builds, skipping the cost
        // for release builds.
        if cfg!(debug_assertions) {
            SortKeyIds::check_for_deplicates(&columns);
        }

        // Must continue with columns in original order
        columns.shrink_to_fit();

        Self(columns)
    }

    /// Given another set of sort key IDs, merge them together and, if needed, return a value to
    /// use to update the catalog.
    ///
    /// If `other` contains any column IDs that are not present in `self`, create a new
    /// `SortKeyIds` instance that includes the new columns in `other` (in the same order they
    /// appear in `other`) appended to the existing columns, but keeping the time column ID last.
    ///
    /// If existing columns appear in `self` in a different order than they appear in `other`, the
    /// order in `self` takes precedence and remains unchanged.
    ///
    /// If `self` contains all the sort keys in `other` already (regardless of order), this will
    /// return `None` as no update to the catalog is needed.
    pub fn maybe_append(&self, other: &Self, time_column_id: ColumnId) -> Option<Self> {
        let existing_columns_without_time = self
            .iter()
            .cloned()
            .filter(|&column_id| column_id != time_column_id);

        let mut new_columns = other
            .iter()
            .cloned()
            .filter(|column_id| !self.contains(column_id))
            .peekable();

        if new_columns.peek().is_none() {
            None
        } else {
            Some(SortKeyIds::new(
                existing_columns_without_time
                    .chain(new_columns)
                    .chain(std::iter::once(time_column_id)),
            ))
        }
    }

    /// Estimate the memory consumption of this object and its contents
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self) + (std::mem::size_of::<ColumnId>() * self.0.capacity())
    }

    /// Build a [`SortKey`] from [`SortKeyIds`]; looking up column names in the provided
    /// [`ColumnsByName`] map by converting it to a `HashMap<ColumnId, &str>. If you already have
    /// an id-to-name column map, use [`SortKeyIds::to_sort_key_using_map`] instead.
    ///
    /// If you have a [`Partition`][super::Partition], it may be more convenient to call the
    /// [`Partition::sort_key`][super::Partition::sort_key] method instead!
    ///
    /// # Panics
    ///
    /// Will panic if an ID isn't found in the column map.
    pub fn to_sort_key(&self, columns: &ColumnsByName) -> SortKey {
        let column_id_map = columns.id_map();
        self.to_sort_key_using_map(&column_id_map)
    }

    /// Build a [`SortKey`] from [`SortKeyIds`]; looking up column names in the provided
    /// [`HashMap<ColumnId, &str>`] map.
    ///
    /// If you have a [`Partition`][super::Partition], it may be more convenient to call the
    /// [`Partition::sort_key`][super::Partition::sort_key] method instead!
    ///
    /// # Panics
    ///
    /// Will panic if an ID isn't found in the column map.
    pub fn to_sort_key_using_map(&self, column_id_map: &HashMap<ColumnId, Arc<str>>) -> SortKey {
        SortKey::from_columns(self.0.iter().map(|id| {
            Arc::clone(
                column_id_map.get(id).unwrap_or_else(|| {
                    panic!("cannot find column names for sort key id {}", id.get())
                }),
            )
        }))
    }

    /// Returns `true` if `other` is a monotonic update of `self`.
    ///
    /// # Panics
    ///
    /// Assumes "time" is the last column in both sets, and panics if the last
    /// columns are not identical.
    pub fn is_monotonic_update(&self, other: &Self) -> bool {
        // The SortKeyIds always reference the time column last (if set).
        if self.0.last().is_some() {
            assert_eq!(
                self.0.last(),
                other.last(),
                "last column in sort IDs must be time, and cannot change"
            );
        }

        // Ensure the values in other are a prefix match, with the exception of
        // the last "time" column.
        self.0.len() <= other.len()
            && self
                .0
                .iter()
                .take(self.0.len().saturating_sub(1))
                .zip(other.iter())
                .all(|(a, b)| a == b)
    }

    fn check_for_deplicates(columns: &[ColumnId]) {
        let mut column_ids: HashSet<i64> = HashSet::with_capacity(columns.len());
        for c in columns {
            match column_ids.get(&c.0) {
                Some(_) => {
                    panic!("set contains duplicates");
                }
                _ => {
                    column_ids.insert(c.0);
                }
            }
        }
    }
}

impl From<SortKeyIds> for Vec<ColumnId> {
    fn from(set: SortKeyIds) -> Self {
        set.0
    }
}

impl Deref for SortKeyIds {
    type Target = [ColumnId];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<I> From<I> for SortKeyIds
where
    I: IntoIterator<Item = i64>,
{
    fn from(ids: I) -> Self {
        Self::new(ids.into_iter().map(ColumnId::new).collect::<Vec<_>>())
    }
}

impl From<&SortKeyIds> for Vec<i64> {
    fn from(val: &SortKeyIds) -> Self {
        val.0.iter().map(|id| id.get()).collect()
    }
}

impl From<&SortKeyIds> for generated_types::influxdata::iox::catalog::v1::SortKeyIds {
    fn from(val: &SortKeyIds) -> Self {
        generated_types::influxdata::iox::catalog::v1::SortKeyIds {
            array_sort_key_ids: val.into(),
        }
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
    fn test_column_set_eq() {
        let set_1 = ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]);
        let set_2 = ColumnSet::new([ColumnId::new(2), ColumnId::new(1)]);
        assert_eq!(set_1, set_2);
    }

    #[test]
    fn test_column_set_union_intersect() {
        let a = ColumnSet::new([1, 2, 5, 7].into_iter().map(ColumnId::new));
        let b = ColumnSet::new([1, 5, 6, 7, 8].into_iter().map(ColumnId::new));

        let mut t = ColumnSet::empty();
        t.union(&a);
        assert_eq!(t, a);

        assert_eq!(
            t.intersect(&a).collect::<Vec<_>>(),
            vec![(0, a[0]), (1, a[1]), (2, a[2]), (3, a[3])]
        );

        t.union(&b);
        let expected = ColumnSet::new([1, 2, 5, 6, 7, 8].into_iter().map(ColumnId::new));
        assert_eq!(t, expected);

        assert_eq!(
            t.intersect(&a).collect::<Vec<_>>(),
            vec![(0, a[0]), (1, a[1]), (2, a[2]), (4, a[3])]
        );

        assert_eq!(
            t.intersect(&b).collect::<Vec<_>>(),
            vec![(0, b[0]), (2, b[1]), (3, b[2]), (4, b[3]), (5, b[4])]
        );
    }

    #[test]
    #[should_panic = "set contains duplicates"]
    fn test_sorted_column_set_duplicates() {
        SortKeyIds::new([
            ColumnId::new(2),
            ColumnId::new(1),
            ColumnId::new(3),
            ColumnId::new(1),
        ]);
    }

    #[test]
    fn test_sorted_column_set() {
        let set = SortKeyIds::new([ColumnId::new(2), ColumnId::new(1), ColumnId::new(3)]);
        // verify the order is preserved
        assert_eq!(set[0], ColumnId::new(2));
        assert_eq!(set[1], ColumnId::new(1));
        assert_eq!(set[2], ColumnId::new(3));
    }

    #[test]
    fn test_column_schema() {
        assert_eq!(
            ColumnType::try_from(proto::ColumnType::I64).unwrap(),
            ColumnType::I64,
        );
        assert_eq!(
            ColumnType::try_from(proto::ColumnType::U64).unwrap(),
            ColumnType::U64,
        );
        assert_eq!(
            ColumnType::try_from(proto::ColumnType::F64).unwrap(),
            ColumnType::F64,
        );
        assert_eq!(
            ColumnType::try_from(proto::ColumnType::Bool).unwrap(),
            ColumnType::Bool,
        );
        assert_eq!(
            ColumnType::try_from(proto::ColumnType::String).unwrap(),
            ColumnType::String,
        );
        assert_eq!(
            ColumnType::try_from(proto::ColumnType::Time).unwrap(),
            ColumnType::Time,
        );
        assert_eq!(
            ColumnType::try_from(proto::ColumnType::Tag).unwrap(),
            ColumnType::Tag,
        );

        assert!(ColumnType::try_from(proto::ColumnType::Unspecified).is_err());
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

    #[test]
    fn test_columns_by_names_exist() {
        let columns = build_columns_by_names();

        let ids = columns.ids_for_names(["foo", "bar"]);
        assert_eq!(ids, SortKeyIds::from([1, 2]));
    }

    #[test]
    fn test_columns_by_names_exist_different_order() {
        let columns = build_columns_by_names();

        let ids = columns.ids_for_names(["bar", "foo"]);
        assert_eq!(ids, SortKeyIds::from([2, 1]));
    }

    #[test]
    #[should_panic = "column name not found: baz"]
    fn test_columns_by_names_not_exist() {
        let columns = build_columns_by_names();
        columns.ids_for_names(["foo", "baz"]);
    }

    fn build_columns_by_names() -> ColumnsByName {
        let mut columns: BTreeMap<Arc<str>, ColumnSchema> = BTreeMap::new();
        columns.insert(
            "foo".into(),
            ColumnSchema {
                id: ColumnId::new(1),
                column_type: ColumnType::I64,
            },
        );
        columns.insert(
            "bar".into(),
            ColumnSchema {
                id: ColumnId::new(2),
                column_type: ColumnType::I64,
            },
        );
        columns.insert(
            "time".into(),
            ColumnSchema {
                id: ColumnId::new(3),
                column_type: ColumnType::Time,
            },
        );
        columns.insert(
            "tag1".into(),
            ColumnSchema {
                id: ColumnId::new(4),
                column_type: ColumnType::Tag,
            },
        );

        ColumnsByName(columns)
    }

    // panic if the sort_key_ids are not found in the columns
    #[test]
    #[should_panic(expected = "cannot find column names for sort key id 3")]
    fn test_panic_build_sort_key_from_ids_and_map() {
        // table columns
        let uno = ColumnSchema {
            id: ColumnId::new(1),
            column_type: ColumnType::Tag,
        };
        let dos = ColumnSchema {
            id: ColumnId::new(2),
            column_type: ColumnType::Tag,
        };
        let mut column_map = ColumnsByName::default();
        column_map.add_column("uno", uno);
        column_map.add_column("dos", dos);

        // sort_key_ids include some columns that are not in the columns
        let sort_key_ids = SortKeyIds::from([2, 3]);
        sort_key_ids.to_sort_key(&column_map);
    }

    #[test]
    fn test_build_sort_key_from_ids_and_map() {
        // table columns
        let uno = ColumnSchema {
            id: ColumnId::new(1),
            column_type: ColumnType::Tag,
        };
        let dos = ColumnSchema {
            id: ColumnId::new(2),
            column_type: ColumnType::Tag,
        };
        let tres = ColumnSchema {
            id: ColumnId::new(3),
            column_type: ColumnType::Tag,
        };
        let mut column_map = ColumnsByName::default();
        column_map.add_column("uno", uno);
        column_map.add_column("dos", dos);
        column_map.add_column("tres", tres);

        // sort_key_ids is empty
        let sort_key_ids = SortKeyIds::default();
        let sort_key = sort_key_ids.to_sort_key(&column_map);
        assert_eq!(sort_key, SortKey::empty());

        // sort_key_ids include all columns and in the same order
        let sort_key_ids = SortKeyIds::from([1, 2, 3]);
        let sort_key = sort_key_ids.to_sort_key(&column_map);
        assert_eq!(sort_key, SortKey::from_columns(vec!["uno", "dos", "tres"]));

        // sort_key_ids include all columns but in different order
        let sort_key_ids = SortKeyIds::from([2, 3, 1]);
        let sort_key = sort_key_ids.to_sort_key(&column_map);
        assert_eq!(sort_key, SortKey::from_columns(vec!["dos", "tres", "uno"]));

        // sort_key_ids include some columns
        let sort_key_ids = SortKeyIds::from([2, 3]);
        let sort_key = sort_key_ids.to_sort_key(&column_map);
        assert_eq!(sort_key, SortKey::from_columns(vec!["dos", "tres"]));

        // sort_key_ids include some columns in different order
        let sort_key_ids = SortKeyIds::from([3, 1]);
        let sort_key = sort_key_ids.to_sort_key(&column_map);
        assert_eq!(sort_key, SortKey::from_columns(vec!["tres", "uno"]));
    }

    #[test]
    fn test_sort_key_ids_round_trip_encoding() {
        let original = SortKeyIds::from([1, 2, 3]);

        let encoded: generated_types::influxdata::iox::catalog::v1::SortKeyIds = (&original).into();

        let decoded: SortKeyIds = encoded.array_sort_key_ids.into();
        assert_eq!(decoded, original);
    }

    macro_rules! test_is_monotonic_update {
        (
            $name:ident,
            a = $a:expr,
            b = $b:expr,
            want = $want:expr
        ) => {
            paste::paste! {
                #[test]
                fn [<test_is_monotonic_update_ $name>]() {
                    let a = SortKeyIds::new($a.into_iter().map(ColumnId::new));
                    let b = SortKeyIds::new($b.into_iter().map(ColumnId::new));
                    assert_eq!(a.is_monotonic_update(&b), $want)
                }
            }
        };
    }

    test_is_monotonic_update!(equal, a = [42, 24, 1], b = [42, 24, 1], want = true);

    test_is_monotonic_update!(empty, a = [], b = [42, 24, 1], want = true);

    test_is_monotonic_update!(
        longer_with_time,
        a = [42, 24, 1],
        b = [42, 24, 13, 1],
        want = true
    );

    test_is_monotonic_update!(shorter_with_time, a = [42, 24, 1], b = [1], want = false);

    test_is_monotonic_update!(
        mismatch_with_time,
        a = [42, 24, 1],
        b = [24, 42, 1],
        want = false
    );

    test_is_monotonic_update!(mismatch, a = [42, 24, 1], b = [24, 42, 1], want = false);
}
