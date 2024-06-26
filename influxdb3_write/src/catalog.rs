//! Implementation of the Catalog that sits entirely in memory.

use crate::SequenceNumber;
use influxdb_line_protocol::FieldValue;
use observability_deps::tracing::info;
use parking_lot::RwLock;
use schema::{InfluxColumnType, InfluxFieldType, Schema, SchemaBuilder};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use thiserror::Error;

mod serialize;

#[derive(Debug, Error)]
pub enum Error {
    #[error("catalog updated elsewhere")]
    CatalogUpdatedElsewhere,

    #[error(
        "Update to schema would exceed number of columns per table limit of {} columns",
        Catalog::NUM_COLUMNS_PER_TABLE_LIMIT - 1
    )]
    TooManyColumns,

    #[error(
        "Update to schema would exceed number of tables limit of {} tables",
        Catalog::NUM_TABLES_LIMIT
    )]
    TooManyTables,

    #[error(
        "Adding a new database would exceed limit of {} databases",
        Catalog::NUM_DBS_LIMIT
    )]
    TooManyDbs,

    #[error("last cache size must be from 1 to 10")]
    InvalidLastCacheSize,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub const TIME_COLUMN_NAME: &str = "time";

#[derive(Debug)]
pub struct Catalog {
    inner: RwLock<InnerCatalog>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for Catalog {
    fn eq(&self, other: &Self) -> bool {
        self.inner.read().eq(&other.inner.read())
    }
}

impl Serialize for Catalog {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.inner.read().serialize(serializer)
    }
}

impl Catalog {
    /// Limit for the number of Databases that InfluxDB Edge can have
    pub(crate) const NUM_DBS_LIMIT: usize = 5;
    /// Limit for the number of columns per table that InfluxDB Edge can have
    pub(crate) const NUM_COLUMNS_PER_TABLE_LIMIT: usize = 500;
    /// Limit for the number of tables across all DBs that InfluxDB Edge can have
    pub(crate) const NUM_TABLES_LIMIT: usize = 2000;

    pub fn new() -> Self {
        Self {
            inner: RwLock::new(InnerCatalog::new()),
        }
    }

    pub fn from_inner(inner: InnerCatalog) -> Self {
        Self {
            inner: RwLock::new(inner),
        }
    }

    pub(crate) fn replace_database(
        &self,
        sequence: SequenceNumber,
        db: Arc<DatabaseSchema>,
    ) -> Result<()> {
        let mut inner = self.inner.write();
        if inner.sequence != sequence {
            info!("catalog updated elsewhere");
            return Err(Error::CatalogUpdatedElsewhere);
        }

        // Check we have not gone over the table limit with this updated DB
        let mut num_tables = inner
            .databases
            .iter()
            .filter(|(k, _)| *k != &db.name)
            .map(|(_, v)| v)
            .fold(0, |acc, db| acc + db.tables.len());

        num_tables += db.tables.len();

        if num_tables > Self::NUM_TABLES_LIMIT {
            return Err(Error::TooManyTables);
        }

        for table in db.tables.values() {
            if table.num_columns() > Self::NUM_COLUMNS_PER_TABLE_LIMIT {
                return Err(Error::TooManyColumns);
            }
        }

        info!("inserted/updated database in catalog: {}", db.name);
        inner.sequence = inner.sequence.next();
        inner.databases.insert(db.name.clone(), db);
        Ok(())
    }

    pub(crate) fn db_or_create(
        &self,
        db_name: &str,
    ) -> Result<(SequenceNumber, Arc<DatabaseSchema>)> {
        let (sequence, db) = {
            let inner = self.inner.read();
            (inner.sequence, inner.databases.get(db_name).cloned())
        };

        let db = match db {
            Some(db) => {
                info!("return existing db {}", db_name);
                db
            }
            None => {
                let mut inner = self.inner.write();

                if inner.databases.len() >= Self::NUM_DBS_LIMIT {
                    return Err(Error::TooManyDbs);
                }

                info!("return new db {}", db_name);
                let db = Arc::new(DatabaseSchema::new(db_name));
                inner.databases.insert(db.name.clone(), Arc::clone(&db));
                db
            }
        };

        Ok((sequence, db))
    }

    pub fn db_schema(&self, name: &str) -> Option<Arc<DatabaseSchema>> {
        info!("db_schema {}", name);
        self.inner.read().databases.get(name).cloned()
    }

    pub fn sequence_number(&self) -> SequenceNumber {
        self.inner.read().sequence
    }

    pub fn clone_inner(&self) -> InnerCatalog {
        self.inner.read().clone()
    }

    pub fn list_databases(&self) -> Vec<String> {
        self.inner.read().databases.keys().cloned().collect()
    }

    #[cfg(test)]
    pub fn db_exists(&self, db_name: &str) -> bool {
        self.inner.read().db_exists(db_name)
    }
}

#[serde_with::serde_as]
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
pub struct InnerCatalog {
    /// The catalog is a map of databases with their table schemas
    #[serde_as(as = "serde_with::MapPreventDuplicates<_, _>")]
    databases: HashMap<String, Arc<DatabaseSchema>>,
    sequence: SequenceNumber,
}

impl InnerCatalog {
    pub(crate) fn new() -> Self {
        Self {
            databases: HashMap::new(),
            sequence: SequenceNumber::new(0),
        }
    }

    pub fn sequence_number(&self) -> SequenceNumber {
        self.sequence
    }

    #[cfg(test)]
    pub fn db_exists(&self, db_name: &str) -> bool {
        self.databases.contains_key(db_name)
    }
}

#[serde_with::serde_as]
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct DatabaseSchema {
    pub name: String,
    /// The database is a map of tables
    #[serde_as(as = "serde_with::MapPreventDuplicates<_, _>")]
    pub(crate) tables: BTreeMap<String, TableDefinition>,
}

impl DatabaseSchema {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tables: BTreeMap::new(),
        }
    }

    pub fn get_table_schema(&self, table_name: &str) -> Option<&Schema> {
        self.tables.get(table_name).map(|table| &table.schema)
    }

    pub fn get_table(&self, table_name: &str) -> Option<&TableDefinition> {
        self.tables.get(table_name)
    }

    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    pub fn table_exists(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TableDefinition {
    pub name: String,
    pub schema: Schema,
    pub last_caches: Vec<LastCacheDefinition>,
}

impl TableDefinition {
    /// Create a new [`TableDefinition`]
    ///
    /// Ensures the the provided columns will be ordered before constructing the schema.
    pub(crate) fn new<N: Into<String>, CN: AsRef<str>>(
        name: N,
        columns: impl AsRef<[(CN, InfluxColumnType)]>,
        series_key: Option<impl IntoIterator<Item: AsRef<str>>>,
    ) -> Self {
        // Use a BTree to ensure that the columns are ordered:
        let mut ordered_columns = BTreeMap::new();
        for (name, column_type) in columns.as_ref() {
            ordered_columns.insert(name.as_ref(), column_type);
        }
        let mut schema_builder = SchemaBuilder::with_capacity(columns.as_ref().len());
        let name = name.into();
        schema_builder.measurement(&name);
        if let Some(sk) = series_key {
            schema_builder.with_series_key(sk);
        }
        for (name, column_type) in ordered_columns {
            schema_builder.influx_column(name, *column_type);
        }
        let schema = schema_builder.build().unwrap();

        Self {
            name,
            schema,
            last_caches: vec![],
        }
    }

    /// Check if the column exists in the [`TableDefinition`]s schema
    pub(crate) fn column_exists(&self, column: &str) -> bool {
        self.schema.find_index_of(column).is_some()
    }

    /// Add the columns to this [`TableDefinition`]
    ///
    /// This ensures that the resulting schema has its columns ordered
    pub(crate) fn add_columns(&mut self, columns: Vec<(String, InfluxColumnType)>) {
        // Use BTree to insert existing and new columns, and use that to generate the
        // resulting schema, to ensure column order is consistent:
        let mut cols = BTreeMap::new();
        for (col_type, field) in self.schema.iter() {
            cols.insert(field.name(), col_type);
        }
        for (name, column_type) in columns.iter() {
            cols.insert(name, *column_type);
        }
        let mut schema_builder = SchemaBuilder::with_capacity(columns.len());
        // TODO: may need to capture some schema-level metadata, currently, this causes trouble in
        // tests, so I am omitting this for now:
        // schema_builder.measurement(&self.name);
        for (name, col_type) in cols {
            schema_builder.influx_column(name, col_type);
        }
        let schema = schema_builder.build().unwrap();

        self.schema = schema;
    }

    pub(crate) fn index_columns(&self) -> Vec<&str> {
        self.schema
            .iter()
            .filter_map(|(col_type, field)| match col_type {
                InfluxColumnType::Tag => Some(field.name().as_str()),
                InfluxColumnType::Field(_) | InfluxColumnType::Timestamp => None,
            })
            .collect()
    }

    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(crate) fn num_columns(&self) -> usize {
        self.schema.len()
    }

    pub(crate) fn field_type_by_name(&self, name: &str) -> Option<InfluxColumnType> {
        self.schema.field_type_by_name(name)
    }

    pub(crate) fn is_v3(&self) -> bool {
        self.schema.series_key().is_some()
    }
}

/// Defines a last cache in a given table and database
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct LastCacheDefinition {
    /// Given name of the cache
    pub name: String,
    /// Columns intended to be used as predicates in the cache
    pub key_columns: Vec<String>,
    /// Columns that store values in the cache
    pub value_columns: Vec<String>,
    /// The number of last values to hold in the cache
    count: LastCacheSize,
}

/// The maximum allowed size for a last cache
const LAST_CACHE_MAX_SIZE: usize = 10;

/// The size of the last cache
///
/// Must be between 1 and [`LAST_CACHE_MAX_SIZE`]
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy)]
pub struct LastCacheSize(usize);

impl LastCacheSize {
    pub fn new(size: usize) -> Result<Self, Error> {
        if size == 0 || size > LAST_CACHE_MAX_SIZE {
            Err(Error::InvalidLastCacheSize)
        } else {
            Ok(Self(size))
        }
    }
}

impl TryFrom<usize> for LastCacheSize {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<LastCacheSize> for usize {
    fn from(value: LastCacheSize) -> Self {
        value.0
    }
}

pub fn influx_column_type_from_field_value(fv: &FieldValue<'_>) -> InfluxColumnType {
    match fv {
        FieldValue::I64(_) => InfluxColumnType::Field(InfluxFieldType::Integer),
        FieldValue::U64(_) => InfluxColumnType::Field(InfluxFieldType::UInteger),
        FieldValue::F64(_) => InfluxColumnType::Field(InfluxFieldType::Float),
        FieldValue::String(_) => InfluxColumnType::Field(InfluxFieldType::String),
        FieldValue::Boolean(_) => InfluxColumnType::Field(InfluxFieldType::Boolean),
    }
}

#[cfg(test)]
mod tests {
    use insta::assert_json_snapshot;
    use pretty_assertions::assert_eq;
    use test_helpers::assert_contains;

    use super::*;

    type SeriesKey = Option<Vec<String>>;

    #[test]
    fn catalog_serialization() {
        let catalog = Catalog::new();
        let mut database = DatabaseSchema {
            name: "test_db".to_string(),
            tables: BTreeMap::new(),
        };
        use InfluxColumnType::*;
        use InfluxFieldType::*;
        database.tables.insert(
            "test_table_1".into(),
            TableDefinition::new(
                "test_table_1",
                [
                    ("tag_1", Tag),
                    ("tag_2", Tag),
                    ("tag_3", Tag),
                    ("time", Timestamp),
                    ("string_field", Field(String)),
                    ("bool_field", Field(Boolean)),
                    ("i64_field", Field(Integer)),
                    ("u64_field", Field(UInteger)),
                    ("f64_field", Field(Float)),
                ],
                SeriesKey::None,
            ),
        );
        database.tables.insert(
            "test_table_2".into(),
            TableDefinition::new(
                "test_table_2",
                [
                    ("tag_1", Tag),
                    ("tag_2", Tag),
                    ("tag_3", Tag),
                    ("time", Timestamp),
                    ("string_field", Field(String)),
                    ("bool_field", Field(Boolean)),
                    ("i64_field", Field(Integer)),
                    ("u64_field", Field(UInteger)),
                    ("f64_field", Field(Float)),
                ],
                SeriesKey::None,
            ),
        );
        let database = Arc::new(database);
        catalog
            .replace_database(SequenceNumber::new(0), database)
            .unwrap();

        // Perform a snapshot test to check that the JSON serialized catalog does not change in an
        // undesired way when introducing features etc.
        assert_json_snapshot!(catalog);

        // Serialize/deserialize to ensure roundtrip to/from JSON
        let serialized = serde_json::to_string(&catalog).unwrap();
        let deserialized_inner: InnerCatalog = serde_json::from_str(&serialized).unwrap();
        let deserialized = Catalog::from_inner(deserialized_inner);
        assert_eq!(catalog, deserialized);
    }

    #[test]
    fn invalid_catalog_deserialization() {
        // Duplicate databases
        {
            let json = r#"{
                "databases": {
                    "db1": {
                        "name": "db1",
                        "tables": {}
                    },
                    "db1": {
                        "name": "db1",
                        "tables": {}
                    }
                }
            }"#;
            let err = serde_json::from_str::<InnerCatalog>(json).unwrap_err();
            assert_contains!(err.to_string(), "found duplicate key");
        }
        // Duplicate tables
        {
            let json = r#"{
                "databases": {
                    "db1": {
                        "name": "db1",
                        "tables": {
                            "tbl1": {
                                "name": "tbl1",
                                "cols": {}
                            },
                            "tbl1": {
                                "name": "tbl1",
                                "cols": {}
                            }
                        }
                    }
                }
            }"#;
            let err = serde_json::from_str::<InnerCatalog>(json).unwrap_err();
            assert_contains!(err.to_string(), "found duplicate key");
        }
        // Duplicate columns
        {
            let json = r#"{
                "databases": {
                    "db1": {
                        "name": "db1",
                        "tables": {
                            "tbl1": {
                                "name": "tbl1",
                                "cols": {
                                    "col1": {
                                        "type": "i64",
                                        "influx_type": "field",
                                        "nullable": true
                                    },
                                    "col1": {
                                        "type": "u64",
                                        "influx_type": "field",
                                        "nullable": true
                                    }
                                }
                            }
                        }
                    }
                }
            }"#;
            let err = serde_json::from_str::<InnerCatalog>(json).unwrap_err();
            assert_contains!(err.to_string(), "found duplicate key");
        }
    }

    #[test]
    fn add_columns_updates_schema() {
        let mut database = DatabaseSchema {
            name: "test".to_string(),
            tables: BTreeMap::new(),
        };
        database.tables.insert(
            "test".into(),
            TableDefinition::new(
                "test",
                [(
                    "test".to_string(),
                    InfluxColumnType::Field(InfluxFieldType::String),
                )],
                SeriesKey::None,
            ),
        );

        let table = database.tables.get_mut("test").unwrap();
        table.add_columns(vec![("test2".to_string(), InfluxColumnType::Tag)]);
        let schema = table.schema();
        assert_eq!(
            schema.field(0).0,
            InfluxColumnType::Field(InfluxFieldType::String)
        );
        assert_eq!(schema.field(1).0, InfluxColumnType::Tag);
    }
}
