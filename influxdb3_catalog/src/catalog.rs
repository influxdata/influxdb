//! Implementation of the Catalog that sits entirely in memory.

use influxdb3_wal::{CatalogBatch, CatalogOp, LastCacheDefinition};
use influxdb_line_protocol::FieldValue;
use observability_deps::tracing::info;
use parking_lot::RwLock;
use schema::{InfluxColumnType, InfluxFieldType, Schema, SchemaBuilder};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error, Clone, Copy)]
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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub const TIME_COLUMN_NAME: &str = "time";

/// The sequence number of a batch of WAL operations.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct SequenceNumber(u32);

impl SequenceNumber {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

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

    pub fn apply_catalog_batch(&self, catalog_batch: &CatalogBatch) -> Result<()> {
        self.inner.write().apply_catalog_batch(catalog_batch)
    }

    pub fn db_or_create(&self, db_name: &str) -> Result<Arc<DatabaseSchema>> {
        let db = self.inner.read().databases.get(db_name).cloned();

        let db = match db {
            Some(db) => db,
            None => {
                let mut inner = self.inner.write();

                if inner.databases.len() >= Self::NUM_DBS_LIMIT {
                    return Err(Error::TooManyDbs);
                }

                info!("return new db {}", db_name);
                let db = Arc::new(DatabaseSchema::new(db_name.into()));
                inner
                    .databases
                    .insert(Arc::clone(&db.name), Arc::clone(&db));
                inner.sequence = inner.sequence.next();
                inner.updated = true;
                db
            }
        };

        Ok(db)
    }

    pub fn db_schema(&self, name: &str) -> Option<Arc<DatabaseSchema>> {
        self.inner.read().databases.get(name).cloned()
    }

    pub fn sequence_number(&self) -> SequenceNumber {
        self.inner.read().sequence
    }

    pub fn clone_inner(&self) -> InnerCatalog {
        self.inner.read().clone()
    }

    pub fn list_databases(&self) -> Vec<String> {
        self.inner
            .read()
            .databases
            .keys()
            .map(|db| db.to_string())
            .collect()
    }

    pub fn add_last_cache(&self, db_name: &str, table_name: &str, last_cache: LastCacheDefinition) {
        let mut inner = self.inner.write();
        let mut db = inner
            .databases
            .get(db_name)
            .expect("db should exist")
            .as_ref()
            .clone();
        let table = db.tables.get_mut(table_name).expect("table should exist");
        table.add_last_cache(last_cache);
        inner.databases.insert(Arc::clone(&db.name), Arc::new(db));
        inner.sequence = inner.sequence.next();
        inner.updated = true;
    }

    pub fn delete_last_cache(&self, db_name: &str, table_name: &str, name: &str) {
        let mut inner = self.inner.write();
        let mut db = inner
            .databases
            .get(db_name)
            .expect("db should exist")
            .as_ref()
            .clone();
        let table = db.tables.get_mut(table_name).expect("table should exist");
        table.remove_last_cache(name);
        inner.databases.insert(Arc::clone(&db.name), Arc::new(db));
        inner.sequence = inner.sequence.next();
        inner.updated = true;
    }

    #[cfg(test)]
    pub fn db_exists(&self, db_name: &str) -> bool {
        self.inner.read().db_exists(db_name)
    }

    pub fn insert_database(&mut self, db: DatabaseSchema) {
        let mut inner = self.inner.write();
        inner.databases.insert(Arc::clone(&db.name), Arc::new(db));
        inner.sequence = inner.sequence.next();
        inner.updated = true;
    }
}

#[serde_with::serde_as]
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
pub struct InnerCatalog {
    /// The catalog is a map of databases with their table schemas
    #[serde_as(as = "serde_with::MapPreventDuplicates<_, _>")]
    databases: HashMap<Arc<str>, Arc<DatabaseSchema>>,
    sequence: SequenceNumber,
    /// If true, the catalog has been updated since the last time it was serialized
    #[serde(skip)]
    updated: bool,
}

impl InnerCatalog {
    pub(crate) fn new() -> Self {
        Self {
            databases: HashMap::new(),
            sequence: SequenceNumber::new(0),
            updated: false,
        }
    }

    pub fn sequence_number(&self) -> SequenceNumber {
        self.sequence
    }

    pub fn table_count(&self) -> usize {
        self.databases.values().map(|db| db.tables.len()).sum()
    }

    pub fn apply_catalog_batch(&mut self, catalog_batch: &CatalogBatch) -> Result<()> {
        let table_count = self.table_count();

        let db = self.databases.get(catalog_batch.database_name.as_ref());

        let db = match db {
            Some(db) => {
                let previous_table_count = db.tables.len();
                let db = DatabaseSchema::new_from_existing_and_batch(db, catalog_batch)?;

                let new_table_count = db.tables.len() - previous_table_count;
                if table_count + new_table_count > Catalog::NUM_TABLES_LIMIT {
                    return Err(Error::TooManyTables);
                }

                db
            }
            None => {
                if self.databases.len() >= Catalog::NUM_DBS_LIMIT {
                    return Err(Error::TooManyDbs);
                }
                let db = DatabaseSchema::new_from_batch(catalog_batch)?;
                if table_count + db.tables.len() > Catalog::NUM_TABLES_LIMIT {
                    return Err(Error::TooManyTables);
                }

                db
            }
        };

        self.databases
            .insert(Arc::clone(&catalog_batch.database_name), Arc::new(db));
        self.sequence = self.sequence.next();
        self.updated = true;

        Ok(())
    }

    pub fn databases(&self) -> impl Iterator<Item = &Arc<DatabaseSchema>> {
        self.databases.values()
    }

    pub fn db_exists(&self, db_name: &str) -> bool {
        self.databases.contains_key(db_name)
    }
}

#[serde_with::serde_as]
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct DatabaseSchema {
    pub name: Arc<str>,
    /// The database is a map of tables
    #[serde_as(as = "serde_with::MapPreventDuplicates<_, _>")]
    pub tables: BTreeMap<Arc<str>, TableDefinition>,
}

impl DatabaseSchema {
    pub fn new(name: Arc<str>) -> Self {
        Self {
            name,
            tables: BTreeMap::new(),
        }
    }

    pub fn new_from_existing_and_batch(
        db: &Arc<DatabaseSchema>,
        catalog_batch: &CatalogBatch,
    ) -> Result<Self> {
        let tables = db.tables.clone();
        Self::new_from_tables_and_batch(tables, catalog_batch)
    }

    pub fn new_from_batch(catalog_batch: &CatalogBatch) -> Result<Self> {
        let tables = BTreeMap::new();
        Self::new_from_tables_and_batch(tables, catalog_batch)
    }

    pub fn new_from_tables_and_batch(
        mut tables: BTreeMap<Arc<str>, TableDefinition>,
        catalog_batch: &CatalogBatch,
    ) -> Result<Self> {
        for catalog_op in &catalog_batch.ops {
            match catalog_op {
                CatalogOp::CreateTable(table_def) => {
                    let table = TableDefinition::new_from_op(table_def);
                    tables.insert(Arc::clone(&table.name), table);
                }
                CatalogOp::AddFields(field_additions) => {
                    let columns: Vec<(String, InfluxColumnType)> = field_additions
                        .field_definitions
                        .iter()
                        .map(|field_def| (field_def.name.to_string(), field_def.data_type.into()))
                        .collect();
                    let table = tables.get_mut(field_additions.table_name.as_ref());
                    match table {
                        Some(table) => {
                            table.add_columns(columns)?;
                        }
                        None => {
                            let table = TableDefinition::new(
                                Arc::clone(&field_additions.table_name),
                                columns,
                                <Option<Vec<String>>>::None,
                            )?;
                            tables.insert(Arc::clone(&field_additions.table_name), table);
                        }
                    }
                }
                CatalogOp::CreateDatabase(_) => {
                    // Do nothing
                }
                CatalogOp::CreateLastCache(definition) => {
                    let table_name: Arc<str> = definition.table.as_str().into();
                    let table = tables.get_mut(table_name.as_ref());
                    match table {
                        Some(table) => {
                            table
                                .last_caches
                                .insert(definition.name.clone(), definition.clone());
                        }
                        None => panic!("table must exist before last cache creation"),
                    }
                }
                CatalogOp::DeleteLastCache(definition) => {
                    let table_name: Arc<str> = definition.table.as_str().into();
                    let table = tables.get_mut(table_name.as_ref());
                    if let Some(table) = table {
                        table.last_caches.remove(&definition.name);
                    }
                }
            }
        }

        Ok(Self {
            name: Arc::clone(&catalog_batch.database_name),
            tables,
        })
    }

    pub fn get_table_schema(&self, table_name: &str) -> Option<&Schema> {
        self.tables.get(table_name).map(|table| &table.schema)
    }

    pub fn get_table(&self, table_name: &str) -> Option<&TableDefinition> {
        self.tables.get(table_name)
    }

    pub fn table_names(&self) -> Vec<Arc<str>> {
        self.tables.keys().cloned().collect()
    }

    pub fn table_exists(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableDefinition> {
        self.tables.values()
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TableDefinition {
    pub name: Arc<str>,
    pub schema: Schema,
    pub last_caches: BTreeMap<String, LastCacheDefinition>,
}

impl TableDefinition {
    /// Create a new [`TableDefinition`]
    ///
    /// Ensures the provided columns will be ordered before constructing the schema.
    pub fn new<CN: AsRef<str>>(
        name: Arc<str>,
        columns: impl AsRef<[(CN, InfluxColumnType)]>,
        series_key: Option<impl IntoIterator<Item: AsRef<str>>>,
    ) -> Result<Self> {
        // ensure we're under the column limit
        if columns.as_ref().len() > Catalog::NUM_COLUMNS_PER_TABLE_LIMIT {
            return Err(Error::TooManyColumns);
        }

        // Use a BTree to ensure that the columns are ordered:
        let mut ordered_columns = BTreeMap::new();
        for (name, column_type) in columns.as_ref() {
            ordered_columns.insert(name.as_ref(), column_type);
        }
        let mut schema_builder = SchemaBuilder::with_capacity(columns.as_ref().len());
        schema_builder.measurement(name.as_ref());
        if let Some(sk) = series_key {
            schema_builder.with_series_key(sk);
        }
        for (name, column_type) in ordered_columns {
            schema_builder.influx_column(name, *column_type);
        }
        let schema = schema_builder.build().unwrap();

        Ok(Self {
            name,
            schema,
            last_caches: BTreeMap::new(),
        })
    }

    /// Create a new table definition from a catalog op
    pub fn new_from_op(table_definition: &influxdb3_wal::TableDefinition) -> Self {
        let mut columns = Vec::new();
        for field_def in &table_definition.field_definitions {
            columns.push((field_def.name.as_ref(), field_def.data_type.into()));
        }
        Self::new(
            Arc::clone(&table_definition.table_name),
            columns,
            table_definition.key.clone(),
        )
        .expect("tables defined from ops should not exceed column limits")
    }

    /// Check if the column exists in the [`TableDefinition`]s schema
    pub fn column_exists(&self, column: &str) -> bool {
        self.schema.find_index_of(column).is_some()
    }

    /// Add the columns to this [`TableDefinition`]
    ///
    /// This ensures that the resulting schema has its columns ordered
    pub fn add_columns(&mut self, columns: Vec<(String, InfluxColumnType)>) -> Result<()> {
        // Use BTree to insert existing and new columns, and use that to generate the
        // resulting schema, to ensure column order is consistent:
        let mut cols = BTreeMap::new();
        for (col_type, field) in self.schema.iter() {
            cols.insert(field.name(), col_type);
        }
        for (name, column_type) in columns.iter() {
            cols.insert(name, *column_type);
        }

        // ensure we don't go over the column limit
        if cols.len() > Catalog::NUM_COLUMNS_PER_TABLE_LIMIT {
            return Err(Error::TooManyColumns);
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

        Ok(())
    }

    pub fn index_columns(&self) -> Vec<&str> {
        self.schema
            .iter()
            .filter_map(|(col_type, field)| match col_type {
                InfluxColumnType::Tag => Some(field.name().as_str()),
                InfluxColumnType::Field(_) | InfluxColumnType::Timestamp => None,
            })
            .collect()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn num_columns(&self) -> usize {
        self.schema.len()
    }

    pub fn field_type_by_name(&self, name: &str) -> Option<InfluxColumnType> {
        self.schema.field_type_by_name(name)
    }

    pub fn is_v3(&self) -> bool {
        self.schema.series_key().is_some()
    }

    /// Add a new last cache to this table definition
    pub fn add_last_cache(&mut self, last_cache: LastCacheDefinition) {
        self.last_caches
            .insert(last_cache.name.to_string(), last_cache);
    }

    /// Remove a last cache from the table definition
    pub fn remove_last_cache(&mut self, name: &str) {
        self.last_caches.remove(name);
    }

    pub fn last_caches(&self) -> impl Iterator<Item = (&String, &LastCacheDefinition)> {
        self.last_caches.iter()
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
            name: "test_db".into(),
            tables: BTreeMap::new(),
        };
        use InfluxColumnType::*;
        use InfluxFieldType::*;
        database.tables.insert(
            "test_table_1".into(),
            TableDefinition::new(
                "test_table_1".into(),
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
            )
            .unwrap(),
        );
        database.tables.insert(
            "test_table_2".into(),
            TableDefinition::new(
                "test_table_2".into(),
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
            )
            .unwrap(),
        );
        catalog
            .inner
            .write()
            .databases
            .insert(Arc::clone(&database.name), Arc::new(database));

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
            name: "test".into(),
            tables: BTreeMap::new(),
        };
        database.tables.insert(
            "test".into(),
            TableDefinition::new(
                "test".into(),
                [(
                    "test".to_string(),
                    InfluxColumnType::Field(InfluxFieldType::String),
                )],
                SeriesKey::None,
            )
            .unwrap(),
        );

        let table = database.tables.get_mut("test").unwrap();
        table
            .add_columns(vec![("test2".to_string(), InfluxColumnType::Tag)])
            .unwrap();
        let schema = table.schema();
        assert_eq!(
            schema.field(0).0,
            InfluxColumnType::Field(InfluxFieldType::String)
        );
        assert_eq!(schema.field(1).0, InfluxColumnType::Tag);
    }

    #[test]
    fn serialize_series_keys() {
        let catalog = Catalog::new();
        let mut database = DatabaseSchema {
            name: "test_db".into(),
            tables: BTreeMap::new(),
        };
        use InfluxColumnType::*;
        use InfluxFieldType::*;
        database.tables.insert(
            "test_table_1".into(),
            TableDefinition::new(
                "test_table_1".into(),
                [
                    ("tag_1", Tag),
                    ("tag_2", Tag),
                    ("tag_3", Tag),
                    ("time", Timestamp),
                    ("field", Field(String)),
                ],
                SeriesKey::Some(vec![
                    "tag_1".to_string(),
                    "tag_2".to_string(),
                    "tag_3".to_string(),
                ]),
            )
            .unwrap(),
        );
        catalog
            .inner
            .write()
            .databases
            .insert(Arc::clone(&database.name), Arc::new(database));

        assert_json_snapshot!(catalog);

        let serialized = serde_json::to_string(&catalog).unwrap();
        let deserialized_inner: InnerCatalog = serde_json::from_str(&serialized).unwrap();
        let deserialized = Catalog::from_inner(deserialized_inner);
        assert_eq!(catalog, deserialized);
    }

    #[test]
    fn serialize_last_cache() {
        let catalog = Catalog::new();
        let mut database = DatabaseSchema {
            name: "test_db".into(),
            tables: BTreeMap::new(),
        };
        use InfluxColumnType::*;
        use InfluxFieldType::*;
        let mut table_def = TableDefinition::new(
            "test".into(),
            [
                ("tag_1", Tag),
                ("tag_2", Tag),
                ("tag_3", Tag),
                ("time", Timestamp),
                ("field", Field(String)),
            ],
            SeriesKey::None,
        )
        .unwrap();
        table_def.add_last_cache(
            LastCacheDefinition::new_with_explicit_value_columns(
                "test",
                "test_table_last_cache",
                ["tag_2", "tag_3"],
                ["field"],
                1,
                600,
            )
            .unwrap(),
        );
        database.tables.insert("test_table_1".into(), table_def);
        catalog
            .inner
            .write()
            .databases
            .insert(Arc::clone(&database.name), Arc::new(database));

        assert_json_snapshot!(catalog);

        let serialized = serde_json::to_string(&catalog).unwrap();
        let deserialized_inner: InnerCatalog = serde_json::from_str(&serialized).unwrap();
        let deserialized = Catalog::from_inner(deserialized_inner);
        assert_eq!(catalog, deserialized);
    }
}
