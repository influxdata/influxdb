//! Implementation of the Catalog that sits entirely in memory.

use crate::SequenceNumber;
use data_types::ColumnType;
use observability_deps::tracing::info;
use parking_lot::RwLock;
use schema::{InfluxColumnType, InfluxFieldType, Schema, SchemaBuilder};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("catalog updated elsewhere")]
    CatalogUpdatedElsewhere,
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

impl Catalog {
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

        info!("inserted/updated database in catalog: {}", db.name);

        inner.sequence = inner.sequence.next();
        inner.databases.insert(db.name.clone(), db);

        Ok(())
    }

    pub(crate) fn db_or_create(&self, db_name: &str) -> (SequenceNumber, Arc<DatabaseSchema>) {
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
                info!("return new db {}", db_name);
                let mut inner = self.inner.write();
                let db = Arc::new(DatabaseSchema::new(db_name));
                inner.databases.insert(db.name.clone(), Arc::clone(&db));
                db
            }
        };

        (sequence, db)
    }

    pub fn db_schema(&self, name: &str) -> Option<Arc<DatabaseSchema>> {
        info!("db_schema {}", name);
        self.inner.read().databases.get(name).cloned()
    }

    pub fn into_inner(self) -> InnerCatalog {
        self.inner.into_inner()
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
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
pub struct InnerCatalog {
    /// The catalog is a map of databases with their table schemas
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
        self.databases.get(db_name).is_some()
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct DatabaseSchema {
    pub name: String,
    /// The database is a map of tables
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

#[derive(Debug, Serialize, Eq, PartialEq, Clone)]
pub struct TableDefinition {
    pub name: String,
    #[serde(skip_serializing, skip_deserializing)]
    pub schema: Schema,
    columns: BTreeMap<String, i16>,
}

struct TableDefinitionVisitor;

impl<'de> Visitor<'de> for TableDefinitionVisitor {
    type Value = TableDefinition;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("struct TableDefinition")
    }

    fn visit_map<V>(self, mut map: V) -> Result<TableDefinition, V::Error>
    where
        V: serde::de::MapAccess<'de>,
    {
        let mut name = None;
        let mut columns = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "name" => {
                    if name.is_some() {
                        return Err(serde::de::Error::duplicate_field("name"));
                    }
                    name = Some(map.next_value::<String>()?);
                }
                "columns" => {
                    if columns.is_some() {
                        return Err(serde::de::Error::duplicate_field("columns"));
                    }
                    columns = Some(map.next_value::<BTreeMap<String, i16>>()?);
                }
                _ => {
                    let _ = map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }
        let name = name.ok_or_else(|| serde::de::Error::missing_field("name"))?;
        let columns = columns.ok_or_else(|| serde::de::Error::missing_field("columns"))?;

        Ok(TableDefinition::new(name, columns))
    }
}

impl<'de> Deserialize<'de> for TableDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(TableDefinitionVisitor)
    }
}

impl TableDefinition {
    pub(crate) fn new(name: impl Into<String>, columns: BTreeMap<String, i16>) -> Self {
        let mut schema_builder = SchemaBuilder::with_capacity(columns.len());
        for (name, column_type) in &columns {
            schema_builder.influx_column(
                name,
                column_type_to_influx_column_type(&ColumnType::try_from(*column_type).unwrap()),
            );
        }
        let schema = schema_builder.build().unwrap();

        Self {
            name: name.into(),
            schema,
            columns,
        }
    }

    pub(crate) fn column_exists(&self, column: &str) -> bool {
        self.columns.contains_key(column)
    }

    pub(crate) fn add_columns(&mut self, mut columns: Vec<(String, i16)>) {
        let mut schema_builder = SchemaBuilder::with_capacity(columns.len());
        columns.sort_by(|(a, _), (b, _)| a.cmp(b));
        for (name, column_type) in &columns {
            schema_builder.influx_column(
                name,
                column_type_to_influx_column_type(&ColumnType::try_from(*column_type).unwrap()),
            );
        }
        let schema = schema_builder.build().unwrap();

        for (name, column_type) in columns.into_iter() {
            self.columns.insert(name, column_type);
        }
        self.schema = schema;
    }

    #[allow(dead_code)]
    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(crate) fn columns(&self) -> &BTreeMap<String, i16> {
        &self.columns
    }
}

fn column_type_to_influx_column_type(column_type: &ColumnType) -> InfluxColumnType {
    match column_type {
        ColumnType::I64 => InfluxColumnType::Field(InfluxFieldType::Integer),
        ColumnType::U64 => InfluxColumnType::Field(InfluxFieldType::UInteger),
        ColumnType::F64 => InfluxColumnType::Field(InfluxFieldType::Float),
        ColumnType::Bool => InfluxColumnType::Field(InfluxFieldType::Boolean),
        ColumnType::String => InfluxColumnType::Field(InfluxFieldType::String),
        ColumnType::Time => InfluxColumnType::Timestamp,
        ColumnType::Tag => InfluxColumnType::Tag,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_serialization() {
        let catalog = Catalog::new();
        let mut database = DatabaseSchema {
            name: "test".to_string(),
            tables: BTreeMap::new(),
        };
        database.tables.insert(
            "test".into(),
            TableDefinition::new(
                "test",
                BTreeMap::from([("test".to_string(), ColumnType::String as i16)]),
            ),
        );
        let database = Arc::new(database);
        catalog
            .replace_database(SequenceNumber::new(0), database)
            .unwrap();
        let inner = catalog.inner.read();

        let serialized = serde_json::to_string(&*inner).unwrap();
        let deserialized: InnerCatalog = serde_json::from_str(&serialized).unwrap();

        assert_eq!(*inner, deserialized);
    }
}
