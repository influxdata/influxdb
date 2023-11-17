//! Implementation of the Catalog that sits entirely in memory.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use datafusion::catalog::CatalogProvider;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::{TableProvider};
use datafusion::physical_plan::ExecutionPlan;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use data_types::ColumnType;
use iox_query::{QueryChunk, QueryNamespace};
use observability_deps::tracing::info;
use schema::{InfluxColumnType, InfluxFieldType, Schema, SchemaBuilder};
use crate::WriteBuffer;

#[derive(Debug, Error)]
pub enum Error {
    #[error("catalog updated elsewhere")]
    CatalogUpdatedElsewhere,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Catalog {
    inner: RwLock<InnerCatalog>,
}

impl Catalog {
    pub fn new() -> Self {
        Self{
            inner: RwLock::new(InnerCatalog::new()),
        }
    }

    pub(crate) fn replace_database(&self, sequence: u64, db: Arc<DatabaseSchema>) -> Result<()>{
        let mut inner = self.inner.write();
        if inner.sequence != sequence {
            info!("catalog updated elsewhere");
            return Err(Error::CatalogUpdatedElsewhere);
        }

        info!("inserted {}", db.name);

        inner.sequence += 1;
        inner.databases.insert(db.name.clone(), db);

        Ok(())
    }

    pub(crate) fn db_or_create(&self, db_name: &str) -> (u64, Arc<DatabaseSchema>) {
        let (sequence, db) = {
            let inner = self.inner.read();
            (inner.sequence, inner.databases.get(db_name).cloned())
        };

        let db = match db {
            Some(db) => {
                info!("return existing db {}", db_name);
                db
            },
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

    pub(crate) fn db_schema(&self, name: &str) -> Option<Arc<DatabaseSchema>> {
        info!("db_schema {}", name);
        self.inner.read().databases.get(name).cloned()
    }


    pub(crate) fn inner(&self) -> &RwLock<InnerCatalog> {
        &self.inner
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) struct InnerCatalog {
    /// The catalog is a map of databases with their table schemas
    databases: HashMap<String, Arc<DatabaseSchema>>,
    sequence: u64,
}

impl InnerCatalog {
    pub(crate) fn new() -> Self {
        Self{
            databases: HashMap::new(),
            sequence: 0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct DatabaseSchema {
    pub(crate) name: String,
    /// The database is a map of tables
    pub(crate) tables: HashMap<String, TableDefinition>,
}

impl DatabaseSchema {
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self{
            name: name.into(),
            tables: HashMap::new(),
        }
    }

    pub(crate) fn get_table_schema(&self, table_name: &str) -> Option<Schema> {
        self.tables.get(table_name).and_then(|table| table.schema.clone())
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub(crate) struct TableDefinition {
    pub(crate) name: String,
    #[serde(skip_serializing, skip_deserializing)]
    pub(crate) schema: Option<Schema>,
    columns: HashMap<String, ColumnType>,
}

impl TableDefinition {
    pub(crate) fn new(name: impl Into<String>, columns: HashMap<String, ColumnType>) -> Self {
        let mut schema_builder = SchemaBuilder::with_capacity(columns.len());
        for (name, column_type) in &columns {
            schema_builder.influx_column(name, column_type_to_influx_column_type(column_type));
        }
        let schema = schema_builder.build().unwrap();

        Self{
            name: name.into(),
            schema: Some(schema),
            columns,
        }
    }

    pub(crate) fn column_exists(&self, column: &str) -> bool {
        self.columns.contains_key(column)
    }

    pub(crate) fn add_columns(&mut self, columns: Vec<(String, ColumnType)>) {
        let mut schema_builder = SchemaBuilder::with_capacity(columns.len());
        for (name, column_type) in &columns {
            schema_builder.influx_column(name, column_type_to_influx_column_type(column_type));
        }
        let schema = schema_builder.build().unwrap();

        for (name, column_type) in columns.into_iter() {
            self.columns.insert(name, column_type);
        }
        self.schema = Some(schema);
    }

    pub(crate) fn columns(&self) -> &HashMap<String, ColumnType> {
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
            tables: HashMap::new(),
        };
        database.tables.insert("test".into(), TableDefinition::new("test", HashMap::from([("test".to_string(), ColumnType::String)])));
        let database = Arc::new(database);
        catalog.replace_database(0, database).unwrap();
        let inner = catalog.inner().read();

        let serialized = serde_json::to_string(&*inner).unwrap();
        let deserialized: InnerCatalog = serde_json::from_str(&serialized).unwrap();
        assert_eq!(*inner, deserialized);
    }
}