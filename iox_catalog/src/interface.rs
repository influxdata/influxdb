//! This module contains the traits and data objects for the Catalog API.

use async_trait::async_trait;
use influxdb_line_protocol::FieldValue;
use snafu::{OptionExt, Snafu};
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Name {} already exists", name))]
    NameExists { name: String },

    #[snafu(display("Unhandled sqlx error: {}", source))]
    SqlxError { source: sqlx::Error },

    #[snafu(display("Foreign key violation: {}", source))]
    ForeignKeyViolation { source: sqlx::Error },

    #[snafu(display("Column {} is type {} but write has type {}", name, existing, new))]
    ColumnTypeMismatch {
        name: String,
        existing: String,
        new: String,
    },

    #[snafu(display(
        "Column type {} is in the db for column {}, which is unknown",
        data_type,
        name
    ))]
    UnknownColumnType { data_type: i16, name: String },

    #[snafu(display("namespace {} not found", name))]
    NamespaceNotFound { name: String },
}

/// A specialized `Error` for Catalog errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Container that can return repos for each of the catalog data types.
#[async_trait]
pub trait RepoCollection {
    /// repo for kafka topics
    fn kafka_topic(&self) -> Arc<dyn KafkaTopicRepo + Sync + Send>;
    /// repo fo rquery pools
    fn query_pool(&self) -> Arc<dyn QueryPoolRepo + Sync + Send>;
    /// repo for namespaces
    fn namespace(&self) -> Arc<dyn NamespaceRepo + Sync + Send>;
    /// repo for tables
    fn table(&self) -> Arc<dyn TableRepo + Sync + Send>;
    /// repo for columns
    fn column(&self) -> Arc<dyn ColumnRepo + Sync + Send>;
    /// repo for sequencers
    fn sequencer(&self) -> Arc<dyn SequencerRepo + Sync + Send>;
}

/// Functions for working with Kafka topics in the catalog.
#[async_trait]
pub trait KafkaTopicRepo {
    /// Creates the kafka topic in the catalog or gets the existing record by name.
    async fn create_or_get(&self, name: &str) -> Result<KafkaTopic>;
}

/// Functions for working with query pools in the catalog.
#[async_trait]
pub trait QueryPoolRepo {
    /// Creates the query pool in the catalog or gets the existing record by name.
    async fn create_or_get(&self, name: &str) -> Result<QueryPool>;
}

/// Functions for working with namespaces in the catalog
#[async_trait]
pub trait NamespaceRepo {
    /// Creates the namespace in the catalog. If one by the same name already exists, an
    /// error is returned.
    async fn create(
        &self,
        name: &str,
        retention_duration: &str,
        kafka_topic_id: i32,
        query_pool_id: i16,
    ) -> Result<Namespace>;

    /// Gets the namespace by its unique name.
    async fn get_by_name(&self, name: &str) -> Result<Option<Namespace>>;
}

/// Functions for working with tables in the catalog
#[async_trait]
pub trait TableRepo {
    /// Creates the table in the catalog or get the existing record by name.
    async fn create_or_get(&self, name: &str, namespace_id: i32) -> Result<Table>;

    /// Lists all tables in the catalog for the given namespace id.
    async fn list_by_namespace_id(&self, namespace_id: i32) -> Result<Vec<Table>>;
}

/// Functions for working with columns in the catalog
#[async_trait]
pub trait ColumnRepo {
    /// Creates the column in the catalog or returns the existing column. Will return a
    /// `Error::ColumnTypeMismatch` if the existing column type doesn't match the type
    /// the caller is attempting to create.
    async fn create_or_get(
        &self,
        name: &str,
        table_id: i32,
        column_type: ColumnType,
    ) -> Result<Column>;

    /// Lists all columns in the passed in namespace id.
    async fn list_by_namespace_id(&self, namespace_id: i32) -> Result<Vec<Column>>;
}

/// Functions for working with sequencers in the catalog
#[async_trait]
pub trait SequencerRepo {
    /// create a sequencer record for the kafka topic and partition or return the existing record
    async fn create_or_get(&self, topic: &KafkaTopic, partition: i32) -> Result<Sequencer>;

    /// list all sequencers
    async fn list(&self) -> Result<Vec<Sequencer>>;
}

/// Data object for a kafka topic
#[derive(Debug, Clone, Eq, PartialEq, sqlx::FromRow)]
pub struct KafkaTopic {
    /// The id of the topic
    pub id: i32,
    /// The unique name of the topic
    pub name: String,
}

/// Data object for a query pool
#[derive(Debug, Clone, Eq, PartialEq, sqlx::FromRow)]
pub struct QueryPool {
    /// The id of the pool
    pub id: i16,
    /// The unique name of the pool
    pub name: String,
}

/// Data object for a namespace
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Namespace {
    /// The id of the namespace
    pub id: i32,
    /// The unique name of the namespace
    pub name: String,
    /// The retention duration as a string. 'inf' or not present represents infinite duration (i.e. never drop data).
    #[sqlx(default)]
    pub retention_duration: Option<String>,
    /// The kafka topic that writes to this namespace will land in
    pub kafka_topic_id: i32,
    /// The query pool assigned to answer queries for this namespace
    pub query_pool_id: i16,
}

/// Schema collection for a namespace
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NamespaceSchema {
    /// the namespace id
    pub id: i32,
    /// the kafka topic this namespace gets data written to
    pub kafka_topic_id: i32,
    /// the query pool assigned to answer queries for this namespace
    pub query_pool_id: i16,
    /// the tables in the namespace by name
    pub tables: BTreeMap<String, TableSchema>,
}

impl NamespaceSchema {
    /// Create a new `NamespaceSchema`
    pub fn new(id: i32, kafka_topic_id: i32, query_pool_id: i16) -> Self {
        Self {
            id,
            tables: BTreeMap::new(),
            kafka_topic_id,
            query_pool_id,
        }
    }

    /// Gets the namespace schema including all tables and columns.
    pub async fn get_by_name<T: RepoCollection + Send + Sync>(
        name: &str,
        repo: &T,
    ) -> Result<Option<Self>> {
        let namespace_repo = repo.namespace();
        let table_repo = repo.table();
        let column_repo = repo.column();

        let namespace = namespace_repo
            .get_by_name(name)
            .await?
            .context(NamespaceNotFoundSnafu { name })?;

        // get the columns first just in case someone else is creating schema while we're doing this.
        let columns = column_repo.list_by_namespace_id(namespace.id).await?;
        let tables = table_repo.list_by_namespace_id(namespace.id).await?;

        let mut namespace = Self::new(
            namespace.id,
            namespace.kafka_topic_id,
            namespace.query_pool_id,
        );

        let mut table_id_to_schema = BTreeMap::new();
        for t in tables {
            table_id_to_schema.insert(t.id, (t.name, TableSchema::new(t.id)));
        }

        for c in columns {
            let (_, t) = table_id_to_schema.get_mut(&c.table_id).unwrap();
            match ColumnType::try_from(c.column_type) {
                Ok(column_type) => {
                    t.columns.insert(
                        c.name,
                        ColumnSchema {
                            id: c.id,
                            column_type,
                        },
                    );
                }
                _ => {
                    return Err(Error::UnknownColumnType {
                        data_type: c.column_type,
                        name: c.name.to_string(),
                    });
                }
            }
        }

        for (_, (table_name, schema)) in table_id_to_schema {
            namespace.tables.insert(table_name, schema);
        }

        Ok(Some(namespace))
    }

    /// Adds tables and columns to the `NamespaceSchema`. These are created
    /// incrementally while validating the schema for a write and this helper
    /// method takes them in to add them to the schema.
    pub fn add_tables_and_columns(
        &mut self,
        new_tables: BTreeMap<String, i32>,
        new_columns: BTreeMap<i32, BTreeMap<String, ColumnSchema>>,
    ) {
        for (table_name, table_id) in new_tables {
            self.tables
                .entry(table_name)
                .or_insert_with(|| TableSchema::new(table_id));
        }

        for (table_id, new_columns) in new_columns {
            let table = self
                .get_table_mut(table_id)
                .expect("table must be in namespace to add columns");
            table.add_columns(new_columns);
        }
    }

    fn get_table_mut(&mut self, table_id: i32) -> Option<&mut TableSchema> {
        for table in self.tables.values_mut() {
            if table.id == table_id {
                return Some(table);
            }
        }

        None
    }
}

/// Data object for a table
#[derive(Debug, Clone, sqlx::FromRow, Eq, PartialEq)]
pub struct Table {
    /// The id of the table
    pub id: i32,
    /// The namespace id that the table is in
    pub namespace_id: i32,
    /// The name of the table, which is unique within the associated namespace
    pub name: String,
}

/// Column definitions for a table
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TableSchema {
    /// the table id
    pub id: i32,
    /// the table's columns by their name
    pub columns: BTreeMap<String, ColumnSchema>,
}

impl TableSchema {
    /// Initialize new `TableSchema`
    pub fn new(id: i32) -> Self {
        Self {
            id,
            columns: BTreeMap::new(),
        }
    }

    /// Add the map of columns to the `TableSchema`
    pub fn add_columns(&mut self, columns: BTreeMap<String, ColumnSchema>) {
        for (name, column) in columns {
            self.columns.insert(name, column);
        }
    }
}

/// Data object for a column
#[derive(Debug, Clone, sqlx::FromRow, Eq, PartialEq)]
pub struct Column {
    /// the column id
    pub id: i32,
    /// the table id the column is in
    pub table_id: i32,
    /// the name of the column, which is unique in the table
    pub name: String,
    /// the logical type of the column
    pub column_type: i16,
}

impl Column {
    /// returns true if the column type is a tag
    pub fn is_tag(&self) -> bool {
        self.column_type == ColumnType::Tag as i16
    }

    /// returns true if the column type matches the line protocol field value type
    pub fn matches_field_type(&self, field_value: &FieldValue) -> bool {
        match field_value {
            FieldValue::I64(_) => self.column_type == ColumnType::I64 as i16,
            FieldValue::U64(_) => self.column_type == ColumnType::U64 as i16,
            FieldValue::F64(_) => self.column_type == ColumnType::F64 as i16,
            FieldValue::String(_) => self.column_type == ColumnType::String as i16,
            FieldValue::Boolean(_) => self.column_type == ColumnType::Bool as i16,
        }
    }
}

/// The column id and its type for a column
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ColumnSchema {
    /// the column id
    pub id: i32,
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
}

/// The column data type
#[allow(missing_docs)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
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
            ColumnType::I64 => "i64",
            ColumnType::U64 => "u64",
            ColumnType::F64 => "f64",
            ColumnType::Bool => "bool",
            ColumnType::String => "string",
            ColumnType::Time => "time",
            ColumnType::Tag => "tag",
        }
    }
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = self.as_str();

        write!(f, "{}", s)
    }
}

impl TryFrom<i16> for ColumnType {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: i16) -> std::prelude::rust_2015::Result<Self, Self::Error> {
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

/// Data object for a sequencer. Only one sequencer record can exist for a given
/// kafka topic and partition (enforced via uniqueness constraint).
#[derive(Debug, Copy, Clone, PartialEq, sqlx::FromRow)]
pub struct Sequencer {
    /// the id of the sequencer
    pub id: i16,
    /// the topic the sequencer is reading from
    pub kafka_topic_id: i32,
    /// the kafka partition the sequencer is reading from
    pub kafka_partition: i32,
    /// The minimum unpersisted sequence number. Because different tables
    /// can be persisted at different times, it is possible some data has been persisted
    /// with a higher sequence number than this. However, all data with a sequence number
    /// lower than this must have been persisted to Parquet.
    pub min_unpersisted_sequence_number: i64,
}
