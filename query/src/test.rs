//! This module provides a reference implementaton of `query::DatabaseSource`
//! and `query::Database` for use in testing.
//!
//! AKA it is a Mock

use arrow::{
    array::{ArrayRef, DictionaryArray, Int64Array, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, Int32Type, TimeUnit},
    record_batch::RecordBatch,
};
use datafusion::physical_plan::{common::SizedRecordBatchStream, SendableRecordBatchStream};

use crate::exec::Executor;
use crate::{
    exec::stringset::{StringSet, StringSetRef},
    Database, DatabaseStore, PartitionChunk, Predicate,
};

use internal_types::{
    schema::{
        builder::{SchemaBuilder, SchemaMerger},
        Schema,
    },
    selection::Selection,
};

use async_trait::async_trait;
use parking_lot::Mutex;
use snafu::{OptionExt, Snafu};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Default)]
pub struct TestDatabase {
    /// Partitions which have been saved to this test database
    /// Key is partition name
    /// Value is map of chunk_id to chunk
    partitions: Mutex<BTreeMap<String, BTreeMap<u32, Arc<TestChunk>>>>,

    /// `column_names` to return upon next request
    column_names: Arc<Mutex<Option<StringSetRef>>>,
}

#[derive(Snafu, Debug)]
pub enum TestError {
    #[snafu(display("Test database error: {}", message))]
    General { message: String },

    #[snafu(display("Test database execution:  {:?}", source))]
    Execution { source: crate::exec::Error },

    #[snafu(display("Test error writing to database: {}", source))]
    DatabaseWrite {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

pub type Result<T, E = TestError> = std::result::Result<T, E>;

impl TestDatabase {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a test chunk to the database
    pub fn add_chunk(&self, partition_key: &str, chunk: Arc<TestChunk>) {
        let mut partitions = self.partitions.lock();
        let chunks = partitions
            .entry(partition_key.to_string())
            .or_insert_with(BTreeMap::new);
        chunks.insert(chunk.id(), chunk);
    }

    /// Get the specified chunk
    pub fn get_chunk(&self, partition_key: &str, id: u32) -> Option<Arc<TestChunk>> {
        self.partitions
            .lock()
            .get(partition_key)
            .and_then(|p| p.get(&id).cloned())
    }

    /// Set the list of column names that will be returned on a call to
    /// column_names
    pub fn set_column_names(&self, column_names: Vec<String>) {
        let column_names = column_names.into_iter().collect::<StringSet>();
        let column_names = Arc::new(column_names);

        *Arc::clone(&self.column_names).lock() = Some(column_names)
    }
}

impl Database for TestDatabase {
    type Error = TestError;
    type Chunk = TestChunk;

    /// Return the partition keys for data in this DB
    fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        let partitions = self.partitions.lock();
        let keys = partitions.keys().cloned().collect();
        Ok(keys)
    }

    fn chunks(&self, _predicate: &Predicate) -> Vec<Arc<Self::Chunk>> {
        let partitions = self.partitions.lock();
        partitions
            .values()
            .flat_map(|x| x.values())
            .cloned()
            .collect()
    }

    fn chunk_summaries(&self) -> Result<Vec<data_types::chunk::ChunkSummary>, Self::Error> {
        unimplemented!("summaries not implemented TestDatabase")
    }
}

#[derive(Debug, Default)]
pub struct TestChunk {
    id: u32,

    /// A copy of the captured predicate passed
    predicate: Mutex<Option<Predicate>>,

    /// Column names: table_name -> Schema
    table_schemas: BTreeMap<String, Schema>,

    /// RecordBatches that are returned on each request
    table_data: BTreeMap<String, Vec<Arc<RecordBatch>>>,

    /// A saved error that is returned instead of actual results
    saved_error: Option<String>,
}

impl TestChunk {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    /// specify that any call should result in an error with the message
    /// specified
    pub fn with_error(mut self, error_message: impl Into<String>) -> Self {
        self.saved_error = Some(error_message.into());
        self
    }

    /// Checks the saved error, and returns it if any, otherwise returns OK
    fn check_error(&self) -> Result<()> {
        if let Some(message) = self.saved_error.as_ref() {
            General { message }.fail()
        } else {
            Ok(())
        }
    }

    /// Register a table with the test chunk and a "dummy" column
    pub fn with_table(self, table_name: impl Into<String>) -> Self {
        self.with_tag_column(table_name, "dummy_col")
    }

    /// Register an tag column with the test chunk
    pub fn with_tag_column(
        self,
        table_name: impl Into<String>,
        column_name: impl Into<String>,
    ) -> Self {
        let table_name = table_name.into();
        let column_name = column_name.into();

        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new().tag(&column_name).build().unwrap();

        self.add_schema_to_table(table_name, new_column_schema)
    }

    /// Register a timetamp column with the test chunk
    pub fn with_time_column(self, table_name: impl Into<String>) -> Self {
        let table_name = table_name.into();

        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new().timestamp().build().unwrap();

        self.add_schema_to_table(table_name, new_column_schema)
    }

    /// Register an int field column with the test chunk
    pub fn with_int_field_column(
        self,
        table_name: impl Into<String>,
        column_name: impl Into<String>,
    ) -> Self {
        let column_name = column_name.into();

        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new()
            .field(&column_name, DataType::Int64)
            .build()
            .unwrap();
        self.add_schema_to_table(table_name, new_column_schema)
    }

    fn add_schema_to_table(
        mut self,
        table_name: impl Into<String>,
        new_column_schema: Schema,
    ) -> Self {
        let table_name = table_name.into();
        let mut merger = SchemaMerger::new().merge(new_column_schema).unwrap();

        if let Some(existing_schema) = self.table_schemas.remove(&table_name) {
            merger = merger
                .merge(existing_schema)
                .expect("merging was successful");
        }
        let new_schema = merger.build().unwrap();

        self.table_schemas.insert(table_name, new_schema);
        self
    }

    /// Get a copy of any predicate passed to the function
    pub fn predicate(&self) -> Option<Predicate> {
        self.predicate
            .lock()
            .as_ref()
            //.map(|v| v.clone())
            .cloned()
    }

    /// Prepares this chunk to return a specific record batch with one
    /// row of non null data.
    pub fn with_one_row_of_null_data(mut self, table_name: impl Into<String>) -> Self {
        let table_name = table_name.into();
        let schema = self
            .table_schemas
            .get(&table_name)
            .expect("table must exist in TestChunk");

        // create arrays
        let columns = schema
            .iter()
            .map(|(_influxdb_column_type, field)| match field.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(vec![1000])) as ArrayRef,
                DataType::Utf8 => Arc::new(StringArray::from(vec!["MA"])) as ArrayRef,
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    Arc::new(TimestampNanosecondArray::from_vec(vec![1000], None)) as ArrayRef
                }
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    let dict: DictionaryArray<Int32Type> = vec!["MA"].into_iter().collect();
                    Arc::new(dict) as ArrayRef
                }
                _ => unimplemented!(
                    "Unimplemented data type for test database: {:?}",
                    field.data_type()
                ),
            })
            .collect::<Vec<_>>();

        let batch = RecordBatch::try_new(schema.into(), columns).expect("made record batch");

        self.table_data
            .entry(table_name)
            .or_default()
            .push(Arc::new(batch));
        self
    }

    /// Returns all columns of the table
    pub fn all_column_names(&self, table_name: &str) -> Option<StringSet> {
        let column_names = self.table_schemas.get(table_name).map(|schema| {
            schema
                .iter()
                .map(|(_, field)| field.name().to_string())
                .collect::<StringSet>()
        });

        column_names
    }

    /// Returns just the specified columns
    pub fn specific_column_names_selection(
        &self,
        table_name: &str,
        columns: &[&str],
    ) -> Option<StringSet> {
        let column_names = self.table_schemas.get(table_name).map(|schema| {
            schema
                .iter()
                .map(|(_, field)| field.name().to_string())
                .filter(|col| columns.contains(&col.as_str()))
                .collect::<StringSet>()
        });

        column_names
    }
}

impl PartitionChunk for TestChunk {
    type Error = TestError;

    fn id(&self) -> u32 {
        self.id
    }

    fn read_filter(
        &self,
        table_name: &str,
        predicate: &Predicate,
        _selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        self.check_error()?;

        // save the predicate
        self.predicate.lock().replace(predicate.clone());

        let batches = self.table_data.get(table_name).expect("Table had data");
        let stream = SizedRecordBatchStream::new(batches[0].schema(), batches.clone());
        Ok(Box::pin(stream))
    }

    fn table_names(
        &self,
        predicate: &Predicate,
        _known_tables: &StringSet,
    ) -> Result<Option<StringSet>, Self::Error> {
        self.check_error()?;

        // save the predicate
        self.predicate.lock().replace(predicate.clone());

        // do basic filtering based on table name predicate.
        let names = self
            .table_schemas
            .keys()
            .filter(|table_name| predicate.should_include_table(&table_name))
            .cloned()
            .collect();

        Ok(Some(names))
    }

    fn all_table_names(&self, known_tables: &mut StringSet) {
        known_tables.extend(self.table_schemas.keys().cloned())
    }

    fn table_schema(
        &self,
        table_name: &str,
        selection: Selection<'_>,
    ) -> Result<Schema, Self::Error> {
        if !matches!(selection, Selection::All) {
            unimplemented!("Selection in TestChunk::table_schema");
        }

        self.table_schemas
            .get(table_name)
            .cloned()
            .context(General {
                message: format!("TestChunk had no schema for table {}", table_name),
            })
    }

    fn column_values(
        &self,
        _table_name: &str,
        _column_name: &str,
        _predicate: &Predicate,
    ) -> Result<Option<StringSet>, Self::Error> {
        // Model not being able to get column values from metadata
        Ok(None)
    }

    fn has_table(&self, table_name: &str) -> bool {
        self.table_schemas.contains_key(table_name)
    }

    fn column_names(
        &self,
        table_name: &str,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<Option<StringSet>, Self::Error> {
        self.check_error()?;

        // save the predicate
        self.predicate.lock().replace(predicate.clone());

        // only return columns specified in selection
        let column_names = match selection {
            Selection::All => self.all_column_names(table_name),
            Selection::Some(cols) => self.specific_column_names_selection(table_name, cols),
        };

        Ok(column_names)
    }
}

#[derive(Debug)]
pub struct TestDatabaseStore {
    databases: Mutex<BTreeMap<String, Arc<TestDatabase>>>,
    executor: Arc<Executor>,
    pub metrics_registry: metrics::TestMetricRegistry,
}

impl TestDatabaseStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for TestDatabaseStore {
    fn default() -> Self {
        Self {
            databases: Mutex::new(BTreeMap::new()),
            executor: Arc::new(Executor::new(1)),
            metrics_registry: metrics::TestMetricRegistry::default(),
        }
    }
}

#[async_trait]
impl DatabaseStore for TestDatabaseStore {
    type Database = TestDatabase;
    type Error = TestError;

    /// List the database names.
    fn db_names_sorted(&self) -> Vec<String> {
        let databases = self.databases.lock();

        databases.keys().cloned().collect()
    }

    /// Retrieve the database specified name
    fn db(&self, name: &str) -> Option<Arc<Self::Database>> {
        let databases = self.databases.lock();

        databases.get(name).cloned()
    }

    /// Retrieve the database specified by name, creating it if it
    /// doesn't exist.
    async fn db_or_create(&self, name: &str) -> Result<Arc<Self::Database>, Self::Error> {
        let mut databases = self.databases.lock();

        if let Some(db) = databases.get(name) {
            Ok(Arc::clone(&db))
        } else {
            let new_db = Arc::new(TestDatabase::new());
            databases.insert(name.to_string(), Arc::clone(&new_db));
            Ok(new_db)
        }
    }

    fn executor(&self) -> Arc<Executor> {
        Arc::clone(&self.executor)
    }
}
