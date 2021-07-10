//! This module provides a reference implementaton of `query::DatabaseSource`
//! and `query::Database` for use in testing.
//!
//! AKA it is a Mock

use crate::exec::Executor;
use crate::{
    exec::stringset::{StringSet, StringSetRef},
    DatabaseStore, Predicate, PredicateMatch, QueryChunk, QueryChunkMeta, QueryDatabase,
};
use arrow::{
    array::{ArrayRef, DictionaryArray, Int64Array, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, Int32Type, TimeUnit},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use data_types::{
    chunk_metadata::ChunkSummary,
    partition_metadata::{ColumnSummary, InfluxDbType, StatValues, Statistics, TableSummary},
};
use datafusion::physical_plan::{common::SizedRecordBatchStream, SendableRecordBatchStream};
use futures::StreamExt;
use internal_types::{
    schema::{builder::SchemaBuilder, merge::SchemaMerger, InfluxColumnType, Schema},
    selection::Selection,
};
use parking_lot::Mutex;
use snafu::Snafu;
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
    pub fn add_chunk(&self, partition_key: &str, chunk: Arc<TestChunk>) -> &Self {
        let mut partitions = self.partitions.lock();
        let chunks = partitions
            .entry(partition_key.to_string())
            .or_insert_with(BTreeMap::new);
        chunks.insert(chunk.id(), chunk);
        self
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

impl QueryDatabase for TestDatabase {
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

    fn chunk_summaries(&self) -> Result<Vec<ChunkSummary>, Self::Error> {
        unimplemented!("summaries not implemented TestDatabase")
    }

    fn table_schema(&self, table_name: &str) -> Option<Arc<Schema>> {
        let mut merger = SchemaMerger::new();
        let mut found_one = false;

        let partitions = self.partitions.lock();
        for partition in partitions.values() {
            for chunk in partition.values() {
                if chunk.table_name() == table_name {
                    merger = merger.merge(&chunk.schema()).expect("consistent schemas");
                    found_one = true;
                }
            }
        }

        found_one.then(|| Arc::new(merger.build()))
    }
}

#[derive(Debug)]
pub struct TestChunk {
    /// Table name
    table_name: String,

    /// Schema of the table
    schema: Arc<Schema>,

    /// Return value for summary()
    table_summary: TableSummary,

    id: u32,

    /// Set the flag if this chunk might contain duplicates
    may_contain_pk_duplicates: bool,

    /// A copy of the captured predicates passed
    predicates: Mutex<Vec<Predicate>>,

    /// RecordBatches that are returned on each request
    table_data: Vec<Arc<RecordBatch>>,

    /// A saved error that is returned instead of actual results
    saved_error: Option<String>,

    /// Return value for apply_predicate, if desired
    predicate_match: Option<PredicateMatch>,
}

impl TestChunk {
    pub fn new(table_name: impl Into<String>) -> Self {
        let table_name = table_name.into();
        Self {
            table_name: table_name.clone(),
            schema: Arc::new(SchemaBuilder::new().build().unwrap()),
            table_summary: TableSummary::new(table_name),
            id: Default::default(),
            may_contain_pk_duplicates: Default::default(),
            predicates: Default::default(),
            table_data: Default::default(),
            saved_error: Default::default(),
            predicate_match: Default::default(),
        }
    }

    pub fn with_id(mut self, id: u32) -> Self {
        self.id = id;
        self
    }

    /// specify that any call should result in an error with the message
    /// specified
    pub fn with_error(mut self, error_message: impl Into<String>) -> Self {
        self.saved_error = Some(error_message.into());
        self
    }

    /// specify that any call to apply_predicate should return this value
    pub fn with_predicate_match(mut self, predicate_match: PredicateMatch) -> Self {
        self.predicate_match = Some(predicate_match);
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

    /// Set the `may_contain_pk_duplicates` flag
    pub fn with_may_contain_pk_duplicates(mut self, v: bool) -> Self {
        self.may_contain_pk_duplicates = v;
        self
    }

    /// Register an tag column with the test chunk
    pub fn with_tag_column(self, column_name: impl Into<String>) -> Self {
        let column_name = column_name.into();

        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new().tag(&column_name).build().unwrap();

        self.add_schema_to_table(new_column_schema, None)
    }

    /// Register an tag column with the test chunk
    pub fn with_tag_column_with_stats(
        self,
        column_name: impl Into<String>,
        min: &str,
        max: &str,
    ) -> Self {
        let column_name = column_name.into();

        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new().tag(&column_name).build().unwrap();

        // Construct stats
        let stats = Statistics::String(StatValues {
            min: Some(min.to_string()),
            max: Some(max.to_string()),
            ..Default::default()
        });

        self.add_schema_to_table(new_column_schema, Some(stats))
    }

    /// Register a timestamp column with the test chunk
    pub fn with_time_column(self) -> Self {
        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new().timestamp().build().unwrap();

        self.add_schema_to_table(new_column_schema, None)
    }

    /// Register a timestamp column with the test chunk
    pub fn with_time_column_with_stats(self, min: i64, max: i64) -> Self {
        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new().timestamp().build().unwrap();

        // Construct stats
        let stats = Statistics::I64(StatValues {
            min: Some(min),
            max: Some(max),
            ..Default::default()
        });

        self.add_schema_to_table(new_column_schema, Some(stats))
    }

    /// Register an int field column with the test chunk
    pub fn with_int_field_column(self, column_name: impl Into<String>) -> Self {
        let column_name = column_name.into();

        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new()
            .field(&column_name, DataType::Int64)
            .build()
            .unwrap();
        self.add_schema_to_table(new_column_schema, None)
    }

    fn add_schema_to_table(mut self, new_column_schema: Schema, stats: Option<Statistics>) -> Self {
        // assume the new schema has exactly a single table
        assert_eq!(new_column_schema.len(), 1);
        let (col_type, new_field) = new_column_schema.field(0);

        let influxdb_type = col_type.map(|t| match t {
            InfluxColumnType::IOx(_) => todo!(),
            InfluxColumnType::Tag => InfluxDbType::Tag,
            InfluxColumnType::Field(_) => InfluxDbType::Field,
            InfluxColumnType::Timestamp => InfluxDbType::Timestamp,
        });

        let stats = stats.unwrap_or_else(|| match new_field.data_type() {
            DataType::Boolean => Statistics::Bool(StatValues::default()),
            DataType::Int64 => Statistics::I64(StatValues::default()),
            DataType::UInt64 => Statistics::U64(StatValues::default()),
            DataType::Utf8 => Statistics::String(StatValues::default()),
            DataType::Dictionary(_, value_type) => {
                assert!(matches!(**value_type, DataType::Utf8));
                Statistics::String(StatValues::default())
            }
            DataType::Float64 => Statistics::String(StatValues::default()),
            DataType::Timestamp(_, _) => Statistics::I64(StatValues::default()),
            _ => panic!("Unsupported type in TestChunk: {:?}", new_field.data_type()),
        });

        let column_summary = ColumnSummary {
            name: new_field.name().clone(),
            influxdb_type,
            stats,
        };

        let mut merger = SchemaMerger::new();
        merger = merger.merge(&new_column_schema).unwrap();
        merger = merger
            .merge(self.schema.as_ref())
            .expect("merging was successful");
        self.schema = Arc::new(merger.build());

        self.table_summary.columns.push(column_summary);

        self
    }

    /// Get a copy of any predicate passed to the function
    pub fn predicates(&self) -> Vec<Predicate> {
        self.predicates.lock().clone()
    }

    /// Prepares this chunk to return a specific record batch with one
    /// row of non null data.
    pub fn with_one_row_of_data(mut self) -> Self {
        // create arrays
        let columns = self
            .schema
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

        let batch =
            RecordBatch::try_new(self.schema.as_ref().into(), columns).expect("made record batch");
        println!("TestChunk batch data: {:#?}", batch);

        self.table_data.push(Arc::new(batch));
        self
    }

    /// Prepares this chunk to return a specific record batch with three
    /// rows of non null data that look like, no duplicates within
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| tag1 | tag2 | field_int | time                          |",
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| WA   | SC   | 1000      | 1970-01-01 00:00:00.000008    |",
    ///   "| VT   | NC   | 10        | 1970-01-01 00:00:00.000010    |",
    ///   "| UT   | RI   | 70        | 1970-01-01 00:00:00.000020    |",
    ///   "+------+------+-----------+-------------------------------+",
    /// Stats(min, max) : tag1(UT, WA), tag2(RI, SC), time(8000, 20000)
    pub fn with_three_rows_of_data(mut self) -> Self {
        // create arrays
        let columns = self
            .schema
            .iter()
            .map(|(_influxdb_column_type, field)| match field.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(vec![1000, 10, 70])) as ArrayRef,
                DataType::Utf8 => match field.name().as_str() {
                    "tag1" => Arc::new(StringArray::from(vec!["WA", "VT", "UT"])) as ArrayRef,
                    "tag2" => Arc::new(StringArray::from(vec!["SC", "NC", "RI"])) as ArrayRef,
                    _ => Arc::new(StringArray::from(vec!["TX", "PR", "OR"])) as ArrayRef,
                },
                DataType::Timestamp(TimeUnit::Nanosecond, _) => Arc::new(
                    TimestampNanosecondArray::from_vec(vec![8000, 10000, 20000], None),
                ) as ArrayRef,
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    match field.name().as_str() {
                        "tag1" => Arc::new(
                            vec!["WA", "VT", "UT"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        "tag2" => Arc::new(
                            vec!["SC", "NC", "RI"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        _ => Arc::new(
                            vec!["TX", "PR", "OR"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                    }
                }
                _ => unimplemented!(
                    "Unimplemented data type for test database: {:?}",
                    field.data_type()
                ),
            })
            .collect::<Vec<_>>();

        let batch =
            RecordBatch::try_new(self.schema.as_ref().into(), columns).expect("made record batch");

        self.table_data.push(Arc::new(batch));
        self
    }

    /// Prepares this chunk to return a specific record batch with four
    /// rows of non null data that look like, duplicates within
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| tag1 | tag2 | field_int | time                          |",
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| WA   | SC   | 1000      | 1970-01-01 00:00:00.000028    |",
    ///   "| VT   | NC   | 10        | 1970-01-01 00:00:00.000210    |", (1)
    ///   "| UT   | RI   | 70        | 1970-01-01 00:00:00.000220    |",
    ///   "| VT   | NC   | 50        | 1970-01-01 00:00:00.000210    |", // duplicate of (1)
    ///   "+------+------+-----------+-------------------------------+",
    /// Stats(min, max) : tag1(UT, WA), tag2(RI, SC), time(28000, 220000)
    pub fn with_four_rows_of_data(mut self) -> Self {
        // create arrays
        let columns = self
            .schema
            .iter()
            .map(|(_influxdb_column_type, field)| match field.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(vec![1000, 10, 70, 50])) as ArrayRef,
                DataType::Utf8 => match field.name().as_str() {
                    "tag1" => Arc::new(StringArray::from(vec!["WA", "VT", "UT", "VT"])) as ArrayRef,
                    "tag2" => Arc::new(StringArray::from(vec!["SC", "NC", "RI", "NC"])) as ArrayRef,
                    _ => Arc::new(StringArray::from(vec!["TX", "PR", "OR", "AL"])) as ArrayRef,
                },
                DataType::Timestamp(TimeUnit::Nanosecond, _) => Arc::new(
                    TimestampNanosecondArray::from_vec(vec![8000, 10000, 20000, 10000], None),
                ) as ArrayRef,
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    match field.name().as_str() {
                        "tag1" => Arc::new(
                            vec!["WA", "VT", "UT", "VT"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        "tag2" => Arc::new(
                            vec!["SC", "NC", "RI", "NC"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        _ => Arc::new(
                            vec!["TX", "PR", "OR", "AL"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                    }
                }
                _ => unimplemented!(
                    "Unimplemented data type for test database: {:?}",
                    field.data_type()
                ),
            })
            .collect::<Vec<_>>();

        let batch =
            RecordBatch::try_new(self.schema.as_ref().into(), columns).expect("made record batch");

        self.table_data.push(Arc::new(batch));
        self
    }

    /// Prepares this chunk to return a specific record batch with five
    /// rows of non null data that look like, no duplicates within
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| tag1 | tag2 | field_int | time                          |",
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| MT   | CT   | 1000      | 1970-01-01 00:00:00.000001    |",
    ///   "| MT   | AL   | 10        | 1970-01-01 00:00:00.000007    |",
    ///   "| CT   | CT   | 70        | 1970-01-01 00:00:00.000000100 |",
    ///   "| AL   | MA   | 100       | 1970-01-01 00:00:00.000000050 |",
    ///   "| MT   | AL   | 5         | 1970-01-01 00:00:00.000005    |",
    ///   "+------+------+-----------+-------------------------------+",
    /// Stats(min, max) : tag1(AL, MT), tag2(AL, MA), time(5, 7000)
    pub fn with_five_rows_of_data(mut self) -> Self {
        // create arrays
        let columns = self
            .schema
            .iter()
            .map(|(_influxdb_column_type, field)| match field.data_type() {
                DataType::Int64 => {
                    Arc::new(Int64Array::from(vec![1000, 10, 70, 100, 5])) as ArrayRef
                }
                DataType::Utf8 => {
                    match field.name().as_str() {
                        "tag1" => Arc::new(StringArray::from(vec!["MT", "MT", "CT", "AL", "MT"]))
                            as ArrayRef,
                        "tag2" => Arc::new(StringArray::from(vec!["CT", "AL", "CT", "MA", "AL"]))
                            as ArrayRef,
                        _ => Arc::new(StringArray::from(vec!["CT", "MT", "AL", "AL", "MT"]))
                            as ArrayRef,
                    }
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => Arc::new(
                    TimestampNanosecondArray::from_vec(vec![1000, 7000, 100, 50, 5000], None),
                ) as ArrayRef,
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    match field.name().as_str() {
                        "tag1" => Arc::new(
                            vec!["MT", "MT", "CT", "AL", "MT"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        "tag2" => Arc::new(
                            vec!["CT", "AL", "CT", "MA", "AL"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        _ => Arc::new(
                            vec!["CT", "MT", "AL", "AL", "MT"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                    }
                }
                _ => unimplemented!(
                    "Unimplemented data type for test database: {:?}",
                    field.data_type()
                ),
            })
            .collect::<Vec<_>>();

        let batch =
            RecordBatch::try_new(self.schema.as_ref().into(), columns).expect("made record batch");

        self.table_data.push(Arc::new(batch));
        self
    }

    /// Prepares this chunk to return a specific record batch with ten
    /// rows of non null data that look like, duplicates within
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| tag1 | tag2 | field_int | time                          |",
    ///   "+------+------+-----------+-------------------------------+",
    ///   "| MT   | CT   | 1000      | 1970-01-01 00:00:00.000001    |",
    ///   "| MT   | AL   | 10        | 1970-01-01 00:00:00.000007    |", (1)
    ///   "| CT   | CT   | 70        | 1970-01-01 00:00:00.000000100 |",
    ///   "| AL   | MA   | 100       | 1970-01-01 00:00:00.000000050 |", (2)
    ///   "| MT   | AL   | 5         | 1970-01-01 00:00:00.000005    |", (3)
    ///   "| MT   | CT   | 1000      | 1970-01-01 00:00:00.000002    |",
    ///   "| MT   | AL   | 20        | 1970-01-01 00:00:00.000007    |",  // Duplicate with (1)
    ///   "| CT   | CT   | 70        | 1970-01-01 00:00:00.000000500 |",
    ///   "| AL   | MA   | 10        | 1970-01-01 00:00:00.000000050 |",  // Duplicate with (2)
    ///   "| MT   | AL   | 30        | 1970-01-01 00:00:00.000005    |",  // Duplicate with (3)
    ///   "+------+------+-----------+-------------------------------+",
    /// Stats(min, max) : tag1(AL, MT), tag2(AL, MA), time(5, 7000)
    pub fn with_ten_rows_of_data_some_duplicates(mut self) -> Self {
        // create arrays
        let columns = self
            .schema
            .iter()
            .map(|(_influxdb_column_type, field)| match field.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(vec![
                    1000, 10, 70, 100, 5, 1000, 20, 70, 10, 30,
                ])) as ArrayRef,
                DataType::Utf8 => match field.name().as_str() {
                    "tag1" => Arc::new(StringArray::from(vec![
                        "MT", "MT", "CT", "AL", "MT", "MT", "MT", "CT", "AL", "MT",
                    ])) as ArrayRef,
                    "tag2" => Arc::new(StringArray::from(vec![
                        "CT", "AL", "CT", "MA", "AL", "CT", "AL", "CT", "MA", "AL",
                    ])) as ArrayRef,
                    _ => Arc::new(StringArray::from(vec![
                        "CT", "MT", "AL", "AL", "MT", "CT", "MT", "AL", "AL", "MT",
                    ])) as ArrayRef,
                },
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    Arc::new(TimestampNanosecondArray::from_vec(
                        vec![1000, 7000, 100, 50, 5, 2000, 7000, 500, 50, 5],
                        None,
                    )) as ArrayRef
                }
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    match field.name().as_str() {
                        "tag1" => Arc::new(
                            vec!["MT", "MT", "CT", "AL", "MT", "MT", "MT", "CT", "AL", "MT"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        "tag2" => Arc::new(
                            vec!["CT", "AL", "CT", "MA", "AL", "CT", "AL", "CT", "MA", "AL"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                        _ => Arc::new(
                            vec!["CT", "MT", "AL", "AL", "MT", "CT", "MT", "AL", "AL", "MT"]
                                .into_iter()
                                .collect::<DictionaryArray<Int32Type>>(),
                        ) as ArrayRef,
                    }
                }
                _ => unimplemented!(
                    "Unimplemented data type for test database: {:?}",
                    field.data_type()
                ),
            })
            .collect::<Vec<_>>();

        let batch =
            RecordBatch::try_new(self.schema.as_ref().into(), columns).expect("made record batch");

        self.table_data.push(Arc::new(batch));
        self
    }

    /// Returns all columns of the table
    pub fn all_column_names(&self) -> StringSet {
        self.schema
            .iter()
            .map(|(_, field)| field.name().to_string())
            .collect()
    }

    /// Returns just the specified columns
    pub fn specific_column_names_selection(&self, columns: &[&str]) -> StringSet {
        self.schema
            .iter()
            .map(|(_, field)| field.name().to_string())
            .filter(|col| columns.contains(&col.as_str()))
            .collect()
    }
}

impl QueryChunk for TestChunk {
    type Error = TestError;

    fn id(&self) -> u32 {
        self.id
    }

    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        self.may_contain_pk_duplicates
    }

    fn read_filter(
        &self,
        predicate: &Predicate,
        _selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        self.check_error()?;

        // save the predicate
        self.predicates.lock().push(predicate.clone());

        let batches = self.table_data.clone();
        let stream = SizedRecordBatchStream::new(batches[0].schema(), batches);
        Ok(Box::pin(stream))
    }

    /// Returns true if data of this chunk is sorted
    fn is_sorted_on_pk(&self) -> bool {
        false
    }

    fn apply_predicate_to_metadata(&self, predicate: &Predicate) -> Result<PredicateMatch> {
        self.check_error()?;

        // save the predicate
        self.predicates.lock().push(predicate.clone());

        // check if there is a saved result to return
        if let Some(&predicate_match) = self.predicate_match.as_ref() {
            return Ok(predicate_match);
        }

        // otherwise fall back to basic filtering based on table name predicate.
        let predicate_match = if !predicate.should_include_table(&self.table_name) {
            PredicateMatch::Zero
        } else {
            PredicateMatch::Unknown
        };

        Ok(predicate_match)
    }

    fn column_values(
        &self,
        _column_name: &str,
        _predicate: &Predicate,
    ) -> Result<Option<StringSet>, Self::Error> {
        // Model not being able to get column values from metadata
        Ok(None)
    }

    fn column_names(
        &self,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<Option<StringSet>, Self::Error> {
        self.check_error()?;

        // save the predicate
        self.predicates.lock().push(predicate.clone());

        // only return columns specified in selection
        let column_names = match selection {
            Selection::All => self.all_column_names(),
            Selection::Some(cols) => self.specific_column_names_selection(cols),
        };

        Ok(Some(column_names))
    }
}

impl QueryChunkMeta for TestChunk {
    fn summary(&self) -> &TableSummary {
        &self.table_summary
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
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

/// Return the raw data from the list of chunks
pub async fn raw_data(chunks: &[Arc<TestChunk>]) -> Vec<RecordBatch> {
    let mut batches = vec![];
    for c in chunks {
        let pred = Predicate::default();
        let selection = Selection::All;
        let mut stream = c
            .read_filter(&pred, selection)
            .expect("Error in read_filter");
        while let Some(b) = stream.next().await {
            let b = b.expect("Error in stream");
            batches.push(b)
        }
    }
    batches
}
