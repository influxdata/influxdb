//! This module provides a reference implementation of [`QueryNamespace`] for use in testing.
//!
//! AKA it is a Mock

use crate::{
    Extension, QueryChunk, QueryChunkData, QueryCompletedToken, QueryDatabase, QueryNamespace,
    QueryText,
    exec::{Executor, IOxSessionContext, QueryConfig},
    provider::ProviderBuilder,
    query_log::{QueryLog, QueryLogEntries, StateReceived},
};
use arrow::array::{BooleanArray, Float64Array};
use arrow::datatypes::SchemaRef;
use arrow::{
    array::{
        ArrayRef, DictionaryArray, Int64Array, StringArray, TimestampNanosecondArray, UInt64Array,
    },
    datatypes::{DataType, Int32Type, TimeUnit},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use data_types::{
    ChunkId, ChunkOrder, Namespace, NamespaceId, PartitionHashId, PartitionKey, TableId,
};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{catalog::CatalogProvider, physical_plan::displayable};
use datafusion::{
    catalog::{SchemaProvider, Session},
    logical_expr::LogicalPlan,
};
use datafusion::{
    common::stats::Precision,
    datasource::{TableProvider, TableType, object_store::ObjectStoreUrl},
    physical_plan::{ColumnStatistics, Statistics as DataFusionStatistics},
    scalar::ScalarValue,
};
use datafusion_util::{config::DEFAULT_SCHEMA, option_to_precision, timestamptz_nano};
use iox_query_params::StatementParams;
use iox_time::SystemProvider;
use itertools::Itertools;
use object_store::{ObjectMeta, path::Path};
use parking_lot::Mutex;
use parquet_file::storage::DataSourceExecInput;
use schema::{
    Schema, TIME_COLUMN_NAME, builder::SchemaBuilder, merge::SchemaMerger, sort::SortKey,
};
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::{self},
    num::NonZeroU64,
    sync::Arc,
};
use trace::{ctx::SpanContext, span::Span};
use tracker::{AsyncSemaphoreMetrics, InstrumentedAsyncOwnedSemaphorePermit};

#[derive(Debug)]
pub struct TestDatabaseStore {
    databases: Mutex<BTreeMap<String, Arc<TestDatabase>>>,
    executor: Arc<Executor>,
    pub metric_registry: Arc<metric::Registry>,
    pub query_semaphore: Arc<tracker::InstrumentedAsyncSemaphore>,
}

impl TestDatabaseStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_semaphore_size(semaphore_size: usize) -> Self {
        let metric_registry = Arc::new(metric::Registry::default());
        let semaphore_metrics = Arc::new(AsyncSemaphoreMetrics::new(
            &metric_registry,
            &[("semaphore", "query_execution")],
        ));
        Self {
            databases: Mutex::new(BTreeMap::new()),
            executor: Arc::new(Executor::new_testing()),
            metric_registry,
            query_semaphore: Arc::new(semaphore_metrics.new_semaphore(semaphore_size)),
        }
    }

    pub async fn db_or_create(&self, name: &str) -> Arc<TestDatabase> {
        let mut databases = self.databases.lock();

        if let Some(db) = databases.get(name) {
            Arc::clone(db)
        } else {
            let new_db = Arc::new(TestDatabase::new(Arc::clone(&self.executor)));
            databases.insert(name.to_string(), Arc::clone(&new_db));
            new_db
        }
    }
}

impl Default for TestDatabaseStore {
    fn default() -> Self {
        Self::new_with_semaphore_size(u16::MAX as usize)
    }
}

#[async_trait]
impl QueryDatabase for TestDatabaseStore {
    /// Retrieve the database specified name
    async fn namespace(
        &self,
        name: &str,
        _span: Option<Span>,
        _include_debug_info_tables: bool,
    ) -> Result<Option<Arc<dyn QueryNamespace>>, DataFusionError> {
        let databases = self.databases.lock();

        Ok(databases.get(name).cloned().map(|ns| ns as _))
    }

    async fn list_namespaces(
        &self,
        _span: Option<Span>,
    ) -> Result<Vec<Namespace>, DataFusionError> {
        Ok(self
            .databases
            .lock()
            .iter()
            .enumerate()
            .map(|(i, (name, db))| Namespace {
                id: NamespaceId::new(i as i64),
                name: name.to_owned(),
                retention_period_ns: db.retention_time_ns,
                max_tables: Default::default(),
                max_columns_per_table: Default::default(),
                deleted_at: Default::default(),
                partition_template: Default::default(),
                router_version: Default::default(),
                created_at: Default::default(),
            })
            .collect())
    }

    async fn acquire_semaphore(&self, span: Option<Span>) -> InstrumentedAsyncOwnedSemaphorePermit {
        Arc::clone(&self.query_semaphore)
            .acquire_owned(span)
            .await
            .unwrap()
    }

    fn query_log(&self) -> QueryLogEntries {
        QueryLogEntries::default()
    }
}

#[derive(Debug)]
pub struct TestDatabase {
    executor: Arc<Executor>,
    /// Partitions which have been saved to this test database
    /// Key is partition name
    /// Value is map of chunk_id to chunk
    partitions: Mutex<BTreeMap<String, BTreeMap<ChunkId, Arc<TestChunk>>>>,

    /// `column_names` to return upon next request
    column_names: Arc<Mutex<Option<BTreeSet<String>>>>,

    /// The predicate passed to the most recent call to `chunks()`
    chunks_predicate: Mutex<Vec<Expr>>,

    /// Retention time ns.
    retention_time_ns: Option<i64>,
}

impl TestDatabase {
    pub fn new(executor: Arc<Executor>) -> Self {
        Self {
            executor,
            partitions: Default::default(),
            column_names: Default::default(),
            chunks_predicate: Default::default(),
            retention_time_ns: None,
        }
    }

    /// Add a test chunk to the database
    pub fn add_chunk(&self, partition_key: &str, chunk: Arc<TestChunk>) -> &Self {
        let mut partitions = self.partitions.lock();
        let chunks = partitions.entry(partition_key.to_string()).or_default();
        chunks.insert(chunk.id(), chunk);
        self
    }

    /// Add a test chunk to the database
    pub fn with_chunk(self, partition_key: &str, chunk: Arc<TestChunk>) -> Self {
        self.add_chunk(partition_key, chunk);
        self
    }

    /// Get the specified chunk
    pub fn get_chunk(&self, partition_key: &str, id: ChunkId) -> Option<Arc<TestChunk>> {
        self.partitions
            .lock()
            .get(partition_key)
            .and_then(|p| p.get(&id).cloned())
    }

    /// Return the most recent predicate passed to get_chunks()
    pub fn get_chunks_predicate(&self) -> Vec<Expr> {
        self.chunks_predicate.lock().clone()
    }

    /// Set the list of column names that will be returned on a call to
    /// column_names
    pub fn set_column_names(&self, column_names: Vec<String>) {
        let column_names = column_names.into_iter().collect::<BTreeSet<String>>();

        *Arc::clone(&self.column_names).lock() = Some(column_names)
    }

    /// Set retention time.
    pub fn with_retention_time_ns(mut self, retention_time_ns: Option<i64>) -> Self {
        self.retention_time_ns = retention_time_ns;
        self
    }
}

impl QueryNamespace for TestDatabase {
    fn retention_time_ns(&self) -> Option<i64> {
        self.retention_time_ns
    }

    fn record_query(
        &self,
        span_ctx: Option<&SpanContext>,
        query_type: &'static str,
        query_text: QueryText,
        query_params: StatementParams,
        auth_id: Option<String>,
    ) -> QueryCompletedToken<StateReceived> {
        QueryLog::new(
            0,
            Arc::new(SystemProvider::new()),
            &metric::Registry::new(),
            None,
        )
        .push(
            NamespaceId::new(1),
            Arc::from("ns"),
            query_type,
            query_text,
            query_params,
            auth_id,
            span_ctx.map(|s| s.trace_id),
        )
    }

    fn new_extended_query_context(
        &self,
        extension: Option<Arc<dyn Extension>>,
        span_ctx: Option<SpanContext>,
        query_config: Option<&QueryConfig>,
    ) -> IOxSessionContext {
        // Note: unlike Db this does not register a catalog provider
        let mut cmd = self
            .executor
            .new_session_config()
            .with_default_catalog(Arc::new(TestDatabaseCatalogProvider::from_test_database(
                self,
            )))
            .with_span_context(span_ctx);
        if let Some(extension) = extension {
            cmd = cmd.with_query_extension(extension);
        }
        if let Some(query_config) = query_config {
            cmd = cmd.with_query_config(query_config);
        }
        cmd.build()
    }
}

#[derive(Debug)]
struct TestDatabaseCatalogProvider {
    partitions: BTreeMap<String, BTreeMap<ChunkId, Arc<TestChunk>>>,
}

impl TestDatabaseCatalogProvider {
    fn from_test_database(db: &TestDatabase) -> Self {
        Self {
            partitions: db.partitions.lock().clone(),
        }
    }
}

impl CatalogProvider for TestDatabaseCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![DEFAULT_SCHEMA.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match name {
            DEFAULT_SCHEMA => Some(Arc::new(TestDatabaseSchemaProvider {
                partitions: self.partitions.clone(),
            })),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct TestDatabaseSchemaProvider {
    partitions: BTreeMap<String, BTreeMap<ChunkId, Arc<TestChunk>>>,
}

#[async_trait]
impl SchemaProvider for TestDatabaseSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.partitions
            .values()
            .flat_map(|c| c.values())
            .map(|c| c.table_name.to_owned())
            .unique()
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(Some(Arc::new(TestDatabaseTableProvider {
            name: Arc::from(name),
            partitions: self
                .partitions
                .values()
                .flat_map(|chunks| chunks.values().filter(|c| c.table_name() == name))
                .map(Clone::clone)
                .collect(),
        })))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names().contains(&name.to_string())
    }
}

#[derive(Debug)]
struct TestDatabaseTableProvider {
    name: Arc<str>,
    partitions: Vec<Arc<TestChunk>>,
}

impl TestDatabaseTableProvider {
    fn iox_schema(&self) -> Schema {
        self.partitions
            .iter()
            .fold(SchemaMerger::new(), |merger, chunk| {
                merger.merge(chunk.schema()).expect("consistent schemas")
            })
            .build()
    }
}

#[async_trait]
impl TableProvider for TestDatabaseTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.iox_schema().as_arrow()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> crate::exec::context::Result<Arc<dyn ExecutionPlan>> {
        let mut builder = ProviderBuilder::new(Arc::clone(&self.name), self.iox_schema());
        for chunk in &self.partitions {
            builder = builder.add_chunk(Arc::clone(chunk) as Arc<dyn QueryChunk>);
        }
        let provider = builder
            .build()
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        provider.scan(ctx, projection, filters, limit).await
    }
}

#[derive(Debug, Clone)]
enum TestChunkData {
    RecordBatches(Vec<RecordBatch>),
    Parquet(DataSourceExecInput),
}

#[derive(Debug, Clone)]
pub struct TestChunk {
    /// Table name
    table_name: String,

    /// Schema of the table
    schema: Schema,

    /// Values for stats()
    column_stats: HashMap<String, ColumnStatistics>,
    num_rows: Option<u64>,

    id: ChunkId,

    partition_id: PartitionHashId,

    /// Set the flag if this chunk might contain duplicates
    may_contain_pk_duplicates: bool,

    /// Data in this chunk.
    table_data: TestChunkData,

    /// A saved error that is returned instead of actual results
    saved_error: Option<String>,

    /// Order of this chunk relative to other overlapping chunks.
    order: ChunkOrder,

    /// The sort key of this chunk
    sort_key: Option<SortKey>,

    /// Suppress output
    quiet: bool,
}

/// Implements a method for adding a column with default stats
macro_rules! impl_with_column {
    ($NAME:ident, $DATA_TYPE:ident) => {
        pub fn $NAME(self, column_name: impl Into<String>) -> Self {
            let column_name = column_name.into();

            let new_column_schema = SchemaBuilder::new()
                .field(&column_name, DataType::$DATA_TYPE)
                .unwrap()
                .build()
                .unwrap();
            self.add_schema_to_table(new_column_schema, None)
        }
    };
}

/// Implements a method for adding a column with stats that have the specified min and max
macro_rules! impl_with_column_with_stats {
    ($NAME:ident, $DATA_TYPE:ident, $RUST_TYPE:ty, $STAT_TYPE:ident) => {
        pub fn $NAME(
            self,
            column_name: impl Into<String>,
            min: Option<$RUST_TYPE>,
            max: Option<$RUST_TYPE>,
        ) -> Self {
            let column_name = column_name.into();

            let new_column_schema = SchemaBuilder::new()
                .field(&column_name, DataType::$DATA_TYPE)
                .unwrap()
                .build()
                .unwrap();

            let stats = ColumnStatistics {
                null_count: Precision::Absent,
                max_value: option_to_precision(max.map(|s| ScalarValue::from(s))),
                min_value: option_to_precision(min.map(|s| ScalarValue::from(s))),
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
            };

            self.add_schema_to_table(new_column_schema, Some(stats))
        }
    };
}

impl TestChunk {
    pub fn new(table_name: impl Into<String>) -> Self {
        let table_name = table_name.into();
        Self {
            table_name,
            schema: SchemaBuilder::new().build().unwrap(),
            column_stats: Default::default(),
            num_rows: None,
            id: ChunkId::new_test(0),
            may_contain_pk_duplicates: Default::default(),
            table_data: TestChunkData::RecordBatches(vec![]),
            saved_error: Default::default(),
            order: ChunkOrder::MIN,
            sort_key: None,
            partition_id: PartitionHashId::arbitrary_for_testing(),
            quiet: false,
        }
    }

    fn push_record_batch(&mut self, batch: RecordBatch) {
        match &mut self.table_data {
            TestChunkData::RecordBatches(batches) => {
                batches.push(batch);
            }
            TestChunkData::Parquet(_) => panic!("chunk is parquet-based"),
        }
    }

    pub fn with_order(self, order: i64) -> Self {
        Self {
            order: ChunkOrder::new(order),
            ..self
        }
    }

    pub fn with_dummy_parquet_file(self) -> Self {
        self.with_dummy_parquet_file_and_store("iox://store")
    }

    pub fn with_dummy_parquet_file_and_size(self, size: u64) -> Self {
        self.with_dummy_parquet_file_and_store_and_size("iox://store", size)
    }

    pub fn with_dummy_parquet_file_and_store(self, store: &str) -> Self {
        self.with_dummy_parquet_file_and_store_and_size(store, 1)
    }

    pub fn with_dummy_parquet_file_and_store_and_size(self, store: &str, size: u64) -> Self {
        match self.table_data {
            TestChunkData::RecordBatches(batches) => {
                assert!(batches.is_empty(), "chunk already has record batches");
            }
            TestChunkData::Parquet(_) => panic!("chunk already has a file"),
        }

        Self {
            table_data: TestChunkData::Parquet(DataSourceExecInput {
                object_store_url: ObjectStoreUrl::parse(store).unwrap(),
                object_store: Arc::new(object_store::memory::InMemory::new()),
                object_meta: ObjectMeta {
                    location: Self::parquet_location(self.id),
                    last_modified: Default::default(),
                    size,
                    e_tag: None,
                    version: None,
                },
            }),
            ..self
        }
    }

    fn parquet_location(chunk_id: ChunkId) -> Path {
        Path::parse(format!("{}.parquet", chunk_id.get().as_u128())).unwrap()
    }

    /// Returns the receiver configured to suppress any output to STDOUT.
    pub fn with_quiet(mut self) -> Self {
        self.quiet = true;
        self
    }

    pub fn with_id(mut self, id: u128) -> Self {
        self.id = ChunkId::new_test(id);

        if let TestChunkData::Parquet(parquet_input) = &mut self.table_data {
            parquet_input.object_meta.location = Self::parquet_location(self.id);
        }

        self
    }

    pub fn with_partition(mut self, id: i64) -> Self {
        self.partition_id =
            PartitionHashId::new(TableId::new(id), &PartitionKey::from("arbitrary"));
        self
    }

    pub fn with_partition_id(mut self, id: PartitionHashId) -> Self {
        self.partition_id = id;
        self
    }

    /// specify that any call should result in an error with the message
    /// specified
    pub fn with_error(mut self, error_message: impl Into<String>) -> Self {
        self.saved_error = Some(error_message.into());
        self
    }

    /// Checks the saved error, and returns it if any, otherwise returns OK
    fn check_error(&self) -> Result<(), DataFusionError> {
        if let Some(message) = self.saved_error.as_ref() {
            Err(DataFusionError::External(message.clone().into()))
        } else {
            Ok(())
        }
    }

    /// Set the `may_contain_pk_duplicates` flag
    pub fn with_may_contain_pk_duplicates(mut self, v: bool) -> Self {
        self.may_contain_pk_duplicates = v;
        self
    }

    /// Register a tag column with the test chunk with default stats
    pub fn with_tag_column(self, column_name: impl Into<String>) -> Self {
        let column_name = column_name.into();

        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new().tag(&column_name).build().unwrap();

        self.add_schema_to_table(new_column_schema, None)
    }

    /// Register a tag column with stats with the test chunk
    pub fn with_tag_column_with_stats(
        self,
        column_name: impl Into<String>,
        min: Option<&str>,
        max: Option<&str>,
    ) -> Self {
        self.with_tag_column_with_full_stats(column_name, min, max, None)
    }

    /// Register a tag column with stats with the test chunk
    pub fn with_tag_column_with_full_stats(
        self,
        column_name: impl Into<String>,
        min: Option<&str>,
        max: Option<&str>,
        distinct_count: Option<NonZeroU64>,
    ) -> Self {
        let null_count = 0;
        self.with_tag_column_with_nulls_and_full_stats(
            column_name,
            min,
            max,
            distinct_count,
            null_count,
        )
    }

    pub fn with_row_count(mut self, count: u64) -> Self {
        self.num_rows = Some(count);
        self
    }

    /// Register a tag column with stats with the test chunk
    pub fn with_tag_column_with_nulls_and_full_stats(
        self,
        column_name: impl Into<String>,
        min: Option<&str>,
        max: Option<&str>,
        distinct_count: Option<NonZeroU64>,
        null_count: u64,
    ) -> Self {
        let column_name = column_name.into();

        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new().tag(&column_name).build().unwrap();

        // Construct stats
        let stats = ColumnStatistics {
            null_count: Precision::Exact(null_count as usize),
            max_value: option_to_precision(max.map(|v| {
                ScalarValue::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(ScalarValue::Utf8(Some(v.to_owned()))),
                )
            })),
            min_value: option_to_precision(min.map(|v| {
                ScalarValue::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(ScalarValue::Utf8(Some(v.to_owned()))),
                )
            })),
            distinct_count: option_to_precision(distinct_count.map(|c| c.get() as usize)),
            sum_value: Precision::Absent,
        };

        self.add_schema_to_table(new_column_schema, Some(stats))
    }

    /// Register a timestamp column with the test chunk with default stats
    pub fn with_time_column(self) -> Self {
        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new().timestamp().build().unwrap();

        self.add_schema_to_table(new_column_schema, None)
    }

    /// Register a timestamp column with the test chunk
    pub fn with_time_column_with_stats(self, min: Option<i64>, max: Option<i64>) -> Self {
        self.with_time_column_with_full_stats(min, max, None)
    }

    /// Register a timestamp column with full stats with the test chunk
    pub fn with_time_column_with_full_stats(
        self,
        min: Option<i64>,
        max: Option<i64>,
        distinct_count: Option<NonZeroU64>,
    ) -> Self {
        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new().timestamp().build().unwrap();
        let null_count = 0;

        // Construct stats
        let stats = ColumnStatistics {
            null_count: Precision::Exact(null_count as usize),
            max_value: option_to_precision(max.map(timestamptz_nano)),
            min_value: option_to_precision(min.map(timestamptz_nano)),
            distinct_count: option_to_precision(distinct_count.map(|c| c.get() as usize)),
            sum_value: Precision::Absent,
        };

        self.add_schema_to_table(new_column_schema, Some(stats))
    }

    pub fn with_timestamp_min_max(mut self, min: i64, max: i64) -> Self {
        let stats = self
            .column_stats
            .get_mut(TIME_COLUMN_NAME)
            .expect("stats in sync w/ columns");

        stats.min_value = Precision::Exact(timestamptz_nano(min));
        stats.max_value = Precision::Exact(timestamptz_nano(max));

        self
    }

    impl_with_column!(with_i64_field_column, Int64);
    impl_with_column_with_stats!(with_i64_field_column_with_stats, Int64, i64, I64);

    impl_with_column!(with_u64_column, UInt64);
    impl_with_column_with_stats!(with_u64_field_column_with_stats, UInt64, u64, U64);

    impl_with_column!(with_f64_field_column, Float64);
    impl_with_column_with_stats!(with_f64_field_column_with_stats, Float64, f64, F64);

    impl_with_column!(with_bool_field_column, Boolean);
    impl_with_column_with_stats!(with_bool_field_column_with_stats, Boolean, bool, Bool);

    /// Register a string field column with the test chunk
    pub fn with_string_field_column_with_stats(
        self,
        column_name: impl Into<String>,
        min: Option<&str>,
        max: Option<&str>,
    ) -> Self {
        let column_name = column_name.into();

        // make a new schema with the specified column and
        // merge it in to any existing schema
        let new_column_schema = SchemaBuilder::new()
            .field(&column_name, DataType::Utf8)
            .unwrap()
            .build()
            .unwrap();

        // Construct stats
        let stats = ColumnStatistics {
            null_count: Precision::Absent,
            max_value: option_to_precision(max.map(ScalarValue::from)),
            min_value: option_to_precision(min.map(ScalarValue::from)),
            distinct_count: Precision::Absent,
            sum_value: Precision::Absent,
        };

        self.add_schema_to_table(new_column_schema, Some(stats))
    }

    /// Adds the specified schema and optionally a column summary containing optional stats.
    /// If `add_column_summary` is false, `stats` is ignored. If `add_column_summary` is true but
    /// `stats` is `None`, default stats will be added to the column summary.
    fn add_schema_to_table(
        mut self,
        new_column_schema: Schema,
        input_stats: Option<ColumnStatistics>,
    ) -> Self {
        let mut merger = SchemaMerger::new();
        merger = merger.merge(&new_column_schema).unwrap();
        merger = merger.merge(&self.schema).expect("merging was successful");
        self.schema = merger.build();

        for f in new_column_schema.inner().fields() {
            self.column_stats.insert(
                f.name().clone(),
                input_stats.as_ref().cloned().unwrap_or_default(),
            );
        }

        self
    }

    /// Prepares this chunk to return a specific record batch with one
    /// row of non null data.
    /// tag: MA
    pub fn with_one_row_of_data(mut self) -> Self {
        // create arrays
        let columns = self
            .schema
            .iter()
            .map(|(_influxdb_column_type, field)| match field.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(vec![1000])) as ArrayRef,
                DataType::UInt64 => Arc::new(UInt64Array::from(vec![1000])) as ArrayRef,
                DataType::Utf8 => Arc::new(StringArray::from(vec!["MA"])) as ArrayRef,
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => Arc::new(
                    TimestampNanosecondArray::from(vec![1000]).with_timezone_opt(tz.clone()),
                ) as ArrayRef,
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    let dict: DictionaryArray<Int32Type> = vec!["MA"].into_iter().collect();
                    Arc::new(dict) as ArrayRef
                }
                DataType::Float64 => Arc::new(Float64Array::from(vec![99.5])) as ArrayRef,
                DataType::Boolean => Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
                _ => unimplemented!(
                    "Unimplemented data type for test database: {:?}",
                    field.data_type()
                ),
            })
            .collect::<Vec<_>>();

        let batch =
            RecordBatch::try_new(self.schema.as_arrow(), columns).expect("made record batch");
        if !self.quiet {
            println!("TestChunk batch data: {batch:#?}");
        }

        self.push_record_batch(batch);
        self
    }

    /// Prepares this chunk to return a specific record batch with a single tag, field and timestamp like
    pub fn with_one_row_of_specific_data(
        mut self,
        tag_val: impl AsRef<str>,
        field_val: i64,
        ts_val: i64,
    ) -> Self {
        // create arrays
        let columns = self
            .schema
            .iter()
            .map(|(_influxdb_column_type, field)| match field.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(vec![field_val])) as ArrayRef,
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => Arc::new(
                    TimestampNanosecondArray::from(vec![ts_val]).with_timezone_opt(tz.clone()),
                ) as ArrayRef,
                DataType::Dictionary(key, value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
                {
                    let dict: DictionaryArray<Int32Type> =
                        vec![tag_val.as_ref()].into_iter().collect();
                    Arc::new(dict) as ArrayRef
                }
                _ => unimplemented!(
                    "Unimplemented data type for test database: {:?}",
                    field.data_type()
                ),
            })
            .collect::<Vec<_>>();

        let batch =
            RecordBatch::try_new(self.schema.as_arrow(), columns).expect("made record batch");
        if !self.quiet {
            println!("TestChunk batch data: {batch:#?}");
        }

        self.push_record_batch(batch);
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
                DataType::UInt64 => Arc::new(UInt64Array::from(vec![1000, 10, 70])) as ArrayRef,
                DataType::Utf8 => match field.name().as_str() {
                    "tag1" => Arc::new(StringArray::from(vec!["WA", "VT", "UT"])) as ArrayRef,
                    "tag2" => Arc::new(StringArray::from(vec!["SC", "NC", "RI"])) as ArrayRef,
                    _ => Arc::new(StringArray::from(vec!["TX", "PR", "OR"])) as ArrayRef,
                },
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => Arc::new(
                    TimestampNanosecondArray::from(vec![8000, 10000, 20000])
                        .with_timezone_opt(tz.clone()),
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
            RecordBatch::try_new(self.schema.as_arrow(), columns).expect("made record batch");

        self.push_record_batch(batch);
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
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => Arc::new(
                    TimestampNanosecondArray::from(vec![28000, 210000, 220000, 210000])
                        .with_timezone_opt(tz.clone()),
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
            RecordBatch::try_new(self.schema.as_arrow(), columns).expect("made record batch");

        self.push_record_batch(batch);
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
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => Arc::new(
                    TimestampNanosecondArray::from(vec![1000, 7000, 100, 50, 5000])
                        .with_timezone_opt(tz.clone()),
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
            RecordBatch::try_new(self.schema.as_arrow(), columns).expect("made record batch");

        self.push_record_batch(batch);
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
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => Arc::new(
                    TimestampNanosecondArray::from(vec![
                        1000, 7000, 100, 50, 5, 2000, 7000, 500, 50, 5,
                    ])
                    .with_timezone_opt(tz.clone()),
                ) as ArrayRef,
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
            RecordBatch::try_new(self.schema.as_arrow(), columns).expect("made record batch");

        self.push_record_batch(batch);
        self
    }

    /// Set the sort key for this chunk
    pub fn with_sort_key(self, sort_key: SortKey) -> Self {
        Self {
            sort_key: Some(sort_key),
            ..self
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

impl fmt::Display for TestChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.table_name())
    }
}

impl QueryChunk for TestChunk {
    fn stats(&self) -> Arc<DataFusionStatistics> {
        self.check_error().unwrap();

        Arc::new(DataFusionStatistics {
            num_rows: option_to_precision(self.num_rows.map(|c| c as usize)),
            total_byte_size: Precision::Absent,
            column_statistics: self
                .schema
                .inner()
                .fields()
                .iter()
                .map(|f| self.column_stats.get(f.name()).cloned().unwrap_or_default())
                .collect(),
        })
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn partition_id(&self) -> &PartitionHashId {
        &self.partition_id
    }

    fn sort_key(&self) -> Option<&SortKey> {
        self.sort_key.as_ref()
    }

    fn id(&self) -> ChunkId {
        self.id
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        self.may_contain_pk_duplicates
    }

    fn data(&self) -> QueryChunkData {
        self.check_error().unwrap();

        match &self.table_data {
            TestChunkData::RecordBatches(batches) => {
                QueryChunkData::in_mem(batches.clone(), Arc::clone(self.schema.inner()))
            }
            TestChunkData::Parquet(input) => QueryChunkData::Parquet(input.clone()),
        }
    }

    fn chunk_type(&self) -> &str {
        "Test Chunk"
    }

    fn order(&self) -> ChunkOrder {
        self.order
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Return the raw data from the list of chunks
pub async fn raw_data(chunks: &[Arc<dyn QueryChunk>]) -> Vec<RecordBatch> {
    let ctx = IOxSessionContext::with_testing();
    let mut batches = vec![];
    for c in chunks {
        batches.append(&mut c.data().read_to_batches(c.schema(), ctx.inner()).await);
    }
    batches
}

pub fn format_logical_plan(plan: &LogicalPlan) -> Vec<String> {
    format_lines(&plan.display_indent().to_string())
}

pub fn format_execution_plan(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    format_lines(&displayable(plan.as_ref()).indent(false).to_string())
}

fn format_lines(s: &str) -> Vec<String> {
    s.trim()
        .split('\n')
        .map(|s| {
            // Always add a leading space to ensure tha all lines in the YAML insta snapshots are quoted, otherwise the
            // alignment gets messed up and the snapshot would be hard to read.
            format!(" {s}")
        })
        .collect()
}

/// crate test utils
#[cfg(test)]
pub(crate) mod test_utils {
    use crate::provider::DeduplicateExec;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::SchemaRef;
    use datafusion::catalog::memory::DataSourceExec;
    use datafusion::{
        common::{Statistics, stats::Precision},
        datasource::{
            listing::PartitionedFile,
            physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource},
        },
        physical_expr::LexOrdering,
        physical_plan::{
            ExecutionPlan, Partitioning, PhysicalExpr,
            coalesce_batches::CoalesceBatchesExec,
            filter::FilterExec,
            joins::CrossJoinExec,
            limit::LocalLimitExec,
            projection::ProjectionExec,
            repartition::RepartitionExec,
            sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
            union::UnionExec,
        },
    };
    use datafusion::{
        datasource::object_store::ObjectStoreUrl, physical_plan::ColumnStatistics,
        scalar::ScalarValue,
    };
    use datafusion_util::config::table_parquet_options;
    use itertools::Itertools;

    use std::{
        fmt::{self, Display, Formatter},
        sync::Arc,
    };

    /// Return a schema with a single column `a` of type int64.
    pub fn single_column_schema() -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "a",
            DataType::Int64,
            true,
        )]))
    }

    #[derive(Debug, Copy, Clone)]
    pub struct SortKeyRange {
        pub min: Option<i32>,
        pub max: Option<i32>,
        pub null_count: usize,
    }

    impl From<SortKeyRange> for ColumnStatistics {
        fn from(val: SortKeyRange) -> Self {
            Self {
                null_count: Precision::Exact(val.null_count),
                max_value: Precision::Exact(ScalarValue::Int32(val.max)),
                min_value: Precision::Exact(ScalarValue::Int32(val.min)),
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
            }
        }
    }

    impl Display for SortKeyRange {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "({:?})->({:?})", self.min, self.max)?;
            if self.null_count > 0 {
                write!(f, " null_count={}", self.null_count)?;
            }
            Ok(())
        }
    }

    /// Create a single parquet, with a given ordering and using the statistics from the [`SortKeyRange`]
    pub fn data_source_exec_parquet_with_sort_with_statistics(
        output_ordering: Vec<LexOrdering>,
        key_ranges: &[&SortKeyRange],
    ) -> Arc<dyn ExecutionPlan> {
        data_source_exec_parquet_with_sort_with_statistics_and_schema(
            &single_column_schema(),
            output_ordering,
            key_ranges,
        )
    }

    pub type RangeForMultipleColumns = Vec<SortKeyRange>; // vec![col0, col1, col2]
    pub struct PartitionedFilesAndRanges {
        pub per_file: Vec<RangeForMultipleColumns>,
    }

    /// Create a single `DataSourceExec`, with multiple parquet, with a given ordering and using the statistics from the [`SortKeyRange`].
    /// Assumes a single column schema.
    pub fn data_source_exec_parquet_with_sort_with_statistics_and_schema(
        schema: &SchemaRef,
        output_ordering: Vec<LexOrdering>,
        key_ranges_for_single_column_multiple_files: &[&SortKeyRange], // VecPerFile<KeyForSingleColumn>
    ) -> Arc<dyn ExecutionPlan> {
        let per_file_ranges = PartitionedFilesAndRanges {
            per_file: key_ranges_for_single_column_multiple_files
                .iter()
                .map(|single_col_range_per_file| vec![**single_col_range_per_file])
                .collect_vec(),
        };

        let file_scan_config =
            file_scan_config_builder(schema, output_ordering, per_file_ranges).build();

        DataSourceExec::from_data_source(file_scan_config)
    }

    /// Create a file scan config with a given file [`SchemaRef`], ordering,
    /// and [`ColumnStatistics`] for multiple columns.
    pub fn file_scan_config_builder(
        schema: &SchemaRef,
        output_ordering: Vec<LexOrdering>,
        multiple_column_key_ranges_per_file: PartitionedFilesAndRanges,
    ) -> FileScanConfigBuilder {
        let PartitionedFilesAndRanges { per_file } = multiple_column_key_ranges_per_file;
        let mut statistics = Statistics::new_unknown(schema);
        let mut file_groups = Vec::with_capacity(per_file.len());

        // cummulative statistics for the entire `DataSourceExec`, per sort key
        let num_sort_keys = per_file[0].len();
        let mut cum_null_count = vec![0; num_sort_keys];
        let mut cum_min = vec![None; num_sort_keys];
        let mut cum_max = vec![None; num_sort_keys];

        // iterate thru files, creating the PartitionedFile and the associated statistics
        for (file_idx, multiple_column_key_ranges_per_file) in per_file.into_iter().enumerate() {
            // gather stats for all columns
            let mut per_file_col_stats = Vec::with_capacity(num_sort_keys);
            for (col_idx, key_range) in multiple_column_key_ranges_per_file.into_iter().enumerate()
            {
                let SortKeyRange {
                    min,
                    max,
                    null_count,
                } = key_range;

                // update per file stats
                per_file_col_stats.push(ColumnStatistics {
                    null_count: Precision::Exact(null_count),
                    min_value: Precision::Exact(ScalarValue::Int32(min)),
                    max_value: Precision::Exact(ScalarValue::Int32(max)),
                    ..Default::default()
                });

                // update cummulative statistics for entire `DataSourceExec`
                cum_min[col_idx] = match (cum_min[col_idx], min) {
                    (None, x) => x,
                    (x, None) => x,
                    (Some(a), Some(b)) => Some(std::cmp::min(a, b)),
                };
                cum_max[col_idx] = match (cum_max[col_idx], max) {
                    (None, x) => x,
                    (x, None) => x,
                    (Some(a), Some(b)) => Some(std::cmp::max(a, b)),
                };
                cum_null_count[col_idx] += null_count;
            }

            // Create single file with statistics.
            let mut file = PartitionedFile::new(format!("{file_idx}.parquet"), 100);
            file.statistics = Some(Arc::new(Statistics {
                num_rows: Precision::Absent,
                total_byte_size: Precision::Absent,
                column_statistics: per_file_col_stats,
            }));
            file_groups.push(FileGroup::new(vec![file]));
        }

        // add stats, for the whole `DataSourceExec`, for all columns
        for col_idx in 0..num_sort_keys {
            statistics.column_statistics[col_idx] = ColumnStatistics {
                null_count: Precision::Exact(cum_null_count[col_idx]),
                min_value: Precision::Exact(ScalarValue::Int32(cum_min[col_idx])),
                max_value: Precision::Exact(ScalarValue::Int32(cum_max[col_idx])),
                ..Default::default()
            };
        }

        FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test:///").unwrap(),
            Arc::clone(schema),
            Arc::new(ParquetSource::new(table_parquet_options())),
        )
        .with_file_groups(file_groups)
        .with_output_ordering(output_ordering)
        .with_statistics(statistics)
    }

    pub fn union_exec(input: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
        Arc::new(UnionExec::new(input))
    }

    pub fn sort_exec(
        sort_exprs: &LexOrdering,
        input: &Arc<dyn ExecutionPlan>,
        preserve_partitioning: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let new_sort = SortExec::new(sort_exprs.clone(), Arc::clone(input))
            .with_preserve_partitioning(preserve_partitioning);
        Arc::new(new_sort)
    }

    pub fn dedupe_exec(
        input: &Arc<dyn ExecutionPlan>,
        sort_exprs: &LexOrdering,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(DeduplicateExec::new(
            Arc::clone(input),
            sort_exprs.clone(),
            false,
        ))
    }

    pub fn coalesce_exec(input: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(CoalesceBatchesExec::new(Arc::clone(input), 10))
    }

    pub fn filter_exec(
        input: &Arc<dyn ExecutionPlan>,
        predicate: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(FilterExec::try_new(predicate, Arc::clone(input)).unwrap())
    }

    pub fn limit_exec(input: &Arc<dyn ExecutionPlan>, fetch: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(LocalLimitExec::new(Arc::clone(input), fetch))
    }

    pub fn proj_exec(
        input: &Arc<dyn ExecutionPlan>,
        projects: Vec<(Arc<dyn PhysicalExpr>, String)>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(ProjectionExec::try_new(projects, Arc::clone(input)).unwrap())
    }

    pub fn spm_exec(
        input: &Arc<dyn ExecutionPlan>,
        sort_exprs: &LexOrdering,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(SortPreservingMergeExec::new(
            sort_exprs.clone(),
            Arc::clone(input),
        ))
    }

    pub fn repartition_exec(
        input: &Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(RepartitionExec::try_new(Arc::clone(input), partitioning).unwrap())
    }

    pub fn crossjoin_exec(
        left: &Arc<dyn ExecutionPlan>,
        right: &Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(CrossJoinExec::new(Arc::clone(left), Arc::clone(right)))
    }
}

#[cfg(test)]
mod meta_test {
    use super::*;

    #[test]
    fn test_chunk_stats() {
        let chunk = TestChunk::new("table")
            .with_row_count(42)
            .with_bool_field_column_with_stats("field_b", Some(false), Some(true))
            .with_f64_field_column_with_stats("field_f", Some(1.0), Some(2.0))
            .with_u64_field_column_with_stats("field_u", Some(1), Some(2))
            .with_i64_field_column_with_stats("field_i", Some(1), Some(2))
            .with_string_field_column_with_stats("field_s", Some("a"), Some("b"))
            .with_tag_column_with_stats("tag", Some("a"), Some("b"))
            .with_time_column_with_stats(Some(1), Some(3));

        insta::assert_debug_snapshot!(chunk.stats(), @r#"
        Statistics {
            num_rows: Exact(42),
            total_byte_size: Absent,
            column_statistics: [
                ColumnStatistics {
                    null_count: Absent,
                    max_value: Exact(Boolean(true)),
                    min_value: Exact(Boolean(false)),
                    sum_value: Absent,
                    distinct_count: Absent,
                },
                ColumnStatistics {
                    null_count: Absent,
                    max_value: Exact(Float64(2)),
                    min_value: Exact(Float64(1)),
                    sum_value: Absent,
                    distinct_count: Absent,
                },
                ColumnStatistics {
                    null_count: Absent,
                    max_value: Exact(Int64(2)),
                    min_value: Exact(Int64(1)),
                    sum_value: Absent,
                    distinct_count: Absent,
                },
                ColumnStatistics {
                    null_count: Absent,
                    max_value: Exact(Utf8("b")),
                    min_value: Exact(Utf8("a")),
                    sum_value: Absent,
                    distinct_count: Absent,
                },
                ColumnStatistics {
                    null_count: Absent,
                    max_value: Exact(UInt64(2)),
                    min_value: Exact(UInt64(1)),
                    sum_value: Absent,
                    distinct_count: Absent,
                },
                ColumnStatistics {
                    null_count: Exact(0),
                    max_value: Exact(Dictionary(Int32, Utf8("b"))),
                    min_value: Exact(Dictionary(Int32, Utf8("a"))),
                    sum_value: Absent,
                    distinct_count: Absent,
                },
                ColumnStatistics {
                    null_count: Exact(0),
                    max_value: Exact(TimestampNanosecond(3, None)),
                    min_value: Exact(TimestampNanosecond(1, None)),
                    sum_value: Absent,
                    distinct_count: Absent,
                },
            ],
        }
        "#);
    }
}
