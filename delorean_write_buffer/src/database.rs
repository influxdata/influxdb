use delorean_generated_types::wal as wb;
use delorean_line_parser::{FieldValue, ParsedLine};
use delorean_storage_interface::{Database, DatabaseStore, TimestampRange};
use delorean_wal::{Entry as WalEntry, Result as WalResult, WalBuilder};
use delorean_wal_writer::{start_wal_sync_task, Error as WalWriterError, WalDetails};

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::TryFrom;
use std::fs;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder},
    datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use chrono::{offset::TimeZone, Utc};
use datafusion::{
    datasource::MemTable, error::ExecutionError, execution::context::ExecutionContext,
};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use sqlparser::{
    ast::{SetExpr, Statement, TableFactor},
    dialect::GenericDialect,
    parser::Parser,
};
use string_interner::{
    backend::StringBackend, DefaultHashBuilder, DefaultSymbol, StringInterner, Symbol,
};
use tokio::sync::RwLock;
use tracing::info;

const TIME_COLUMN_NAME: &str = "time";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error reading from dir {:?}: {}", dir, source))]
    ReadError {
        dir: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Error reading metadata: {}", source))]
    ReadMetadataError { source: std::io::Error },

    #[snafu(display("Dir {:?} invalid for DB", dir))]
    OpenDb { dir: PathBuf },

    #[snafu(display("Error opening WAL for database {}: {}", database, source))]
    OpeningWal {
        database: String,
        source: WalWriterError,
    },

    #[snafu(display("Error writing to WAL for database {}: {}", database, source))]
    WritingWal {
        database: String,
        source: WalWriterError,
    },

    #[snafu(display("Error opening WAL for database {}: {}", database, source))]
    LoadingWal {
        database: String,
        source: delorean_wal::Error,
    },

    #[snafu(display("Error recovering WAL for database {}: {}", database, source))]
    WalRecoverError {
        database: String,
        source: RestorationError,
    },

    #[snafu(display(
        "Error recovering WAL for partition {} on table {}",
        partition_id,
        table_id
    ))]
    WalPartitionError { partition_id: u32, table_id: u32 },

    #[snafu(display("Error recovering write from WAL, column id {} not found", column_id))]
    WalColumnError { column_id: u16 },

    #[snafu(display("Error creating db dir for {}: {}", database, err))]
    CreatingWalDir {
        database: String,
        err: std::io::Error,
    },

    #[snafu(display(
        "Schema mismatch: for column {}: can't insert {} into column with type {}",
        column_id,
        inserted_value_type,
        existing_column_type
    ))]
    SchemaMismatch {
        column_id: usize,
        existing_column_type: String,
        inserted_value_type: String,
    },

    #[snafu(display("Database {} doesn't exist", database))]
    DatabaseNotFound { database: String },

    #[snafu(display("Partition {} is full", partition_id))]
    PartitionFull { partition_id: String },

    #[snafu(display("Table {} not found in dictionary", table))]
    TableNotFoundInDictionary { table: String },

    #[snafu(display("Table {} not found in partition {}", table, partition))]
    TableNotFoundInPartition { table: u32, partition: u32 },

    #[snafu(display("Internal Error: Column {} not found", column_id))]
    InternaColumnNotFound { column_id: u32 },

    #[snafu(display("Unexpected insert error"))]
    InsertError,

    #[snafu(display("arrow conversion error"))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display("dictionary lookup error on id {}", id))]
    DictionaryIdLookupError { id: u32 },

    #[snafu(display("dictionary lookup error for value {}", value))]
    DictionaryValueLookupError { value: String },

    /// This error "should not happen" and is not expected. However,
    /// for robustness, rather than panic! we handle it gracefully
    #[snafu(display("Internal Error '{}'", description))]
    InternalError { description: String },

    /// This error "should not happen" and is not expected. However,
    /// for robustness, rather than panic! we handle it gracefully
    #[snafu(display("Internal Error '{}': {}", description, source))]
    InternalUnderlyingError {
        description: String,
        source: Box<Error>,
    },

    #[snafu(display("id conversion error"))]
    IdConversionError { source: std::num::TryFromIntError },

    #[snafu(display("Invalid sql query: {} : {}", query, source))]
    InvalidSqlQuery {
        query: String,
        source: sqlparser::parser::ParserError,
    },

    #[snafu(display("error executing query {}: {}", query, source))]
    QueryError {
        query: String,
        source: ExecutionError,
    },

    #[snafu(display("Unsupported SQL statement in query {}: {}", query, statement))]
    UnsupportedStatement {
        query: String,
        statement: Box<Statement>,
    },

    #[snafu(display("query error {} on query {}", message, query))]
    GenericQueryError { message: String, query: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

trait ToInternalError<T>: Sized {
    /// Converts e into an internal error with human readable
    /// description, as opposed to panic!
    fn to_internal(self, description: &str) -> Result<T>;
}

// Add a "to_internal" method to Result to transform errrors to
// something interpretable
impl<T> ToInternalError<T> for Result<T> {
    fn to_internal(self, description: &str) -> Self {
        self.map_err(|e| Error::InternalUnderlyingError {
            description: description.into(),
            source: Box::new(e),
        })
    }
}

#[derive(Debug)]
pub struct WriteBufferDatabases {
    databases: RwLock<BTreeMap<String, Arc<Db>>>,
    base_dir: PathBuf,
}

impl WriteBufferDatabases {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            databases: RwLock::new(BTreeMap::new()),
            base_dir: base_dir.into(),
        }
    }

    /// wal_dirs will traverse the directories from the service base directory and return
    /// the directories that contain WALs for databases, which can be used to restore those DBs.
    pub fn wal_dirs(&self) -> Result<Vec<PathBuf>> {
        let entries = fs::read_dir(&self.base_dir).context(ReadError {
            dir: &self.base_dir,
        })?;

        let mut dirs = vec![];

        for entry in entries {
            let entry = entry.context(ReadError {
                dir: &self.base_dir,
            })?;

            let meta = entry.metadata().context(ReadMetadataError {})?;
            if meta.is_dir() {
                if let Some(p) = entry.path().iter().last() {
                    if let Some(s) = p.to_str() {
                        if !s.starts_with('.') {
                            dirs.push(entry.path());
                        };
                    }
                };
            };
        }

        Ok(dirs)
    }

    pub async fn add_db(&self, db: Db) {
        let mut databases = self.databases.write().await;
        databases.insert(db.name.clone(), Arc::new(db));
    }
}

#[async_trait]
impl DatabaseStore for WriteBufferDatabases {
    type Database = Db;
    type Error = Error;

    async fn db(&self, name: &str) -> Option<Arc<Self::Database>> {
        let databases = self.databases.read().await;

        databases.get(name).cloned()
    }

    async fn db_or_create(&self, name: &str) -> Result<Arc<Self::Database>, Self::Error> {
        // get it through a read lock first if we can
        {
            let databases = self.databases.read().await;

            if let Some(db) = databases.get(name) {
                return Ok(db.clone());
            }
        }

        // database doesn't exist yet so acquire the write lock and get or insert
        let mut databases = self.databases.write().await;

        // make sure it didn't get inserted by someone else while we were waiting for the write lock
        if let Some(db) = databases.get(name) {
            return Ok(db.clone());
        }

        let db = Db::try_with_wal(name, &mut self.base_dir.clone()).await?;
        let db = Arc::new(db);
        databases.insert(name.to_string(), db.clone());

        Ok(db)
    }
}

#[derive(Debug)]
pub struct Db {
    name: String,
    // TODO: partitions need to be wrapped in an Arc if they're going to be used without this lock
    partitions: RwLock<Vec<Partition>>,
    next_partition_id: AtomicU32,
    wal_details: Option<WalDetails>,
    dir: PathBuf,
}

impl Db {
    /// Create a new DB that will create and use the Write Ahead Log
    /// (WAL) directory `wal_dir`
    pub async fn try_with_wal(name: &str, wal_dir: &mut PathBuf) -> Result<Self> {
        wal_dir.push(&name);
        if let Err(e) = std::fs::create_dir(wal_dir.clone()) {
            match e.kind() {
                ErrorKind::AlreadyExists => (),
                _ => {
                    return CreatingWalDir {
                        database: name,
                        err: e,
                    }
                    .fail()
                }
            }
        }
        let dir = wal_dir.clone();
        let wal_builder = WalBuilder::new(wal_dir.clone());
        let wal_details = start_wal_sync_task(wal_builder)
            .await
            .context(OpeningWal { database: name })?;
        wal_details
            .write_metadata()
            .await
            .context(OpeningWal { database: name })?;

        Ok(Self {
            name: name.to_string(),
            dir,
            partitions: RwLock::new(vec![]),
            next_partition_id: AtomicU32::new(1),
            wal_details: Some(wal_details),
        })
    }

    /// Create a new DB and initially restore pre-existing data in the
    /// Write Ahead Log (WAL) directory `wal_dir`
    pub async fn restore_from_wal(wal_dir: PathBuf) -> Result<Self> {
        let now = std::time::Instant::now();
        let name = wal_dir
            .iter()
            .last()
            .with_context(|| OpenDb { dir: &wal_dir })?
            .to_str()
            .with_context(|| OpenDb { dir: &wal_dir })?
            .to_string();

        let wal_builder = WalBuilder::new(wal_dir.clone());
        let wal_details = start_wal_sync_task(wal_builder.clone())
            .await
            .context(OpeningWal { database: &name })?;

        // TODO: check wal metadata format
        let entries = wal_builder
            .entries()
            .context(LoadingWal { database: &name })?;

        let (partitions, next_partition_id, stats) =
            restore_partitions_from_wal(entries).context(WalRecoverError { database: &name })?;

        let elapsed = now.elapsed();
        info!(
            "{} database loaded {} rows in {:?} with {} dictionary adds in {} tables",
            &name,
            stats.row_count,
            elapsed,
            stats.dict_values,
            stats.tables.len(),
        );

        info!(
            "{} database partition count: {}, next_id {}",
            &name,
            partitions.len(),
            next_partition_id
        );

        Ok(Self {
            name,
            dir: wal_dir,
            partitions: RwLock::new(partitions),
            next_partition_id: AtomicU32::new(next_partition_id + 1),
            wal_details: Some(wal_details),
        })
    }
}

#[derive(Debug, Snafu)]
pub enum RestorationError {
    #[snafu(display("Could not read WAL entry: {}", source))]
    WalEntryRead { source: delorean_wal::Error },

    #[snafu(display("Partition {} not found", partition))]
    PartitionNotFound { partition: u32 },

    #[snafu(display("Table {} not found", table))]
    TableNotFound { table: u32 },

    #[snafu(display("Column {} not found", column))]
    ColumnNotFound { column: u32 },

    #[snafu(display(
        "Column {} said it was type {} but extracting a value of that type failed",
        column,
        expected
    ))]
    WalValueTypeMismatch { column: u32, expected: String },

    #[snafu(display(
        "Column type mismatch for column {}: can't insert {} into column with type {}",
        column,
        inserted_value_type,
        existing_column_type
    ))]
    ColumnTypeMismatch {
        column: usize,
        existing_column_type: String,
        inserted_value_type: String,
    },
}

/// Given a set of WAL entries, restore them into a set of Partitions.
pub fn restore_partitions_from_wal(
    wal_entries: impl Iterator<Item = WalResult<WalEntry>>,
) -> Result<(Vec<Partition>, u32, RestorationStats), RestorationError> {
    let mut stats = RestorationStats::default();
    let mut partitions = HashMap::new();
    let mut next_partition_id = 0;

    for wal_entry in wal_entries {
        let wal_entry = wal_entry.context(WalEntryRead)?;
        let bytes = wal_entry.as_data();

        let batch = flatbuffers::get_root::<wb::WriteBufferBatch<'_>>(&bytes);

        if let Some(entries) = batch.entries() {
            for entry in entries {
                if let Some(po) = entry.partition_open() {
                    let id = po.id();
                    if id > next_partition_id {
                        next_partition_id = id;
                    }
                    partitions
                        .entry(id)
                        .or_insert_with(|| Partition::new(id, po.name().unwrap().to_string()));
                } else if let Some(_ps) = entry.partition_snapshot_started() {
                    todo!("handle partition snapshot");
                } else if let Some(_pf) = entry.partition_snapshot_finished() {
                    todo!("handle partition snapshot finished")
                } else if let Some(da) = entry.dictionary_add() {
                    let p = partitions
                        .get_mut(&da.partition_id())
                        .context(PartitionNotFound {
                            partition: da.partition_id(),
                        })?;
                    stats.dict_values += 1;
                    p.intern_new_dict_entry(da.value().unwrap());
                } else if let Some(sa) = entry.schema_append() {
                    let tid = sa.table_id();
                    stats.tables.insert(tid);
                    let p = partitions
                        .get_mut(&sa.partition_id())
                        .context(PartitionNotFound {
                            partition: sa.partition_id(),
                        })?;
                    p.append_wal_schema(sa.table_id(), sa.column_id(), sa.column_type());
                } else if let Some(row) = entry.write() {
                    let p = partitions
                        .get_mut(&row.partition_id())
                        .context(PartitionNotFound {
                            partition: row.partition_id(),
                        })?;
                    stats.row_count += 1;
                    p.add_wal_row(row.table_id(), &row.values().unwrap())?;
                }
            }
        }
    }
    let partitions: Vec<_> = partitions.into_iter().map(|(_, p)| p).collect();

    Ok((partitions, next_partition_id, stats))
}

#[derive(Default, Debug)]
pub struct RestorationStats {
    row_count: usize,
    dict_values: usize,
    tables: HashSet<u32>,
}

#[async_trait]
impl Database for Db {
    type Error = Error;

    // TODO: writes lines creates a column named "time" for the timestmap data. If
    //       we keep this we need to validate that no tag or field has the same name.
    async fn write_lines(&self, lines: &[ParsedLine<'_>]) -> Result<(), Self::Error> {
        let mut partitions = self.partitions.write().await;

        let mut builder = self.wal_details.as_ref().map(|_| WalEntryBuilder::new());

        // TODO: rollback writes to partitions on validation failures
        for line in lines {
            let key = self.partition_key(line);
            match partitions.iter_mut().find(|p| p.should_write(&key)) {
                Some(p) => p.write_line(line, &mut builder)?,
                None => {
                    let id = self.next_partition_id.fetch_add(1, Ordering::Relaxed);

                    if let Some(builder) = &mut builder {
                        builder.add_partition_open(id, &key);
                    }

                    let mut p = Partition::new(id, key);
                    p.write_line(line, &mut builder)?;
                    partitions.push(p)
                }
            }
        }

        if let Some(wal) = &self.wal_details {
            let data = builder
                .expect("Where there's wal_details, there's a builder")
                .data();

            wal.write_and_sync(data).await.context(WritingWal {
                database: self.name.clone(),
            })?;
        }

        Ok(())
    }

    async fn table_names(
        &self,
        range: Option<TimestampRange>,
    ) -> Result<Arc<BTreeSet<String>>, Self::Error> {
        // TODO: Cache this information to avoid creating this each time
        let partitions = self.partitions.read().await;
        let mut table_names = BTreeSet::new();
        for partition in partitions.iter() {
            for (table_name_symbol, table) in &partition.tables {
                let mut add_table_name = true;

                if let Some(range) = range {
                    let time_column_id = partition
                        .lookup_value(TIME_COLUMN_NAME)
                        .to_internal("looking up time column name")?;

                    // if timestamps fall outside the min/max of the
                    // time column, then don't add table name
                    if !table
                        .column(time_column_id)?
                        .has_i64_range(range.start, range.end)?
                    {
                        add_table_name = false;
                    }
                };

                if add_table_name {
                    table_names.insert(
                        partition
                            .lookup_id(*table_name_symbol)
                            .to_internal("Looking up table name in symbol table")?
                            .into(),
                    );
                }
            }
        }
        Ok(Arc::new(table_names))
    }

    async fn table_to_arrow(
        &self,
        table_name: &str,
        _columns: &[&str],
    ) -> Result<Vec<RecordBatch>, Self::Error> {
        let partitions = self.partitions.read().await;

        partitions
            .iter()
            .map(|p| p.table_to_arrow(table_name))
            .collect::<Result<Vec<_>>>()
    }

    async fn query(&self, query: &str) -> Result<Vec<RecordBatch>, Self::Error> {
        let mut tables = vec![];

        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, query).context(InvalidSqlQuery { query })?;

        for statement in ast {
            match statement {
                Statement::Query(q) => {
                    if let SetExpr::Select(q) = q.body {
                        for item in q.from {
                            if let TableFactor::Table { name, .. } = item.relation {
                                let name = name.to_string();
                                let data = self.table_to_arrow(&name, &[]).await?;
                                tables.push(ArrowTable {
                                    name,
                                    schema: data[0].schema().clone(),
                                    data,
                                });
                            }
                        }
                    }
                }
                _ => {
                    return UnsupportedStatement {
                        query: query.to_string(),
                        statement,
                    }
                    .fail()
                }
            }
        }

        let mut ctx = ExecutionContext::new();

        for table in tables {
            let provider =
                MemTable::new(table.schema, vec![table.data]).context(QueryError { query })?;
            ctx.register_table(&table.name, Box::new(provider));
        }

        let plan = ctx
            .create_logical_plan(&query)
            .context(QueryError { query })?;
        let plan = ctx.optimize(&plan).context(QueryError { query })?;
        let plan = ctx
            .create_physical_plan(&plan, 1024 * 1024)
            .context(QueryError { query })?;

        ctx.collect(plan.as_ref()).context(QueryError { query })
    }
}

impl Db {
    // partition_key returns the partition key for the given line. The key will be the prefix of a
    // partition name (multiple partitions can exist for each key). It uses the user defined
    // partitioning rules to construct this key
    fn partition_key(&self, line: &ParsedLine<'_>) -> String {
        // TODO - wire this up to use partitioning rules, for now just partition by day
        let ts = line.timestamp.unwrap();
        let dt = Utc.timestamp_nanos(ts);
        dt.format("%Y-%m-%dT%H").to_string()
    }
}

struct ArrowTable {
    name: String,
    schema: Arc<ArrowSchema>,
    data: Vec<RecordBatch>,
}

#[derive(Debug)]
pub struct Partition {
    name: String,
    id: u32,
    /// `dictionary` maps &str -> u32. The u32s are used in place of
    /// String or str to avoid slow string operations. The same
    /// dictionary is used for table names, tag and field values
    dictionary: StringInterner<DefaultSymbol, StringBackend<DefaultSymbol>, DefaultHashBuilder>,
    /// tables is a map of the dictionary ID for the table name to the table
    tables: HashMap<u32, Table>,
    is_open: bool,
}

impl Partition {
    fn new(id: u32, name: String) -> Self {
        Self {
            name,
            id,
            dictionary: StringInterner::new(),
            tables: HashMap::new(),
            is_open: true,
        }
    }

    /// Creates  a new symbol corresponding to `name` if one does not already exist
    fn intern_new_dict_entry(&mut self, name: &str) {
        self.dictionary.get_or_intern(name);
    }

    /// returns the str in self.dictionary that corresponds to `id`,
    /// if any. Returns an error if no such id is found
    fn lookup_id(&self, id: u32) -> Result<&str> {
        let symbol =
            Symbol::try_from_usize(id as usize).expect("to be able to convert u32 to symbol");
        self.dictionary
            .resolve(symbol)
            .context(DictionaryIdLookupError { id })
    }

    fn append_wal_schema(&mut self, table_id: u32, column_id: u32, column_type: wb::ColumnType) {
        let partition_id = self.id;

        let t = self
            .tables
            .entry(table_id)
            .or_insert_with(|| Table::new(table_id, partition_id));
        t.append_schema(column_id, column_type);
    }

    fn add_wal_row(
        &mut self,
        table_id: u32,
        values: &flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<wb::Value<'_>>>,
    ) -> Result<(), RestorationError> {
        let t = self
            .tables
            .get_mut(&table_id)
            .context(TableNotFound { table: table_id })?;
        t.add_wal_row(values)
    }

    fn write_line(
        &mut self,
        line: &ParsedLine<'_>,
        builder: &mut Option<WalEntryBuilder<'_>>,
    ) -> Result<()> {
        if let Some(b) = builder.as_mut() {
            b.ensure_partition_exists(self.id, &self.name);
        }

        let measurement = line.series.measurement.as_str();
        let table_id = self.dict_or_insert(measurement, builder);
        let partition_id = self.id;

        let column_count = line.column_count();
        let mut values: Vec<ColumnValue<'_>> = Vec::with_capacity(column_count);

        // Make sure the time, tag and field names exist in the dictionary
        if let Some(tags) = &line.series.tag_set {
            for (k, v) in tags {
                let tag_column_id = self.dict_or_insert(k.as_str(), builder);
                let tag_value_id = self.dict_or_insert(v.as_str(), builder);
                values.push(ColumnValue {
                    id: tag_column_id,
                    value: Value::TagValueId(tag_value_id),
                });
            }
        }
        for (field_name, value) in &line.field_set {
            let field_id = self.dict_or_insert(field_name.as_str(), builder);
            values.push(ColumnValue {
                id: field_id,
                value: Value::FieldValue(value),
            });
        }
        let time_id = self.dict_or_insert(TIME_COLUMN_NAME, builder);
        let time = line.timestamp.unwrap_or(0);
        let time_value = FieldValue::I64(time);
        values.push(ColumnValue {
            id: time_id,
            value: Value::FieldValue(&time_value),
        });

        let table = self
            .tables
            .entry(table_id)
            .or_insert_with(|| Table::new(table_id, partition_id));
        table.add_row(&values, builder)?;

        Ok(())
    }

    /// Returns the id corresponding to value, adding an entry for the
    /// id if it is not yet present in the dictionary.
    fn dict_or_insert(&mut self, value: &str, builder: &mut Option<WalEntryBuilder<'_>>) -> u32 {
        match self.dictionary.get(value) {
            Some(id) => symbol_to_u32(id),
            None => {
                let id = symbol_to_u32(self.dictionary.get_or_intern(value));

                if let Some(builder) = builder {
                    builder.add_dictionary_entry(self.id, value, id);
                }

                id
            }
        }
    }

    /// Returns the id corresponding to value, or an error if no such
    /// symbol exists. Note: this does not add to the dictionary as
    /// doing so requires updating the WAL as well.
    fn lookup_value(&self, value: &str) -> Result<u32> {
        self.dictionary
            .get(value)
            .map(symbol_to_u32)
            .context(DictionaryValueLookupError { value })
    }

    fn should_write(&self, key: &str) -> bool {
        self.name.starts_with(key) && self.is_open
    }

    fn table_to_arrow(&self, table_name: &str) -> Result<RecordBatch> {
        let table_id = self
            .dictionary
            .get(table_name)
            .context(TableNotFoundInDictionary {
                table: table_name.to_string(),
            })?;
        let table_id = u32::try_from(table_id.to_usize()).context(IdConversionError {})?;

        let table = self
            .tables
            .get(&table_id)
            .context(TableNotFoundInPartition {
                table: table_id,
                partition: self.id,
            })?;
        table.to_arrow(|id| self.lookup_id(id))
    }
}

// ColumnValue is a temporary holder of the column ID (name to dict mapping) and its value
#[derive(Debug)]
struct ColumnValue<'a> {
    id: u32,
    value: Value<'a>,
}

#[derive(Debug)]
enum Value<'a> {
    TagValueId(u32),
    FieldValue(&'a delorean_line_parser::FieldValue<'a>),
}

impl<'a> Value<'a> {
    fn type_description(&self) -> &'static str {
        match self {
            Value::TagValueId(_) => "tag",
            Value::FieldValue(FieldValue::I64(_)) => "i64",
            Value::FieldValue(FieldValue::F64(_)) => "f64",
            Value::FieldValue(FieldValue::Boolean(_)) => "bool",
            Value::FieldValue(FieldValue::String(_)) => "String",
        }
    }
}

fn symbol_to_u32(sym: DefaultSymbol) -> u32 {
    sym.to_usize() as u32
}

fn type_description(value: wb::ColumnValue) -> &'static str {
    use wb::ColumnValue::*;

    match value {
        NONE => "none",
        TagValue => "tag",
        I64Value => "i64",
        U64Value => "u64",
        F64Value => "f64",
        BoolValue => "bool",
        StringValue => "String",
    }
}

#[derive(Debug)]
struct Table {
    id: u32,
    partition_id: u32,
    /// Maps column name (as a u32 in the partition dictionary) to an index in self.columns
    column_id_to_index: HashMap<u32, usize>,
    columns: Vec<Column>,
}

impl Table {
    fn new(id: u32, partition_id: u32) -> Self {
        Self {
            id,
            partition_id,
            column_id_to_index: HashMap::new(),
            columns: Vec::new(),
        }
    }

    fn append_schema(&mut self, column_id: u32, column_type: wb::ColumnType) {
        let column_index = self.columns.len();
        self.column_id_to_index.insert(column_id, column_index);
        let row_count = self.row_count();

        match column_type {
            wb::ColumnType::Tag => {
                let mut v = Vec::with_capacity(row_count + 1);
                v.resize_with(row_count, || None);
                self.columns.push(Column::Tag(v));
            }
            wb::ColumnType::I64 => {
                let mut v = Vec::with_capacity(row_count + 1);
                v.resize_with(row_count, || None);
                self.columns.push(Column::I64(v));
            }
            wb::ColumnType::F64 => {
                let mut v = Vec::with_capacity(row_count + 1);
                v.resize_with(row_count, || None);
                self.columns.push(Column::F64(v));
            }
            wb::ColumnType::U64 => {
                todo!("handle this (write it out, in mem, and recover");
            }
            wb::ColumnType::String => {
                let mut v = Vec::with_capacity(row_count + 1);
                v.resize_with(row_count, || None);
                self.columns.push(Column::String(v));
            }
            wb::ColumnType::Bool => {
                let mut v = Vec::with_capacity(row_count + 1);
                v.resize_with(row_count, || None);
                self.columns.push(Column::Bool(v));
            }
        }
    }

    fn add_wal_row(
        &mut self,
        values: &flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<wb::Value<'_>>>,
    ) -> Result<(), RestorationError> {
        let row_count = self.row_count();

        for value in values {
            let col = self
                .columns
                .get_mut(value.column_index() as usize)
                .context(ColumnNotFound {
                    column: value.column_index(),
                })?;
            match (col, value.value_type()) {
                (Column::Tag(vals), wb::ColumnValue::TagValue) => {
                    let v = value.value_as_tag_value().context(WalValueTypeMismatch {
                        column: value.column_index(),
                        expected: "tag",
                    })?;
                    vals.push(Some(v.value()));
                }
                (Column::Bool(vals), wb::ColumnValue::BoolValue) => {
                    let v = value.value_as_bool_value().context(WalValueTypeMismatch {
                        column: value.column_index(),
                        expected: "bool",
                    })?;
                    vals.push(Some(v.value()));
                }
                (Column::String(vals), wb::ColumnValue::StringValue) => {
                    let v = value
                        .value_as_string_value()
                        .context(WalValueTypeMismatch {
                            column: value.column_index(),
                            expected: "String",
                        })?;
                    vals.push(Some(v.value().unwrap().to_string()));
                }
                (Column::I64(vals), wb::ColumnValue::I64Value) => {
                    let v = value.value_as_i64value().context(WalValueTypeMismatch {
                        column: value.column_index(),
                        expected: "i64",
                    })?;
                    vals.push(Some(v.value()));
                }
                (Column::F64(vals), wb::ColumnValue::F64Value) => {
                    let v = value.value_as_f64value().context(WalValueTypeMismatch {
                        column: value.column_index(),
                        expected: "f64",
                    })?;
                    vals.push(Some(v.value()));
                }
                (existing_column, inserted_value) => {
                    return ColumnTypeMismatch {
                        column: value.column_index(),
                        existing_column_type: existing_column.type_description(),
                        inserted_value_type: type_description(inserted_value),
                    }
                    .fail()
                }
            }
        }

        for col in &mut self.columns {
            col.push_none_if_len_equal(row_count);
        }

        Ok(())
    }

    fn row_count(&self) -> usize {
        self.columns.first().map_or(0, |v| v.len())
    }

    /// Returns a reference to the specified column
    fn column(&self, column_id: u32) -> Result<&Column> {
        match self.column_id_to_index.get(&column_id) {
            Some(column_index) => Ok(&self.columns[*column_index]),
            None => InternalError {
                description: format!("Column id {} not found", column_id),
            }
            .fail(),
        }
    }

    fn add_row(
        &mut self,
        values: &[ColumnValue<'_>],
        builder: &mut Option<WalEntryBuilder<'_>>,
    ) -> Result<()> {
        let row_count = self.row_count();

        // insert new columns and validate existing ones
        for val in values {
            let (column, column_id) = match self.column_id_to_index.get(&val.id) {
                Some(idx) => (&mut self.columns[*idx], *idx),
                None => {
                    // Add the column and make all values for existing rows None
                    let index = self.columns.len();
                    self.column_id_to_index.insert(val.id, index);

                    let (col, wal_type) = match val.value {
                        Value::TagValueId(_) => {
                            let mut v = Vec::with_capacity(row_count + 1);
                            v.resize_with(row_count, || None);
                            (Column::Tag(v), wb::ColumnType::Tag)
                        }
                        Value::FieldValue(FieldValue::I64(_)) => {
                            let mut v = Vec::with_capacity(row_count + 1);
                            v.resize_with(row_count, || None);
                            (Column::I64(v), wb::ColumnType::I64)
                        }
                        Value::FieldValue(FieldValue::F64(_)) => {
                            let mut v = Vec::with_capacity(row_count + 1);
                            v.resize_with(row_count, || None);
                            (Column::F64(v), wb::ColumnType::F64)
                        }
                        Value::FieldValue(FieldValue::Boolean(_)) => {
                            let mut v = Vec::with_capacity(row_count + 1);
                            v.resize_with(row_count, || None);
                            (Column::Bool(v), wb::ColumnType::Bool)
                        }
                        Value::FieldValue(FieldValue::String(_)) => {
                            let mut v = Vec::with_capacity(row_count + 1);
                            v.resize_with(row_count, || None);
                            (Column::String(v), wb::ColumnType::String)
                        }
                    };
                    self.columns.push(col);

                    if let Some(builder) = builder {
                        builder.add_schema_append(self.partition_id, self.id, val.id, wal_type);
                    }

                    (&mut self.columns[index], index)
                }
            };

            ensure!(
                column.matches_type(&val),
                SchemaMismatch {
                    column_id,
                    existing_column_type: column.type_description(),
                    inserted_value_type: val.value.type_description(),
                }
            );
        }

        // insert the actual values
        for val in values {
            let idx = self
                .column_id_to_index
                .get(&val.id)
                .context(InsertError {})?;
            let column = self.columns.get_mut(*idx).context(InsertError {})?;

            match &val.value {
                Value::TagValueId(val) => match column {
                    Column::Tag(vals) => {
                        if let Some(builder) = builder {
                            builder.add_tag_value(u16::try_from(*idx).unwrap(), *val);
                        }
                        vals.push(Some(*val))
                    }
                    col => {
                        return SchemaMismatch {
                            column_id: *idx,
                            existing_column_type: col.type_description(),
                            inserted_value_type: "tag",
                        }
                        .fail()
                    }
                },
                Value::FieldValue(field) => match (column, field) {
                    (Column::Tag(_), _) => {
                        return SchemaMismatch {
                            column_id: *idx,
                            existing_column_type: "tag",
                            inserted_value_type: val.value.type_description(),
                        }
                        .fail()
                    }
                    (Column::String(vals), FieldValue::String(val)) => {
                        if let Some(builder) = builder {
                            builder.add_string_value(u16::try_from(*idx).unwrap(), val.as_str());
                        }
                        vals.push(Some(val.as_str().to_string()))
                    }
                    (Column::Bool(vals), FieldValue::Boolean(val)) => {
                        if let Some(builder) = builder {
                            builder.add_bool_value(u16::try_from(*idx).unwrap(), *val);
                        }
                        vals.push(Some(*val))
                    }
                    (Column::I64(vals), FieldValue::I64(val)) => {
                        if let Some(builder) = builder {
                            builder.add_i64_value(u16::try_from(*idx).unwrap(), *val);
                        }
                        vals.push(Some(*val))
                    }
                    (Column::F64(vals), FieldValue::F64(val)) => {
                        if let Some(builder) = builder {
                            builder.add_f64_value(u16::try_from(*idx).unwrap(), *val);
                        }
                        vals.push(Some(*val))
                    }
                    (column, _) => {
                        return SchemaMismatch {
                            column_id: *idx,
                            existing_column_type: column.type_description(),
                            inserted_value_type: val.value.type_description(),
                        }
                        .fail()
                    }
                },
            }
        }

        if let Some(builder) = builder {
            builder.add_row(self.partition_id, self.id);
        }

        // make sure all columns are of the same length
        for col in &mut self.columns {
            col.push_none_if_len_equal(row_count);
        }

        Ok(())
    }

    fn to_arrow<'a, F>(&self, lookup_id: F) -> Result<RecordBatch>
    where
        F: Fn(u32) -> Result<&'a str>,
    {
        let mut index: Vec<_> = self.column_id_to_index.iter().collect();
        index.sort_by(|a, b| a.1.cmp(b.1));
        let ids: Vec<_> = index.iter().map(|(a, _)| **a).collect();

        let mut fields = Vec::with_capacity(self.columns.len());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());

        for (col, id) in self.columns.iter().zip(ids) {
            let column_name = lookup_id(id)?;

            let arrow_col: ArrayRef = match col {
                Column::String(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Utf8, true));
                    let mut builder = StringBuilder::with_capacity(vals.len(), vals.len() * 10);

                    for v in vals {
                        match v {
                            None => builder.append_null(),
                            Some(s) => builder.append_value(s),
                        }
                        .context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::Tag(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Utf8, true));
                    let mut builder = StringBuilder::with_capacity(vals.len(), vals.len() * 10);

                    for v in vals {
                        match v {
                            None => builder.append_null(),
                            Some(id) => {
                                let tag_value = lookup_id(*id)?;
                                builder.append_value(tag_value)
                            }
                        }
                        .context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::F64(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Float64, true));
                    let mut builder = Float64Builder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::I64(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Int64, true));
                    let mut builder = Int64Builder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::Bool(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Boolean, true));
                    let mut builder = BooleanBuilder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
            };

            columns.push(arrow_col);
        }

        let schema = ArrowSchema::new(fields);

        RecordBatch::try_new(Arc::new(schema), columns).context(ArrowError {})
    }
}

struct WalEntryBuilder<'a> {
    fbb: flatbuffers::FlatBufferBuilder<'a>,
    entries: Vec<flatbuffers::WIPOffset<wb::WriteBufferEntry<'a>>>,
    row_values: Vec<flatbuffers::WIPOffset<wb::Value<'a>>>,
    partitions: BTreeSet<u32>,
}

impl WalEntryBuilder<'_> {
    fn new() -> Self {
        Self {
            fbb: flatbuffers::FlatBufferBuilder::new_with_capacity(1024),
            entries: vec![],
            row_values: vec![],
            partitions: BTreeSet::new(),
        }
    }

    fn add_dictionary_entry(&mut self, partition_id: u32, value: &str, id: u32) {
        let value_offset = self.fbb.create_string(value);

        let dictionary_add = wb::DictionaryAdd::create(
            &mut self.fbb,
            &wb::DictionaryAddArgs {
                id,
                partition_id,
                value: Some(value_offset),
            },
        );

        let entry = wb::WriteBufferEntry::create(
            &mut self.fbb,
            &wb::WriteBufferEntryArgs {
                dictionary_add: Some(dictionary_add),
                ..Default::default()
            },
        );

        self.entries.push(entry);
    }

    fn ensure_partition_exists(&mut self, id: u32, name: &str) {
        if !self.partitions.contains(&id) {
            self.add_partition_open(id, name);
        }
    }

    fn add_partition_open(&mut self, id: u32, name: &str) {
        self.partitions.insert(id);
        let partition_name = self.fbb.create_string(&name);

        let partition_open = wb::PartitionOpen::create(
            &mut self.fbb,
            &wb::PartitionOpenArgs {
                id,
                name: Some(partition_name),
            },
        );

        let entry = wb::WriteBufferEntry::create(
            &mut self.fbb,
            &wb::WriteBufferEntryArgs {
                partition_open: Some(partition_open),
                ..Default::default()
            },
        );

        self.entries.push(entry);
    }

    fn add_schema_append(
        &mut self,
        partition_id: u32,
        table_id: u32,
        column_id: u32,
        column_type: wb::ColumnType,
    ) {
        let schema_append = wb::SchemaAppend::create(
            &mut self.fbb,
            &wb::SchemaAppendArgs {
                partition_id,
                table_id,
                column_id,
                column_type,
            },
        );

        let entry = wb::WriteBufferEntry::create(
            &mut self.fbb,
            &wb::WriteBufferEntryArgs {
                schema_append: Some(schema_append),
                ..Default::default()
            },
        );

        self.entries.push(entry);
    }

    fn add_tag_value(&mut self, column_index: u16, value: u32) {
        let tv = wb::TagValue::create(&mut self.fbb, &wb::TagValueArgs { value });

        self.add_value(column_index, wb::ColumnValue::TagValue, tv.as_union_value());
    }

    fn add_string_value(&mut self, column_index: u16, value: &str) {
        let value_offset = self.fbb.create_string(value);

        let sv = wb::StringValue::create(
            &mut self.fbb,
            &wb::StringValueArgs {
                value: Some(value_offset),
            },
        );

        self.add_value(
            column_index,
            wb::ColumnValue::StringValue,
            sv.as_union_value(),
        );
    }

    fn add_f64_value(&mut self, column_index: u16, value: f64) {
        let fv = wb::F64Value::create(&mut self.fbb, &wb::F64ValueArgs { value });

        self.add_value(column_index, wb::ColumnValue::F64Value, fv.as_union_value());
    }

    fn add_i64_value(&mut self, column_index: u16, value: i64) {
        let iv = wb::I64Value::create(&mut self.fbb, &wb::I64ValueArgs { value });

        self.add_value(column_index, wb::ColumnValue::I64Value, iv.as_union_value());
    }

    fn add_bool_value(&mut self, column_index: u16, value: bool) {
        let bv = wb::BoolValue::create(&mut self.fbb, &wb::BoolValueArgs { value });

        self.add_value(
            column_index,
            wb::ColumnValue::BoolValue,
            bv.as_union_value(),
        );
    }

    fn add_value(
        &mut self,
        column_index: u16,
        value_type: wb::ColumnValue,
        value: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>,
    ) {
        let row_value = wb::Value::create(
            &mut self.fbb,
            &wb::ValueArgs {
                column_index,
                value_type,
                value: Some(value),
            },
        );

        self.row_values.push(row_value);
    }

    fn add_row(&mut self, partition_id: u32, table_id: u32) {
        let values_vec = self.fbb.create_vector(&self.row_values);

        let row = wb::Row::create(
            &mut self.fbb,
            &wb::RowArgs {
                partition_id,
                table_id,
                values: Some(values_vec),
            },
        );

        let entry = wb::WriteBufferEntry::create(
            &mut self.fbb,
            &wb::WriteBufferEntryArgs {
                write: Some(row),
                ..Default::default()
            },
        );

        self.entries.push(entry);
        self.row_values = vec![];
    }

    fn create_batch(&mut self) {
        let entry_vec = self.fbb.create_vector(&self.entries);

        let batch = wb::WriteBufferBatch::create(
            &mut self.fbb,
            &wb::WriteBufferBatchArgs {
                entries: Some(entry_vec),
            },
        );

        self.fbb.finish(batch, None);
    }

    fn data(mut self) -> Vec<u8> {
        self.create_batch();

        let (mut data, idx) = self.fbb.collapse();
        data.split_off(idx)
    }
}

#[derive(Debug)]
/// Stores the actual data for columns in a  partition
///
/// TODO: add some summary statistics (like min/max for example)
enum Column {
    F64(Vec<Option<f64>>),
    I64(Vec<Option<i64>>),
    String(Vec<Option<String>>),
    Bool(Vec<Option<bool>>),
    Tag(Vec<Option<u32>>),
}

impl Column {
    fn len(&self) -> usize {
        match self {
            Self::F64(v) => v.len(),
            Self::I64(v) => v.len(),
            Self::String(v) => v.len(),
            Self::Bool(v) => v.len(),
            Self::Tag(v) => v.len(),
        }
    }

    fn type_description(&self) -> &'static str {
        match self {
            Self::F64(_) => "f64",
            Self::I64(_) => "i64",
            Self::String(_) => "String",
            Self::Bool(_) => "bool",
            Self::Tag(_) => "tag",
        }
    }

    // TODO: have type mismatches return helpful error
    fn matches_type(&self, val: &ColumnValue<'_>) -> bool {
        match (self, &val.value) {
            (Self::Tag(_), Value::TagValueId(_)) => true,
            (col, Value::FieldValue(field)) => match (col, field) {
                (Self::F64(_), FieldValue::F64(_)) => true,
                (Self::I64(_), FieldValue::I64(_)) => true,
                (Self::Bool(_), FieldValue::Boolean(_)) => true,
                (Self::String(_), FieldValue::String(_)) => true,
                _ => false,
            },
            _ => false,
        }
    }

    // push_none_if_len_equal will add a None value to the end of the Vec of values if the
    // length is equal to the passed in value. This is used to ensure columns are all the same length.
    fn push_none_if_len_equal(&mut self, len: usize) {
        match self {
            Self::F64(v) => {
                if v.len() == len {
                    v.push(None);
                }
            }
            Self::I64(v) => {
                if v.len() == len {
                    v.push(None);
                }
            }
            Self::String(v) => {
                if v.len() == len {
                    v.push(None);
                }
            }
            Self::Bool(v) => {
                if v.len() == len {
                    v.push(None);
                }
            }
            Self::Tag(v) => {
                if v.len() == len {
                    v.push(None);
                }
            }
        }
    }

    /// Returns true if any rows are within the range [min_value,
    /// max_value], inclusive
    fn has_i64_range(&self, start: i64, end: i64) -> Result<bool> {
        match self {
            Self::I64(v) => {
                for val in v.iter() {
                    if let Some(val) = val {
                        if start <= *val && *val <= end {
                            return Ok(true);
                        }
                    }
                }
                Ok(false)
            }
            _ => InternalError {
                description: "Applying i64 range on a column with non-i64 type",
            }
            .fail(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use delorean_storage_interface::{Database, TimestampRange};

    use arrow::util::pretty::pretty_format_batches;
    use delorean_line_parser::parse_lines;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    fn to_set(v: Vec<&str>) -> BTreeSet<String> {
        v.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>()
    }

    #[test]
    fn test_has_i64_range() -> Result {
        let col = Column::I64(vec![]);
        assert!(!col.has_i64_range(-1, 0)?);

        let col = Column::I64(vec![Some(1), None, Some(2)]);
        assert!(!col.has_i64_range(-1, 0)?);
        assert!(col.has_i64_range(0, 1)?);
        assert!(col.has_i64_range(1, 2)?);
        assert!(col.has_i64_range(2, 3)?);
        assert!(!col.has_i64_range(3, 4)?);

        let col = Column::I64(vec![Some(2), None, Some(1)]);
        assert!(!col.has_i64_range(-1, 0)?);
        assert!(col.has_i64_range(0, 1)?);
        assert!(col.has_i64_range(1, 2)?);
        assert!(col.has_i64_range(2, 3)?);
        assert!(!col.has_i64_range(3, 4)?);

        Ok(())
    }

    #[test]
    fn test_has_i64_range_does_not_panic() -> Result {
        // providing the wrong column type should get an internal error, not a panic
        let col = Column::F64(vec![]);
        let res = col.has_i64_range(-1, 0);
        assert!(res.is_err());
        let res_string = format!("{:?}", res);
        let expected = "InternalError";
        assert!(
            res_string.contains(expected),
            "Did not find expected text '{}' in '{}'",
            expected,
            res_string
        );
        Ok(())
    }

    // Abstract a bit of boilerplate around table assertions and improve the failure output.
    // The default failure message uses Debug formatting, which prints newlines as `\n`.
    // This prints the pretty_format_batches using Display so it's easier to read the tables.
    fn assert_table_eq(table: &str, partitions: &[arrow::record_batch::RecordBatch]) {
        let res = pretty_format_batches(partitions).unwrap();
        assert_eq!(table, res, "\n\nleft:\n\n{}\nright:\n\n{}", table, res);
    }

    #[tokio::test(threaded_scheduler)]
    async fn list_table_names() -> Result {
        let mut dir = delorean_test_helpers::tmp_dir()?.into_path();

        let db = Db::try_with_wal("mydb", &mut dir).await?;

        // no tables initially
        assert_eq!(*db.table_names(None).await?, BTreeSet::new());

        // write two different tables
        let lines: Vec<_> =
            parse_lines("cpu,region=west user=23.2 10\ndisk,region=east bytes=99i 11")
                .map(|l| l.unwrap())
                .collect();
        db.write_lines(&lines).await?;

        // Now, we should see the two tables
        assert_eq!(*db.table_names(None).await?, to_set(vec!["cpu", "disk"]));

        Ok(())
    }

    #[tokio::test(threaded_scheduler)]
    async fn list_table_names_timestamps() -> Result {
        let mut dir = delorean_test_helpers::tmp_dir()?.into_path();

        let db = Db::try_with_wal("mydb", &mut dir).await?;

        // write two different tables at the following times:
        // cpu: 100 and 150
        // disk: 200
        let lines: Vec<_> =
            parse_lines("cpu,region=west user=23.2 100\ncpu,region=west user=21.0 150\ndisk,region=east bytes=99i 200")
                .map(|l| l.unwrap())
                .collect();
        db.write_lines(&lines).await?;

        // Cover all times
        let range = Some(TimestampRange { start: 0, end: 200 });
        assert_eq!(*db.table_names(range).await?, to_set(vec!["cpu", "disk"]));

        // Right before disk
        let range = Some(TimestampRange { start: 0, end: 199 });
        assert_eq!(*db.table_names(range).await?, to_set(vec!["cpu"]));

        // only one point of cpu
        let range = Some(TimestampRange {
            start: 50,
            end: 100,
        });
        assert_eq!(*db.table_names(range).await?, to_set(vec!["cpu"]));

        // no ranges
        let range = Some(TimestampRange {
            start: 250,
            end: 350,
        });
        assert_eq!(*db.table_names(range).await?, to_set(vec![]));

        Ok(())
    }

    #[tokio::test(threaded_scheduler)]
    async fn write_data_and_recover() -> Result {
        let mut dir = delorean_test_helpers::tmp_dir()?.into_path();

        let expected_cpu_table = r#"+--------+------+------+-------+-------------+-------+------+---------+-----------+
| region | host | user | other | str         | b     | time | new_tag | new_field |
+--------+------+------+-------+-------------+-------+------+---------+-----------+
| west   | A    | 23.2 | 1     | some string | true  | 10   |         | 0         |
| west   | B    | 23.1 | 0     |             | false | 15   |         | 0         |
|        | A    | 0    | 0     |             | false | 20   | foo     | 15.1      |
+--------+------+------+-------+-------------+-------+------+---------+-----------+
"#;
        let expected_mem_table = r#"+--------+------+-------+------+
| region | host | val   | time |
+--------+------+-------+------+
| east   | C    | 23432 | 10   |
+--------+------+-------+------+
"#;
        let expected_disk_table = r#"+--------+------+----------+--------------+------+
| region | host | bytes    | used_percent | time |
+--------+------+----------+--------------+------+
| west   | A    | 23432323 | 76.2         | 10   |
+--------+------+----------+--------------+------+
"#;

        {
            let db = Db::try_with_wal("mydb", &mut dir).await?;
            let lines: Vec<_> = parse_lines("cpu,region=west,host=A user=23.2,other=1i,str=\"some string\",b=true 10\ndisk,region=west,host=A bytes=23432323i,used_percent=76.2 10").map(|l| l.unwrap()).collect();
            db.write_lines(&lines).await?;
            let lines: Vec<_> = parse_lines("cpu,region=west,host=B user=23.1 15")
                .map(|l| l.unwrap())
                .collect();
            db.write_lines(&lines).await?;
            let lines: Vec<_> = parse_lines("cpu,host=A,new_tag=foo new_field=15.1 20")
                .map(|l| l.unwrap())
                .collect();
            db.write_lines(&lines).await?;
            let lines: Vec<_> = parse_lines("mem,region=east,host=C val=23432 10")
                .map(|l| l.unwrap())
                .collect();
            db.write_lines(&lines).await?;

            let partitions = db.table_to_arrow("cpu", &["region", "host"]).await?;
            assert_table_eq(expected_cpu_table, &partitions);

            let partitions = db.table_to_arrow("mem", &[]).await?;
            assert_table_eq(expected_mem_table, &partitions);

            let partitions = db.table_to_arrow("disk", &[]).await?;
            assert_table_eq(expected_disk_table, &partitions);
        }

        // check that it recovers from the wal
        {
            let db = Db::restore_from_wal(dir).await?;

            let partitions = db.table_to_arrow("cpu", &["region", "host"]).await?;
            assert_table_eq(expected_cpu_table, &partitions);

            let partitions = db.table_to_arrow("mem", &[]).await?;
            assert_table_eq(expected_mem_table, &partitions);

            let partitions = db.table_to_arrow("disk", &[]).await?;
            assert_table_eq(expected_disk_table, &partitions);
        }

        Ok(())
    }

    #[tokio::test(threaded_scheduler)]
    #[ignore]
    async fn recover_partial_entries() -> Result {
        let mut dir = delorean_test_helpers::tmp_dir()?.into_path();

        let expected_cpu_table = r#"+--------+------+------+-------+-------------+-------+------+---------+-----------+
| region | host | user | other | str         | b     | time | new_tag | new_field |
+--------+------+------+-------+-------------+-------+------+---------+-----------+
| west   | A    | 23.2 | 1     | some string | true  | 10   |         | 0         |
| west   | B    | 23.1 | 0     |             | false | 15   |         | 0         |
|        | A    | 0    | 0     |             | false | 20   | foo     | 15.1      |
+--------+------+------+-------+-------------+-------+------+---------+-----------+
"#;
        let expected_mem_table = r#"+--------+------+-------+------+
| region | host | val   | time |
+--------+------+-------+------+
| east   | C    | 23432 | 10   |
+--------+------+-------+------+
"#;
        let expected_disk_table = r#"+--------+------+----------+--------------+------+
| region | host | bytes    | used_percent | time |
+--------+------+----------+--------------+------+
| west   | A    | 23432323 | 76.2         | 10   |
+--------+------+----------+--------------+------+
"#;

        {
            let db = Db::try_with_wal("mydb", &mut dir).await?;
            let lines: Vec<_> = parse_lines("cpu,region=west,host=A user=23.2,other=1i,str=\"some string\",b=true 10\ndisk,region=west,host=A bytes=23432323i,used_percent=76.2 10").map(|l| l.unwrap()).collect();
            db.write_lines(&lines).await?;
            let lines: Vec<_> = parse_lines("cpu,region=west,host=B user=23.1 15")
                .map(|l| l.unwrap())
                .collect();
            db.write_lines(&lines).await?;
            let lines: Vec<_> = parse_lines("cpu,host=A,new_tag=foo new_field=15.1 20")
                .map(|l| l.unwrap())
                .collect();
            db.write_lines(&lines).await?;
            let lines: Vec<_> = parse_lines("mem,region=east,host=C val=23432 10")
                .map(|l| l.unwrap())
                .collect();
            db.write_lines(&lines).await?;

            let partitions = db.table_to_arrow("cpu", &["region", "host"]).await?;
            assert_table_eq(expected_cpu_table, &partitions);

            let partitions = db.table_to_arrow("mem", &[]).await?;
            assert_table_eq(expected_mem_table, &partitions);

            let partitions = db.table_to_arrow("disk", &[]).await?;
            assert_table_eq(expected_disk_table, &partitions);
        }

        // check that it can recover from the last 2 self-describing entries of the wal
        {
            let name = dir.iter().last().unwrap().to_str().unwrap().to_string();

            let wal_builder = WalBuilder::new(&dir);

            let wal_entries = wal_builder
                .entries()
                .context(LoadingWal { database: &name })?;

            // Skip the first 2 entries in the wal; only restore from the last 2
            let wal_entries = wal_entries.skip(2);

            let (partitions, next_partition_id, _stats) = restore_partitions_from_wal(wal_entries)?;

            let db = Db {
                name,
                dir,
                partitions: RwLock::new(partitions),
                next_partition_id: AtomicU32::new(next_partition_id + 1),
                wal_details: None,
            };

            // some cpu
            let smaller_cpu_table = r#"+------+------+-------+-------------+-------+------+---------+-----------+
| host | user | other | str         | b     | time | new_tag | new_field |
+------+------+-------+-------------+-------+------+---------+-----------+
| A    | 0    | 0     |             | false | 20   | foo     | 15.1      |
+------+------+-------+-------------+-------+------+---------+-----------+
"#;
            let partitions = db.table_to_arrow("cpu", &["region", "host"]).await?;
            assert_table_eq(smaller_cpu_table, &partitions);

            // all of mem
            let partitions = db.table_to_arrow("mem", &[]).await?;
            assert_table_eq(expected_mem_table, &partitions);

            // no disk
            let partitions = db.table_to_arrow("disk", &[]).await?;
            assert_table_eq("", &partitions);
        }

        Ok(())
    }

    #[tokio::test]
    async fn db_partition_key() -> Result {
        let mut dir = delorean_test_helpers::tmp_dir()?.into_path();
        let db = Db::try_with_wal("mydb", &mut dir).await?;

        let partition_keys: Vec<_> = parse_lines(
            "\
cpu user=23.2 1600107710000000000
disk bytes=23432323i 1600136510000000000",
        )
        .map(|line| db.partition_key(&line.unwrap()))
        .collect();

        assert_eq!(partition_keys, vec!["2020-09-14T18", "2020-09-15T02"]);

        Ok(())
    }
}
