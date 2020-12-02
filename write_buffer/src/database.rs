use generated_types::wal as wb;
use influxdb_line_protocol::ParsedLine;
use query::group_by::Aggregate;
use query::group_by::GroupByAndAggregate;
use query::group_by::WindowDuration;
use query::{
    exec::{stringset::StringSet, FieldListPlan, SeriesSetPlan, SeriesSetPlans, StringSetPlan},
    predicate::Predicate,
    SQLDatabase, TSDatabase,
};
use wal::{
    writer::{start_wal_sync_task, Error as WalWriterError, WalDetails},
    WalBuilder,
};

use crate::column::Column;
use crate::partition::Partition;
use crate::{partition::PartitionPredicate, table::Table};

use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use std::{
    collections::{BTreeSet, HashSet},
    path::Path,
};

use arrow_deps::{
    arrow,
    arrow::{datatypes::Schema as ArrowSchema, record_batch::RecordBatch},
    datafusion::logical_plan::LogicalPlan,
    datafusion::prelude::ExecutionConfig,
    datafusion::{
        datasource::MemTable, error::DataFusionError, execution::context::ExecutionContext,
    },
};
use data_types::data::{split_lines_into_write_entry_partitions, ReplicatedWrite};

use crate::dictionary::Error as DictionaryError;
use crate::partition::restore_partitions_from_wal;

use async_trait::async_trait;
use chrono::{offset::TimeZone, Utc};
use snafu::{OptionExt, ResultExt, Snafu};
use sqlparser::{
    ast::{SetExpr, Statement, TableFactor},
    dialect::GenericDialect,
    parser::Parser,
};
use tokio::sync::RwLock;
use tracing::info;

#[derive(Debug, Snafu)]
pub enum Error {
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
        source: wal::Error,
    },

    #[snafu(display("Error recovering WAL for database {}: {}", database, source))]
    WalRecoverError {
        database: String,
        source: crate::partition::Error,
    },

    #[snafu(display("Error recovering WAL for partition {} on table {}", partition, table))]
    WalPartitionError { partition: String, table: String },

    #[snafu(display("Error recovering write from WAL, column id {} not found", column_id))]
    WalColumnError { column_id: u16 },

    #[snafu(display("Error creating db dir for {}: {}", database, err))]
    CreatingWalDir {
        database: String,
        err: std::io::Error,
    },

    #[snafu(display("Database {} doesn't exist", database))]
    DatabaseNotFound { database: String },

    #[snafu(display("Partition {} is full", partition))]
    PartitionFull { partition: String },

    #[snafu(display("Error in {}: {}", source_module, source))]
    PassThrough {
        source_module: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display(
        "Table name {} not found in dictionary of partition {}",
        table,
        partition
    ))]
    TableNameNotFoundInDictionary {
        table: String,
        partition: String,
        source: DictionaryError,
    },

    #[snafu(display(
        "Table ID {} not found in dictionary of partition {}",
        table,
        partition
    ))]
    TableIdNotFoundInDictionary {
        table: u32,
        partition: String,
        source: DictionaryError,
    },

    #[snafu(display(
        "Column name {} not found in dictionary of partition {}",
        column_name,
        partition
    ))]
    ColumnNameNotFoundInDictionary {
        column_name: String,
        partition: String,
        source: DictionaryError,
    },

    #[snafu(display(
        "Column ID {} not found in dictionary of partition {}",
        column_id,
        partition
    ))]
    ColumnIdNotFoundInDictionary {
        column_id: u32,
        partition: String,
        source: DictionaryError,
    },

    #[snafu(display(
        "Value ID {} not found in dictionary of partition {}",
        value_id,
        partition
    ))]
    ColumnValueIdNotFoundInDictionary {
        value_id: u32,
        partition: String,
        source: DictionaryError,
    },

    #[snafu(display(
        "Column '{}' is not a tag column and thus can not list values",
        column_name
    ))]
    UnsupportedColumnTypeForListingValues { column_name: String },

    #[snafu(display("Table {} not found in partition {}", table, partition))]
    TableNotFoundInPartition { table: u32, partition: String },

    #[snafu(display("Internal Error: Column {} not found", column))]
    InternalColumnNotFound { column: u32 },

    #[snafu(display("Unexpected insert error"))]
    InsertError,

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowError { source: arrow::error::ArrowError },

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
        source: DataFusionError,
    },

    #[snafu(display("Unsupported SQL statement in query {}: {}", query, statement))]
    UnsupportedStatement {
        query: String,
        statement: Box<Statement>,
    },

    #[snafu(display("query error {} on query {}", message, query))]
    GenericQueryError { message: String, query: String },

    #[snafu(display("replicated write from writer {} missing payload", writer))]
    MissingPayload { writer: u32 },
}

impl From<crate::table::Error> for Error {
    fn from(e: crate::table::Error) -> Self {
        Self::PassThrough {
            source_module: "Table",
            source: Box::new(e),
        }
    }
}

impl From<crate::partition::Error> for Error {
    fn from(e: crate::partition::Error) -> Self {
        Self::PassThrough {
            source_module: "Partition",
            source: Box::new(e),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Default)]
pub struct Db {
    pub name: String,
    // TODO: partitions need to be wrapped in an Arc if they're going to be used without this lock
    partitions: RwLock<Vec<Partition>>,
    wal_details: Option<WalDetails>,
}

impl Db {
    /// New creates a new in-memory only write buffer database
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Create a new DB that will create and use the Write Ahead Log
    /// (WAL) directory `wal_dir`
    pub async fn try_with_wal(name: impl Into<String>, wal_dir: &mut PathBuf) -> Result<Self> {
        let name = name.into();
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
        let wal_builder = WalBuilder::new(wal_dir.clone());
        let wal_details = start_wal_sync_task(wal_builder)
            .await
            .context(OpeningWal { database: &name })?;
        wal_details
            .write_metadata()
            .await
            .context(OpeningWal { database: &name })?;

        Ok(Self {
            name,
            wal_details: Some(wal_details),
            ..Default::default()
        })
    }

    /// Create a new DB and initially restore pre-existing data in the
    /// Write Ahead Log (WAL) directory `wal_dir`
    pub async fn restore_from_wal(wal_dir: &Path) -> Result<Self> {
        let now = std::time::Instant::now();
        let name = wal_dir
            .iter()
            .last()
            .with_context(|| OpenDb { dir: &wal_dir })?
            .to_str()
            .with_context(|| OpenDb { dir: &wal_dir })?
            .to_string();

        let wal_builder = WalBuilder::new(wal_dir);
        let wal_details = start_wal_sync_task(wal_builder.clone())
            .await
            .context(OpeningWal { database: &name })?;

        // TODO: check wal metadata format
        let entries = wal_builder
            .entries()
            .context(LoadingWal { database: &name })?;

        let (partitions, stats) =
            restore_partitions_from_wal(entries).context(WalRecoverError { database: &name })?;

        let elapsed = now.elapsed();
        info!(
            "{} database loaded {} rows in {:?} in {} tables",
            &name,
            stats.row_count,
            elapsed,
            stats.tables.len(),
        );

        info!("{} database partition count: {}", &name, partitions.len(),);

        Ok(Self {
            name,
            partitions: RwLock::new(partitions),
            wal_details: Some(wal_details),
        })
    }

    async fn write_entries_to_partitions(&self, batch: &wb::WriteBufferBatch<'_>) -> Result<()> {
        if let Some(entries) = batch.entries() {
            let mut partitions = self.partitions.write().await;

            for entry in entries {
                let key = entry
                    .partition_key()
                    .expect("partition key should have been inserted");

                match partitions.iter_mut().find(|p| p.should_write(key)) {
                    Some(p) => p.write_entry(&entry)?,
                    None => {
                        let mut p = Partition::new(key);
                        p.write_entry(&entry)?;
                        partitions.push(p)
                    }
                }
            }
        }

        Ok(())
    }

    async fn table_to_arrow(&self, table_name: &str, columns: &[&str]) -> Result<Vec<RecordBatch>> {
        let partitions = self.partitions.read().await;

        let batches = partitions
            .iter()
            .map(|p| p.table_to_arrow(table_name, columns))
            .collect::<Result<Vec<_>, crate::partition::Error>>()?;

        Ok(batches)
    }
}

#[async_trait]
impl TSDatabase for Db {
    type Error = Error;

    // TODO: writes lines creates a column named "time" for the timestamp data. If
    //       we keep this we need to validate that no tag or field has the same name.
    async fn write_lines(&self, lines: &[ParsedLine<'_>]) -> Result<(), Self::Error> {
        let data = split_lines_into_write_entry_partitions(partition_key, lines);
        let batch = flatbuffers::get_root::<wb::WriteBufferBatch<'_>>(&data);

        self.write_entries_to_partitions(&batch).await?;

        if let Some(wal) = &self.wal_details {
            wal.write_and_sync(data).await.context(WritingWal {
                database: &self.name,
            })?;
        }

        Ok(())
    }

    async fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<(), Self::Error> {
        match write.write_buffer_batch() {
            Some(b) => self.write_entries_to_partitions(&b).await?,
            None => {
                return MissingPayload {
                    writer: write.to_fb().writer(),
                }
                .fail()
            }
        };

        if let Some(wal) = &self.wal_details {
            // TODO(paul): refactor this so we're not cloning. Although replicated writes shouldn't
            //  be using a WAL and how the WAL is used at all is likely to have a larger refactor soon.
            wal.write_and_sync(write.data.clone())
                .await
                .context(WritingWal {
                    database: &self.name,
                })?;
        }

        Ok(())
    }

    async fn table_names(&self, predicate: Predicate) -> Result<StringSetPlan, Self::Error> {
        // TODO: Cache this information to avoid creating this each time
        let partitions = self.partitions.read().await;

        let mut table_names: BTreeSet<String> = BTreeSet::new();
        for partition in partitions.iter() {
            let partition_predicate = partition.compile_predicate(&predicate)?;
            // this doesn't seem to make any sense
            assert!(
                partition_predicate.field_restriction.is_none(),
                "Column selection for table names not supported"
            );

            // It might make sense to ask for all table names that
            // have rows that pass a general purpose predicate. I am
            // not sure if it is needed now, so panic
            assert!(
                partition_predicate.partition_exprs.is_empty(),
                "General partition exprs on table name list are not supported"
            );

            for (table_name_symbol, table) in &partition.tables {
                if table.could_match_predicate(&partition_predicate)? {
                    let table_name = partition.dictionary.lookup_id(*table_name_symbol).unwrap();

                    if !table_names.contains(table_name) {
                        table_names.insert(table_name.to_string());
                    }
                }
            }
        }
        Ok(table_names.into())
    }

    // return all column names in this database, while applying optional predicates
    async fn tag_column_names(&self, predicate: Predicate) -> Result<StringSetPlan, Self::Error> {
        let has_exprs = predicate.has_exprs();
        let mut filter = PartitionTableFilter::new(predicate);

        if has_exprs {
            let mut visitor = NamePredVisitor::new();
            self.visit_tables(&mut filter, &mut visitor).await?;
            Ok(visitor.plans.into())
        } else {
            let mut visitor = NameVisitor::new();
            self.visit_tables(&mut filter, &mut visitor).await?;
            Ok(visitor.column_names.into())
        }
    }

    /// return all field names in this database, while applying optional predicates
    async fn field_column_names(&self, predicate: Predicate) -> Result<FieldListPlan, Self::Error> {
        let mut filter = PartitionTableFilter::new(predicate);
        let mut visitor = TableFieldPredVisitor::new();
        self.visit_tables(&mut filter, &mut visitor).await?;
        Ok(visitor.into_fieldlist_plan())
    }

    /// return all column values in this database, while applying optional predicates
    async fn column_values(
        &self,
        column_name: &str,
        predicate: Predicate,
    ) -> Result<StringSetPlan, Self::Error> {
        let has_exprs = predicate.has_exprs();
        let mut filter = PartitionTableFilter::new(predicate);

        if has_exprs {
            let mut visitor = ValuePredVisitor::new(column_name);
            self.visit_tables(&mut filter, &mut visitor).await?;
            Ok(visitor.plans.into())
        } else {
            let mut visitor = ValueVisitor::new(column_name);
            self.visit_tables(&mut filter, &mut visitor).await?;
            Ok(visitor.column_values.into())
        }
    }

    async fn query_series(&self, predicate: Predicate) -> Result<SeriesSetPlans, Self::Error> {
        let mut filter = PartitionTableFilter::new(predicate);
        let mut visitor = SeriesVisitor::new();
        self.visit_tables(&mut filter, &mut visitor).await?;
        Ok(visitor.plans.into())
    }

    async fn query_groups(
        &self,
        predicate: Predicate,
        gby_agg: GroupByAndAggregate,
    ) -> Result<SeriesSetPlans, Self::Error> {
        let mut filter = PartitionTableFilter::new(predicate);

        match gby_agg {
            GroupByAndAggregate::Columns {
                // TODO use aggregate:  https://github.com/influxdata/influxdb_iox/issues/448
                agg: _agg,
                group_columns,
            } => {
                // Add any specified groups as predicate columns (so we
                // can skip tables without those tags)
                let mut filter = filter.add_required_columns(&group_columns);
                let mut visitor = GroupsVisitor::new(group_columns);
                self.visit_tables(&mut filter, &mut visitor).await?;
                Ok(visitor.plans.into())
            }
            GroupByAndAggregate::Window { agg, every, offset } => {
                let mut visitor = WindowGroupsVisitor::new(agg, every, offset);
                self.visit_tables(&mut filter, &mut visitor).await?;
                Ok(visitor.plans.into())
            }
        }
    }
}

#[async_trait]
impl SQLDatabase for Db {
    type Error = Error;

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

        let config = ExecutionConfig::new().with_batch_size(1024 * 1024);
        let mut ctx = ExecutionContext::with_config(config);

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
            .create_physical_plan(&plan)
            .context(QueryError { query })?;

        ctx.collect(plan).await.context(QueryError { query })
    }
}

/// This trait is used to implement a "Visitor" pattern for Database
/// which can be used to define logic that shares a common Depth First
/// Search (DFS) traversal of the Database --> Partition --> Table -->
/// Column datastructure heirarchy.
///
/// Specifically, if we had a database like the following:
///
/// YesterdayPartition
///   CPU Table1
///    Col1
/// TodayPartition
///   CPU Table2
///    Col2
///
/// Then the methods would be invoked in the following order
///
///  visitor.pre_visit_partition(YesterdayPartition)
///  visitor.pre_visit_table(CPU Table1)
///  visitor.visit_column(Col1)
///  visitor.post_visit_table(CPU Table1)
///  visitor.post_visit_partition(YesterdayPartition)
///  visitor.pre_visit_partition(TodayPartition)
///  visitor.pre_visit_table(CPU Table2)
///  visitor.visit_column(Col2)
///  visitor.post_visit_table(CPU Table2)
///  visitor.post_visit_partition(TodayPartition)
trait Visitor {
    // called once before any column in a partition is visisted
    fn pre_visit_partition(&mut self, _partition: &Partition) -> Result<()> {
        Ok(())
    }

    // called once before any column in a Table is visited
    fn pre_visit_table(
        &mut self,
        _table: &Table,
        _partition: &Partition,
        _filter: &mut PartitionTableFilter,
    ) -> Result<()> {
        Ok(())
    }

    // called every time a column is visited
    fn visit_column(
        &mut self,
        _table: &Table,
        _column_id: u32,
        _column: &Column,
        _filter: &mut PartitionTableFilter,
    ) -> Result<()> {
        Ok(())
    }

    // called once after all columns in a Table are visited
    fn post_visit_table(&mut self, _table: &Table, _partition: &Partition) -> Result<()> {
        Ok(())
    }

    // called once after all columns in a partition is visited
    fn post_visit_partition(&mut self, _partition: &Partition) -> Result<()> {
        Ok(())
    }
}

impl Db {
    /// returns the number of partitions in this database
    pub async fn len(&self) -> usize {
        self.partitions.read().await.len()
    }

    /// returns true if the database has no partititons
    pub async fn is_empty(&self) -> bool {
        self.partitions.read().await.is_empty()
    }

    /// Traverse this database's tables, calling the relevant
    /// functions, in order, of `visitor`, as described on the Visitor
    /// trait.
    ///
    /// Skips visiting any table or columns of `filter.should_visit_table` returns false
    async fn visit_tables<V: Visitor>(
        &self,
        filter: &mut PartitionTableFilter,
        visitor: &mut V,
    ) -> Result<()> {
        let partitions = self.partitions.read().await;

        for partition in partitions.iter() {
            visitor.pre_visit_partition(partition)?;
            filter.pre_visit_partition(partition)?;

            for table in partition.tables.values() {
                if filter.should_visit_table(table)? {
                    visitor.pre_visit_table(table, partition, filter)?;

                    for (column_id, column_index) in &table.column_id_to_index {
                        visitor.visit_column(
                            table,
                            *column_id,
                            &table.columns[*column_index],
                            filter,
                        )?
                    }

                    visitor.post_visit_table(table, partition)?;
                }
            }
            visitor.post_visit_partition(partition)?;
        } // next partition

        Ok(())
    }
}

/// Common logic for processing and filtering tables in the write buffer
///
/// Note that since each partition has its own dictionary, mappings
/// between Strings --> we cache the String->id mappings per partition
///
/// b) the table doesn't have a column range that overlaps the
/// predicate values, e.g., if you have env = "us-west" and a
/// table's env column has the range ["eu-south", "us-north"].
#[derive(Debug)]
struct PartitionTableFilter {
    predicate: Predicate,

    /// If specififed, only tables with all specified columns will be
    /// visited. Note that just becuase a table has all these columns,
    /// it might not be visited for other reasons (e.g. it is filted
    /// out by a table_name predicate)
    additional_required_columns: Option<HashSet<String>>,

    /// A 'compiled' version of the predicate to evaluate on tables /
    /// columns in a particular partition during the walk
    partition_predicate: Option<PartitionPredicate>,
}

impl PartitionTableFilter {
    fn new(predicate: Predicate) -> Self {
        Self {
            predicate,
            additional_required_columns: None,
            partition_predicate: None,
        }
    }

    /// adds the specified columns to a list of columns that must be
    /// present in a table.
    fn add_required_columns(mut self, column_names: &[String]) -> Self {
        let mut required_columns = self
            .additional_required_columns
            .take()
            .unwrap_or_else(HashSet::new);

        for c in column_names {
            if !required_columns.contains(c) {
                required_columns.insert(c.clone());
            }
        }

        self.additional_required_columns = Some(required_columns);
        self
    }

    /// Called when each partition gets visited. Since ids are
    /// specific to each partitition, the predicates much get
    /// translated each time.
    fn pre_visit_partition(&mut self, partition: &Partition) -> Result<()> {
        let mut partition_predicate = partition.compile_predicate(&self.predicate)?;

        // add any additional column needs
        if let Some(additional_required_columns) = &self.additional_required_columns {
            partition.add_required_columns_to_predicate(
                additional_required_columns,
                &mut partition_predicate,
            );
        }

        self.partition_predicate = Some(partition_predicate);

        Ok(())
    }

    /// If returns false, skips visiting _table and all its columns
    fn should_visit_table(&mut self, table: &Table) -> Result<bool> {
        Ok(table.could_match_predicate(self.partition_predicate())?)
    }

    pub fn partition_predicate(&self) -> &PartitionPredicate {
        self.partition_predicate
            .as_ref()
            .expect("Visited partition to compile predicate")
    }
}

/// return all column names in this database, while applying only the
/// timestamp range (has no general purpose predicates)
struct NameVisitor {
    column_names: StringSet,
    partition_column_ids: BTreeSet<u32>,
}

impl NameVisitor {
    fn new() -> Self {
        Self {
            column_names: StringSet::new(),
            partition_column_ids: BTreeSet::new(),
        }
    }
}

impl Visitor for NameVisitor {
    fn visit_column(
        &mut self,
        table: &Table,
        column_id: u32,
        column: &Column,
        filter: &mut PartitionTableFilter,
    ) -> Result<()> {
        if let Column::Tag(column, _) = column {
            if table.column_matches_predicate(column, filter.partition_predicate())? {
                self.partition_column_ids.insert(column_id);
            }
        }
        Ok(())
    }

    fn pre_visit_partition(&mut self, _partition: &Partition) -> Result<()> {
        self.partition_column_ids.clear();
        Ok(())
    }

    fn post_visit_partition(&mut self, partition: &Partition) -> Result<()> {
        // convert all the partition's column_ids to Strings
        for &column_id in &self.partition_column_ids {
            let column_name = partition.dictionary.lookup_id(column_id).context(
                ColumnIdNotFoundInDictionary {
                    column_id,
                    partition: &partition.key,
                },
            )?;

            if !self.column_names.contains(column_name) {
                self.column_names.insert(column_name.to_string());
            }
        }
        Ok(())
    }
}

/// Return all column names in this database, while applying a
/// general purpose predicates
struct NamePredVisitor {
    plans: Vec<LogicalPlan>,
}

impl NamePredVisitor {
    fn new() -> Self {
        Self { plans: Vec::new() }
    }
}

impl Visitor for NamePredVisitor {
    fn pre_visit_table(
        &mut self,
        table: &Table,
        partition: &Partition,
        filter: &mut PartitionTableFilter,
    ) -> Result<()> {
        self.plans
            .push(table.tag_column_names_plan(filter.partition_predicate(), partition)?);
        Ok(())
    }
}

/// return a plan that selects all values from field columns after
/// applying timestamp and other predicates
#[derive(Debug)]
struct TableFieldPredVisitor {
    // As Each table can be spread across multiple Partitions, we
    // collect all the relevant plans and Union them together.
    plans: Vec<LogicalPlan>,
}

impl Visitor for TableFieldPredVisitor {
    fn pre_visit_table(
        &mut self,
        table: &Table,
        partition: &Partition,
        filter: &mut PartitionTableFilter,
    ) -> Result<()> {
        self.plans
            .push(table.field_names_plan(filter.partition_predicate(), partition)?);
        Ok(())
    }
}

impl TableFieldPredVisitor {
    fn new() -> Self {
        let plans = Vec::new();
        Self { plans }
    }

    fn into_fieldlist_plan(self) -> FieldListPlan {
        FieldListPlan::Plans(self.plans)
    }
}

/// return all values in the `column_name` column
/// in this database, while applying the timestamp range
///
/// Potential optimizations: Run this in parallel (in different
/// futures) for each partition / table, rather than a single one
/// -- but that will require building up parallel hash tables.
struct ValueVisitor<'a> {
    column_name: &'a str,
    // what column id we are looking for
    column_id: Option<u32>,
    partition_value_ids: BTreeSet<u32>,
    column_values: StringSet,
}

impl<'a> ValueVisitor<'a> {
    fn new(column_name: &'a str) -> Self {
        Self {
            column_name,
            column_id: None,
            column_values: StringSet::new(),
            partition_value_ids: BTreeSet::new(),
        }
    }
}

impl<'a> Visitor for ValueVisitor<'a> {
    fn pre_visit_partition(&mut self, partition: &Partition) -> Result<()> {
        self.partition_value_ids.clear();

        self.column_id = Some(
            partition
                .dictionary
                .lookup_value(self.column_name)
                .context(ColumnNameNotFoundInDictionary {
                    column_name: self.column_name,
                    partition: &partition.key,
                })?,
        );

        Ok(())
    }

    fn visit_column(
        &mut self,
        table: &Table,
        column_id: u32,
        column: &Column,
        filter: &mut PartitionTableFilter,
    ) -> Result<()> {
        if Some(column_id) != self.column_id {
            return Ok(());
        }

        match column {
            Column::Tag(column, _) => {
                // if we have a timestamp prediate, find all values
                // where the timestamp is within range. Otherwise take
                // all values.
                let partition_predicate = filter.partition_predicate();
                match partition_predicate.range {
                    None => {
                        // take all non-null values
                        column.iter().filter_map(|&s| s).for_each(|value_id| {
                            self.partition_value_ids.insert(value_id);
                        });
                    }
                    Some(range) => {
                        // filter out all values that don't match the timestmap
                        let time_column = table.column_i64(partition_predicate.time_column_id)?;

                        column
                            .iter()
                            .zip(time_column.iter())
                            .filter_map(|(&column_value_id, &timestamp_value)| {
                                if range.contains_opt(timestamp_value) {
                                    column_value_id
                                } else {
                                    None
                                }
                            })
                            .for_each(|value_id| {
                                self.partition_value_ids.insert(value_id);
                            });
                    }
                }
                Ok(())
            }
            _ => UnsupportedColumnTypeForListingValues {
                column_name: self.column_name,
            }
            .fail(),
        }
    }

    fn post_visit_partition(&mut self, partition: &Partition) -> Result<()> {
        // convert all the partition's column_ids to Strings
        for &value_id in &self.partition_value_ids {
            let value = partition.dictionary.lookup_id(value_id).context(
                ColumnValueIdNotFoundInDictionary {
                    value_id,
                    partition: &partition.key,
                },
            )?;

            if !self.column_values.contains(value) {
                self.column_values.insert(value.to_string());
            }
        }
        Ok(())
    }
}

/// return all column values for the specified column in this
/// database, while applying the timestamp range and predicate
struct ValuePredVisitor<'a> {
    column_name: &'a str,
    plans: Vec<LogicalPlan>,
}

impl<'a> ValuePredVisitor<'a> {
    fn new(column_name: &'a str) -> Self {
        Self {
            column_name,
            plans: Vec::new(),
        }
    }
}

impl<'a> Visitor for ValuePredVisitor<'a> {
    // TODO try and rule out entire tables based on the same critera
    // as explained on NamePredVisitor
    fn pre_visit_table(
        &mut self,
        table: &Table,
        partition: &Partition,
        filter: &mut PartitionTableFilter,
    ) -> Result<()> {
        // skip table entirely if there are no rows that fall in the timestamp
        if table.could_match_predicate(filter.partition_predicate())? {
            self.plans.push(table.tag_values_plan(
                self.column_name,
                filter.partition_predicate(),
                partition,
            )?);
        }
        Ok(())
    }
}

/// Return DataFusion plans to calculate which series pass the
/// specified predicate.
struct SeriesVisitor {
    plans: Vec<SeriesSetPlan>,
}

impl SeriesVisitor {
    fn new() -> Self {
        Self { plans: Vec::new() }
    }
}

impl Visitor for SeriesVisitor {
    fn pre_visit_table(
        &mut self,
        table: &Table,
        partition: &Partition,
        filter: &mut PartitionTableFilter,
    ) -> Result<()> {
        self.plans
            .push(table.series_set_plan(filter.partition_predicate(), partition)?);

        Ok(())
    }
}

/// Return DataFusion plans to calculate series that pass the
/// specified predicate, grouped according to grouped_columns
struct GroupsVisitor {
    group_columns: Vec<String>,
    plans: Vec<SeriesSetPlan>,
}

impl GroupsVisitor {
    fn new(group_columns: Vec<String>) -> Self {
        Self {
            group_columns,
            plans: Vec::new(),
        }
    }
}

impl Visitor for GroupsVisitor {
    fn pre_visit_table(
        &mut self,
        table: &Table,
        partition: &Partition,
        filter: &mut PartitionTableFilter,
    ) -> Result<()> {
        self.plans.push(table.grouped_series_set_plan(
            filter.partition_predicate(),
            &self.group_columns,
            partition,
        )?);

        Ok(())
    }
}

/// Return DataFusion plans to calculate series that pass the
/// specified predicate, grouped using the window definition
struct WindowGroupsVisitor {
    agg: Aggregate,
    every: WindowDuration,
    offset: WindowDuration,

    plans: Vec<SeriesSetPlan>,
}

impl WindowGroupsVisitor {
    fn new(agg: Aggregate, every: WindowDuration, offset: WindowDuration) -> Self {
        Self {
            agg,
            every,
            offset,
            plans: Vec::new(),
        }
    }
}

impl Visitor for WindowGroupsVisitor {
    fn pre_visit_table(
        &mut self,
        table: &Table,
        partition: &Partition,
        filter: &mut PartitionTableFilter,
    ) -> Result<()> {
        self.plans.push(table.window_grouped_series_set_plan(
            filter.partition_predicate(),
            &self.agg,
            &self.every,
            &self.offset,
            partition,
        )?);

        Ok(())
    }
}

// partition_key returns the partition key for the given line. The key will be the prefix of a
// partition name (multiple partitions can exist for each key). It uses the user defined
// partitioning rules to construct this key
pub fn partition_key(line: &ParsedLine<'_>) -> String {
    // TODO - wire this up to use partitioning rules, for now just partition by day
    let ts = line.timestamp.unwrap();
    let dt = Utc.timestamp_nanos(ts);
    dt.format("%Y-%m-%dT%H").to_string()
}

struct ArrowTable {
    name: String,
    schema: Arc<ArrowSchema>,
    data: Vec<RecordBatch>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_deps::datafusion::prelude::*;
    use query::exec::seriesset::SeriesSetItem;
    use query::{
        exec::fieldlist::{Field, FieldList},
        exec::{
            seriesset::{Error as SeriesSetError, SeriesSet},
            Executor,
        },
        predicate::PredicateBuilder,
        TSDatabase,
    };

    use arrow::{
        array::{Array, StringArray},
        datatypes::DataType,
        util::pretty::pretty_format_batches,
    };
    use influxdb_line_protocol::parse_lines;
    use test_helpers::str_pair_vec_to_vec;
    use tokio::sync::mpsc;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    fn to_set(v: &[&str]) -> BTreeSet<String> {
        v.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>()
    }

    // Abstract a bit of boilerplate around table assertions and improve the failure output.
    // The default failure message uses Debug formatting, which prints newlines as `\n`.
    // This prints the pretty_format_batches using Display so it's easier to read the tables.
    fn assert_table_eq(table: &str, partitions: &[arrow::record_batch::RecordBatch]) {
        let res = pretty_format_batches(partitions).unwrap();
        assert_eq!(table, res, "\n\nleft:\n\n{}\nright:\n\n{}", table, res);
    }

    // query the table names, with optional range predicate
    async fn table_names(db: &Db, predicate: Predicate) -> Result<StringSet> {
        let plan = db.table_names(predicate).await?;
        let executor = Executor::default();
        let s = executor.to_string_set(plan).await?;

        // unwrap it (for easy test comparisons)
        let s = Arc::try_unwrap(s).expect("only one reference to the set in the test");
        Ok(s)
    }

    #[tokio::test]
    async fn list_table_names() -> Result {
        let mut dir = test_helpers::tmp_dir()?.into_path();

        let db = Db::try_with_wal("mydb", &mut dir).await?;

        // no tables initially
        assert_eq!(
            table_names(&db, Predicate::default()).await?,
            BTreeSet::new()
        );

        // write two different tables
        let lines: Vec<_> =
            parse_lines("cpu,region=west user=23.2 10\ndisk,region=east bytes=99i 11")
                .map(|l| l.unwrap())
                .collect();
        db.write_lines(&lines).await?;

        // Now, we should see the two tables
        assert_eq!(
            table_names(&db, Predicate::default()).await?,
            to_set(&["cpu", "disk"])
        );

        Ok(())
    }

    #[tokio::test]
    async fn list_table_names_timestamps() -> Result {
        let mut dir = test_helpers::tmp_dir()?.into_path();

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
        let predicate = PredicateBuilder::default().timestamp_range(0, 201).build();
        assert_eq!(table_names(&db, predicate).await?, to_set(&["cpu", "disk"]));

        // Right before disk
        let predicate = PredicateBuilder::default().timestamp_range(0, 200).build();
        assert_eq!(table_names(&db, predicate).await?, to_set(&["cpu"]));

        // only one point of cpu
        let predicate = PredicateBuilder::default().timestamp_range(50, 101).build();
        assert_eq!(table_names(&db, predicate).await?, to_set(&["cpu"]));

        // no ranges
        let predicate = PredicateBuilder::default()
            .timestamp_range(250, 350)
            .build();
        assert_eq!(table_names(&db, predicate).await?, to_set(&[]));

        Ok(())
    }

    #[tokio::test]
    async fn missing_tags_are_null() -> Result {
        let mut dir = test_helpers::tmp_dir()?.into_path();

        let db = Db::try_with_wal("mydb", &mut dir).await?;

        // Note the `region` tag is introduced in the second line, so
        // the values in prior rows for the region column are
        // null. Likewise the `core` tag is introduced in the third
        // line so the prior columns are null
        let lines: Vec<_> = parse_lines(
            "cpu,region=west user=23.2 10\n\
                         cpu, user=10.0 11\n\
                         cpu,core=one user=10.0 11\n",
        )
        .map(|l| l.unwrap())
        .collect();
        db.write_lines(&lines).await?;

        let partitions = db.table_to_arrow("cpu", &["region", "core"]).await?;
        let columns = partitions[0].columns();

        assert_eq!(
            2,
            columns.len(),
            "Got only two columns in partiton: {:#?}",
            columns
        );

        let region_col = columns[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Get region column as a string");

        assert_eq!(region_col.len(), 3);
        assert_eq!(region_col.value(0), "west", "region_col: {:?}", region_col);
        assert!(!region_col.is_null(0), "is_null(0): {:?}", region_col);
        assert!(region_col.is_null(1), "is_null(1): {:?}", region_col);
        assert!(region_col.is_null(2), "is_null(1): {:?}", region_col);

        let host_col = columns[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Get host column as a string");

        assert_eq!(host_col.len(), 3);
        assert!(host_col.is_null(0), "is_null(0): {:?}", host_col);
        assert!(host_col.is_null(1), "is_null(1): {:?}", host_col);
        assert!(!host_col.is_null(2), "is_null(2): {:?}", host_col);
        assert_eq!(host_col.value(2), "one", "host_col: {:?}", host_col);

        Ok(())
    }

    #[tokio::test]
    async fn write_data_and_recover() -> Result {
        let mut dir = test_helpers::tmp_dir()?.into_path();

        let expected_cpu_table = r#"+--------+------+------+-------+-------------+------+------+---------+-----------+
| region | host | user | other | str         | b    | time | new_tag | new_field |
+--------+------+------+-------+-------------+------+------+---------+-----------+
| west   | A    | 23.2 | 1     | some string | true | 10   |         |           |
| west   | B    | 23.1 |       |             |      | 15   |         |           |
|        | A    |      |       |             |      | 20   | foo     | 15.1      |
+--------+------+------+-------+-------------+------+------+---------+-----------+
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

        let cpu_columns = &[
            "region",
            "host",
            "user",
            "other",
            "str",
            "b",
            "time",
            "new_tag",
            "new_field",
        ];
        let mem_columns = &["region", "host", "val", "time"];
        let disk_columns = &["region", "host", "bytes", "used_percent", "time"];

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

            let partitions = db.table_to_arrow("cpu", cpu_columns).await?;
            assert_table_eq(expected_cpu_table, &partitions);

            let partitions = db.table_to_arrow("mem", mem_columns).await?;
            assert_table_eq(expected_mem_table, &partitions);

            let partitions = db.table_to_arrow("disk", disk_columns).await?;
            assert_table_eq(expected_disk_table, &partitions);
        }

        // check that it recovers from the wal
        {
            let db = Db::restore_from_wal(&dir).await?;

            let partitions = db.table_to_arrow("cpu", cpu_columns).await?;
            assert_table_eq(expected_cpu_table, &partitions);

            let partitions = db.table_to_arrow("mem", mem_columns).await?;
            assert_table_eq(expected_mem_table, &partitions);

            let partitions = db.table_to_arrow("disk", disk_columns).await?;
            assert_table_eq(expected_disk_table, &partitions);
        }

        Ok(())
    }

    #[tokio::test]
    async fn write_and_query() -> Result {
        let db = Db::new("foo");

        let lines: Vec<_> = parse_lines("cpu,region=west,host=A user=23.2,other=1i 10")
            .map(|l| l.unwrap())
            .collect();
        db.write_lines(&lines).await?;

        let results = db.query("select * from cpu").await?;

        let expected_cpu_table = r#"+------+-------+--------+------+------+
| host | other | region | time | user |
+------+-------+--------+------+------+
| A    | 1     | west   | 10   | 23.2 |
+------+-------+--------+------+------+
"#;

        assert_table_eq(expected_cpu_table, &results);

        Ok(())
    }

    #[tokio::test]
    async fn recover_partial_entries() -> Result {
        let mut dir = test_helpers::tmp_dir()?.into_path();

        let expected_cpu_table = r#"+--------+------+------+-------+-------------+------+------+---------+-----------+
| region | host | user | other | str         | b    | time | new_tag | new_field |
+--------+------+------+-------+-------------+------+------+---------+-----------+
| west   | A    | 23.2 | 1     | some string | true | 10   |         |           |
| west   | B    | 23.1 |       |             |      | 15   |         |           |
|        | A    |      |       |             |      | 20   | foo     | 15.1      |
+--------+------+------+-------+-------------+------+------+---------+-----------+
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

        let cpu_columns = &[
            "region",
            "host",
            "user",
            "other",
            "str",
            "b",
            "time",
            "new_tag",
            "new_field",
        ];
        let mem_columns = &["region", "host", "val", "time"];
        let disk_columns = &["region", "host", "bytes", "used_percent", "time"];
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

            let partitions = db.table_to_arrow("cpu", cpu_columns).await?;
            assert_table_eq(expected_cpu_table, &partitions);

            let partitions = db.table_to_arrow("mem", mem_columns).await?;
            assert_table_eq(expected_mem_table, &partitions);

            let partitions = db.table_to_arrow("disk", disk_columns).await?;
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

            let (partitions, _stats) = restore_partitions_from_wal(wal_entries)?;

            let db = Db {
                name,
                partitions: RwLock::new(partitions),
                wal_details: None,
            };

            // some cpu
            let smaller_cpu_table = r#"+------+---------+-----------+------+
| host | new_tag | new_field | time |
+------+---------+-----------+------+
| A    | foo     | 15.1      | 20   |
+------+---------+-----------+------+
"#;
            let smaller_cpu_columns = &["host", "new_tag", "new_field", "time"];
            let partitions = db.table_to_arrow("cpu", smaller_cpu_columns).await?;
            assert_table_eq(smaller_cpu_table, &partitions);

            // all of mem
            let partitions = db.table_to_arrow("mem", mem_columns).await?;
            assert_table_eq(expected_mem_table, &partitions);

            // no disk
            let nonexistent_table = db.table_to_arrow("disk", disk_columns).await;
            assert!(nonexistent_table.is_err());
            let actual_message = format!("{:?}", nonexistent_table);
            let expected_message = "TableNameNotFoundInDictionary";
            assert!(
                actual_message.contains(expected_message),
                "Did not find '{}' in '{}'",
                expected_message,
                actual_message
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn db_partition_key() -> Result {
        let partition_keys: Vec<_> = parse_lines(
            "\
cpu user=23.2 1600107710000000000
disk bytes=23432323i 1600136510000000000",
        )
        .map(|line| partition_key(&line.unwrap()))
        .collect();

        assert_eq!(partition_keys, vec!["2020-09-14T18", "2020-09-15T02"]);

        Ok(())
    }

    #[tokio::test]
    async fn list_column_names() -> Result {
        let mut dir = test_helpers::tmp_dir()?.into_path();
        let db = Db::try_with_wal("column_namedb", &mut dir).await?;

        let lp_data = "h2o,state=CA,city=LA,county=LA temp=70.4 100\n\
                       h2o,state=MA,city=Boston,county=Suffolk temp=72.4 250\n\
                       o2,state=MA,city=Boston temp=50.4 200\n\
                       o2,state=CA temp=79.0 300\n\
                       o2,state=NY temp=60.8 400\n\
                       o2,state=NY,city=NYC temp=61.0 500\n\
                       o2,state=NY,city=NYC,borough=Brooklyn temp=61.0 600\n";

        let lines: Vec<_> = parse_lines(lp_data).map(|l| l.unwrap()).collect();
        db.write_lines(&lines).await?;

        #[derive(Debug)]
        struct TestCase<'a> {
            description: &'a str,
            predicate: Predicate,
            expected_tag_keys: Result<Vec<&'a str>>,
        };

        let test_cases = vec![
            TestCase {
                description: "No predicates",
                predicate: PredicateBuilder::default().build(),
                expected_tag_keys: Ok(vec!["borough", "city", "county", "state"]),
            },
            TestCase {
                description: "Restrictions: timestamp",
                predicate: PredicateBuilder::default()
                    .timestamp_range(150, 201)
                    .build(),
                expected_tag_keys: Ok(vec!["city", "state"]),
            },
            TestCase {
                description: "Restrictions: predicate",
                predicate: PredicateBuilder::default()
                    .add_expr(col("state").eq(lit("MA"))) // state=MA
                    .build(),
                expected_tag_keys: Ok(vec!["city", "county", "state"]),
            },
            TestCase {
                description: "Restrictions: timestamp and predicate",
                predicate: PredicateBuilder::default()
                    .timestamp_range(150, 201)
                    .add_expr(col("state").eq(lit("MA"))) // state=MA
                    .build(),
                expected_tag_keys: Ok(vec!["city", "state"]),
            },
            TestCase {
                description: "Restrictions: measurement name",
                predicate: PredicateBuilder::default().table("o2").build(),
                expected_tag_keys: Ok(vec!["borough", "city", "state"]),
            },
            TestCase {
                description: "Restrictions: measurement name and timestamp",
                predicate: PredicateBuilder::default()
                    .table("o2")
                    .timestamp_range(150, 201)
                    .build(),
                expected_tag_keys: Ok(vec!["city", "state"]),
            },
            TestCase {
                description: "Restrictions: measurement name and predicate",
                predicate: PredicateBuilder::default()
                    .table("o2")
                    .add_expr(col("state").eq(lit("NY"))) // state=NY
                    .build(),
                expected_tag_keys: Ok(vec!["borough", "city", "state"]),
            },
            TestCase {
                description: "Restrictions: measurement name, timestamp and predicate",
                predicate: PredicateBuilder::default()
                    .table("o2")
                    .timestamp_range(1, 550)
                    .add_expr(col("state").eq(lit("NY"))) // state=NY
                    .build(),
                expected_tag_keys: Ok(vec!["city", "state"]),
            },
        ];

        for test_case in test_cases.into_iter() {
            let test_case_str = format!("{:#?}", test_case);
            println!("Running test case: {:?}", test_case);

            let tag_keys_plan = db
                .tag_column_names(test_case.predicate)
                .await
                .expect("Created tag_keys plan successfully");

            // run the execution plan (
            let executor = Executor::default();
            let actual_tag_keys = executor.to_string_set(tag_keys_plan).await;

            let is_match = if let Ok(expected_tag_keys) = &test_case.expected_tag_keys {
                let expected_tag_keys = to_set(expected_tag_keys);
                if let Ok(actual_tag_keys) = &actual_tag_keys {
                    **actual_tag_keys == expected_tag_keys
                } else {
                    false
                }
            } else if let Err(e) = &actual_tag_keys {
                // use string compare to compare errors to avoid having to build exact errors
                format!("{:?}", e) == format!("{:?}", test_case.expected_tag_keys)
            } else {
                false
            };

            assert!(
                is_match,
                "Mismatch\n\
                     actual_tag_keys: \n\
                     {:?}\n\
                     expected_tag_keys: \n\
                     {:?}\n\
                     Test_case: \n\
                     {}",
                actual_tag_keys, test_case.expected_tag_keys, test_case_str
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn list_column_names_predicate() -> Result {
        // Demonstration test to show column names with predicate working

        let mut dir = test_helpers::tmp_dir()?.into_path();
        let db = Db::try_with_wal("column_namedb", &mut dir).await?;

        let lp_data = "h2o,state=CA,city=LA,county=LA temp=70.4 100\n\
                       h2o,state=MA,city=Boston,county=Suffolk temp=72.4 250\n\
                       o2,state=MA,city=Boston temp=50.4 200\n\
                       o2,state=CA temp=79.0 300\n\
                       o2,state=NY,city=NYC,borough=Brooklyn temp=60.8 400\n";

        let lines: Vec<_> = parse_lines(lp_data).map(|l| l.unwrap()).collect();
        db.write_lines(&lines).await?;

        // Predicate: state=MA
        let expr = col("state").eq(lit("MA"));
        let predicate = PredicateBuilder::default().add_expr(expr).build();

        let tag_keys_plan = db
            .tag_column_names(predicate)
            .await
            .expect("Created plan successfully");

        // run the execution plan (
        let executor = Executor::default();
        let actual_tag_keys = executor
            .to_string_set(tag_keys_plan)
            .await
            .expect("Execution of predicate plan");

        assert_eq!(to_set(&["state", "city", "county"]), *actual_tag_keys);
        Ok(())
    }

    #[tokio::test]
    async fn list_column_values() -> Result {
        let mut dir = test_helpers::tmp_dir()?.into_path();
        let db = Db::try_with_wal("column_namedb", &mut dir).await?;

        let lp_data = "h2o,state=CA,city=LA temp=70.4 100\n\
                       h2o,state=MA,city=Boston temp=72.4 250\n\
                       o2,state=MA,city=Boston temp=50.4 200\n\
                       o2,state=CA temp=79.0 300\n\
                       o2,state=NY temp=60.8 400\n";

        let lines: Vec<_> = parse_lines(lp_data).map(|l| l.unwrap()).collect();
        db.write_lines(&lines).await?;

        #[derive(Debug)]
        struct TestCase<'a> {
            description: &'a str,
            column_name: &'a str,
            predicate: Predicate,
            expected_column_values: Result<Vec<&'a str>>,
        };

        let test_cases = vec![
            TestCase {
                description: "No predicates, 'state' col",
                column_name: "state",
                predicate: PredicateBuilder::default().build(),
                expected_column_values: Ok(vec!["CA", "MA", "NY"]),
            },
            TestCase {
                description: "No predicates, 'city' col",
                column_name: "city",
                predicate: PredicateBuilder::default().build(),
                expected_column_values: Ok(vec!["Boston", "LA"]),
            },
            TestCase {
                description: "Restrictions: timestamp",
                column_name: "state",
                predicate: PredicateBuilder::default().timestamp_range(50, 201).build(),
                expected_column_values: Ok(vec!["CA", "MA"]),
            },
            TestCase {
                description: "Restrictions: predicate",
                column_name: "city",
                predicate: PredicateBuilder::default()
                    .add_expr(col("state").eq(lit("MA"))) // state=MA
                    .build(),
                expected_column_values: Ok(vec!["Boston"]),
            },
            TestCase {
                description: "Restrictions: timestamp and predicate",
                column_name: "state",
                predicate: PredicateBuilder::default()
                    .timestamp_range(150, 301)
                    .add_expr(col("state").eq(lit("MA"))) // state=MA
                    .build(),
                expected_column_values: Ok(vec!["MA"]),
            },
            TestCase {
                description: "Restrictions: measurement name",
                column_name: "state",
                predicate: PredicateBuilder::default().table("h2o").build(),
                expected_column_values: Ok(vec!["CA", "MA"]),
            },
            TestCase {
                description: "Restrictions: measurement name, with nulls",
                column_name: "city",
                predicate: PredicateBuilder::default().table("o2").build(),
                expected_column_values: Ok(vec!["Boston"]),
            },
            TestCase {
                description: "Restrictions: measurement name and timestamp",
                column_name: "state",
                predicate: PredicateBuilder::default()
                    .table("o2")
                    .timestamp_range(50, 201)
                    .build(),
                expected_column_values: Ok(vec!["MA"]),
            },
            TestCase {
                description: "Restrictions: measurement name and predicate",
                column_name: "state",
                predicate: PredicateBuilder::default()
                    .table("o2")
                    .add_expr(col("state").eq(lit("NY"))) // state=NY
                    .build(),
                expected_column_values: Ok(vec!["NY"]),
            },
            TestCase {
                description: "Restrictions: measurement name, timestamp and predicate",
                column_name: "state",
                predicate: PredicateBuilder::default()
                    .table("o2")
                    .timestamp_range(1, 550)
                    .add_expr(col("state").eq(lit("NY"))) // state=NY
                    .build(),
                expected_column_values: Ok(vec!["NY"]),
            },
            TestCase {
                description: "Restrictions: measurement name, timestamp and predicate: no match",
                column_name: "state",
                predicate: PredicateBuilder::default()
                    .table("o2")
                    .timestamp_range(1, 300) // filters out the NY row
                    .add_expr(col("state").eq(lit("NY"))) // state=NY
                    .build(),
                expected_column_values: Ok(vec![]),
            },
        ];

        for test_case in test_cases.into_iter() {
            let test_case_str = format!("{:#?}", test_case);
            println!("Running test case: {:?}", test_case);

            let column_values_plan = db
                .column_values(test_case.column_name, test_case.predicate)
                .await
                .expect("Created tag_values plan successfully");

            // run the execution plan (
            let executor = Executor::default();
            let actual_column_values = executor.to_string_set(column_values_plan).await;

            let is_match = if let Ok(expected_column_values) = &test_case.expected_column_values {
                let expected_column_values = to_set(expected_column_values);
                if let Ok(actual_column_values) = &actual_column_values {
                    **actual_column_values == expected_column_values
                } else {
                    false
                }
            } else if let Err(e) = &actual_column_values {
                // use string compare to compare errors to avoid having to build exact errors
                format!("{:?}", e) == format!("{:?}", test_case.expected_column_values)
            } else {
                false
            };

            assert!(
                is_match,
                "Mismatch\n\
                     actual_column_values: \n\
                     {:?}\n\
                     expected_column_values: \n\
                     {:?}\n\
                     Test_case: \n\
                     {}",
                actual_column_values, test_case.expected_column_values, test_case_str
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_query_series() -> Result {
        // This test checks that everything is wired together
        // correctly.  There are more detailed tests in table.rs that
        // test the generated queries.
        let mut dir = test_helpers::tmp_dir()?.into_path();
        let db = Db::try_with_wal("column_namedb", &mut dir).await?;

        let mut lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100", // to row 2
            "h2o,state=MA,city=Boston temp=72.4 250", // to row 1
            "h2o,state=CA,city=LA temp=90.0 200",     // to row 0
            "h2o,state=CA,city=LA temp=90.0 350",     // to row 3
            "o2,state=MA,city=Boston temp=50.4,reading=50 100", // to row 5
            "o2,state=MA,city=Boston temp=53.4,reading=51 250", // to row 4
        ];

        // Swap around  data is not inserted in series order
        lp_lines.swap(0, 2);
        lp_lines.swap(4, 5);

        let lp_data = lp_lines.join("\n");

        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();
        db.write_lines(&lines).await?;

        let predicate = Predicate::default();

        let plans = db
            .query_series(predicate)
            .await
            .expect("Created tag_values plan successfully");

        let results = run_and_gather_results(plans).await;

        assert_eq!(results.len(), 3);

        let series_set0 = results[0].as_ref().expect("Correctly converted");
        assert_eq!(*series_set0.table_name, "h2o");
        assert_eq!(
            series_set0.tags,
            str_pair_vec_to_vec(&[("city", "Boston"), ("state", "MA")])
        );
        assert_eq!(series_set0.timestamp_index, 3);
        assert_eq!(series_set0.field_indices, Arc::new(vec![2]));
        assert_eq!(series_set0.start_row, 0);
        assert_eq!(series_set0.num_rows, 2);

        let series_set1 = results[1].as_ref().expect("Correctly converted");
        assert_eq!(*series_set1.table_name, "h2o");
        assert_eq!(
            series_set1.tags,
            str_pair_vec_to_vec(&[("city", "LA"), ("state", "CA")])
        );
        assert_eq!(series_set1.timestamp_index, 3);
        assert_eq!(series_set1.field_indices, Arc::new(vec![2]));
        assert_eq!(series_set1.start_row, 2);
        assert_eq!(series_set1.num_rows, 2);

        let series_set2 = results[2].as_ref().expect("Correctly converted");
        assert_eq!(*series_set2.table_name, "o2");
        assert_eq!(
            series_set2.tags,
            str_pair_vec_to_vec(&[("city", "Boston"), ("state", "MA")])
        );
        assert_eq!(series_set2.timestamp_index, 4);
        assert_eq!(series_set2.field_indices, Arc::new(vec![2, 3]));
        assert_eq!(series_set2.start_row, 0);
        assert_eq!(series_set2.num_rows, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_query_series_filter() -> Result {
        // check the appropriate filters are applied in the datafusion plans
        let mut dir = test_helpers::tmp_dir()?.into_path();
        let db = Db::try_with_wal("column_namedb", &mut dir).await?;

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
            "h2o,state=CA,city=LA temp=90.0 200",
            "h2o,state=CA,city=LA temp=90.0 350",
            "o2,state=MA,city=Boston temp=50.4,reading=50 100",
            "o2,state=MA,city=Boston temp=53.4,reading=51 250",
        ];

        let lp_data = lp_lines.join("\n");

        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();
        db.write_lines(&lines).await?;

        // filter out one row in h20
        let predicate = PredicateBuilder::default()
            .timestamp_range(200, 300)
            .add_expr(col("state").eq(lit("CA"))) // state=CA
            .build();

        let plans = db
            .query_series(predicate)
            .await
            .expect("Created tag_values plan successfully");

        let results = run_and_gather_results(plans).await;

        assert_eq!(results.len(), 1);

        let series_set0 = results[0].as_ref().expect("Correctly converted");
        assert_eq!(*series_set0.table_name, "h2o");
        assert_eq!(
            series_set0.tags,
            str_pair_vec_to_vec(&[("city", "LA"), ("state", "CA")])
        );
        assert_eq!(series_set0.timestamp_index, 3);
        assert_eq!(series_set0.field_indices, Arc::new(vec![2]));
        assert_eq!(series_set0.start_row, 0);
        assert_eq!(series_set0.num_rows, 1); // only has one row!

        Ok(())
    }

    #[tokio::test]
    async fn test_query_series_pred_refers_to_column_not_in_table() -> Result {
        let mut dir = test_helpers::tmp_dir()?.into_path();
        let db = Db::try_with_wal("column_namedb", &mut dir).await?;

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];

        let lp_data = lp_lines.join("\n");

        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();
        db.write_lines(&lines).await?;

        let predicate = PredicateBuilder::default()
            .add_expr(col("tag_not_in_h20").eq(lit("foo")))
            .build();

        let plans = db
            .query_series(predicate)
            .await
            .expect("Created tag_values plan successfully");

        let results = run_and_gather_results(plans).await;
        assert!(results.is_empty());

        // predicate with no columns,
        let predicate = PredicateBuilder::default()
            .add_expr(lit("foo").eq(lit("foo")))
            .build();

        let plans = db
            .query_series(predicate)
            .await
            .expect("Created tag_values plan successfully");

        let results = run_and_gather_results(plans).await;
        assert_eq!(results.len(), 1);

        // predicate with both a column that does and does not appear
        let predicate = PredicateBuilder::default()
            .add_expr(col("state").eq(lit("MA")))
            .add_expr(col("tag_not_in_h20").eq(lit("foo")))
            .build();

        let plans = db
            .query_series(predicate)
            .await
            .expect("Created tag_values plan successfully");

        let results = run_and_gather_results(plans).await;
        assert!(results.is_empty());

        Ok(())
    }

    #[tokio::test]
    #[should_panic(
        expected = "Unsupported binary operator in expression: #state NotEq Utf8(\"MA\")"
    )]
    async fn test_query_series_pred_neq() {
        let mut dir = test_helpers::tmp_dir().unwrap().into_path();
        let db = Db::try_with_wal("column_namedb", &mut dir).await.unwrap();

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];

        let lp_data = lp_lines.join("\n");

        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();
        db.write_lines(&lines).await.unwrap();

        let predicate = PredicateBuilder::default()
            .add_expr(col("state").not_eq(lit("MA")))
            .build();

        // Should panic as the neq path isn't implemented yet
        db.query_series(predicate).await.unwrap();
    }

    #[tokio::test]
    async fn test_field_columns() -> Result {
        // Ensure that the database queries are hooked up correctly

        let mut dir = test_helpers::tmp_dir()?.into_path();
        let db = Db::try_with_wal("column_namedb", &mut dir).await?;

        let lp_data = vec![
            "h2o,state=MA,city=Boston temp=70.4 50",
            "h2o,state=MA,city=Boston other_temp=70.4 250",
            "h2o,state=CA,city=Boston other_temp=72.4 350",
            "o2,state=MA,city=Boston temp=53.4,reading=51 50",
        ]
        .join("\n");

        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();
        db.write_lines(&lines).await?;

        // write a new lp_line that is in a new day and thus a new partition
        let nanoseconds_per_day: i64 = 1_000_000_000 * 60 * 60 * 24;

        let lp_data = vec![format!(
            "h2o,state=MA,city=Boston temp=70.4,moisture=43.0 {}",
            nanoseconds_per_day * 10
        )]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();
        db.write_lines(&lines).await?;

        // ensure there are 2 partitions
        assert_eq!(db.len().await, 2);

        // setup to run the execution plan (
        let executor = Executor::default();

        let predicate = PredicateBuilder::default()
            .table("NoSuchTable")
            .add_expr(col("state").eq(lit("MA"))) // state=MA
            .build();

        // make sure table filtering works (no tables match)
        let plan = db
            .field_column_names(predicate)
            .await
            .expect("Created field_columns plan successfully");

        let fieldlists = executor
            .to_fieldlist(plan)
            .await
            .expect("Running fieldlist plan");
        assert!(fieldlists.fields.is_empty());

        // get only fields from h20 (but both partitions)
        let predicate = PredicateBuilder::default()
            .table("h2o")
            .add_expr(col("state").eq(lit("MA"))) // state=MA
            .build();

        let plan = db
            .field_column_names(predicate)
            .await
            .expect("Created field_columns plan successfully");

        let actual = executor
            .to_fieldlist(plan)
            .await
            .expect("Running fieldlist plan");

        let expected = FieldList {
            fields: vec![
                Field {
                    name: "moisture".into(),
                    data_type: DataType::Float64,
                    last_timestamp: nanoseconds_per_day * 10,
                },
                Field {
                    name: "other_temp".into(),
                    data_type: DataType::Float64,
                    last_timestamp: 250,
                },
                Field {
                    name: "temp".into(),
                    data_type: DataType::Float64,
                    last_timestamp: nanoseconds_per_day * 10,
                },
            ],
        };

        assert_eq!(
            expected, actual,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected, actual
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_field_columns_timestamp_predicate() -> Result {
        // check the appropriate filters are applied in the datafusion plans
        let mut dir = test_helpers::tmp_dir()?.into_path();
        let db = Db::try_with_wal("column_namedb", &mut dir).await?;

        let lp_data = vec![
            "h2o,state=MA,city=Boston temp=70.4 50",
            "h2o,state=MA,city=Boston other_temp=70.4 250",
            "h2o,state=CA,city=Boston other_temp=72.4 350",
            "o2,state=MA,city=Boston temp=53.4,reading=51 50",
        ]
        .join("\n");

        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();
        db.write_lines(&lines).await?;

        // setup to run the execution plan (
        let executor = Executor::default();

        let predicate = PredicateBuilder::default()
            .table("h2o")
            .timestamp_range(200, 300)
            .add_expr(col("state").eq(lit("MA"))) // state=MA
            .build();

        let plan = db
            .field_column_names(predicate)
            .await
            .expect("Created field_columns plan successfully");

        let actual = executor
            .to_fieldlist(plan)
            .await
            .expect("Running fieldlist plan");

        // Should only have other_temp as a field
        let expected = FieldList {
            fields: vec![Field {
                name: "other_temp".into(),
                data_type: DataType::Float64,
                last_timestamp: 250,
            }],
        };

        assert_eq!(
            expected, actual,
            "Expected:\n{:#?}\nActual:\n{:#?}",
            expected, actual
        );

        Ok(())
    }

    /// Run the plan and gather the results in a order that can be compared
    async fn run_and_gather_results(
        plans: SeriesSetPlans,
    ) -> Vec<Result<SeriesSet, SeriesSetError>> {
        // Use a channel sufficiently large to buffer the series
        let (tx, mut rx) = mpsc::channel(100);

        // setup to run the execution plan (
        let executor = Executor::default();
        executor
            .to_series_set(plans, tx)
            .await
            .expect("Running series set plan");

        // gather up the sets and compare them
        let mut results = Vec::new();
        while let Some(r) = rx.recv().await {
            results.push(r.map(|item| {
                if let SeriesSetItem::Data(series_set) = item {
                    series_set
                }
                else {
                    panic!("Unexpected result from converting. Expected SeriesSetItem::Data, got: {:?}", item)
                }
            })
            );
        }

        // sort the results so that we can reliably compare
        results.sort_by(|r1, r2| {
            match (r1, r2) {
                (Ok(r1), Ok(r2)) => r1
                    .table_name
                    .cmp(&r2.table_name)
                    .then(r1.tags.cmp(&r2.tags)),
                // default sort by string representation
                (r1, r2) => format!("{:?}", r1).cmp(&format!("{:?}", r2)),
            }
        });

        // Print to stdout / test log to facilitate debugging if fails on CI
        println!("The results are: {:#?}", results);

        results
    }
}
