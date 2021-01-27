use generated_types::wal;
use query::group_by::Aggregate;
use query::group_by::GroupByAndAggregate;
use query::group_by::WindowDuration;
use query::{
    exec::{stringset::StringSet, FieldListPlan, SeriesSetPlan, SeriesSetPlans, StringSetPlan},
    predicate::Predicate,
    Database,
};

use crate::column::Column;
use crate::table::Table;
use crate::{
    chunk::{Chunk, ChunkPredicate},
    partition::Partition,
};

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use arrow_deps::datafusion::{error::DataFusionError, logical_plan::LogicalPlan};
use data_types::data::ReplicatedWrite;

use crate::dictionary::Error as DictionaryError;

use async_trait::async_trait;
use snafu::{ResultExt, Snafu};
use tokio::sync::RwLock;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error in {}: {}", source_module, source))]
    PassThrough {
        source_module: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display("Table name {} not found in dictionary of chunk {}", table, chunk))]
    TableNameNotFoundInDictionary {
        table: String,
        chunk: u64,
        source: DictionaryError,
    },

    #[snafu(display(
        "Column name {} not found in dictionary of chunk {}",
        column_name,
        chunk
    ))]
    ColumnNameNotFoundInDictionary {
        column_name: String,
        chunk: u64,
        source: DictionaryError,
    },

    #[snafu(display("Column ID {} not found in dictionary of chunk {}", column_id, chunk))]
    ColumnIdNotFoundInDictionary {
        column_id: u32,
        chunk: u64,
        source: DictionaryError,
    },

    #[snafu(display("Value ID {} not found in dictionary of chunk {}", value_id, chunk))]
    ColumnValueIdNotFoundInDictionary {
        value_id: u32,
        chunk: u64,
        source: DictionaryError,
    },

    #[snafu(display(
        "Column '{}' is not a tag column and thus can not list values",
        column_name
    ))]
    UnsupportedColumnTypeForListingValues { column_name: String },

    #[snafu(display("id conversion error"))]
    IdConversionError { source: std::num::TryFromIntError },

    #[snafu(display("error executing query {}: {}", query, source))]
    QueryError {
        query: String,
        source: DataFusionError,
    },

    #[snafu(display("Error dropping chunk from partition '{}': {}", partition_key, source))]
    DroppingChunk {
        partition_key: String,
        source: crate::partition::Error,
    },

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

impl From<crate::chunk::Error> for Error {
    fn from(e: crate::chunk::Error) -> Self {
        Self::PassThrough {
            source_module: "Chunk",
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
/// This implements the mutable buffer. See the module doc comments
/// for more details.
pub struct MutableBufferDb {
    pub name: String,

    /// Maps partition keys to partitions which hold the actual data
    partitions: RwLock<HashMap<String, Arc<RwLock<Partition>>>>,
}

impl MutableBufferDb {
    /// New creates a new in-memory only write buffer database
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Directs the writes from batch into the appropriate partitions
    async fn write_entries_to_partitions(&self, batch: &wal::WriteBufferBatch<'_>) -> Result<()> {
        if let Some(entries) = batch.entries() {
            for entry in entries {
                let key = entry
                    .partition_key()
                    .expect("partition key should have been inserted");

                let partition = self.get_partition(key).await;
                let mut partition = partition.write().await;
                partition.write_entry(&entry)?
            }
        }

        Ok(())
    }

    /// Rolls over the active chunk in this partititon
    pub async fn rollover_partition(&self, partition_key: &str) -> Result<Arc<Chunk>> {
        let partition = self.get_partition(partition_key).await;
        let mut partition = partition.write().await;
        Ok(partition.rollover_chunk())
    }

    /// return the specified chunk from the partition
    /// Returns None if no such chunk exists.
    pub async fn get_chunk(&self, partition_key: &str, chunk_id: u32) -> Option<Arc<Chunk>> {
        self.get_partition(partition_key)
            .await
            .read()
            .await
            .get_chunk(chunk_id)
            .ok()
    }

    /// drop the the specified chunk from the partition
    pub async fn drop_chunk(&self, partition_key: &str, chunk_id: u32) -> Result<Arc<Chunk>> {
        self.get_partition(partition_key)
            .await
            .write()
            .await
            .drop_chunk(chunk_id)
            .context(DroppingChunk { partition_key })
    }
}

#[async_trait]
impl Database for MutableBufferDb {
    type Error = Error;
    type Chunk = Chunk;

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

        Ok(())
    }

    // return all column names in this database, while applying optional predicates
    async fn tag_column_names(&self, predicate: Predicate) -> Result<StringSetPlan, Self::Error> {
        let has_exprs = predicate.has_exprs();
        let mut filter = ChunkTableFilter::new(predicate);

        if has_exprs {
            let mut visitor = NamePredVisitor::new();
            self.accept(&mut filter, &mut visitor).await?;
            Ok(visitor.plans.into())
        } else {
            let mut visitor = NameVisitor::new();
            self.accept(&mut filter, &mut visitor).await?;
            Ok(visitor.column_names.into())
        }
    }

    /// return all field names in this database, while applying optional
    /// predicates
    async fn field_column_names(&self, predicate: Predicate) -> Result<FieldListPlan, Self::Error> {
        let mut filter = ChunkTableFilter::new(predicate);
        let mut visitor = TableFieldPredVisitor::new();
        self.accept(&mut filter, &mut visitor).await?;
        Ok(visitor.into_fieldlist_plan())
    }

    /// return all column values in this database, while applying optional
    /// predicates
    async fn column_values(
        &self,
        column_name: &str,
        predicate: Predicate,
    ) -> Result<StringSetPlan, Self::Error> {
        let has_exprs = predicate.has_exprs();
        let mut filter = ChunkTableFilter::new(predicate);

        if has_exprs {
            let mut visitor = ValuePredVisitor::new(column_name);
            self.accept(&mut filter, &mut visitor).await?;
            Ok(visitor.plans.into())
        } else {
            let mut visitor = ValueVisitor::new(column_name);
            self.accept(&mut filter, &mut visitor).await?;
            Ok(visitor.column_values.into())
        }
    }

    async fn query_series(&self, predicate: Predicate) -> Result<SeriesSetPlans, Self::Error> {
        let mut filter = ChunkTableFilter::new(predicate);
        let mut visitor = SeriesVisitor::new();
        self.accept(&mut filter, &mut visitor).await?;
        Ok(visitor.plans.into())
    }

    async fn query_groups(
        &self,
        predicate: Predicate,
        gby_agg: GroupByAndAggregate,
    ) -> Result<SeriesSetPlans, Self::Error> {
        let mut filter = ChunkTableFilter::new(predicate);

        match gby_agg {
            GroupByAndAggregate::Columns { agg, group_columns } => {
                // Add any specified groups as predicate columns (so we
                // can skip tables without those tags)
                let mut filter = filter.add_required_columns(&group_columns);
                let mut visitor = GroupsVisitor::new(agg, group_columns);
                self.accept(&mut filter, &mut visitor).await?;
                Ok(visitor.plans.into())
            }
            GroupByAndAggregate::Window { agg, every, offset } => {
                let mut visitor = WindowGroupsVisitor::new(agg, every, offset);
                self.accept(&mut filter, &mut visitor).await?;
                Ok(visitor.plans.into())
            }
        }
    }

    /// Return the partition keys for data in this DB
    async fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        let partitions = self.partitions.read().await;
        let keys = partitions.keys().cloned().collect();
        Ok(keys)
    }

    /// Return the list of chunks, in order of id, for the specified
    /// partition_key
    async fn chunks(&self, partition_key: &str) -> Vec<Arc<Chunk>> {
        self.get_partition(partition_key)
            .await
            .read()
            .await
            .chunks()
    }
}

/// This trait is used to implement a "Visitor" pattern for Database
/// which can be used to define logic that shares a common Depth First
/// Search (DFS) traversal of the Database --> Chunk --> Table -->
/// Column datastructure heirarchy.
///
/// Specifically, if we had a database like the following:
///
/// YesterdayPartition
///   Chunk1
///     CPU Table1
///      Col1
///   Chunk2
///     CPU Table1
///      Col2
///  TodayPartition
///   Chunk3
///     CPU Table3
///      Col3
///
/// Then the methods would be invoked in the following order
///
///  visitor.pre_visit_partition(YesterdayPartition)
///  visitor.pre_visit_chunk(Chunk1)
///  visitor.pre_visit_table(CPU Table1)
///  visitor.visit_column(Col1)
///  visitor.post_visit_table(CPU Table1)
///  visitor.post_visit_chunk(Chunk1)
///  visitor.pre_visit_chunk(Chunk2)
///  visitor.pre_visit_table(CPU Table2)
///  visitor.visit_column(Col2)
///  visitor.post_visit_table(CPU Table2)
///  visitor.post_visit_chunk(Chunk2)
///  visitor.pre_visit_partition(TodayPartition)
///  visitor.pre_visit_chunk(Chunk3)
///  visitor.pre_visit_table(CPU Table3)
///  visitor.visit_column(Col3)
///  visitor.post_visit_table(CPU Table3)
///  visitor.post_visit_chunk(Chunk3)
trait Visitor {
    // called once before any chunk in a partition is visisted
    fn pre_visit_partition(&mut self, _partition: &Partition) -> Result<()> {
        Ok(())
    }

    // called once before any column in a chunk is visisted
    fn pre_visit_chunk(&mut self, _chunk: &Chunk) -> Result<()> {
        Ok(())
    }

    // called once before any column in a Table is visited
    fn pre_visit_table(
        &mut self,
        _table: &Table,
        _chunk: &Chunk,
        _filter: &mut ChunkTableFilter,
    ) -> Result<()> {
        Ok(())
    }

    // called every time a column is visited
    fn visit_column(
        &mut self,
        _table: &Table,
        _column_id: u32,
        _column: &Column,
        _filter: &mut ChunkTableFilter,
    ) -> Result<()> {
        Ok(())
    }

    // called once after all columns in a Table are visited
    fn post_visit_table(&mut self, _table: &Table, _chunk: &Chunk) -> Result<()> {
        Ok(())
    }

    // called once after all columns in a chunk is visited
    fn post_visit_chunk(&mut self, _chunk: &Chunk) -> Result<()> {
        Ok(())
    }
}

impl MutableBufferDb {
    /// returns the number of partitions in this database
    pub async fn len(&self) -> usize {
        self.partitions.read().await.len()
    }

    /// returns true if the database has no partititons
    pub async fn is_empty(&self) -> bool {
        self.partitions.read().await.is_empty()
    }

    /// Retrieve (or create) the partition for the specified partition key
    async fn get_partition(&self, partition_key: &str) -> Arc<RwLock<Partition>> {
        // until we think this code is likely to be a contention hot
        // spot, simply use a write lock even when often a read lock
        // would do.
        let mut partitions = self.partitions.write().await;

        if let Some(partition) = partitions.get(partition_key) {
            partition.clone()
        } else {
            let partition = Arc::new(RwLock::new(Partition::new(partition_key)));
            partitions.insert(partition_key.to_string(), partition.clone());
            partition
        }
    }

    /// get a snapshot of all the current partitions -- useful so that
    /// while doing stuff with one partition we don't prevent creating
    /// new partitions
    ///
    /// Note that since we don't hold the lock on self.partitions
    /// after this returns, new partitions can be added, and some
    /// partitions in the snapshot could be dropped from the overall
    /// database
    async fn partition_snapshot(&self) -> Vec<Arc<RwLock<Partition>>> {
        let partitions = self.partitions.read().await;
        partitions.values().cloned().collect()
    }

    /// Traverse this database's tables, calling the relevant
    /// functions, in order, of `visitor`, as described on the Visitor
    /// trait.
    ///
    /// Skips visiting any table or columns of `filter.should_visit_table`
    /// returns false
    async fn accept<V: Visitor>(
        &self,
        filter: &mut ChunkTableFilter,
        visitor: &mut V,
    ) -> Result<()> {
        for partition in self.partition_snapshot().await.into_iter() {
            let partition = partition.read().await;

            if filter.should_visit_partition(&partition)? {
                for chunk in partition.iter() {
                    visitor.pre_visit_chunk(chunk)?;
                    filter.pre_visit_chunk(chunk)?;

                    for table in chunk.tables.values() {
                        if filter.should_visit_table(table)? {
                            visitor.pre_visit_table(table, chunk, filter)?;

                            for (column_id, column_index) in &table.column_id_to_index {
                                visitor.visit_column(
                                    table,
                                    *column_id,
                                    &table.columns[*column_index],
                                    filter,
                                )?
                            }

                            visitor.post_visit_table(table, chunk)?;
                        }
                    }
                    visitor.post_visit_chunk(chunk)?;
                }
            }
        } // next chunk

        Ok(())
    }
}

/// Common logic for processing and filtering tables in the mutable buffer
///
/// Note that since each chunk has its own dictionary, mappings
/// between Strings --> we cache the String->id mappings per chunk
///
/// b) the table doesn't have a column range that overlaps the
/// predicate values, e.g., if you have env = "us-west" and a
/// table's env column has the range ["eu-south", "us-north"].
#[derive(Debug)]
struct ChunkTableFilter {
    predicate: Predicate,

    /// If specififed, only tables with all specified columns will be
    /// visited. Note that just becuase a table has all these columns,
    /// it might not be visited for other reasons (e.g. it is filted
    /// out by a table_name predicate)
    additional_required_columns: Option<HashSet<String>>,

    /// A 'compiled' version of the predicate to evaluate on tables /
    /// columns in a particular chunk during the walk
    chunk_predicate: Option<ChunkPredicate>,
}

impl ChunkTableFilter {
    fn new(predicate: Predicate) -> Self {
        Self {
            predicate,
            additional_required_columns: None,
            chunk_predicate: None,
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

    /// Called when each chunk gets visited. Since ids are
    /// specific to each partitition, the predicates much get
    /// translated each time.
    fn pre_visit_chunk(&mut self, chunk: &Chunk) -> Result<()> {
        let mut chunk_predicate = chunk.compile_predicate(&self.predicate)?;

        // add any additional column needs
        if let Some(additional_required_columns) = &self.additional_required_columns {
            chunk.add_required_columns_to_predicate(
                additional_required_columns,
                &mut chunk_predicate,
            );
        }

        self.chunk_predicate = Some(chunk_predicate);

        Ok(())
    }

    /// If returns false, skips visiting _table and all its columns
    fn should_visit_table(&mut self, table: &Table) -> Result<bool> {
        Ok(table.could_match_predicate(self.chunk_predicate())?)
    }

    /// If returns false, skips visiting partition
    fn should_visit_partition(&mut self, partition: &Partition) -> Result<bool> {
        match &self.predicate.partition_key {
            Some(partition_key) => Ok(partition.key() == partition_key),
            None => Ok(true),
        }
    }

    pub fn chunk_predicate(&self) -> &ChunkPredicate {
        self.chunk_predicate
            .as_ref()
            .expect("Visited chunk to compile predicate")
    }
}

/// return all column names in this database, while applying only the
/// timestamp range (has no general purpose predicates)
struct NameVisitor {
    column_names: StringSet,
    chunk_column_ids: BTreeSet<u32>,
}

impl NameVisitor {
    fn new() -> Self {
        Self {
            column_names: StringSet::new(),
            chunk_column_ids: BTreeSet::new(),
        }
    }
}

impl Visitor for NameVisitor {
    fn visit_column(
        &mut self,
        table: &Table,
        column_id: u32,
        column: &Column,
        filter: &mut ChunkTableFilter,
    ) -> Result<()> {
        if let Column::Tag(column, _) = column {
            if table.column_matches_predicate(column, filter.chunk_predicate())? {
                self.chunk_column_ids.insert(column_id);
            }
        }
        Ok(())
    }

    fn pre_visit_chunk(&mut self, _chunk: &Chunk) -> Result<()> {
        self.chunk_column_ids.clear();
        Ok(())
    }

    fn post_visit_chunk(&mut self, chunk: &Chunk) -> Result<()> {
        // convert all the chunk's column_ids to Strings
        for &column_id in &self.chunk_column_ids {
            let column_name =
                chunk
                    .dictionary
                    .lookup_id(column_id)
                    .context(ColumnIdNotFoundInDictionary {
                        column_id,
                        chunk: chunk.id,
                    })?;

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
        chunk: &Chunk,
        filter: &mut ChunkTableFilter,
    ) -> Result<()> {
        self.plans
            .push(table.tag_column_names_plan(filter.chunk_predicate(), chunk)?);
        Ok(())
    }
}

/// return a plan that selects all values from field columns after
/// applying timestamp and other predicates
#[derive(Debug)]
struct TableFieldPredVisitor {
    // As Each table can be spread across multiple Chunks, we
    // collect all the relevant plans and Union them together.
    plans: Vec<LogicalPlan>,
}

impl Visitor for TableFieldPredVisitor {
    fn pre_visit_table(
        &mut self,
        table: &Table,
        chunk: &Chunk,
        filter: &mut ChunkTableFilter,
    ) -> Result<()> {
        self.plans
            .push(table.field_names_plan(filter.chunk_predicate(), chunk)?);
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
/// futures) for each chunk / table, rather than a single one
/// -- but that will require building up parallel hash tables.
struct ValueVisitor<'a> {
    column_name: &'a str,
    // what column id we are looking for
    column_id: Option<u32>,
    chunk_value_ids: BTreeSet<u32>,
    column_values: StringSet,
}

impl<'a> ValueVisitor<'a> {
    fn new(column_name: &'a str) -> Self {
        Self {
            column_name,
            column_id: None,
            column_values: StringSet::new(),
            chunk_value_ids: BTreeSet::new(),
        }
    }
}

impl<'a> Visitor for ValueVisitor<'a> {
    fn pre_visit_chunk(&mut self, chunk: &Chunk) -> Result<()> {
        self.chunk_value_ids.clear();

        self.column_id = Some(chunk.dictionary.lookup_value(self.column_name).context(
            ColumnNameNotFoundInDictionary {
                column_name: self.column_name,
                chunk: chunk.id,
            },
        )?);

        Ok(())
    }

    fn visit_column(
        &mut self,
        table: &Table,
        column_id: u32,
        column: &Column,
        filter: &mut ChunkTableFilter,
    ) -> Result<()> {
        if Some(column_id) != self.column_id {
            return Ok(());
        }

        match column {
            Column::Tag(column, _) => {
                // if we have a timestamp prediate, find all values
                // where the timestamp is within range. Otherwise take
                // all values.
                let chunk_predicate = filter.chunk_predicate();
                match chunk_predicate.range {
                    None => {
                        // take all non-null values
                        column.iter().filter_map(|&s| s).for_each(|value_id| {
                            self.chunk_value_ids.insert(value_id);
                        });
                    }
                    Some(range) => {
                        // filter out all values that don't match the timestmap
                        let time_column = table.column_i64(chunk_predicate.time_column_id)?;

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
                                self.chunk_value_ids.insert(value_id);
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

    fn post_visit_chunk(&mut self, chunk: &Chunk) -> Result<()> {
        // convert all the chunk's column_ids to Strings
        for &value_id in &self.chunk_value_ids {
            let value = chunk.dictionary.lookup_id(value_id).context(
                ColumnValueIdNotFoundInDictionary {
                    value_id,
                    chunk: chunk.id,
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
        chunk: &Chunk,
        filter: &mut ChunkTableFilter,
    ) -> Result<()> {
        // skip table entirely if there are no rows that fall in the timestamp
        if table.could_match_predicate(filter.chunk_predicate())? {
            self.plans.push(table.tag_values_plan(
                self.column_name,
                filter.chunk_predicate(),
                chunk,
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
        chunk: &Chunk,
        filter: &mut ChunkTableFilter,
    ) -> Result<()> {
        self.plans
            .push(table.series_set_plan(filter.chunk_predicate(), chunk)?);

        Ok(())
    }
}

/// Return DataFusion plans to calculate series that pass the
/// specified predicate, grouped according to grouped_columns
struct GroupsVisitor {
    agg: Aggregate,
    group_columns: Vec<String>,
    plans: Vec<SeriesSetPlan>,
}

impl GroupsVisitor {
    fn new(agg: Aggregate, group_columns: Vec<String>) -> Self {
        Self {
            agg,
            group_columns,
            plans: Vec::new(),
        }
    }
}

impl Visitor for GroupsVisitor {
    fn pre_visit_table(
        &mut self,
        table: &Table,
        chunk: &Chunk,
        filter: &mut ChunkTableFilter,
    ) -> Result<()> {
        self.plans.push(table.grouped_series_set_plan(
            filter.chunk_predicate(),
            self.agg,
            &self.group_columns,
            chunk,
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
        chunk: &Chunk,
        filter: &mut ChunkTableFilter,
    ) -> Result<()> {
        self.plans.push(table.window_grouped_series_set_plan(
            filter.chunk_predicate(),
            self.agg,
            &self.every,
            &self.offset,
            chunk,
        )?);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::chunk::ChunkSelection;

    use super::*;
    use query::{
        exec::fieldlist::{Field, FieldList},
        exec::{
            field::FieldIndexes,
            seriesset::{Error as SeriesSetError, SeriesSet, SeriesSetItem},
            Executor,
        },
        frontend::sql::SQLQueryPlanner,
        predicate::PredicateBuilder,
        Database,
    };

    use arrow_deps::{
        arrow::{
            array::{Array, StringArray},
            datatypes::DataType,
            record_batch::RecordBatch,
        },
        assert_table_eq,
        datafusion::{physical_plan::collect, prelude::*},
    };
    use influxdb_line_protocol::{parse_lines, ParsedLine};
    use test_helpers::{assert_contains, str_pair_vec_to_vec};
    use tokio::sync::mpsc;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    fn to_set(v: &[&str]) -> BTreeSet<String> {
        v.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>()
    }

    #[tokio::test]
    async fn missing_tags_are_null() -> Result {
        let db = MutableBufferDb::new("mydb");

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
        write_lines(&db, &lines).await;

        let partition_key = "1970-01-01T00";

        let chunk = db.get_chunk(partition_key, 0).await.unwrap();
        let mut batches = Vec::new();
        let selection = ChunkSelection::Some(&["region", "core"]);
        chunk
            .table_to_arrow(&mut batches, "cpu", selection)
            .unwrap();
        let columns = batches[0].columns();

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
    async fn write_and_query() -> Result {
        let db = MutableBufferDb::new("foo");

        let lines: Vec<_> = parse_lines("cpu,region=west,host=A user=23.2,other=1i 10")
            .map(|l| l.unwrap())
            .collect();
        write_lines(&db, &lines).await;

        let results = run_sql_query(&db, "select * from cpu").await;

        let expected_cpu_table = &[
            "+------+-------+--------+------+------+",
            "| host | other | region | time | user |",
            "+------+-------+--------+------+------+",
            "| A    | 1     | west   | 10   | 23.2 |",
            "+------+-------+--------+------+------+",
        ];

        assert_table_eq!(expected_cpu_table, &results);

        Ok(())
    }

    #[tokio::test]
    async fn list_column_names() -> Result {
        let db = MutableBufferDb::new("column_namedb");

        let lp_data = "h2o,state=CA,city=LA,county=LA temp=70.4 100\n\
                       h2o,state=MA,city=Boston,county=Suffolk temp=72.4 250\n\
                       o2,state=MA,city=Boston temp=50.4 200\n\
                       o2,state=CA temp=79.0 300\n\
                       o2,state=NY temp=60.8 400\n\
                       o2,state=NY,city=NYC temp=61.0 500\n\
                       o2,state=NY,city=NYC,borough=Brooklyn temp=61.0 600\n";

        let lines: Vec<_> = parse_lines(lp_data).map(|l| l.unwrap()).collect();
        write_lines(&db, &lines).await;

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

        let db = MutableBufferDb::new("column_namedb");

        let lp_data = "h2o,state=CA,city=LA,county=LA temp=70.4 100\n\
                       h2o,state=MA,city=Boston,county=Suffolk temp=72.4 250\n\
                       o2,state=MA,city=Boston temp=50.4 200\n\
                       o2,state=CA temp=79.0 300\n\
                       o2,state=NY,city=NYC,borough=Brooklyn temp=60.8 400\n";

        let lines: Vec<_> = parse_lines(lp_data).map(|l| l.unwrap()).collect();
        write_lines(&db, &lines).await;

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
        let db = MutableBufferDb::new("column_namedb");

        let lp_data = "h2o,state=CA,city=LA temp=70.4 100\n\
                       h2o,state=MA,city=Boston temp=72.4 250\n\
                       o2,state=MA,city=Boston temp=50.4 200\n\
                       o2,state=CA temp=79.0 300\n\
                       o2,state=NY temp=60.8 400\n";

        let lines: Vec<_> = parse_lines(lp_data).map(|l| l.unwrap()).collect();
        write_lines(&db, &lines).await;

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
        let db = MutableBufferDb::new("column_namedb");

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
        write_lines(&db, &lines).await;

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
        assert_eq!(
            series_set0.field_indexes,
            FieldIndexes::from_timestamp_and_value_indexes(3, &[2])
        );
        assert_eq!(series_set0.start_row, 0);
        assert_eq!(series_set0.num_rows, 2);

        let series_set1 = results[1].as_ref().expect("Correctly converted");
        assert_eq!(*series_set1.table_name, "h2o");
        assert_eq!(
            series_set1.tags,
            str_pair_vec_to_vec(&[("city", "LA"), ("state", "CA")])
        );
        assert_eq!(
            series_set1.field_indexes,
            FieldIndexes::from_timestamp_and_value_indexes(3, &[2])
        );
        assert_eq!(series_set1.start_row, 2);
        assert_eq!(series_set1.num_rows, 2);

        let series_set2 = results[2].as_ref().expect("Correctly converted");
        assert_eq!(*series_set2.table_name, "o2");
        assert_eq!(
            series_set2.tags,
            str_pair_vec_to_vec(&[("city", "Boston"), ("state", "MA")])
        );
        assert_eq!(
            series_set2.field_indexes,
            FieldIndexes::from_timestamp_and_value_indexes(4, &[2, 3])
        );
        assert_eq!(series_set2.start_row, 0);
        assert_eq!(series_set2.num_rows, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_query_series_filter() -> Result {
        // check the appropriate filters are applied in the datafusion plans
        let db = MutableBufferDb::new("column_namedb");

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
        write_lines(&db, &lines).await;

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
        assert_eq!(
            series_set0.field_indexes,
            FieldIndexes::from_timestamp_and_value_indexes(3, &[2])
        );
        assert_eq!(series_set0.start_row, 0);
        assert_eq!(series_set0.num_rows, 1); // only has one row!

        Ok(())
    }

    #[tokio::test]
    async fn test_query_series_pred_refers_to_column_not_in_table() -> Result {
        let db = MutableBufferDb::new("column_namedb");

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];

        let lp_data = lp_lines.join("\n");

        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();
        write_lines(&db, &lines).await;

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
    async fn test_query_series_pred_neq() {
        let db = MutableBufferDb::new("column_namedb");

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];

        let lp_data = lp_lines.join("\n");

        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();
        write_lines(&db, &lines).await;

        let predicate = PredicateBuilder::default()
            .add_expr(col("state").not_eq(lit("MA")))
            .build();

        // Should err as the neq path isn't implemented yet
        let err = db.query_series(predicate).await.unwrap_err();
        assert_contains!(
            err.to_string(),
            "Operator NotEq not yet supported in IOx MutableBuffer"
        );
    }

    #[tokio::test]
    async fn test_field_columns() -> Result {
        // Ensure that the database queries are hooked up correctly

        let db = MutableBufferDb::new("column_namedb");

        let lp_data = vec![
            "h2o,state=MA,city=Boston temp=70.4 50",
            "h2o,state=MA,city=Boston other_temp=70.4 250",
            "h2o,state=CA,city=Boston other_temp=72.4 350",
            "o2,state=MA,city=Boston temp=53.4,reading=51 50",
        ]
        .join("\n");

        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();
        write_lines(&db, &lines).await;

        // write a new lp_line that is in a new day and thus a new partititon
        let nanoseconds_per_day: i64 = 1_000_000_000 * 60 * 60 * 24;

        let lp_data = vec![format!(
            "h2o,state=MA,city=Boston temp=70.4,moisture=43.0 {}",
            nanoseconds_per_day * 10
        )]
        .join("\n");
        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();
        write_lines(&db, &lines).await;

        // ensure there are 2 chunks
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
            .to_field_list(plan)
            .await
            .expect("Running fieldlist plan");
        assert!(fieldlists.fields.is_empty());

        // get only fields from h20 (but both chunks)
        let predicate = PredicateBuilder::default()
            .table("h2o")
            .add_expr(col("state").eq(lit("MA"))) // state=MA
            .build();

        let plan = db
            .field_column_names(predicate)
            .await
            .expect("Created field_columns plan successfully");

        let actual = executor
            .to_field_list(plan)
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
        let db = MutableBufferDb::new("column_namedb");

        let lp_data = vec![
            "h2o,state=MA,city=Boston temp=70.4 50",
            "h2o,state=MA,city=Boston other_temp=70.4 250",
            "h2o,state=CA,city=Boston other_temp=72.4 350",
            "o2,state=MA,city=Boston temp=53.4,reading=51 50",
        ]
        .join("\n");

        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();
        write_lines(&db, &lines).await;

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
            .to_field_list(plan)
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

    /// write lines into this database
    async fn write_lines(database: &MutableBufferDb, lines: &[ParsedLine<'_>]) {
        let mut writer = query::test::TestLPWriter::default();
        writer.write_lines(database, lines).await.unwrap()
    }

    async fn run_sql_query(database: &MutableBufferDb, query: &str) -> Vec<RecordBatch> {
        let planner = SQLQueryPlanner::default();
        let executor = Executor::new();

        let physical_plan = planner.query(database, query, &executor).await.unwrap();

        collect(physical_plan).await.unwrap()
    }
}
