use data_types::{
    data::ReplicatedWrite,
    database_rules::{PartitionSort, PartitionSortRules},
};
use generated_types::wal;
use query::group_by::Aggregate;
use query::group_by::GroupByAndAggregate;
use query::group_by::WindowDuration;
use query::{
    exec::{SeriesSetPlan, SeriesSetPlans},
    predicate::Predicate,
    Database,
};

use crate::column::Column;
use crate::table::Table;
use crate::{
    chunk::{Chunk, ChunkPredicate},
    partition::Partition,
};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_deps::datafusion::error::DataFusionError;

use crate::dictionary::Error as DictionaryError;

use async_trait::async_trait;
use data_types::database_rules::Order;
use snafu::{ResultExt, Snafu};
use std::sync::RwLock;

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
    fn write_entries_to_partitions(&self, batch: &wal::WriteBufferBatch<'_>) -> Result<()> {
        if let Some(entries) = batch.entries() {
            for entry in entries {
                let key = entry
                    .partition_key()
                    .expect("partition key should have been inserted");

                let partition = self.get_partition(key);
                let mut partition = partition.write().expect("mutex poisoned");
                partition.write_entry(&entry)?
            }
        }

        Ok(())
    }

    /// Rolls over the active chunk in this partititon
    pub fn rollover_partition(&self, partition_key: &str) -> Result<Arc<Chunk>> {
        let partition = self.get_partition(partition_key);
        let mut partition = partition.write().expect("mutex poisoned");
        Ok(partition.rollover_chunk())
    }

    /// return the specified chunk from the partition
    /// Returns None if no such chunk exists.
    pub fn get_chunk(&self, partition_key: &str, chunk_id: u32) -> Option<Arc<Chunk>> {
        let partition = self.get_partition(partition_key);
        let partition = partition.read().expect("mutex poisoned");
        partition.get_chunk(chunk_id).ok()
    }

    /// drop the the specified chunk from the partition
    pub fn drop_chunk(&self, partition_key: &str, chunk_id: u32) -> Result<Arc<Chunk>> {
        let partition = self.get_partition(partition_key);
        let mut partition = partition.write().expect("mutex poisoned");
        partition
            .drop_chunk(chunk_id)
            .context(DroppingChunk { partition_key })
    }

    /// drop the specified partition
    pub fn drop_partition(&self, partition_key: &str) -> Option<Arc<RwLock<Partition>>> {
        self.partitions
            .write()
            .expect("mutex poisoned")
            .remove(partition_key)
    }

    /// The approximate size in memory of all data in the mutable buffer, in
    /// bytes
    pub fn size(&self) -> usize {
        let partitions = self
            .partitions
            .read()
            .expect("lock poisoned")
            .values()
            .cloned()
            .collect::<Vec<_>>();

        let mut size = 0;
        for p in partitions {
            size += p.read().expect("lock poisoned").size();
        }

        size
    }

    /// Returns the partitions in the requested sort order
    pub fn partitions_sorted_by(
        &self,
        sort_rules: &PartitionSortRules,
    ) -> Vec<Arc<RwLock<Partition>>> {
        let mut partitions: Vec<_> = {
            let partitions = self.partitions.read().expect("poisoned mutex");
            partitions.values().map(Arc::clone).collect()
        };

        match &sort_rules.sort {
            PartitionSort::CreatedAtTime => {
                partitions.sort_by_cached_key(|p| p.read().expect("mutex poisoned").created_at);
            }
            PartitionSort::LastWriteTime => {
                partitions.sort_by_cached_key(|p| p.read().expect("mutex poisoned").last_write_at);
            }
            PartitionSort::Column(_name, _data_type, _val) => {
                unimplemented!()
            }
        }

        if sort_rules.order == Order::Desc {
            partitions.reverse();
        }

        partitions
    }
}

#[async_trait]
impl Database for MutableBufferDb {
    type Error = Error;
    type Chunk = Chunk;

    async fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<(), Self::Error> {
        match write.write_buffer_batch() {
            Some(b) => self.write_entries_to_partitions(&b)?,
            None => {
                return MissingPayload {
                    writer: write.to_fb().writer(),
                }
                .fail()
            }
        };

        Ok(())
    }

    async fn query_series(&self, predicate: Predicate) -> Result<SeriesSetPlans, Self::Error> {
        let mut filter = ChunkTableFilter::new(predicate);
        let mut visitor = SeriesVisitor::new();
        self.accept(&mut filter, &mut visitor)?;
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
                self.accept(&mut filter, &mut visitor)?;
                Ok(visitor.plans.into())
            }
            GroupByAndAggregate::Window { agg, every, offset } => {
                let mut visitor = WindowGroupsVisitor::new(agg, every, offset);
                self.accept(&mut filter, &mut visitor)?;
                Ok(visitor.plans.into())
            }
        }
    }

    /// Return the partition keys for data in this DB
    fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        let partitions = self.partitions.read().expect("mutex poisoned");
        let keys = partitions.keys().cloned().collect();
        Ok(keys)
    }

    /// Return the list of chunks, in order of id, for the specified
    /// partition_key
    fn chunks(&self, partition_key: &str) -> Vec<Arc<Chunk>> {
        let partition = self.get_partition(partition_key);
        let partition = partition.read().expect("mutex poisoned");
        partition.chunks()
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
    pub fn len(&self) -> usize {
        let partitions = self.partitions.read().expect("mutex poisoned");
        partitions.len()
    }

    /// returns true if the database has no partititons
    pub fn is_empty(&self) -> bool {
        let partitions = self.partitions.read().expect("mutex poisoned");
        partitions.is_empty()
    }

    /// Retrieve (or create) the partition for the specified partition key
    fn get_partition(&self, partition_key: &str) -> Arc<RwLock<Partition>> {
        // until we think this code is likely to be a contention hot
        // spot, simply use a write lock even when often a read lock
        // would do.
        let mut partitions = self.partitions.write().expect("mutex poisoned");

        if let Some(partition) = partitions.get(partition_key) {
            Arc::clone(&partition)
        } else {
            let partition = Arc::new(RwLock::new(Partition::new(partition_key)));
            partitions.insert(partition_key.to_string(), Arc::clone(&partition));
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
    fn partition_snapshot(&self) -> Vec<Arc<RwLock<Partition>>> {
        let partitions = self.partitions.read().expect("mutex poisoned");
        partitions.values().cloned().collect()
    }

    /// Traverse this database's tables, calling the relevant
    /// functions, in order, of `visitor`, as described on the Visitor
    /// trait.
    ///
    /// Skips visiting any table or columns of `filter.should_visit_table`
    /// returns false
    fn accept<V: Visitor>(&self, filter: &mut ChunkTableFilter, visitor: &mut V) -> Result<()> {
        for partition in self.partition_snapshot().into_iter() {
            let partition = partition.read().expect("mutex poisoned");

            if filter.should_visit_partition(&partition)? {
                for chunk in partition.iter() {
                    visitor.pre_visit_chunk(chunk)?;
                    filter.pre_visit_chunk(chunk)?;

                    for table in chunk.tables.values() {
                        if filter.should_visit_table(table)? {
                            visitor.pre_visit_table(table, chunk, filter)?;

                            for (column_id, column) in &table.columns {
                                visitor.visit_column(table, *column_id, column, filter)?
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
    use super::*;
    use data_types::selection::Selection;
    use query::{
        exec::{
            field::FieldIndexes,
            seriesset::{Error as SeriesSetError, SeriesSet, SeriesSetItem},
            Executor,
        },
        predicate::PredicateBuilder,
        Database,
    };

    use arrow_deps::{
        arrow::array::{Array, StringArray},
        datafusion::prelude::*,
    };
    use data_types::database_rules::Order;
    use influxdb_line_protocol::{parse_lines, ParsedLine};
    use test_helpers::{assert_contains, str_pair_vec_to_vec};
    use tokio::sync::mpsc;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

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

        let chunk = db.get_chunk(partition_key, 0).unwrap();
        let mut batches = Vec::new();
        let selection = Selection::Some(&["region", "core"]);
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
            .expect("Created query_series plan successfully");

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
            .expect("Created query_series plan successfully");

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
            .expect("Created query_series plan successfully");

        let results = run_and_gather_results(plans).await;
        assert!(results.is_empty());

        // predicate with no columns,
        let predicate = PredicateBuilder::default()
            .add_expr(lit("foo").eq(lit("foo")))
            .build();

        let plans = db
            .query_series(predicate)
            .await
            .expect("Created query_series plan successfully");

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
            .expect("Created query_series plan successfully");

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
    async fn db_size() {
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

        assert_eq!(429, db.size());
    }

    #[tokio::test]
    async fn partitions_sorted_by_times() {
        let db = MutableBufferDb::new("foo");
        write_lp_to_partition(&db, &["cpu val=1 2"], "p1").await;
        write_lp_to_partition(&db, &["mem val=2 1"], "p2").await;
        write_lp_to_partition(&db, &["cpu val=1 2"], "p1").await;
        write_lp_to_partition(&db, &["mem val=2 1"], "p2").await;

        let sort_rules = PartitionSortRules {
            order: Order::Desc,
            sort: PartitionSort::LastWriteTime,
        };
        let partitions = db.partitions_sorted_by(&sort_rules);
        assert_eq!(partitions[0].read().unwrap().key(), "p2");
        assert_eq!(partitions[1].read().unwrap().key(), "p1");

        let sort_rules = PartitionSortRules {
            order: Order::Asc,
            sort: PartitionSort::CreatedAtTime,
        };
        let partitions = db.partitions_sorted_by(&sort_rules);
        assert_eq!(partitions[0].read().unwrap().key(), "p1");
        assert_eq!(partitions[1].read().unwrap().key(), "p2");
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

    async fn write_lp_to_partition(
        database: &MutableBufferDb,
        lp: &[&str],
        partition_key: impl Into<String>,
    ) {
        let lp_data = lp.join("\n");
        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();
        let mut writer = query::test::TestLPWriter::default();
        writer
            .write_lines_to_partition(database, partition_key, &lines)
            .await;
    }
}
