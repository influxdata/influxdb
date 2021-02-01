//! This module contains the main IOx Database object which has the
//! instances of the mutable buffer, read buffer, and object store

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
};

use async_trait::async_trait;
use data_types::{data::ReplicatedWrite, database_rules::DatabaseRules, selection::Selection};
use mutable_buffer::MutableBufferDb;
use query::{Database, PartitionChunk};
use read_buffer::Database as ReadBufferDb;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};

use crate::buffer::Buffer;

mod chunk;
use chunk::DBChunk;
pub mod pred;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Mutable Buffer Chunk Error: {}", source))]
    MutableBufferChunk {
        source: mutable_buffer::chunk::Error,
    },

    #[snafu(display("Unknown Mutable Buffer Chunk {}", chunk_id))]
    UnknownMutableBufferChunk { chunk_id: u32 },

    #[snafu(display("Cannot write to this database: no mutable buffer configured"))]
    DatatbaseNotWriteable {},

    #[snafu(display("Cannot read to this database: no mutable buffer configured"))]
    DatabaseNotReadable {},

    #[snafu(display("Error dropping data from mutable buffer: {}", source))]
    MutableBufferDrop {
        source: mutable_buffer::database::Error,
    },

    #[snafu(display("Error rolling partition: {}", source))]
    RollingPartition {
        source: mutable_buffer::database::Error,
    },

    #[snafu(display("Error querying mutable buffer: {}", source))]
    MutableBufferRead {
        source: mutable_buffer::database::Error,
    },

    #[snafu(display("Error writing to mutable buffer: {}", source))]
    MutableBufferWrite {
        source: mutable_buffer::database::Error,
    },

    #[snafu(display("Error dropping data from read buffer: {}", source))]
    ReadBufferDrop { source: read_buffer::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

const STARTING_SEQUENCE: u64 = 1;

#[derive(Debug, Serialize, Deserialize)]
/// This is the main IOx Database object. It is the root object of any
/// specific InfluxDB IOx instance
pub struct Db {
    #[serde(flatten)]
    pub rules: DatabaseRules,

    #[serde(skip)]
    /// The (optional) mutable buffer stores incoming writes. If a
    /// database does not have a mutable buffer it can not accept
    /// writes (it is a read replica)
    pub mutable_buffer: Option<MutableBufferDb>,

    #[serde(skip)]
    /// The read buffer holds chunk data in an in-memory optimized
    /// format.
    ///
    /// TODO: finer grained locking see ticket https://github.com/influxdata/influxdb_iox/issues/669
    pub read_buffer: Arc<RwLock<ReadBufferDb>>,

    #[serde(skip)]
    /// The wal buffer holds replicated writes in an append in-memory
    /// buffer. This buffer is used for sending data to subscribers
    /// and to persist segments in object storage for recovery.
    pub wal_buffer: Option<Mutex<Buffer>>,

    #[serde(skip)]
    sequence: AtomicU64,
}
impl Db {
    pub fn new(
        rules: DatabaseRules,
        mutable_buffer: Option<MutableBufferDb>,
        read_buffer: ReadBufferDb,
        wal_buffer: Option<Buffer>,
    ) -> Self {
        let wal_buffer = wal_buffer.map(Mutex::new);
        let read_buffer = Arc::new(RwLock::new(read_buffer));
        Self {
            rules,
            mutable_buffer,
            read_buffer,
            wal_buffer,
            sequence: AtomicU64::new(STARTING_SEQUENCE),
        }
    }

    /// Rolls over the active chunk in the database's specified partition
    pub async fn rollover_partition(&self, partition_key: &str) -> Result<Arc<DBChunk>> {
        if let Some(local_store) = self.mutable_buffer.as_ref() {
            local_store
                .rollover_partition(partition_key)
                .await
                .context(RollingPartition)
                .map(DBChunk::new_mb)
        } else {
            DatatbaseNotWriteable {}.fail()
        }
    }

    // Return a list of all chunks in the mutable_buffer (that can
    // potentially be migrated into the read buffer or object store)
    pub async fn mutable_buffer_chunks(&self, partition_key: &str) -> Vec<Arc<DBChunk>> {
        let chunks = if let Some(mutable_buffer) = self.mutable_buffer.as_ref() {
            mutable_buffer
                .chunks(partition_key)
                .await
                .into_iter()
                .map(DBChunk::new_mb)
                .collect()
        } else {
            vec![]
        };
        chunks
    }

    /// List chunks that are currently in the read buffer
    pub async fn read_buffer_chunks(&self, partition_key: &str) -> Vec<Arc<DBChunk>> {
        self.read_buffer
            .read()
            .expect("mutex poisoned")
            .chunk_ids(partition_key)
            .into_iter()
            .map(|chunk_id| DBChunk::new_rb(self.read_buffer.clone(), partition_key, chunk_id))
            .collect()
    }

    /// Drops the specified chunk from the mutable buffer, returning
    /// the dropped chunk.
    pub async fn drop_mutable_buffer_chunk(
        &self,
        partition_key: &str,
        chunk_id: u32,
    ) -> Result<Arc<DBChunk>> {
        self.mutable_buffer
            .as_ref()
            .context(DatatbaseNotWriteable)?
            .drop_chunk(partition_key, chunk_id)
            .await
            .map(DBChunk::new_mb)
            .context(MutableBufferDrop)
    }

    /// Drops the specified chunk from the read buffer, returning
    /// the dropped chunk.
    pub async fn drop_read_buffer_chunk(
        &self,
        partition_key: &str,
        chunk_id: u32,
    ) -> Result<Arc<DBChunk>> {
        self.read_buffer
            .write()
            .expect("mutex poisoned")
            .drop_chunk(partition_key, chunk_id)
            .context(ReadBufferDrop)?;

        Ok(DBChunk::new_rb(
            self.read_buffer.clone(),
            partition_key,
            chunk_id,
        ))
    }

    /// Loads a chunk into the ReadBuffer.
    ///
    /// If the chunk is present in the mutable_buffer then it is
    /// loaded from there. Otherwise, the chunk must be fetched from the
    /// object store (Not yet implemented)
    ///
    /// Also uncontemplated as of yet is ensuring the read buffer does
    /// not exceed a memory limit)
    ///
    /// This (async) function returns when this process is complete,
    /// but the process may take a long time
    ///
    /// Returns a reference to the newly loaded chunk in the read buffer
    pub async fn load_chunk_to_read_buffer(
        &self,
        partition_key: &str,
        chunk_id: u32,
    ) -> Result<Arc<DBChunk>> {
        let mb_chunk = self
            .mutable_buffer
            .as_ref()
            .context(DatatbaseNotWriteable)?
            .get_chunk(partition_key, chunk_id)
            .await
            .context(UnknownMutableBufferChunk { chunk_id })?;

        let mut batches = Vec::new();
        for stats in mb_chunk.table_stats().unwrap() {
            mb_chunk
                .table_to_arrow(&mut batches, &stats.name, Selection::All)
                .unwrap();
            for batch in batches.drain(..) {
                // As implemented now, taking this write lock will wait
                // until all reads to the read buffer to complete and
                // then will block all reads while the insert is occuring
                let mut read_buffer = self.read_buffer.write().expect("mutex poisoned");
                read_buffer.upsert_partition(partition_key, mb_chunk.id(), &stats.name, batch)
            }
        }

        Ok(DBChunk::new_rb(
            self.read_buffer.clone(),
            partition_key,
            mb_chunk.id,
        ))
    }

    /// Returns the next write sequence number
    pub fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst)
    }
}

impl PartialEq for Db {
    fn eq(&self, other: &Self) -> bool {
        self.rules == other.rules
    }
}
impl Eq for Db {}

#[async_trait]
impl Database for Db {
    type Error = Error;
    type Chunk = DBChunk;

    /// Return a covering set of chunks for a particular partition
    async fn chunks(&self, partition_key: &str) -> Vec<Arc<Self::Chunk>> {
        // return a coverting set of chunks. TODO include read buffer
        // chunks and take them preferentially from the read buffer.
        // returns a coverting set of chunks -- aka take chunks from read buffer
        // preferentially
        let mutable_chunk_iter = self.mutable_buffer_chunks(partition_key).await.into_iter();

        let read_buffer_chunk_iter = self.read_buffer_chunks(partition_key).await.into_iter();

        let chunks: BTreeMap<_, _> = mutable_chunk_iter
            .chain(read_buffer_chunk_iter)
            .map(|chunk| (chunk.id(), chunk))
            .collect();

        // inserting into the map will have removed any dupes
        chunks.into_iter().map(|(_id, chunk)| chunk).collect()
    }

    // Note that most of the functions below will eventually be removed from
    // this trait. For now, pass them directly on to the local store

    async fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<(), Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatatbaseNotWriteable)?
            .store_replicated_write(write)
            .await
            .context(MutableBufferWrite)
    }

    async fn tag_column_names(
        &self,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::StringSetPlan, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .tag_column_names(predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn field_column_names(
        &self,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::FieldListPlan, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .field_column_names(predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn column_values(
        &self,
        column_name: &str,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::StringSetPlan, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .column_values(column_name, predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn query_series(
        &self,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::SeriesSetPlans, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .query_series(predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn query_groups(
        &self,
        predicate: query::predicate::Predicate,
        gby_agg: query::group_by::GroupByAndAggregate,
    ) -> Result<query::exec::SeriesSetPlans, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .query_groups(predicate, gby_agg)
            .await
            .context(MutableBufferRead)
    }

    async fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .partition_keys()
            .await
            .context(MutableBufferRead)
    }
}

#[cfg(test)]
mod test_util {
    use super::*;
    /// Create a Database with a local store
    pub fn make_db() -> Db {
        let name = "test_db";
        Db::new(
            DatabaseRules::default(),
            Some(MutableBufferDb::new(name)),
            ReadBufferDb::new(),
            None, // wal buffer
        )
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::make_db;
    use super::*;

    use arrow_deps::{
        arrow::record_batch::RecordBatch, assert_table_eq, datafusion::physical_plan::collect,
    };
    use query::{
        exec::Executor, frontend::sql::SQLQueryPlanner, test::TestLPWriter, PartitionChunk,
    };
    use test_helpers::assert_contains;

    #[tokio::test]
    async fn write_no_mutable_buffer() {
        // Validate that writes are rejected if there is no mutable buffer
        let mutable_buffer = None;
        let db = make_db();
        let db = Db {
            mutable_buffer,
            ..db
        };

        let mut writer = TestLPWriter::default();
        let res = writer.write_lp_string(&db, "cpu bar=1 10").await;
        assert_contains!(
            res.unwrap_err().to_string(),
            "Cannot write to this database: no mutable buffer configured"
        );
    }

    #[tokio::test]
    async fn read_write() {
        let db = make_db();
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(&db, "cpu bar=1 10").await.unwrap();

        let batches = run_query(&db, "select * from cpu").await;

        let expected = vec![
            "+-----+------+",
            "| bar | time |",
            "+-----+------+",
            "| 1   | 10   |",
            "+-----+------+",
        ];
        assert_table_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn write_with_rollover() {
        let db = make_db();
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(&db, "cpu bar=1 10").await.unwrap();
        assert_eq!(vec!["1970-01-01T00"], db.partition_keys().await.unwrap());

        let mb_chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        assert_eq!(mb_chunk.id(), 0);

        let expected = vec![
            "+-----+------+",
            "| bar | time |",
            "+-----+------+",
            "| 1   | 10   |",
            "+-----+------+",
        ];
        let batches = run_query(&db, "select * from cpu").await;
        assert_table_eq!(expected, &batches);

        // add new data
        writer.write_lp_string(&db, "cpu bar=2 20").await.unwrap();
        let expected = vec![
            "+-----+------+",
            "| bar | time |",
            "+-----+------+",
            "| 1   | 10   |",
            "| 2   | 20   |",
            "+-----+------+",
        ];
        let batches = run_query(&db, "select * from cpu").await;
        assert_table_eq!(expected, &batches);

        // And expect that we still get the same thing when data is rolled over again
        let chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        assert_eq!(chunk.id(), 1);

        let batches = run_query(&db, "select * from cpu").await;
        assert_table_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn read_from_read_buffer() {
        // Test that data can be loaded into the ReadBuffer
        let db = make_db();
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(&db, "cpu bar=1 10").await.unwrap();
        writer.write_lp_string(&db, "cpu bar=2 20").await.unwrap();

        let partition_key = "1970-01-01T00";
        let mb_chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        let rb_chunk = db
            .load_chunk_to_read_buffer(partition_key, mb_chunk.id())
            .await
            .unwrap();

        // it should be the same chunk!
        assert_eq!(mb_chunk.id(), rb_chunk.id());

        // we should have chunks in both the mutable buffer and read buffer
        // (Note the currently open chunk is not listed)
        assert_eq!(mutable_chunk_ids(&db, partition_key).await, vec![0, 1]);
        assert_eq!(read_buffer_chunk_ids(&db, partition_key).await, vec![0]);

        // data should be readable
        let expected = vec![
            "+-----+------+",
            "| bar | time |",
            "+-----+------+",
            "| 1   | 10   |",
            "| 2   | 20   |",
            "+-----+------+",
        ];
        let batches = run_query(&db, "select * from cpu").await;
        assert_table_eq!(expected, &batches);

        // now, drop the mutable buffer chunk and results should still be the same
        db.drop_mutable_buffer_chunk(partition_key, mb_chunk.id())
            .await
            .unwrap();

        assert_eq!(mutable_chunk_ids(&db, partition_key).await, vec![1]);
        assert_eq!(read_buffer_chunk_ids(&db, partition_key).await, vec![0]);

        let batches = run_query(&db, "select * from cpu").await;
        assert_table_eq!(expected, &batches);

        // drop, the chunk from the read buffer
        db.drop_read_buffer_chunk(partition_key, mb_chunk.id())
            .await
            .unwrap();
        assert_eq!(
            read_buffer_chunk_ids(&db, partition_key).await,
            vec![] as Vec<u32>
        );

        // Currently this doesn't work (as we need to teach the stores how to
        // purge tables after data bas beend dropped println!("running
        // query after all data dropped!"); let expected = vec![] as
        // Vec<&str>; let batches = run_query(&db, "select * from
        // cpu").await; assert_table_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn chunk_id_listing() {
        // Test that chunk id listing is hooked up
        let db = make_db();
        let partition_key = "1970-01-01T00";
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(&db, "cpu bar=1 10").await.unwrap();
        writer.write_lp_string(&db, "cpu bar=1 20").await.unwrap();

        assert_eq!(mutable_chunk_ids(&db, partition_key).await, vec![0]);
        assert_eq!(
            read_buffer_chunk_ids(&db, partition_key).await,
            vec![] as Vec<u32>
        );

        let partition_key = "1970-01-01T00";
        let mb_chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        assert_eq!(mb_chunk.id(), 0);

        // add a new chunk in mutable buffer, and move chunk1 (but
        // not chunk 0) to read buffer
        writer.write_lp_string(&db, "cpu bar=1 30").await.unwrap();
        let mb_chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        db.load_chunk_to_read_buffer(partition_key, mb_chunk.id())
            .await
            .unwrap();

        writer.write_lp_string(&db, "cpu bar=1 40").await.unwrap();

        assert_eq!(mutable_chunk_ids(&db, partition_key).await, vec![0, 1, 2]);
        assert_eq!(read_buffer_chunk_ids(&db, partition_key).await, vec![1]);
    }

    // run a sql query against the database, returning the results as record batches
    async fn run_query(db: &Db, query: &str) -> Vec<RecordBatch> {
        let planner = SQLQueryPlanner::default();
        let executor = Executor::new();

        let physical_plan = planner.query(db, query, &executor).await.unwrap();

        collect(physical_plan).await.unwrap()
    }

    async fn mutable_chunk_ids(db: &Db, partition_key: &str) -> Vec<u32> {
        let mut chunk_ids: Vec<u32> = db
            .mutable_buffer_chunks(partition_key)
            .await
            .iter()
            .map(|chunk| chunk.id())
            .collect();
        chunk_ids.sort_unstable();
        chunk_ids
    }

    async fn read_buffer_chunk_ids(db: &Db, partition_key: &str) -> Vec<u32> {
        let mut chunk_ids: Vec<u32> = db
            .read_buffer_chunks(partition_key)
            .await
            .iter()
            .map(|chunk| chunk.id())
            .collect();
        chunk_ids.sort_unstable();
        chunk_ids
    }
}

#[cfg(test)]
mod test_influxrpc {
    use query::{
        exec::{
            stringset::{IntoStringSet, StringSetRef},
            Executor,
        },
        frontend::influxrpc::InfluxRPCPlanner,
        predicate::{Predicate, PredicateBuilder},
    };

    use super::test_scenarios::*;

    /// Creates and loads several database scenarios using the db_setup
    /// function.
    ///
    /// runs table_names(predicate) and compares it to the expected
    /// output
    macro_rules! run_table_names_test_case {
        ($DB_SETUP:expr, $PREDICATE:expr, $EXPECTED_NAMES:expr) => {
            let predicate = $PREDICATE;
            for scenario in $DB_SETUP.make().await {
                let DBScenario { scenario_name, db } = scenario;
                println!("Running scenario '{}'", scenario_name);
                println!("Predicate: '{:#?}'", predicate);
                let planner = InfluxRPCPlanner::new();
                let executor = Executor::new();

                let plan = planner
                    .table_names(&db, predicate.clone())
                    .await
                    .expect("built plan successfully");
                let names = executor
                    .to_string_set(plan)
                    .await
                    .expect("converted plan to strings successfully");

                let expected_names = $EXPECTED_NAMES;
                assert_eq!(
                    names,
                    to_stringset(&expected_names),
                    "Error in  scenario '{}'\n\nexpected:\n{:?}\nactual:\n{:?}",
                    scenario_name,
                    expected_names,
                    names
                );
            }
        };
    }

    #[tokio::test]
    async fn list_table_names_no_data_no_pred() {
        run_table_names_test_case!(NoData {}, empty_predicate(), vec![]);
    }

    #[tokio::test]
    async fn list_table_names_no_data_pred() {
        run_table_names_test_case!(TwoMeasurements {}, empty_predicate(), vec!["cpu", "disk"]);
    }

    #[tokio::test]
    async fn list_table_names_data_pred_0_201() {
        run_table_names_test_case!(TwoMeasurements {}, tsp(0, 201), vec!["cpu", "disk"]);
    }

    #[tokio::test]
    async fn list_table_names_data_pred_0_200() {
        run_table_names_test_case!(TwoMeasurements {}, tsp(0, 200), vec!["cpu"]);
    }

    #[tokio::test]
    async fn list_table_names_data_pred_50_101() {
        run_table_names_test_case!(TwoMeasurements {}, tsp(50, 101), vec!["cpu"]);
    }

    #[tokio::test]
    async fn list_table_names_data_pred_250_300() {
        run_table_names_test_case!(TwoMeasurements {}, tsp(250, 300), vec![]);
    }

    // No predicate at all
    fn empty_predicate() -> Predicate {
        Predicate::default()
    }

    // make a single timestamp predicate between r1 and r2
    fn tsp(r1: i64, r2: i64) -> Predicate {
        PredicateBuilder::default().timestamp_range(r1, r2).build()
    }

    fn to_stringset(v: &[&str]) -> StringSetRef {
        v.into_stringset().unwrap()
    }
}

#[cfg(test)]
mod test_table_schema {
    use arrow_deps::arrow::datatypes::DataType;
    use data_types::{schema::builder::SchemaBuilder, selection::Selection};
    use query::{Database, PartitionChunk};

    use super::test_scenarios::*;

    /// Creates and loads several database scenarios using the db_setup
    /// function.
    ///
    /// runs table_schema(predicate) and compares it to the expected
    /// output
    macro_rules! run_table_schema_test_case {
        ($DB_SETUP:expr, $SELECTION:expr, $TABLE_NAME:expr, $EXPECTED_SCHEMA:expr) => {
            let selection = $SELECTION;
            let table_name = $TABLE_NAME;
            let expected_schema = $EXPECTED_SCHEMA;

            for scenario in $DB_SETUP.make().await {
                let DBScenario { scenario_name, db } = scenario;
                println!("Running scenario '{}'", scenario_name);
                println!(
                    "Getting schema for table '{}', selection {:?}",
                    table_name, selection
                );

                // Make sure at least one table has data
                let mut chunks_with_table = 0;

                for partition_key in db.partition_keys().await.unwrap() {
                    for chunk in db.chunks(&partition_key).await {
                        if chunk.has_table(table_name).await {
                            chunks_with_table += 1;
                            let actual_schema = chunk
                                .table_schema(table_name, selection.clone())
                                .await
                                .unwrap();

                            assert_eq!(
                                expected_schema,
                                actual_schema,
                                "Mismatch in chunk {}\nExpected:\n{:#?}\nActual:\n{:#?}\n",
                                chunk.id(),
                                expected_schema,
                                actual_schema
                            );
                        }
                    }
                    assert!(
                        chunks_with_table > 0,
                        "Expected at least one chunk to have data, but none did"
                    );
                }
            }
        };
    }

    #[tokio::test]
    async fn list_schema_cpu_all() {
        // we expect columns to come out in lexographic order by name
        let expected_schema = SchemaBuilder::new()
            .tag("region")
            .timestamp()
            .field("user", DataType::Float64)
            .build()
            .unwrap();

        run_table_schema_test_case!(TwoMeasurements {}, Selection::All, "cpu", expected_schema);
    }

    #[tokio::test]
    async fn list_schema_disk_all() {
        // we expect columns to come out in lexographic order by name
        let expected_schema = SchemaBuilder::new()
            .field("bytes", DataType::Int64)
            .tag("region")
            .timestamp()
            .build()
            .unwrap();

        run_table_schema_test_case!(TwoMeasurements {}, Selection::All, "disk", expected_schema);
    }

    #[tokio::test]
    async fn list_schema_cpu_selection() {
        let expected_schema = SchemaBuilder::new()
            .field("user", DataType::Float64)
            .tag("region")
            .build()
            .unwrap();

        // Pick an order that is not lexographic
        let selection = Selection::Some(&["user", "region"]);

        run_table_schema_test_case!(TwoMeasurements {}, selection, "cpu", expected_schema);
    }

    #[tokio::test]
    async fn list_schema_disk_selection() {
        // we expect columns to come out in lexographic order by name
        let expected_schema = SchemaBuilder::new()
            .timestamp()
            .field("bytes", DataType::Int64)
            .build()
            .unwrap();

        // Pick an order that is not lexographic
        let selection = Selection::Some(&["time", "bytes"]);

        run_table_schema_test_case!(TwoMeasurements {}, selection, "disk", expected_schema);
    }
}

#[cfg(test)]
/// This module contains testing scenarios for Db
mod test_scenarios {
    use super::*;
    use query::test::TestLPWriter;

    use super::test_util::make_db;

    /// Holds a database and a description of how its data was configured
    pub struct DBScenario {
        pub scenario_name: String,
        pub db: Db,
    }

    #[async_trait]
    pub trait DBSetup {
        // Create several scenarios, scenario has the same data, but
        // different physical arrangements (e.g.  the data is in different chunks)
        async fn make(&self) -> Vec<DBScenario>;
    }

    /// No data
    pub struct NoData {}
    #[async_trait]
    impl DBSetup for NoData {
        async fn make(&self) -> Vec<DBScenario> {
            let partition_key = "1970-01-01T00";
            let db = make_db();
            let scenario1 = DBScenario {
                scenario_name: "New, Empty Database".into(),
                db,
            };

            // listing partitions (which may create an entry in a map)
            // in an empty database
            let db = make_db();
            assert_eq!(db.mutable_buffer_chunks(partition_key).await.len(), 1); // only open chunk
            assert_eq!(db.read_buffer_chunks(partition_key).await.len(), 0);
            let scenario2 = DBScenario {
                scenario_name: "New, Empty Database after partitions are listed".into(),
                db,
            };

            // a scenario where the database has had data loaded and then deleted
            let db = make_db();
            let data = "cpu,region=west user=23.2 100";
            let mut writer = TestLPWriter::default();
            writer.write_lp_string(&db, data).await.unwrap();
            // move data out of open chunk
            assert_eq!(db.rollover_partition(partition_key).await.unwrap().id(), 0);
            // drop it
            db.drop_mutable_buffer_chunk(partition_key, 0)
                .await
                .unwrap();

            assert_eq!(db.mutable_buffer_chunks(partition_key).await.len(), 1);

            assert_eq!(db.read_buffer_chunks(partition_key).await.len(), 0); // only open chunk

            let scenario3 = DBScenario {
                scenario_name: "Empty Database after drop chunk".into(),
                db,
            };

            vec![scenario1, scenario2, scenario3]
        }
    }

    /// Two measurements data in a single mutable buffer chunk
    pub struct TwoMeasurements {}
    #[async_trait]
    impl DBSetup for TwoMeasurements {
        async fn make(&self) -> Vec<DBScenario> {
            let partition_key = "1970-01-01T00";
            let data = "cpu,region=west user=23.2 100\n\
                        cpu,region=west user=21.0 150\n\
                        disk,region=east bytes=99i 200";

            let db = make_db();
            let mut writer = TestLPWriter::default();
            writer.write_lp_string(&db, data).await.unwrap();
            let scenario1 = DBScenario {
                scenario_name: "Data in open chunk of mutable buffer".into(),
                db,
            };

            let db = make_db();
            let mut writer = TestLPWriter::default();
            writer.write_lp_string(&db, data).await.unwrap();
            db.rollover_partition(partition_key).await.unwrap();
            let scenario2 = DBScenario {
                scenario_name: "Data in closed chunk of mutable buffer".into(),
                db,
            };

            let db = make_db();
            let mut writer = TestLPWriter::default();
            writer.write_lp_string(&db, data).await.unwrap();
            db.rollover_partition(partition_key).await.unwrap();
            db.load_chunk_to_read_buffer(partition_key, 0)
                .await
                .unwrap();
            let scenario3 = DBScenario {
                scenario_name: "Data in both read buffer and mutable buffer".into(),
                db,
            };

            let db = make_db();
            let mut writer = TestLPWriter::default();
            writer.write_lp_string(&db, data).await.unwrap();
            db.rollover_partition(partition_key).await.unwrap();
            db.load_chunk_to_read_buffer(partition_key, 0)
                .await
                .unwrap();
            db.drop_mutable_buffer_chunk(partition_key, 0)
                .await
                .unwrap();
            let scenario4 = DBScenario {
                scenario_name: "Data in only buffer and not mutable buffer".into(),
                db,
            };

            vec![scenario1, scenario2, scenario3, scenario4]
        }
    }
}
