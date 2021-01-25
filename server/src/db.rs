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
use data_types::{data::ReplicatedWrite, database_rules::DatabaseRules};
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
                .table_to_arrow(&mut batches, &stats.name, &[])
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
    use super::*;
    use query::{
        exec::{
            stringset::{IntoStringSet, StringSetRef},
            Executor,
        },
        frontend::influxrpc::InfluxRPCPlanner,
        predicate::{Predicate, PredicateBuilder},
        test::TestLPWriter,
    };

    use super::test_util::make_db;

    #[async_trait]
    trait DBSetup {
        // Create the database
        async fn make(&self) -> Db;
    }

    /// No data
    struct NoData {}
    #[async_trait]
    impl DBSetup for NoData {
        async fn make(&self) -> Db {
            make_db()
        }
    }

    /// Two measurements data in a single mutable buffer chunk
    struct TwoMeasurements {}
    #[async_trait]
    impl DBSetup for TwoMeasurements {
        async fn make(&self) -> Db {
            let db = make_db();
            let data = "cpu,region=west user=23.2 100\n\
                        cpu,region=west user=21.0 150\n\
                        disk,region=east bytes=99i 200";

            let mut writer = TestLPWriter::default();

            writer.write_lp_string(&db, data).await.unwrap();
            db
        }
    }

    #[tokio::test]
    async fn list_table_names() {
        let empty_predicate = Predicate::default();

        let ts_pred_0_201 = PredicateBuilder::default().timestamp_range(0, 201).build();

        let ts_pred_0_200 = PredicateBuilder::default().timestamp_range(0, 200).build();

        let ts_pred_50_101 = PredicateBuilder::default().timestamp_range(50, 101).build();

        let ts_pred_250_300 = PredicateBuilder::default()
            .timestamp_range(250, 300)
            .build();

        let no_data = Box::new(NoData {}) as Box<dyn DBSetup>;
        let two_measurements = Box::new(TwoMeasurements {}) as Box<dyn DBSetup>;

        let cases = vec![
            (
                "list_table_names_no_data_no_pred",
                &no_data,
                &empty_predicate,
                vec![],
            ),
            (
                "list_table_names_no_data_pred",
                &two_measurements,
                &empty_predicate,
                vec!["cpu", "disk"],
            ),
            (
                "list_table_names_data_pred_0_201",
                &two_measurements,
                &ts_pred_0_201,
                vec!["cpu", "disk"],
            ),
            (
                "list_table_names_data_pred_0_200",
                &two_measurements,
                &ts_pred_0_200,
                vec!["cpu"],
            ),
            (
                "list_table_names_data_pred_50_101",
                &two_measurements,
                &ts_pred_50_101,
                vec!["cpu"],
            ),
            (
                "list_table_names_data_pred_250_300",
                &two_measurements,
                &ts_pred_250_300,
                vec![],
            ),
            /* cases with multiple chunks in mutable buffer */

            /* cases with chunks in the read buffer */

            /* cases with chunks in both immutbale and read buffer */
        ];

        // Run all cases before reporting errors
        let mut results = Vec::new();

        for (testcase_name, db_setup, predicate, expected_names) in cases {
            results.push(
                run_table_names_test_case(testcase_name, db_setup, predicate, expected_names).await,
            )
        }

        // Collect up any errors
        let errors: Vec<String> = results
            .into_iter()
            .filter_map(|res| if let Err(e) = res { Some(e) } else { None })
            .collect();

        assert!(errors.is_empty(), "Errors:\n{}", errors.join("\n"));
    }

    fn to_stringset(v: &[&str]) -> StringSetRef {
        v.into_stringset().unwrap()
    }

    // Creates and loads a database using the db)_setup function in
    // data, sets up the data using the `db_setup` function, and then
    // runs table_names(predicate) and compares it to the expected
    // output
    ///
    /// If the test passes returns Ok(()) otherwise returns an error message
    #[allow(clippy::borrowed_box)]
    async fn run_table_names_test_case(
        testcase_name: &str,
        db_setup: &Box<dyn DBSetup>,
        predicate: &Predicate,
        expected_names: Vec<&str>,
    ) -> Result<(), String> {
        let db = db_setup.make().await;

        let planner = InfluxRPCPlanner::new();
        let executor = Executor::new();

        let plan = planner.table_names(&db, predicate.clone()).await.unwrap();
        let names = executor.to_string_set(plan).await.unwrap();

        if names == to_stringset(&expected_names) {
            Ok(())
        } else {
            let msg = format!(
                "Error in test case '{}', expected:\n{:?}, actual:\n{:?}",
                testcase_name, expected_names, names
            );
            println!("msg: {}", msg);

            Err(msg)
        }
    }
}
