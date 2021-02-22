//! This module contains the main IOx Database object which has the
//! instances of the mutable buffer, read buffer, and object store

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use data_types::{data::ReplicatedWrite, database_rules::DatabaseRules, selection::Selection};
use mutable_buffer::MutableBufferDb;
use parking_lot::Mutex;
use query::{plan::stringset::StringSetPlan, Database, PartitionChunk};
use read_buffer::Database as ReadBufferDb;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};

use crate::buffer::Buffer;

mod chunk;
use chunk::DBChunk;
pub mod pred;
mod streams;

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
    pub read_buffer: Arc<ReadBufferDb>,

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
        let read_buffer = Arc::new(read_buffer);
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
                .context(RollingPartition)
                .map(DBChunk::new_mb)
        } else {
            DatatbaseNotWriteable {}.fail()
        }
    }

    // Return a list of all chunks in the mutable_buffer (that can
    // potentially be migrated into the read buffer or object store)
    pub fn mutable_buffer_chunks(&self, partition_key: &str) -> Vec<Arc<DBChunk>> {
        let chunks = if let Some(mutable_buffer) = self.mutable_buffer.as_ref() {
            mutable_buffer
                .chunks(partition_key)
                .into_iter()
                .map(DBChunk::new_mb)
                .collect()
        } else {
            vec![]
        };
        chunks
    }

    /// List chunks that are currently in the read buffer
    pub fn read_buffer_chunks(&self, partition_key: &str) -> Vec<Arc<DBChunk>> {
        self.read_buffer
            .chunk_ids(partition_key)
            .into_iter()
            .map(|chunk_id| DBChunk::new_rb(Arc::clone(&self.read_buffer), partition_key, chunk_id))
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
            .drop_chunk(partition_key, chunk_id)
            .context(ReadBufferDrop)?;

        Ok(DBChunk::new_rb(
            Arc::clone(&self.read_buffer),
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
                self.read_buffer
                    .upsert_partition(partition_key, mb_chunk.id(), &stats.name, batch)
            }
        }

        Ok(DBChunk::new_rb(
            Arc::clone(&self.read_buffer),
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
    fn chunks(&self, partition_key: &str) -> Vec<Arc<Self::Chunk>> {
        // return a coverting set of chunks. TODO include read buffer
        // chunks and take them preferentially from the read buffer.
        // returns a coverting set of chunks -- aka take chunks from read buffer
        // preferentially
        let mutable_chunk_iter = self.mutable_buffer_chunks(partition_key).into_iter();

        let read_buffer_chunk_iter = self.read_buffer_chunks(partition_key).into_iter();

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

    async fn column_values(
        &self,
        column_name: &str,
        predicate: query::predicate::Predicate,
    ) -> Result<StringSetPlan, Self::Error> {
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

    fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .partition_keys()
            .context(MutableBufferRead)
    }
}

#[cfg(test)]
mod tests {
    use crate::query_tests::utils::make_db;

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
        assert_eq!(vec!["1970-01-01T00"], db.partition_keys().unwrap());

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
        assert_table_eq!(&expected, &batches);

        // And expect that we still get the same thing when data is rolled over again
        let chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        assert_eq!(chunk.id(), 1);

        let batches = run_query(&db, "select * from cpu").await;
        assert_table_eq!(&expected, &batches);
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
        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![0, 1]);
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![0]);

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
        assert_table_eq!(&expected, &batches);

        // now, drop the mutable buffer chunk and results should still be the same
        db.drop_mutable_buffer_chunk(partition_key, mb_chunk.id())
            .await
            .unwrap();

        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![1]);
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![0]);

        let batches = run_query(&db, "select * from cpu").await;
        assert_table_eq!(&expected, &batches);

        // drop, the chunk from the read buffer
        db.drop_read_buffer_chunk(partition_key, mb_chunk.id())
            .await
            .unwrap();
        assert_eq!(
            read_buffer_chunk_ids(&db, partition_key),
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

        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![0]);
        assert_eq!(
            read_buffer_chunk_ids(&db, partition_key),
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

        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![0, 1, 2]);
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![1]);
    }

    // run a sql query against the database, returning the results as record batches
    async fn run_query(db: &Db, query: &str) -> Vec<RecordBatch> {
        let planner = SQLQueryPlanner::default();
        let executor = Executor::new();

        let physical_plan = planner.query(db, query, &executor).await.unwrap();

        collect(physical_plan).await.unwrap()
    }

    fn mutable_chunk_ids(db: &Db, partition_key: &str) -> Vec<u32> {
        let mut chunk_ids: Vec<u32> = db
            .mutable_buffer_chunks(partition_key)
            .iter()
            .map(|chunk| chunk.id())
            .collect();
        chunk_ids.sort_unstable();
        chunk_ids
    }

    fn read_buffer_chunk_ids(db: &Db, partition_key: &str) -> Vec<u32> {
        let mut chunk_ids: Vec<u32> = db
            .read_buffer_chunks(partition_key)
            .iter()
            .map(|chunk| chunk.id())
            .collect();
        chunk_ids.sort_unstable();
        chunk_ids
    }
}
