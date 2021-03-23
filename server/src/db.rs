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
use data_types::{chunk::ChunkSummary, database_rules::DatabaseRules};
use internal_types::{data::ReplicatedWrite, selection::Selection};
use mutable_buffer::MutableBufferDb;
use parking_lot::Mutex;
use query::{Database, PartitionChunk};
use read_buffer::Database as ReadBufferDb;
use snafu::{OptionExt, ResultExt, Snafu};

use crate::buffer::Buffer;

use tracing::info;

mod chunk;
pub(crate) use chunk::DBChunk;
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

    #[snafu(display(
        "Only closed chunks can be moved to read buffer. Chunk {} {} was open",
        partition_key,
        chunk_id
    ))]
    ChunkNotClosed {
        partition_key: String,
        chunk_id: u32,
    },

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

#[derive(Debug)]
/// This is the main IOx Database object. It is the root object of any
/// specific InfluxDB IOx instance
pub struct Db {
    pub rules: DatabaseRules,

    /// The (optional) mutable buffer stores incoming writes. If a
    /// database does not have a mutable buffer it can not accept
    /// writes (it is a read replica)
    pub mutable_buffer: Option<MutableBufferDb>,

    /// The read buffer holds chunk data in an in-memory optimized
    /// format.
    pub read_buffer: Arc<ReadBufferDb>,

    /// The wal buffer holds replicated writes in an append in-memory
    /// buffer. This buffer is used for sending data to subscribers
    /// and to persist segments in object storage for recovery.
    pub wal_buffer: Option<Mutex<Buffer>>,

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

    /// Rolls over the active chunk in the database's specified
    /// partition. Returns the previously open (now closed) Chunk
    pub async fn rollover_partition(&self, partition_key: &str) -> Result<Arc<DBChunk>> {
        if let Some(local_store) = self.mutable_buffer.as_ref() {
            local_store
                .rollover_partition(partition_key)
                .context(RollingPartition)
                .map(|c| DBChunk::new_mb(c, partition_key, false))
        } else {
            DatatbaseNotWriteable {}.fail()
        }
    }

    /// Return true if the specified chunk is still open for new writes
    pub fn is_open_chunk(&self, partition_key: &str, chunk_id: u32) -> bool {
        if let Some(mutable_buffer) = self.mutable_buffer.as_ref() {
            let open_chunk_id = mutable_buffer.open_chunk_id(partition_key);
            open_chunk_id == chunk_id
        } else {
            false
        }
    }

    // Return a list of all chunks in the mutable_buffer (that can
    // potentially be migrated into the read buffer or object store)
    pub fn mutable_buffer_chunks(&self, partition_key: &str) -> Vec<Arc<DBChunk>> {
        let chunks = if let Some(mutable_buffer) = self.mutable_buffer.as_ref() {
            let open_chunk_id = mutable_buffer.open_chunk_id(partition_key);

            mutable_buffer
                .chunks(partition_key)
                .into_iter()
                .map(|c| {
                    let open = c.id() == open_chunk_id;
                    DBChunk::new_mb(c, partition_key, open)
                })
                .collect()
        } else {
            vec![]
        };
        chunks
    }

    // Return the specified chunk in the mutable buffer
    pub fn mutable_buffer_chunk(
        &self,
        partition_key: &str,
        chunk_id: u32,
    ) -> Result<Arc<mutable_buffer::chunk::Chunk>> {
        self.mutable_buffer
            .as_ref()
            .context(DatatbaseNotWriteable)?
            .get_chunk(partition_key, chunk_id)
            .context(UnknownMutableBufferChunk { chunk_id })
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
            .map(|c| DBChunk::new_mb(c, partition_key, false))
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
        let mb_chunk = self.mutable_buffer_chunk(partition_key, chunk_id)?;

        // Can't load an open chunk to the read buffer
        if self.is_open_chunk(partition_key, chunk_id) {
            return ChunkNotClosed {
                partition_key,
                chunk_id,
            }
            .fail();
        }

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

    /// Drops partitions from the mutable buffer if it is over size
    pub fn check_size_and_drop_partitions(&self) -> Result<()> {
        if let (Some(db), Some(config)) = (&self.mutable_buffer, &self.rules.mutable_buffer_config)
        {
            let mut size = db.size();
            if size > config.buffer_size {
                let mut partitions = db.partitions_sorted_by(&config.partition_drop_order);
                while let Some(p) = partitions.pop() {
                    let p = p.read().expect("mutex poisoned");
                    let partition_size = p.size();
                    size -= partition_size;
                    let key = p.key();
                    db.drop_partition(key);
                    info!(
                        partition_key = key,
                        partition_size, "dropped partition from mutable buffer",
                    );
                    if size < config.buffer_size {
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }

    /// Return Summary information for chunks in the specified partition
    pub fn partition_chunk_summaries(
        &self,
        partition_key: &str,
    ) -> impl Iterator<Item = ChunkSummary> {
        self.mutable_buffer_chunks(&partition_key)
            .into_iter()
            .chain(self.read_buffer_chunks(&partition_key).into_iter())
            .map(|c| c.summary())
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

    fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .partition_keys()
            .context(MutableBufferRead)
    }

    fn chunk_summaries(&self) -> Result<Vec<ChunkSummary>> {
        let summaries = self
            .partition_keys()?
            .into_iter()
            .map(|partition_key| self.partition_chunk_summaries(&partition_key))
            .flatten()
            .collect();
        Ok(summaries)
    }
}

#[cfg(test)]
mod tests {
    use crate::query_tests::utils::make_db;

    use super::*;

    use arrow_deps::{
        arrow::record_batch::RecordBatch, assert_table_eq, datafusion::physical_plan::collect,
    };
    use data_types::{
        chunk::ChunkStorage,
        database_rules::{MutableBufferConfig, Order, PartitionSort, PartitionSortRules},
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
    async fn no_load_open_chunk() {
        // Test that data can not be loaded into the ReadBuffer while
        // still open (no way to ensure that new data gets into the
        // read buffer)
        let db = make_db();
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(&db, "cpu bar=1 10").await.unwrap();

        let partition_key = "1970-01-01T00";
        let err = db
            .load_chunk_to_read_buffer(partition_key, 0)
            .await
            .unwrap_err();

        // it should be the same chunk!
        assert_contains!(
            err.to_string(),
            "Only closed chunks can be moved to read buffer. Chunk 1970-01-01T00 0 was open"
        );
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

    #[tokio::test]
    async fn check_size_and_drop_partitions() {
        let mut mbconf = MutableBufferConfig {
            buffer_size: 300,
            ..Default::default()
        };
        let rules = DatabaseRules {
            mutable_buffer_config: Some(mbconf.clone()),
            ..Default::default()
        };

        let mut db = Db::new(
            rules,
            Some(MutableBufferDb::new("foo")),
            read_buffer::Database::new(),
            None, // wal buffer
        );

        let mut writer = TestLPWriter::default();

        writer
            .write_lp_to_partition(&db, "cpu,adsf=jkl,foo=bar val=1 1", "p1")
            .await;
        writer
            .write_lp_to_partition(&db, "cpu,foo=bar val=1 1", "p2")
            .await;
        writer
            .write_lp_to_partition(&db, "cpu,foo=bar val=1 1", "p3")
            .await;

        assert!(db.mutable_buffer.as_ref().unwrap().size() > 300);
        db.check_size_and_drop_partitions().unwrap();
        assert!(db.mutable_buffer.as_ref().unwrap().size() < 300);

        let mut partitions = db
            .mutable_buffer
            .as_ref()
            .unwrap()
            .partition_keys()
            .unwrap();
        partitions.sort();
        assert_eq!(&partitions[0], "p2");
        assert_eq!(&partitions[1], "p3");

        writer
            .write_lp_to_partition(&db, "cpu,foo=bar val=1 1", "p4")
            .await;
        mbconf.buffer_size = db.mutable_buffer.as_ref().unwrap().size();
        mbconf.partition_drop_order = PartitionSortRules {
            order: Order::Desc,
            sort: PartitionSort::LastWriteTime,
        };
        db.rules.mutable_buffer_config = Some(mbconf);
    }

    #[tokio::test]
    async fn partition_chunk_summaries() {
        // Test that chunk id listing is hooked up
        let db = make_db();
        let mut writer = TestLPWriter::default();

        writer.write_lp_string(&db, "cpu bar=1 1").await.unwrap();
        db.rollover_partition("1970-01-01T00").await.unwrap();

        // write into a separate partitiion
        writer
            .write_lp_string(&db, "cpu bar=1,baz2,frob=3 400000000000000")
            .await
            .unwrap();

        print!("Partitions: {:?}", db.partition_keys().unwrap());

        fn to_arc(s: &str) -> Arc<String> {
            Arc::new(s.to_string())
        }

        let mut chunk_summaries = db
            .partition_chunk_summaries("1970-01-05T15")
            .collect::<Vec<_>>();

        chunk_summaries.sort_unstable();

        let expected = vec![ChunkSummary {
            partition_key: to_arc("1970-01-05T15"),
            id: 0,
            storage: ChunkStorage::OpenMutableBuffer,
            estimated_bytes: 107,
        }];

        assert_eq!(
            expected, chunk_summaries,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, chunk_summaries
        );
    }

    #[tokio::test]
    async fn chunk_summaries() {
        // Test that chunk id listing is hooked up
        let db = make_db();
        let mut writer = TestLPWriter::default();

        // get three chunks: one open, one closed in mb and one close in rb

        writer.write_lp_string(&db, "cpu bar=1 1").await.unwrap();
        db.rollover_partition("1970-01-01T00").await.unwrap();

        writer
            .write_lp_string(&db, "cpu bar=1,baz=2 2")
            .await
            .unwrap();

        // a fourth chunk in a different partition
        writer
            .write_lp_string(&db, "cpu bar=1,baz2,frob=3 400000000000000")
            .await
            .unwrap();

        print!("Partitions: {:?}", db.partition_keys().unwrap());

        db.load_chunk_to_read_buffer("1970-01-01T00", 0)
            .await
            .unwrap();

        fn to_arc(s: &str) -> Arc<String> {
            Arc::new(s.to_string())
        }

        let mut chunk_summaries = db.chunk_summaries().expect("expected summary to return");
        chunk_summaries.sort_unstable();

        let expected = vec![
            ChunkSummary {
                partition_key: to_arc("1970-01-01T00"),
                id: 0,
                storage: ChunkStorage::ClosedMutableBuffer,
                estimated_bytes: 70,
            },
            ChunkSummary {
                partition_key: to_arc("1970-01-01T00"),
                id: 0,
                storage: ChunkStorage::ReadBuffer,
                estimated_bytes: 1221,
            },
            ChunkSummary {
                partition_key: to_arc("1970-01-01T00"),
                id: 1,
                storage: ChunkStorage::OpenMutableBuffer,
                estimated_bytes: 101,
            },
            ChunkSummary {
                partition_key: to_arc("1970-01-05T15"),
                id: 0,
                storage: ChunkStorage::OpenMutableBuffer,
                estimated_bytes: 107,
            },
        ];

        assert_eq!(
            expected, chunk_summaries,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, chunk_summaries
        );
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
