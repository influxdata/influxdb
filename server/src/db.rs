//! This module contains the main IOx Database object which has the
//! instances of the mutable buffer, read buffer, and object store

use std::any::Any;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use parking_lot::Mutex;
use snafu::{OptionExt, ResultExt, Snafu};
use tracing::{debug, info};

use arrow_deps::datafusion::catalog::{catalog::CatalogProvider, schema::SchemaProvider};
use catalog::{chunk::ChunkState, Catalog};
use data_types::{chunk::ChunkSummary, database_rules::DatabaseRules};
use internal_types::{data::ReplicatedWrite, selection::Selection};
use mutable_buffer::chunk::Chunk;
use query::{Database, DEFAULT_SCHEMA};
use read_buffer::Database as ReadBufferDb;

pub(crate) use chunk::DBChunk;

use crate::{buffer::Buffer, JobRegistry};

pub mod catalog;
mod chunk;
pub mod pred;
mod streams;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Can not drop chunk {} {} from catalog: {}",
        partition_key,
        chunk_id,
        source
    ))]
    DroppingChunk {
        partition_key: String,
        chunk_id: u32,
        source: catalog::Error,
    },

    #[snafu(display("Can not rollover partition {}: {}", partition_key, source))]
    RollingOverPartition {
        partition_key: String,
        source: catalog::Error,
    },

    #[snafu(display(
        "Internal error: no open chunk while rolling over partition {}",
        partition_key,
    ))]
    InternalNoOpenChunk { partition_key: String },

    #[snafu(display("Internal error: unexpected state rolling over partition chunk {} {}: expected {:?}, but was {:?}",
                    partition_key, chunk_id, expected_state, actual_state))]
    InternalRollingOverUnexpectedState {
        partition_key: String,
        chunk_id: u32,
        expected_state: String,
        actual_state: String,
    },

    #[snafu(display(
        "Can not drop chunk {} {} which is {:?}. Wait for the movement to complete",
        partition_key,
        chunk_id,
        chunk_state
    ))]
    DropMovingChunk {
        partition_key: String,
        chunk_id: u32,
        chunk_state: String,
    },

    #[snafu(display(
        "Can not load partition chunk {} {} to read buffer: {}",
        partition_key,
        chunk_id,
        source
    ))]
    LoadingChunk {
        partition_key: String,
        chunk_id: u32,
        source: catalog::Error,
    },

    #[snafu(display("Unknown Mutable Buffer Chunk {}", chunk_id))]
    UnknownMutableBufferChunk { chunk_id: u32 },

    #[snafu(display("Cannot write to this database: no mutable buffer configured"))]
    DatabaseNotWriteable {},

    #[snafu(display("Internal error: cannot create partition in catalog: {}", source))]
    CreatingPartition {
        partition_key: String,
        source: catalog::Error,
    },

    #[snafu(display("Internal error: cannot create chunk in catalog: {}", source))]
    CreatingChunk {
        partition_key: String,
        source: catalog::Error,
    },

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
///
/// Catalog Usage: the state of the catalog and the state of the `Db`
/// must remain in sync. If they are ever out of sync, the IOx system
/// should be shutdown and forced through a "recovery" to correctly
/// recconcile the state.
///
/// Ensuring the Catalog and Db remain in sync is accomplished by
/// manipulating the catalog state alongside the state in the `Db`
/// itself. The catalog state can be observed (but not mutated) by things
/// outside of the Db
pub struct Db {
    pub rules: DatabaseRules,

    /// The metadata catalog
    catalog: Arc<Catalog>,

    /// The read buffer holds chunk data in an in-memory optimized
    /// format.
    read_buffer: Arc<ReadBufferDb>,

    /// The wal buffer holds replicated writes in an append in-memory
    /// buffer. This buffer is used for sending data to subscribers
    /// and to persist segments in object storage for recovery.
    pub wal_buffer: Option<Mutex<Buffer>>,

    /// A handle to the global jobs registry for long running tasks
    jobs: Arc<JobRegistry>,

    sequence: AtomicU64,

    worker_iterations: AtomicUsize,
}

impl Db {
    pub fn new(
        rules: DatabaseRules,
        read_buffer: ReadBufferDb,
        wal_buffer: Option<Buffer>,
        jobs: Arc<JobRegistry>,
    ) -> Self {
        let wal_buffer = wal_buffer.map(Mutex::new);
        let read_buffer = Arc::new(read_buffer);
        let catalog = Arc::new(Catalog::new());
        Self {
            rules,
            catalog,
            read_buffer,
            wal_buffer,
            jobs,
            sequence: AtomicU64::new(STARTING_SEQUENCE),
            worker_iterations: AtomicUsize::new(0),
        }
    }

    /// Rolls over the active chunk in the database's specified
    /// partition. Returns the previously open (now closed) Chunk
    pub async fn rollover_partition(&self, partition_key: &str) -> Result<Arc<DBChunk>> {
        // After this point, the catalog transaction has to succeed or
        // we need to crash / force a recovery as the in-memory state
        // may be inconsistent with the catalog state

        let partition = self
            .catalog
            .valid_partition(partition_key)
            .context(RollingOverPartition { partition_key })?;

        let mut partition = partition.write();
        let chunk = partition
            .open_chunk()
            .context(InternalNoOpenChunk { partition_key })?;

        let mut chunk = chunk.write();
        chunk
            .set_closing()
            .context(RollingOverPartition { partition_key })?;

        // make a new chunk to track the newly created chunk in this partition
        let new_chunk = partition
            .create_chunk()
            .context(RollingOverPartition { partition_key })?;

        let mut new_chunk = new_chunk.write();
        let chunk_id = new_chunk.id();
        new_chunk
            .set_open(Chunk::new(chunk_id))
            .context(RollingOverPartition { partition_key })?;

        return Ok(DBChunk::snapshot(&chunk));
    }

    /// Drops the specified chunk from the catalog and all storage systems
    pub fn drop_chunk(&self, partition_key: &str, chunk_id: u32) -> Result<()> {
        debug!(%partition_key, %chunk_id, "dropping chunk");

        let partition = self
            .catalog
            .valid_partition(partition_key)
            .context(DroppingChunk {
                partition_key,
                chunk_id,
            })?;

        // lock the partition so that no one else can be messing /
        // with it while we drop the chunk
        let mut partition = partition.write();

        // We can remove the need to drop the read buffer chunk once
        // we inline its ownership into Catalog::Chunk
        let mut drop_rb = false;
        let chunk_state;

        {
            let chunk = partition.chunk(chunk_id).context(DroppingChunk {
                partition_key,
                chunk_id,
            })?;
            let chunk = chunk.read();
            chunk_state = chunk.state().name();

            // prevent chunks that are actively being moved. TODO it
            // would be nicer to allow this to happen have the chunk
            // migration logic cleanup afterwards so that users
            // weren't prevented from dropping chunks due to
            // background tasks
            match chunk.state() {
                ChunkState::Moving(_) => {
                    return DropMovingChunk {
                        partition_key,
                        chunk_id,
                        chunk_state,
                    }
                    .fail()
                }
                ChunkState::Moved(_) => {
                    drop_rb = true;
                }
                _ => {}
            }
        };

        debug!(%partition_key, %chunk_id, %chunk_state, drop_rb, "dropping chunk");

        partition.drop_chunk(chunk_id).context(DroppingChunk {
            partition_key,
            chunk_id,
        })?;

        if drop_rb {
            self.read_buffer
                .drop_chunk(partition_key, chunk_id)
                .expect("Dropping read buffer chunk");
        }

        Ok(())
    }

    /// Copies a chunk in the Closing state into the ReadBuffer from
    /// the mutable buffer and marks the chunk with `Moved` state
    ///
    /// This code does not do any checking of the read buffer against
    /// memory limits, etc
    ///
    /// This (async) function returns when this process is complete,
    /// but the process may take a long time
    ///
    /// Returns a handle to the newly loaded chunk in the read buffer
    pub async fn load_chunk_to_read_buffer(
        &self,
        partition_key: &str,
        chunk_id: u32,
    ) -> Result<Arc<DBChunk>> {
        let chunk = {
            let partition = self
                .catalog
                .valid_partition(partition_key)
                .context(LoadingChunk {
                    partition_key,
                    chunk_id,
                })?;
            let partition = partition.read();

            partition.chunk(chunk_id).context(LoadingChunk {
                partition_key,
                chunk_id,
            })?
        };

        // update the catalog to say we are processing this chunk and
        // then drop the lock while we do the work
        let mb_chunk = {
            let mut chunk = chunk.write();

            chunk.set_moving().context(LoadingChunk {
                partition_key,
                chunk_id,
            })?
        };

        debug!(%partition_key, %chunk_id, "chunk marked MOVING, loading tables into read buffer");

        let mut batches = Vec::new();
        let table_stats = mb_chunk
            .table_stats()
            .expect("Figuring out what tables are in the mutable buffer");

        for stats in table_stats {
            debug!(%partition_key, %chunk_id, table=%stats.name, "loading table to read buffer");
            mb_chunk
                .table_to_arrow(&mut batches, &stats.name, Selection::All)
                // It is probably reasonable to recover from this error
                // (reset the chunk state to Open) but until that is
                // implemented (and tested) just panic
                .expect("Loading chunk to mutable buffer");

            for batch in batches.drain(..) {
                // As implemented now, taking this write lock will wait
                // until all reads to the read buffer to complete and
                // then will block all reads while the insert is occuring
                self.read_buffer
                    .upsert_partition(partition_key, mb_chunk.id(), &stats.name, batch)
            }
        }

        // Relock the chunk again (nothing else should have been able
        // to modify the chunk state while we were moving it
        let mut chunk = chunk.write();
        // update the catalog to say we are done processing
        chunk
            .set_moved(Arc::clone(&self.read_buffer))
            .context(LoadingChunk {
                partition_key,
                chunk_id,
            })?;

        debug!(%partition_key, %chunk_id, "chunk marked MOVED. loading complete");

        Ok(DBChunk::snapshot(&chunk))
    }

    /// Returns the next write sequence number
    pub fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst)
    }

    /// Return Summary information for all chunks in the specified
    /// partition across all storage systems
    pub fn partition_chunk_summaries(
        &self,
        partition_key: &str,
    ) -> impl Iterator<Item = ChunkSummary> {
        self.chunks(partition_key).into_iter().map(|c| c.summary())
    }

    /// Returns the number of iterations of the background worker loop
    pub fn worker_iterations(&self) -> usize {
        self.worker_iterations.load(Ordering::Relaxed)
    }

    /// Background worker function
    pub async fn background_worker(&self, shutdown: tokio_util::sync::CancellationToken) {
        info!("started background worker");

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

        while !shutdown.is_cancelled() {
            self.worker_iterations.fetch_add(1, Ordering::Relaxed);

            tokio::select! {
                _ = interval.tick() => {},
                _ = shutdown.cancelled() => break
            }
        }

        info!("finished background worker");
    }

    /// Returns true if this database can accept writes
    pub fn writeable(&self) -> bool {
        !self.rules.lifecycle_rules.immutable
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
    ///
    /// Note there could/should be an error here (if the partition
    /// doesn't exist... but the trait doesn't have an error)
    fn chunks(&self, partition_key: &str) -> Vec<Arc<Self::Chunk>> {
        let partition = match self.catalog.partition(partition_key) {
            Some(partition) => partition,
            None => return vec![],
        };

        let partition = partition.read();

        partition
            .chunks()
            .map(|chunk| {
                let chunk = chunk.read();
                DBChunk::snapshot(&chunk)
            })
            .collect()
    }

    fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<(), Self::Error> {
        if !self.writeable() {
            return DatabaseNotWriteable {}.fail();
        }

        let batch = if let Some(batch) = write.write_buffer_batch() {
            batch
        } else {
            return Ok(());
        };

        let entries = if let Some(entries) = batch.entries() {
            entries
        } else {
            return Ok(());
        };

        for entry in entries.into_iter() {
            if let Some(partition_key) = entry.partition_key() {
                let chunk = match self.catalog.partition(partition_key) {
                    // no partition key
                    None => {
                        // Create a new partition with an empty chunk
                        let partition = self
                            .catalog
                            .create_partition(partition_key)
                            .context(CreatingPartition { partition_key })?;

                        let mut partition = partition.write();
                        let chunk = partition
                            .create_chunk()
                            .context(CreatingChunk { partition_key })?;
                        {
                            let mut chunk = chunk.write();
                            let chunk_id = chunk.id();
                            chunk
                                .set_open(Chunk::new(chunk_id))
                                .context(CreatingChunk { partition_key })?;
                        }
                        chunk
                    }
                    Some(partition) => {
                        let partition = partition.read();
                        partition
                            .open_chunk()
                            .context(InternalNoOpenChunk { partition_key })?
                    }
                };
                let mut chunk = chunk.write();
                // TODO update the last read/write time of this partition
                chunk.mutable_buffer().unwrap().write_entry(&entry).unwrap()
            }
        }
        Ok(())
    }

    fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        Ok(self.catalog.partition_keys())
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

impl CatalogProvider for Db {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        vec![DEFAULT_SCHEMA.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        info!(%name, "using schema");
        match name {
            DEFAULT_SCHEMA => Some(Arc::<Catalog>::clone(&self.catalog)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_deps::{
        arrow::record_batch::RecordBatch, assert_table_eq, datafusion::physical_plan::collect,
    };
    use data_types::{chunk::ChunkStorage, database_rules::LifecycleRules};
    use query::{
        exec::Executor, frontend::sql::SQLQueryPlanner, test::TestLPWriter, PartitionChunk,
    };
    use test_helpers::assert_contains;

    use crate::query_tests::utils::make_db;

    use super::*;

    #[tokio::test]
    async fn write_no_mutable_buffer() {
        // Validate that writes are rejected if there is no mutable buffer
        let db = make_db();
        let rules = DatabaseRules {
            lifecycle_rules: LifecycleRules {
                immutable: true,
                ..Default::default()
            },
            ..DatabaseRules::new()
        };
        let db = Db { rules, ..db };
        assert!(!db.writeable());

        let mut writer = TestLPWriter::default();
        let res = writer.write_lp_string(&db, "cpu bar=1 10");
        assert_contains!(
            res.unwrap_err().to_string(),
            "Cannot write to this database: no mutable buffer configured"
        );
    }

    #[tokio::test]
    async fn read_write() {
        let db = Arc::new(make_db());
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(db.as_ref(), "cpu bar=1 10").unwrap();

        let batches = run_query(db, "select * from cpu").await;

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
        let db = Arc::new(make_db());
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(db.as_ref(), "cpu bar=1 10").unwrap();
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
        let batches = run_query(Arc::clone(&db), "select * from cpu").await;
        assert_table_eq!(expected, &batches);

        // add new data
        writer.write_lp_string(db.as_ref(), "cpu bar=2 20").unwrap();
        let expected = vec![
            "+-----+------+",
            "| bar | time |",
            "+-----+------+",
            "| 1   | 10   |",
            "| 2   | 20   |",
            "+-----+------+",
        ];
        let batches = run_query(Arc::clone(&db), "select * from cpu").await;
        assert_table_eq!(&expected, &batches);

        // And expect that we still get the same thing when data is rolled over again
        let chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        assert_eq!(chunk.id(), 1);

        let batches = run_query(db, "select * from cpu").await;
        assert_table_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn read_from_read_buffer() {
        // Test that data can be loaded into the ReadBuffer
        let db = Arc::new(make_db());
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(db.as_ref(), "cpu bar=1 10").unwrap();
        writer.write_lp_string(db.as_ref(), "cpu bar=2 20").unwrap();

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
        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![1]);
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
        let batches = run_query(Arc::clone(&db), "select * from cpu").await;
        assert_table_eq!(&expected, &batches);

        // drop, the chunk from the read buffer
        db.drop_chunk(partition_key, mb_chunk.id()).unwrap();
        assert_eq!(
            read_buffer_chunk_ids(db.as_ref(), partition_key),
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
        writer.write_lp_string(&db, "cpu bar=1 10").unwrap();
        writer.write_lp_string(&db, "cpu bar=1 20").unwrap();

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
        writer.write_lp_string(&db, "cpu bar=1 30").unwrap();
        let mb_chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        db.load_chunk_to_read_buffer(partition_key, mb_chunk.id())
            .await
            .unwrap();

        writer.write_lp_string(&db, "cpu bar=1 40").unwrap();

        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![0, 2]);
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![1]);
    }

    #[tokio::test]
    async fn partition_chunk_summaries() {
        // Test that chunk id listing is hooked up
        let db = make_db();
        let mut writer = TestLPWriter::default();

        writer.write_lp_string(&db, "cpu bar=1 1").unwrap();
        db.rollover_partition("1970-01-01T00").await.unwrap();

        // write into a separate partitiion
        writer
            .write_lp_string(&db, "cpu bar=1,baz2,frob=3 400000000000000")
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
        writer.write_lp_string(&db, "cpu bar=1 1").unwrap();
        db.rollover_partition("1970-01-01T00").await.unwrap();

        writer.write_lp_string(&db, "cpu bar=1,baz=2 2").unwrap();

        writer
            .write_lp_string(&db, "cpu bar=1,baz=2,frob=3 400000000000000")
            .unwrap();

        print!("Partitions: {:?}", db.partition_keys().unwrap());

        db.load_chunk_to_read_buffer("1970-01-01T00", 0)
            .await
            .unwrap();

        print!("Partitions2: {:?}", db.partition_keys().unwrap());

        db.rollover_partition("1970-01-05T15").await.unwrap();
        writer
            .write_lp_string(&db, "cpu bar=1,baz=3,blargh=3 400000000000000")
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
                storage: ChunkStorage::ClosedMutableBuffer,
                estimated_bytes: 133,
            },
            ChunkSummary {
                partition_key: to_arc("1970-01-05T15"),
                id: 1,
                storage: ChunkStorage::OpenMutableBuffer,
                estimated_bytes: 135,
            },
        ];

        assert_eq!(
            expected, chunk_summaries,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, chunk_summaries
        );
    }

    // run a sql query against the database, returning the results as record batches
    async fn run_query(db: Arc<Db>, query: &str) -> Vec<RecordBatch> {
        let planner = SQLQueryPlanner::default();
        let executor = Executor::new();

        let physical_plan = planner.query(db, query, &executor).await.unwrap();

        collect(physical_plan).await.unwrap()
    }

    fn mutable_chunk_ids(db: &Db, partition_key: &str) -> Vec<u32> {
        let mut chunk_ids: Vec<u32> = db
            .partition_chunk_summaries(partition_key)
            .filter_map(|chunk| match chunk.storage {
                ChunkStorage::OpenMutableBuffer | ChunkStorage::ClosedMutableBuffer => {
                    Some(chunk.id)
                }
                _ => None,
            })
            .collect();
        chunk_ids.sort_unstable();
        chunk_ids
    }

    fn read_buffer_chunk_ids(db: &Db, partition_key: &str) -> Vec<u32> {
        let mut chunk_ids: Vec<u32> = db
            .partition_chunk_summaries(partition_key)
            .filter_map(|chunk| match chunk.storage {
                ChunkStorage::ReadBuffer => Some(chunk.id),
                _ => None,
            })
            .collect();
        chunk_ids.sort_unstable();
        chunk_ids
    }
}
