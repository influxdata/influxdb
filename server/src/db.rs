//! This module contains the main IOx Database object which has the
//! instances of the mutable buffer, read buffer, and object store

use std::any::Any;
use std::collections::BTreeSet;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use parking_lot::Mutex;
use snafu::{OptionExt, ResultExt, Snafu};
use tracing::{debug, info};

use arrow_deps::datafusion::{
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    datasource::TableProvider,
};
use catalog::{chunk::ChunkState, Catalog};
use data_types::{chunk::ChunkSummary, database_rules::DatabaseRules, error::ErrorLogger};
use internal_types::{data::ReplicatedWrite, selection::Selection};
use mutable_buffer::{pred::ChunkPredicate, MutableBufferDb};
use query::{
    provider::{self, ProviderBuilder},
    Database, PartitionChunk, DEFAULT_SCHEMA,
};
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

    #[snafu(display(
        "Can not rollover partition chunk {} {}: {}",
        partition_key,
        chunk_id,
        source
    ))]
    RollingOverChunk {
        partition_key: String,
        chunk_id: u32,
        source: catalog::Error,
    },

    #[snafu(display("Internal error: unexpected state rolling over partition chunk {} {}: expected {:?}, but was {:?}",
                    partition_key, chunk_id, expected_state, actual_state))]
    InternalRollingOverUnexpectedState {
        partition_key: String,
        chunk_id: u32,
        expected_state: ChunkState,
        actual_state: ChunkState,
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
        chunk_state: ChunkState,
    },

    #[snafu(display(
        "Can load chunk {} {} to read buffer. Chunk was in state {:?}, needs to be Closing",
        partition_key,
        chunk_id,
        chunk_state
    ))]
    LoadNonClosingChunk {
        partition_key: String,
        chunk_id: u32,
        chunk_state: ChunkState,
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
    DatatbaseNotWriteable {},

    #[snafu(display("Internal error: cannot create partition in catalog: {}", source))]
    CreatingPartition {
        partition_key: String,
        source: catalog::Error,
    },

    #[snafu(display("Internal error: cannot create chunk in catalog: {}", source))]
    CreatingChunk {
        partition_key: String,
        chunk_id: u32,
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
    catalog: Catalog,

    /// The (optional) mutable buffer stores incoming writes. If a
    /// database does not have a mutable buffer it can not accept
    /// writes (it is a read replica)
    mutable_buffer: Option<MutableBufferDb>,

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
        mutable_buffer: Option<MutableBufferDb>,
        read_buffer: ReadBufferDb,
        wal_buffer: Option<Buffer>,
        jobs: Arc<JobRegistry>,
    ) -> Self {
        let wal_buffer = wal_buffer.map(Mutex::new);
        let read_buffer = Arc::new(read_buffer);
        let catalog = Catalog::new();
        Self {
            rules,
            catalog,
            mutable_buffer,
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
        let mutable_buffer = self
            .mutable_buffer
            .as_ref()
            .context(DatatbaseNotWriteable)?;

        // After this point, the catalog transaction has to succeed or
        // we need to crash / force a recovery as the in-memory state
        // may be inconsistent with the catalog state

        let chunk_id = mutable_buffer.open_chunk_id(partition_key);

        let partition = self
            .catalog
            .valid_partition(partition_key)
            .context(RollingOverChunk {
                partition_key,
                chunk_id,
            })?;

        let mut partition = partition.write();

        let chunk = partition.chunk(chunk_id).context(RollingOverChunk {
            partition_key,
            chunk_id,
        })?;

        let mut chunk = chunk.write();

        if chunk.state() != ChunkState::Open {
            return InternalRollingOverUnexpectedState {
                partition_key,
                chunk_id,
                expected_state: ChunkState::Open,
                actual_state: chunk.state(),
            }
            .fail();
        }

        // after here, the operation must be infallable (otherwise
        // the catalog/state are out of sync)
        let new_mb_chunk = mutable_buffer.rollover_partition(partition_key);
        chunk.set_state(ChunkState::Closing);

        // make a new chunk to track the newly created chunk in this partition
        partition
            .create_chunk(mutable_buffer.open_chunk_id(partition_key))
            .expect("Creating new chunk after partition rollover");

        return Ok(DBChunk::new_mb(new_mb_chunk, partition_key, false));
    }

    /// Get handles to all chunks in the mutable_buffer
    ///
    /// TODO: make this function non pub and use partition_summary
    /// information in the query_tests
    fn mutable_buffer_chunks(&self, partition_key: &str) -> Vec<Arc<DBChunk>> {
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

    /// Return a handle to the specified chunk in the mutable buffer
    fn mutable_buffer_chunk(
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

    /// Get handles to all chunks currently loaded the read buffer
    ///
    /// NOTE the results may contain partially loaded chunks. The catalog
    /// should always be used to determine where to find each chunk
    fn read_buffer_chunks(&self, partition_key: &str) -> Vec<Arc<DBChunk>> {
        self.read_buffer
            .chunk_ids(partition_key)
            .into_iter()
            .map(|chunk_id| DBChunk::new_rb(Arc::clone(&self.read_buffer), partition_key, chunk_id))
            .collect()
    }

    /// Drops the specified chunk from the catalog and all storage systems
    pub fn drop_chunk(&self, partition_key: &str, chunk_id: u32) -> Result<()> {
        debug!(%partition_key, %chunk_id, "dropping chunk");

        let mutable_buffer = self
            .mutable_buffer
            .as_ref()
            .context(DatatbaseNotWriteable)?;

        let partition = self
            .catalog
            .valid_partition(partition_key)
            .context(DroppingChunk {
                partition_key,
                chunk_id,
            })?;

        // lock the partition so that no one else can be messing with
        // it while we drop the chunk
        let mut partition = partition.write();

        let chunk_state = {
            let chunk = partition.chunk(chunk_id).context(DroppingChunk {
                partition_key,
                chunk_id,
            })?;
            let chunk = chunk.read();

            let chunk_state = chunk.state();
            // prevent chunks that are actively being moved. TODO it
            // would be nicer to allow this to happen have the chunk
            // migration logic cleanup afterwards so that users
            // weren't prevented from dropping chunks due to
            // background tasks
            if chunk_state == ChunkState::Moving {
                return DropMovingChunk {
                    partition_key,
                    chunk_id,
                    chunk_state,
                }
                .fail();
            }
            chunk_state
        };

        partition.drop_chunk(chunk_id).context(DroppingChunk {
            partition_key,
            chunk_id,
        })?;

        // clear it also from the read buffer / mutable buffer, if needed
        let (drop_mb, drop_rb) = match chunk_state {
            ChunkState::Open => (true, false),
            ChunkState::Closing => (true, false),
            ChunkState::Closed => (true, false),
            ChunkState::Moving => (true, true), /* not possible as `ChunkState::Moving` checked */
            // above
            ChunkState::Moved => (false, true),
        };

        debug!(%partition_key, %chunk_id, ?chunk_state, drop_mb, drop_rb, "clearing chunk memory");

        if drop_mb {
            mutable_buffer
                .drop_chunk(partition_key, chunk_id)
                .expect("Dropping mutable buffer chunk");
        }

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
        let mutable_buffer = self
            .mutable_buffer
            .as_ref()
            .context(DatatbaseNotWriteable)?;

        if chunk_id == mutable_buffer.open_chunk_id(partition_key) {
            debug!(%partition_key, %chunk_id, "rolling over partition to load into read buffer");
            self.rollover_partition(partition_key).await?;
        }

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
        {
            let mut chunk = chunk.write();
            let chunk_state = chunk.state();

            if chunk_state != ChunkState::Closing {
                return LoadNonClosingChunk {
                    partition_key,
                    chunk_id,
                    chunk_state,
                }
                .fail();
            }

            chunk.set_state(ChunkState::Moving);
        }

        let mb_chunk = self.mutable_buffer_chunk(partition_key, chunk_id)?;
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

        if chunk.state() != ChunkState::Moving {
            panic!("Chunk state change while it was moving");
        }

        // Drop chunk data from the mutable buffer
        mutable_buffer
            .drop_chunk(partition_key, chunk_id)
            .expect("dropping mutable buffer chunk after movement");

        // update the catalog to say we are done processing
        chunk.set_state(ChunkState::Moved);
        debug!(%partition_key, %chunk_id, "chunk marked MOVED. loading complete");

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
        // TODO: update the catalog as well??
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

    /// Return Summary information for all chunks in the specified
    /// partition across all storage systems
    pub fn partition_chunk_summaries(
        &self,
        partition_key: &str,
    ) -> impl Iterator<Item = ChunkSummary> {
        self.mutable_buffer_chunks(&partition_key)
            .into_iter()
            .chain(self.read_buffer_chunks(&partition_key).into_iter())
            .map(|c| c.summary())
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
        self.mutable_buffer.is_some()
    }

    /// Ensures that all partition_keys referenced in the `write` have been
    /// created in the catalog, doing so if necessary
    fn create_partitions_if_needed(&self, write: &ReplicatedWrite) -> Result<()> {
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

        // Create any partitions in the catalog that might be created by these writes
        for key in entries.into_iter().filter_map(|k| k.partition_key()) {
            if self.catalog.partition(key).is_none() {
                let partition = self
                    .catalog
                    .create_partition(key)
                    .context(CreatingPartition { partition_key: key })?;
                let chunk_id = 0;
                let mut partition = partition.write();
                partition.create_chunk(chunk_id).context(CreatingChunk {
                    partition_key: key,
                    chunk_id,
                })?;
            }
        }
        Ok(())
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
                match chunk.state() {
                    ChunkState::Open
                    | ChunkState::Closing
                    | ChunkState::Closed
                    | ChunkState::Moving => {
                        let mb_chunk = self
                            .mutable_buffer_chunk(chunk.key(), chunk.id())
                            .expect("catalog mismatch");
                        // TODO remove the bool flag here
                        DBChunk::new_mb(mb_chunk, partition_key, chunk.state() == ChunkState::Open)
                    }
                    ChunkState::Moved => {
                        DBChunk::new_rb(Arc::clone(&self.read_buffer), partition_key, chunk.id())
                    }
                }
            })
            .collect()
    }

    async fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<(), Self::Error> {
        self.create_partitions_if_needed(write)?;

        self.mutable_buffer
            .as_ref()
            .context(DatatbaseNotWriteable)?
            .store_replicated_write(write)
            .await
            .context(MutableBufferWrite)
    }

    fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        let partition_keys = self
            .catalog
            .partitions()
            .map(|partition| {
                let partition = partition.read();
                partition.key().to_string()
            })
            .collect();

        Ok(partition_keys)
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

/// Temporary newtype Db wrapper to allow it to act as a CatalogProvider
///
/// TODO: Make Db implement CatalogProvider and Catalog implement SchemaProvider
#[derive(Debug)]
pub struct DbCatalog(Arc<Db>);

impl DbCatalog {
    pub fn new(db: Arc<Db>) -> Self {
        Self(db)
    }
}

impl CatalogProvider for DbCatalog {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        vec![DEFAULT_SCHEMA.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        info!(%name, "using schema");
        match name {
            DEFAULT_SCHEMA => Some(Arc::<Db>::clone(&self.0)),
            _ => None,
        }
    }
}

impl SchemaProvider for Db {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        // TODO: Get information from catalog potentially with caching and less
        // buffering
        let mut names = BTreeSet::new();

        // Currently only support getting tables from the mutable buffer
        if let Some(mutable_buffer) = self.mutable_buffer.as_ref() {
            for partition in self.catalog.partitions() {
                let partition = partition.read();

                for chunk_id in partition.chunk_ids() {
                    if let Some(chunk) = mutable_buffer.get_chunk(partition.key(), chunk_id) {
                        // This is a hack until infallible table listing supported on catalog
                        if let Ok(tables) = chunk.table_names(&ChunkPredicate::default()) {
                            names.extend(tables.into_iter().map(ToString::to_string))
                        }
                    }
                }
            }

            names.into_iter().collect()
        } else {
            vec![]
        }
    }

    fn table(&self, table_name: &str) -> Option<Arc<dyn TableProvider>> {
        let mut builder = ProviderBuilder::new(table_name);
        for partition_key in self.partition_keys().expect("cannot fail") {
            for chunk in self.chunks(&partition_key) {
                if chunk.has_table(table_name) {
                    // This should only fail if the table doesn't exist which isn't possible
                    let schema = chunk
                        .table_schema(table_name, Selection::All)
                        .expect("cannot fail");

                    // This is unfortunate - a table with incompatible chunks ceases to
                    // be visible to the query engine
                    builder = builder
                        .add_chunk(chunk, schema)
                        .log_if_error("Adding chunks to table")
                        .ok()?
                }
            }
        }

        match builder.build() {
            Ok(provider) => Some(Arc::new(provider)),
            Err(provider::Error::InternalNoChunks { .. }) => None,
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }
}

#[cfg(test)]
mod tests {
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

    use crate::query_tests::utils::make_db;

    use super::*;

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
        let db = Arc::new(make_db());
        let mut writer = TestLPWriter::default();
        writer
            .write_lp_string(db.as_ref(), "cpu bar=1 10")
            .await
            .unwrap();

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
        writer
            .write_lp_string(db.as_ref(), "cpu bar=1 10")
            .await
            .unwrap();
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
        writer
            .write_lp_string(db.as_ref(), "cpu bar=2 20")
            .await
            .unwrap();
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
        writer
            .write_lp_string(db.as_ref(), "cpu bar=1 10")
            .await
            .unwrap();
        writer
            .write_lp_string(db.as_ref(), "cpu bar=2 20")
            .await
            .unwrap();

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

        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![0, 2]);
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
            Arc::new(JobRegistry::new()),
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

        writer
            .write_lp_string(&db, "cpu bar=1,baz=2,frob=3 400000000000000")
            .await
            .unwrap();

        print!("Partitions: {:?}", db.partition_keys().unwrap());

        db.load_chunk_to_read_buffer("1970-01-01T00", 0)
            .await
            .unwrap();

        print!("Partitions2: {:?}", db.partition_keys().unwrap());

        db.rollover_partition("1970-01-05T15").await.unwrap();
        writer
            .write_lp_string(&db, "cpu bar=1,baz=3,blargh=3 400000000000000")
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

        let physical_plan = planner
            .query(Arc::new(DbCatalog::new(db)), query, &executor)
            .await
            .unwrap();

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
