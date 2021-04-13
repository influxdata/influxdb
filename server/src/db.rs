//! This module contains the main IOx Database object which has the
//! instances of the mutable buffer, read buffer, and object store

use std::any::Any;
use std::{
    convert::TryInto,
    num::NonZeroU32,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use observability_deps::tracing::{debug, info};
use parking_lot::{Mutex, RwLock};
use snafu::{ensure, OptionExt, ResultExt, Snafu};

use arrow_deps::{
    arrow::datatypes::SchemaRef as ArrowSchemaRef,
    datafusion::{
        catalog::{catalog::CatalogProvider, schema::SchemaProvider},
        physical_plan::SendableRecordBatchStream,
    },
};

use catalog::{chunk::ChunkState, Catalog};
pub(crate) use chunk::DBChunk;
use data_types::{
    chunk::ChunkSummary, database_rules::DatabaseRules, partition_metadata::PartitionSummary,
    timestamp::TimestampRange,
};
use internal_types::selection::Selection;
use object_store::ObjectStore;
use parquet_file::{chunk::Chunk, storage::Storage};
use query::{Database, DEFAULT_SCHEMA};
use read_buffer::Chunk as ReadBufferChunk;
use tracker::{MemRegistry, TaskTracker, TrackedFutureExt};

use super::{buffer::Buffer, JobRegistry};
use data_types::job::Job;

use data_types::partition_metadata::TableSummary;
use internal_types::entry::{self, ClockValue, Entry, SequencedEntry};
use lifecycle::LifecycleManager;
use system_tables::{SystemSchemaProvider, SYSTEM_SCHEMA};

pub mod catalog;
mod chunk;
mod lifecycle;
pub mod pred;
mod streams;
mod system_tables;

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

    #[snafu(display(
        "Can not load partition chunk {} {} to parquet format in memory: {}",
        partition_key,
        chunk_id,
        source
    ))]
    LoadingChunkToParquet {
        partition_key: String,
        chunk_id: u32,
        source: catalog::Error,
    },

    #[snafu(display("Read Buffer Error in chunk {}: {}", chunk_id, source))]
    ReadBufferChunkError {
        source: read_buffer::Error,
        chunk_id: u32,
    },

    #[snafu(display("Read Buffer Schema Error in chunk {}: {}", chunk_id, source))]
    ReadBufferChunkSchemaError {
        source: read_buffer::Error,
        chunk_id: u32,
    },

    #[snafu(display("Read Buffer Timestamp Error in chunk {}: {}", chunk_id, source))]
    ReadBufferChunkTimestampError {
        chunk_id: u32,
        source: read_buffer::Error,
    },

    #[snafu(display("Error writing to object store: {}", source))]
    WritingToObjectStore {
        source: parquet_file::storage::Error,
    },

    #[snafu(display("Unknown Mutable Buffer Chunk {}", chunk_id))]
    UnknownMutableBufferChunk { chunk_id: u32 },

    #[snafu(display("Cannot write to this database: no mutable buffer configured"))]
    DatabaseNotWriteable {},

    #[snafu(display("Can not write entry {} {}: {}", partition_key, chunk_id, source))]
    WriteEntry {
        partition_key: String,
        chunk_id: u32,
        source: mutable_buffer::chunk::Error,
    },

    #[snafu(display("Error building sequenced entry: {}", source))]
    SequencedEntryError { source: entry::Error },

    #[snafu(display("Error building sequenced entry: {}", source))]
    SchemaConversion {
        source: internal_types::schema::Error,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

const STARTING_SEQUENCE: u64 = 1;

/// This is the main IOx Database object. It is the root object of any
/// specific InfluxDB IOx instance
///
///
/// The data in a `Db` is structured in this way:
///
/// ┌───────────────────────────────────────────────┐
/// │                                               │
/// │    ┌────────────────┐                         │
/// │    │    Database    │                         │
/// │    └────────────────┘                         │
/// │             │ one partition per               │
/// │             │ partition_key                   │
/// │             ▼                                 │
/// │    ┌────────────────┐                         │
/// │    │   Partition    │                         │
/// │    └────────────────┘                         │
/// │             │  one open Chunk                 │
/// │             │  zero or more closed            │
/// │             ▼  Chunks                         │
/// │    ┌────────────────┐                         │
/// │    │     Chunk      │                         │
/// │    └────────────────┘                         │
/// │             │  multiple Tables (measurements) │
/// │             ▼                                 │
/// │    ┌────────────────┐                         │
/// │    │     Table      │                         │
/// │    └────────────────┘                         │
/// │             │  multiple Colums                │
/// │             ▼                                 │
/// │    ┌────────────────┐                         │
/// │    │     Column     │                         │
/// │    └────────────────┘                         │
/// │                              MutableBuffer    │
/// │                                               │
/// └───────────────────────────────────────────────┘
///
/// Each row of data is routed into a particular partitions based on
/// column values in that row. The partition's open chunk is updated
/// with the new data.
///
/// The currently open chunk in a partition can be rolled over. When
/// this happens, the chunk is closed (becomes read-only) and stops
/// taking writes. Any new writes to the same partition will create a
/// new active open chunk.
///
/// Catalog Usage: the state of the catalog and the state of the `Db`
/// must remain in sync. If they are ever out of sync, the IOx system
/// should be shutdown and forced through a "recovery" to correctly
/// reconcile the state.
///
/// Ensuring the Catalog and Db remain in sync is accomplished by
/// manipulating the catalog state alongside the state in the `Db`
/// itself. The catalog state can be observed (but not mutated) by things
/// outside of the Db
#[derive(Debug)]
pub struct Db {
    pub rules: RwLock<DatabaseRules>,

    pub server_id: NonZeroU32, // this is also the Query Server ID

    pub store: Arc<ObjectStore>,

    /// The catalog holds chunks of data under partitions for the database.
    /// The underlying chunks may be backed by different execution engines
    /// depending on their stage in the data lifecycle. Currently there are
    /// three backing engines for Chunks:
    ///
    ///  - The Mutable Buffer where chunks are mutable but also queryable;
    ///  - The Read Buffer where chunks are immutable and stored in an optimised
    ///    compressed form for small footprint and fast query execution; and
    ///  - The Parquet Buffer where chunks are backed by Parquet file data.
    catalog: Arc<Catalog>,

    /// The wal buffer holds replicated writes in an append in-memory
    /// buffer. This buffer is used for sending data to subscribers
    /// and to persist segments in object storage for recovery.
    pub wal_buffer: Option<Mutex<Buffer>>,

    /// A handle to the global jobs registry for long running tasks
    jobs: Arc<JobRegistry>,

    /// Memory registries used for tracking memory usage by this Db
    memory_registries: MemoryRegistries,

    /// The system schema provider
    system_tables: Arc<SystemSchemaProvider>,

    /// Used to allocated sequence numbers for writes
    sequence: AtomicU64,

    /// Number of iterations of the worker loop for this Db
    worker_iterations: AtomicUsize,
}

#[derive(Debug, Default)]
struct MemoryRegistries {
    mutable_buffer: Arc<MemRegistry>,

    read_buffer: Arc<MemRegistry>,

    parquet: Arc<MemRegistry>,
}

impl Db {
    pub fn new(
        rules: DatabaseRules,
        server_id: NonZeroU32,
        object_store: Arc<ObjectStore>,
        wal_buffer: Option<Buffer>,
        jobs: Arc<JobRegistry>,
    ) -> Self {
        let rules = RwLock::new(rules);
        let server_id = server_id;
        let store = Arc::clone(&object_store);
        let wal_buffer = wal_buffer.map(Mutex::new);
        let catalog = Arc::new(Catalog::new());
        let system_tables = Arc::new(SystemSchemaProvider::new(Arc::clone(&catalog)));
        Self {
            rules,
            server_id,
            store,
            catalog,
            wal_buffer,
            jobs,
            system_tables,
            memory_registries: Default::default(),
            sequence: AtomicU64::new(STARTING_SEQUENCE),
            worker_iterations: AtomicUsize::new(0),
        }
    }

    /// Rolls over the active chunk in the database's specified
    /// partition. Returns the previously open (now closed) Chunk
    pub async fn rollover_partition(&self, partition_key: &str) -> Result<Arc<DBChunk>> {
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
        partition.create_open_chunk(self.memory_registries.mutable_buffer.as_ref());

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
            ensure!(
                !matches!(chunk.state(), ChunkState::Moving(_)),
                DropMovingChunk {
                    partition_key,
                    chunk_id,
                    chunk_state,
                }
            );
        };

        debug!(%partition_key, %chunk_id, %chunk_state, "dropping chunk");

        partition.drop_chunk(chunk_id).context(DroppingChunk {
            partition_key,
            chunk_id,
        })
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
        let table_stats = mb_chunk.table_summaries();

        // create a new read buffer chunk with memory tracking
        let rb_chunk =
            ReadBufferChunk::new_with_memory_tracker(chunk_id, &self.memory_registries.read_buffer);

        // load tables into the new chunk one by one.
        for stats in table_stats {
            debug!(%partition_key, %chunk_id, table=%stats.name, "loading table to read buffer");
            mb_chunk
                .table_to_arrow(&mut batches, &stats.name, Selection::All)
                // It is probably reasonable to recover from this error
                // (reset the chunk state to Open) but until that is
                // implemented (and tested) just panic
                .expect("Loading chunk to mutable buffer");

            for batch in batches.drain(..) {
                rb_chunk.upsert_table(&stats.name, batch)
            }
        }

        // Relock the chunk again (nothing else should have been able
        // to modify the chunk state while we were moving it
        let mut chunk = chunk.write();
        // update the catalog to say we are done processing
        chunk.set_moved(Arc::new(rb_chunk)).context(LoadingChunk {
            partition_key,
            chunk_id,
        })?;

        debug!(%partition_key, %chunk_id, "chunk marked MOVED. loading complete");

        Ok(DBChunk::snapshot(&chunk))
    }

    pub async fn load_chunk_to_object_store(
        &self,
        partition_key: &str,
        chunk_id: u32,
    ) -> Result<Arc<DBChunk>> {
        // Get the chunk from the catalog
        let chunk = {
            let partition =
                self.catalog
                    .valid_partition(partition_key)
                    .context(LoadingChunkToParquet {
                        partition_key,
                        chunk_id,
                    })?;
            let partition = partition.read();

            partition.chunk(chunk_id).context(LoadingChunkToParquet {
                partition_key,
                chunk_id,
            })?
        };

        // update the catalog to say we are processing this chunk and
        // then drop the lock while we do the work
        let rb_chunk = {
            let mut chunk = chunk.write();

            chunk
                .set_writing_to_object_store()
                .context(LoadingChunkToParquet {
                    partition_key,
                    chunk_id,
                })?
        };

        debug!(%partition_key, %chunk_id, "chunk marked WRITING , loading tables into object store");

        // Get all tables in this chunk
        let table_stats = rb_chunk.table_summaries();

        // Create a parquet chunk for this chunk
        let mut parquet_chunk = Chunk::new(
            partition_key.to_string(),
            chunk_id,
            self.memory_registries.parquet.as_ref(),
        );
        // Create a storage to save data of this chunk
        let storage = Storage::new(
            Arc::clone(&self.store),
            self.server_id,
            self.rules.read().name.to_string(),
        );

        for stats in table_stats {
            debug!(%partition_key, %chunk_id, table=%stats.name, "loading table to object store");

            let predicate = read_buffer::Predicate::default();

            // Get RecordBatchStream of data from the read buffer chunk
            let read_results = rb_chunk
                .read_filter(stats.name.as_str(), predicate, Selection::All)
                .context(ReadBufferChunkError { chunk_id })?;
            let arrow_schema: ArrowSchemaRef = rb_chunk
                .read_filter_table_schema(stats.name.as_str(), Selection::All)
                .context(ReadBufferChunkSchemaError { chunk_id })?
                .into();
            let time_range = rb_chunk
                .read_time_range(stats.name.as_str())
                .context(ReadBufferChunkTimestampError { chunk_id })?;
            let stream: SendableRecordBatchStream = Box::pin(
                streams::ReadFilterResultsStream::new(read_results, Arc::clone(&arrow_schema)),
            );

            // Write this table data into the object store
            let path = storage
                .write_to_object_store(
                    partition_key.to_string(),
                    chunk_id,
                    stats.name.to_string(),
                    stream,
                )
                .await
                .context(WritingToObjectStore)?;

            // Now add the saved info into the parquet_chunk
            let schema = Arc::clone(&arrow_schema)
                .try_into()
                .context(SchemaConversion)?;
            let table_time_range = match time_range {
                None => None,
                Some((start, end)) => {
                    if start < end {
                        Some(TimestampRange::new(start, end))
                    } else {
                        None
                    }
                }
            };
            parquet_chunk.add_table(stats, path, schema, table_time_range);
        }

        // Relock the chunk again (nothing else should have been able
        // to modify the chunk state while we were moving it
        let mut chunk = chunk.write();
        // update the catalog to say we are done processing
        let parquet_chunk = Arc::clone(&Arc::new(parquet_chunk));
        chunk
            .set_written_to_object_store(parquet_chunk)
            .context(LoadingChunkToParquet {
                partition_key,
                chunk_id,
            })?;

        debug!(%partition_key, %chunk_id, "chunk marked MOVED. Persisting to object store complete");

        Ok(DBChunk::snapshot(&chunk))
    }

    /// Spawns a task to perform load_chunk_to_read_buffer
    pub fn load_chunk_to_read_buffer_in_background(
        self: &Arc<Self>,
        partition_key: String,
        chunk_id: u32,
    ) -> TaskTracker<Job> {
        let name = self.rules.read().name.clone();
        let (tracker, registration) = self.jobs.register(Job::CloseChunk {
            db_name: name.to_string(),
            partition_key: partition_key.clone(),
            chunk_id,
        });

        let captured = Arc::clone(&self);
        let task = async move {
            debug!(%name, %partition_key, %chunk_id, "background task loading chunk to read buffer");
            let result = captured
                .load_chunk_to_read_buffer(&partition_key, chunk_id)
                .await;
            if let Err(e) = result {
                info!(?e, %name, %partition_key, %chunk_id, "background task error loading read buffer chunk");
                return Err(e);
            }

            debug!(%name, %partition_key, %chunk_id, "background task completed closing chunk");

            Ok(())
        };

        tokio::spawn(task.track(registration));

        tracker
    }

    /// Returns the next write sequence number
    pub fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst)
    }

    /// Return chunk summary information for all chunks in the specified
    /// partition across all storage systems
    pub fn partition_chunk_summaries(&self, partition_key: &str) -> Vec<ChunkSummary> {
        self.catalog
            .partition(partition_key)
            .map(|partition| partition.read().chunk_summaries().collect())
            .unwrap_or_default()
    }

    /// Return Summary information for all columns in all chunks in the
    /// partition across all storage systems
    pub fn partition_summary(&self, partition_key: &str) -> PartitionSummary {
        self.catalog
            .partition(partition_key)
            .map(|partition| partition.read().summary())
            .unwrap_or_else(|| PartitionSummary {
                key: partition_key.to_string(),
                tables: vec![],
            })
    }

    /// Return table summary information for the given chunk in the specified
    /// partition
    pub fn table_summaries(&self, partition_key: &str, chunk_id: u32) -> Vec<TableSummary> {
        if let Some(partition) = self.catalog.partition(partition_key) {
            let partition = partition.read();
            if let Ok(chunk) = partition.chunk(chunk_id) {
                return chunk.read().table_summaries();
            }
        }
        Default::default()
    }

    /// Returns the number of iterations of the background worker loop
    pub fn worker_iterations(&self) -> usize {
        self.worker_iterations.load(Ordering::Relaxed)
    }

    /// Background worker function
    pub async fn background_worker(
        self: &Arc<Self>,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        info!("started background worker");

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        let mut lifecycle_manager = LifecycleManager::new(Arc::clone(&self));

        while !shutdown.is_cancelled() {
            self.worker_iterations.fetch_add(1, Ordering::Relaxed);

            lifecycle_manager.check_for_work();

            tokio::select! {
                _ = interval.tick() => {},
                _ = shutdown.cancelled() => break
            }
        }

        info!("finished background worker");
    }

    /// Stores an entry based on the configuration. The Entry will first be
    /// converted into a Sequenced Entry with the logical clock assigned
    /// from the database. If the write buffer is configured, the sequenced
    /// entry is written into the buffer and replicated based on the
    /// configured rules. If the mutable buffer is configured, the sequenced
    /// entry is then written into the mutable buffer.
    pub fn store_entry(&self, entry: Entry) -> Result<()> {
        // TODO: build this based on either this or on the write buffer, if configured
        let sequenced_entry = SequencedEntry::new_from_entry_bytes(
            ClockValue::new(self.next_sequence()),
            self.server_id.get(),
            entry.data(),
        )
        .context(SequencedEntryError)?;

        if self.rules.read().wal_buffer_config.is_some() {
            todo!("route to the Write Buffer. TODO: carols10cents #1157")
        }

        self.store_sequenced_entry(sequenced_entry)
    }

    pub fn store_sequenced_entry(&self, sequenced_entry: SequencedEntry) -> Result<()> {
        let rules = self.rules.read();
        let mutable_size_threshold = rules.lifecycle_rules.mutable_size_threshold;
        if rules.lifecycle_rules.immutable {
            return DatabaseNotWriteable {}.fail();
        }
        std::mem::drop(rules);

        // TODO: Direct writes to closing chunks

        if let Some(partitioned_writes) = sequenced_entry.partition_writes() {
            for write in partitioned_writes {
                let partition_key = write.key();
                let partition = self.catalog.get_or_create_partition(partition_key);
                let mut partition = partition.write();
                partition.update_last_write_at();

                let chunk = partition.open_chunk().unwrap_or_else(|| {
                    partition.create_open_chunk(self.memory_registries.mutable_buffer.as_ref())
                });

                let mut chunk = chunk.write();
                chunk.record_write();
                let chunk_id = chunk.id();

                let mb_chunk = chunk.mutable_buffer().expect("cannot mutate open chunk");

                mb_chunk
                    .write_table_batches(
                        sequenced_entry.clock_value(),
                        sequenced_entry.writer_id(),
                        &write.table_batches(),
                    )
                    .context(WriteEntry {
                        partition_key,
                        chunk_id,
                    })?;

                let size = mb_chunk.size();

                if let Some(threshold) = mutable_size_threshold {
                    if size > threshold.get() {
                        chunk.set_closing().expect("cannot close open chunk")
                    }
                }
            }
        }

        Ok(())
    }
}

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

    fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        Ok(self.catalog.partition_keys())
    }

    fn chunk_summaries(&self) -> Result<Vec<ChunkSummary>> {
        Ok(self.catalog.chunk_summaries())
    }
}

impl CatalogProvider for Db {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        vec![
            DEFAULT_SCHEMA.to_string(),
            system_tables::SYSTEM_SCHEMA.to_string(),
        ]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        info!(%name, "using schema");
        match name {
            DEFAULT_SCHEMA => Some(Arc::<Catalog>::clone(&self.catalog)),
            SYSTEM_SCHEMA => Some(Arc::<SystemSchemaProvider>::clone(&self.system_tables)),
            _ => None,
        }
    }
}

pub mod test_helpers {
    use super::*;
    use internal_types::entry::test_helpers::lp_to_entries;

    pub fn write_lp(db: &Db, lp: &str) {
        let entries = lp_to_entries(lp);
        for entry in entries {
            db.store_entry(entry).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::query_tests::utils::{make_database, make_db};
    use ::test_helpers::assert_contains;
    use arrow_deps::{
        arrow::record_batch::RecordBatch,
        assert_table_eq,
        datafusion::{execution::context, physical_plan::collect},
    };
    use chrono::Utc;
    use data_types::{
        chunk::ChunkStorage,
        database_rules::{Order, Sort, SortOrder},
        partition_metadata::{ColumnSummary, StatValues, Statistics, TableSummary},
    };
    use object_store::{
        disk::File, path::ObjectStorePath, path::Path, ObjectStore, ObjectStoreApi,
    };
    use query::{exec::Executor, frontend::sql::SQLQueryPlanner, PartitionChunk};

    use super::*;
    use futures::stream;
    use futures::{StreamExt, TryStreamExt};
    use std::iter::Iterator;

    use super::test_helpers::write_lp;
    use internal_types::entry::test_helpers::lp_to_entry;
    use std::num::NonZeroUsize;
    use std::str;
    use tempfile::TempDir;

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    #[tokio::test]
    async fn write_no_mutable_buffer() {
        // Validate that writes are rejected if there is no mutable buffer
        let db = make_db();
        db.rules.write().lifecycle_rules.immutable = true;
        let entry = lp_to_entry("cpu bar=1 10");
        let res = db.store_entry(entry);
        assert_contains!(
            res.unwrap_err().to_string(),
            "Cannot write to this database: no mutable buffer configured"
        );
    }

    #[tokio::test]
    async fn read_write() {
        let db = Arc::new(make_db());
        write_lp(db.as_ref(), "cpu bar=1 10");

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
        write_lp(db.as_ref(), "cpu bar=1 10");
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
        write_lp(db.as_ref(), "cpu bar=2 20");
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
    async fn write_with_missing_tags_are_null() {
        let db = Arc::new(make_db());
        // Note the `region` tag is introduced in the second line, so
        // the values in prior rows for the region column are
        // null. Likewise the `core` tag is introduced in the third
        // line so the prior columns are null
        let lines = vec![
            "cpu,region=west user=23.2 10",
            "cpu, user=10.0 11",
            "cpu,core=one user=10.0 11",
        ];

        write_lp(db.as_ref(), &lines.join("\n"));
        assert_eq!(vec!["1970-01-01T00"], db.partition_keys().unwrap());

        let mb_chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        assert_eq!(mb_chunk.id(), 0);

        let expected = vec![
            "+------+--------+------+------+",
            "| core | region | time | user |",
            "+------+--------+------+------+",
            "|      | west   | 10   | 23.2 |",
            "|      |        | 11   | 10   |",
            "| one  |        | 11   | 10   |",
            "+------+--------+------+------+",
        ];
        let batches = run_query(Arc::clone(&db), "select * from cpu").await;
        assert_table_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn read_from_read_buffer() {
        // Test that data can be loaded into the ReadBuffer
        let db = Arc::new(make_db());
        write_lp(db.as_ref(), "cpu bar=1 10");
        write_lp(db.as_ref(), "cpu bar=2 20");

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
        // purge tables after data bas been dropped println!("running
        // query after all data dropped!"); let expected = vec![] as
        // Vec<&str>; let batches = run_query(&db, "select * from
        // cpu").await; assert_table_eq!(expected, &batches);
    }

    async fn flatten_list_stream(
        storage: Arc<ObjectStore>,
        prefix: Option<&Path>,
    ) -> Result<Vec<Path>> {
        storage
            .list(prefix)
            .await?
            .map_ok(|v| stream::iter(v).map(Ok))
            .try_flatten()
            .try_collect()
            .await
    }

    #[tokio::test]
    async fn write_one_chunk_one_table_to_parquet_file() {
        // Test that data can be written into parquet files

        // Create an object store with a specified location in a local disk
        let root = TempDir::new().unwrap();
        let object_store = Arc::new(ObjectStore::new_file(File::new(root.path())));

        // Create a DB given a server id, an object store and a db name
        let server_id: NonZeroU32 = NonZeroU32::new(10).unwrap();
        let db_name = "parquet_test_db";
        let db = Arc::new(make_database(server_id, Arc::clone(&object_store), db_name));

        // Write some line protocols in Mutable buffer of the DB
        write_lp(db.as_ref(), "cpu bar=1 10");
        write_lp(db.as_ref(), "cpu bar=2 20");

        //Now mark the MB chunk close
        let partition_key = "1970-01-01T00";
        let mb_chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        // Move that MB chunk to RB chunk and drop it from MB
        let rb_chunk = db
            .load_chunk_to_read_buffer(partition_key, mb_chunk.id())
            .await
            .unwrap();
        // Write the RB chunk to Object Store but keep it in RB
        let pq_chunk = db
            .load_chunk_to_object_store(partition_key, mb_chunk.id())
            .await
            .unwrap();

        // it should be the same chunk!
        assert_eq!(mb_chunk.id(), rb_chunk.id());
        assert_eq!(mb_chunk.id(), pq_chunk.id());

        // we should have chunks in the mutable buffer, read buffer, and object store
        // (Note the currently open chunk is not listed)
        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![1]);
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![0]);
        assert_eq!(read_parquet_file_chunk_ids(&db, partition_key), vec![0]);

        // Verify data written to the parquet file in object store
        // First, there must be one path of object store in the catalog
        let paths = pq_chunk.object_store_paths();
        assert_eq!(paths.len(), 1);

        // Check that the path must exist in the object store
        let path_list = flatten_list_stream(Arc::clone(&object_store), Some(&paths[0]))
            .await
            .unwrap();
        println!("path_list: {:#?}", path_list);
        assert_eq!(path_list.len(), 1);
        assert_eq!(path_list, paths.clone());

        // Get full string path
        let root_path = format!("{:?}", root.path());
        let root_path = root_path.trim_matches('"');
        let path = format!("{}/{}", root_path, paths[0].display());
        println!("path: {}", path);

        // Create External table of this parquet file to get its content in a human
        // readable form
        // Note: We do not care about escaping quotes here because it is just a test
        let sql = format!(
            "CREATE EXTERNAL TABLE parquet_table STORED AS PARQUET LOCATION '{}'",
            path
        );

        let mut ctx = context::ExecutionContext::new();
        let df = ctx.sql(&sql).unwrap();
        df.collect().await.unwrap();

        // Select data from that table
        let sql = "SELECT * FROM parquet_table";
        let content = ctx.sql(&sql).unwrap().collect().await.unwrap();
        println!("Content: {:?}", content);
        let expected = vec![
            "+-----+------+",
            "| bar | time |",
            "+-----+------+",
            "| 1   | 10   |",
            "| 2   | 20   |",
            "+-----+------+",
        ];
        assert_table_eq!(expected, &content);
    }

    #[tokio::test]
    async fn write_one_chunk_many_tables_to_parquet_files() {
        // Test that data can be written into parquet files

        // Create an object store with a specified location in a local disk
        let root = TempDir::new().unwrap();
        let object_store = Arc::new(ObjectStore::new_file(File::new(root.path())));

        // Create a DB given a server id, an object store and a db name
        let server_id: NonZeroU32 = NonZeroU32::new(10).unwrap();
        let db_name = "parquet_test_db";
        let db = Arc::new(make_database(server_id, Arc::clone(&object_store), db_name));

        // Write some line protocols in Mutable buffer of the DB
        write_lp(db.as_ref(), "cpu bar=1 10");
        write_lp(db.as_ref(), "disk ops=1 20");
        write_lp(db.as_ref(), "cpu bar=2 20");

        //Now mark the MB chunk close
        let partition_key = "1970-01-01T00";
        let mb_chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        // Move that MB chunk to RB chunk and drop it from MB
        let rb_chunk = db
            .load_chunk_to_read_buffer(partition_key, mb_chunk.id())
            .await
            .unwrap();
        // Write the RB chunk to Object Store but keep it in RB
        let pq_chunk = db
            .load_chunk_to_object_store(partition_key, mb_chunk.id())
            .await
            .unwrap();

        // it should be the same chunk!
        assert_eq!(mb_chunk.id(), rb_chunk.id());
        assert_eq!(mb_chunk.id(), pq_chunk.id());

        // we should have chunks in the mutable buffer, read buffer, and object store
        // (Note the currently open chunk is not listed)
        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![1]);
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![0]);
        assert_eq!(read_parquet_file_chunk_ids(&db, partition_key), vec![0]);

        // Verify data written to the parquet files in object store
        // First, there must be 2 paths of object store in the catalog
        // that represents 2 files
        let paths = pq_chunk.object_store_paths();
        assert_eq!(paths.len(), 2);

        // Check that the path must exist in the object store
        let prefix = object_store.new_path();
        let path_list = flatten_list_stream(Arc::clone(&object_store), Some(&prefix))
            .await
            .unwrap();
        println!("path_list: {:#?}", path_list);
        assert_eq!(path_list.len(), 2);

        // Check the content of each path
        //
        // Root path
        let root_path = format!("{:?}", root.path());
        let root_path = root_path.trim_matches('"');

        for path in path_list {
            // Get full string path
            let path_string = format!("{}/{}", root_path, path.display());
            println!("path: {}", path_string);

            // Create External table of this parquet file to get its content in a human
            // readable form
            // Note: We do not care about escaping quotes here because it is just a test
            let sql = format!(
                "CREATE EXTERNAL TABLE parquet_table STORED AS PARQUET LOCATION '{}'",
                path_string
            );

            let mut ctx = context::ExecutionContext::new();
            let df = ctx.sql(&sql).unwrap();
            df.collect().await.unwrap();

            // Select data from that table
            let sql = "SELECT * FROM parquet_table";
            let content = ctx.sql(&sql).unwrap().collect().await.unwrap();
            println!("Content: {:?}", content);
            let expected = if path_string.contains("cpu") {
                // file name: cpu.parquet
                vec![
                    "+-----+------+",
                    "| bar | time |",
                    "+-----+------+",
                    "| 1   | 10   |",
                    "| 2   | 20   |",
                    "+-----+------+",
                ]
            } else {
                // file name: disk.parquet
                vec![
                    "+-----+------+",
                    "| ops | time |",
                    "+-----+------+",
                    "| 1   | 20   |",
                    "+-----+------+",
                ]
            };

            assert_table_eq!(expected, &content);
        }
    }

    #[tokio::test]
    async fn write_updates_last_write_at() {
        let db = make_db();
        let before_create = Utc::now();

        let partition_key = "1970-01-01T00";
        write_lp(&db, "cpu bar=1 10");
        let after_write = Utc::now();

        let last_write_prev = {
            let partition = db.catalog.valid_partition(partition_key).unwrap();
            let partition = partition.read();

            assert_ne!(partition.created_at(), partition.last_write_at());
            assert!(before_create < partition.last_write_at());
            assert!(after_write > partition.last_write_at());
            partition.last_write_at()
        };

        write_lp(&db, "cpu bar=1 20");
        {
            let partition = db.catalog.valid_partition(partition_key).unwrap();
            let partition = partition.read();
            assert!(last_write_prev < partition.last_write_at());
        }
    }

    #[tokio::test]
    async fn test_chunk_timestamps() {
        let start = Utc::now();
        let db = make_db();

        // Given data loaded into two chunks
        write_lp(&db, "cpu bar=1 10");
        let after_data_load = Utc::now();

        // When the chunk is rolled over
        let partition_key = "1970-01-01T00";
        let chunk_id = db.rollover_partition("1970-01-01T00").await.unwrap().id();
        let after_rollover = Utc::now();

        let partition = db.catalog.valid_partition(partition_key).unwrap();
        let partition = partition.read();
        let chunk = partition.chunk(chunk_id).unwrap();
        let chunk = chunk.read();

        println!(
            "start: {:?}, after_data_load: {:?}, after_rollover: {:?}",
            start, after_data_load, after_rollover
        );
        println!("Chunk: {:#?}", chunk);

        // then the chunk creation and rollover times are as expected
        assert!(start < chunk.time_of_first_write().unwrap());
        assert!(chunk.time_of_first_write().unwrap() < after_data_load);
        assert!(chunk.time_of_first_write().unwrap() == chunk.time_of_last_write().unwrap());
        assert!(after_data_load < chunk.time_closing().unwrap());
        assert!(chunk.time_closing().unwrap() < after_rollover);
    }

    #[tokio::test]
    async fn test_chunk_closing() {
        let db = make_db();
        db.rules.write().lifecycle_rules.mutable_size_threshold =
            Some(NonZeroUsize::new(2).unwrap());

        write_lp(&db, "cpu bar=1 10");
        write_lp(&db, "cpu bar=1 20");

        let partitions = db.catalog.partition_keys();
        assert_eq!(partitions.len(), 1);

        let partition = db.catalog.partition(&partitions[0]).unwrap();
        let partition = partition.read();

        let chunks: Vec<_> = partition.chunks().collect();
        assert_eq!(chunks.len(), 2);
        assert!(matches!(chunks[0].read().state(), ChunkState::Closing(_)));
        assert!(matches!(chunks[1].read().state(), ChunkState::Closing(_)));
    }

    #[tokio::test]
    async fn chunks_sorted_by_times() {
        let db = make_db();
        write_lp(&db, "cpu val=1 1");
        write_lp(&db, "mem val=2 400000000000001");
        write_lp(&db, "cpu val=1 2");
        write_lp(&db, "mem val=2 400000000000002");

        let sort_rules = SortOrder {
            order: Order::Desc,
            sort: Sort::LastWriteTime,
        };
        let chunks = db.catalog.chunks_sorted_by(&sort_rules);
        let partitions: Vec<_> = chunks
            .into_iter()
            .map(|x| x.read().key().to_string())
            .collect();

        assert_eq!(partitions, vec!["1970-01-05T15", "1970-01-01T00"]);

        let sort_rules = SortOrder {
            order: Order::Asc,
            sort: Sort::CreatedAtTime,
        };
        let chunks = db.catalog.chunks_sorted_by(&sort_rules);
        let partitions: Vec<_> = chunks
            .into_iter()
            .map(|x| x.read().key().to_string())
            .collect();
        assert_eq!(partitions, vec!["1970-01-01T00", "1970-01-05T15"]);
    }

    #[tokio::test]
    async fn chunk_id_listing() {
        // Test that chunk id listing is hooked up
        let db = make_db();
        let partition_key = "1970-01-01T00";

        write_lp(&db, "cpu bar=1 10");
        write_lp(&db, "cpu bar=1 20");

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
        write_lp(&db, "cpu bar=1 30");
        let mb_chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        db.load_chunk_to_read_buffer(partition_key, mb_chunk.id())
            .await
            .unwrap();

        write_lp(&db, "cpu bar=1 40");

        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![0, 2]);
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![1]);
    }

    /// Normalizes a set of ChunkSummaries for comparison by removing timestamps
    fn normalize_summaries(summaries: Vec<ChunkSummary>) -> Vec<ChunkSummary> {
        let mut summaries = summaries
            .into_iter()
            .map(|summary| {
                let ChunkSummary {
                    partition_key,
                    id,
                    storage,
                    estimated_bytes,
                    ..
                } = summary;
                ChunkSummary::new_without_timestamps(partition_key, id, storage, estimated_bytes)
            })
            .collect::<Vec<_>>();
        summaries.sort_unstable();
        summaries
    }

    #[tokio::test]
    async fn partition_chunk_summaries() {
        // Test that chunk id listing is hooked up
        let db = make_db();

        write_lp(&db, "cpu bar=1 1");
        db.rollover_partition("1970-01-01T00").await.unwrap();

        // write into a separate partitiion
        write_lp(&db, "cpu bar=1,baz2,frob=3 400000000000000");

        print!("Partitions: {:?}", db.partition_keys().unwrap());

        fn to_arc(s: &str) -> Arc<String> {
            Arc::new(s.to_string())
        }

        let chunk_summaries = db.partition_chunk_summaries("1970-01-05T15");
        let chunk_summaries = normalize_summaries(chunk_summaries);

        let expected = vec![ChunkSummary::new_without_timestamps(
            to_arc("1970-01-05T15"),
            0,
            ChunkStorage::OpenMutableBuffer,
            107,
        )];

        let size: usize = db
            .chunk_summaries()
            .unwrap()
            .into_iter()
            .map(|x| x.estimated_bytes)
            .sum();

        assert_eq!(db.memory_registries.mutable_buffer.bytes(), size);

        assert_eq!(
            expected, chunk_summaries,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, chunk_summaries
        );
    }

    #[tokio::test]
    async fn partition_chunk_summaries_timestamp() {
        let db = make_db();
        let start = Utc::now();
        write_lp(&db, "cpu bar=1 1");
        let after_first_write = Utc::now();
        write_lp(&db, "cpu bar=2 2");
        db.rollover_partition("1970-01-01T00").await.unwrap();
        let after_close = Utc::now();

        let mut chunk_summaries = db.chunk_summaries().unwrap();

        chunk_summaries.sort_by_key(|s| s.id);

        let summary = &chunk_summaries[0];
        assert_eq!(summary.id, 0, "summary; {:#?}", summary);
        assert!(
            summary.time_of_first_write.unwrap() > start,
            "summary; {:#?}",
            summary
        );
        assert!(
            summary.time_of_first_write.unwrap() < after_close,
            "summary; {:#?}",
            summary
        );

        assert!(
            summary.time_of_last_write.unwrap() > after_first_write,
            "summary; {:#?}",
            summary
        );
        assert!(
            summary.time_of_last_write.unwrap() < after_close,
            "summary; {:#?}",
            summary
        );

        assert!(
            summary.time_closing.unwrap() > after_first_write,
            "summary; {:#?}",
            summary
        );
        assert!(
            summary.time_closing.unwrap() < after_close,
            "summary; {:#?}",
            summary
        );
    }

    #[tokio::test]
    async fn chunk_summaries() {
        // Test that chunk id listing is hooked up
        let db = make_db();

        // get three chunks: one open, one closed in mb and one close in rb
        write_lp(&db, "cpu bar=1 1");
        db.rollover_partition("1970-01-01T00").await.unwrap();

        write_lp(&db, "cpu bar=1,baz=2 2");
        write_lp(&db, "cpu bar=1,baz=2,frob=3 400000000000000");

        print!("Partitions: {:?}", db.partition_keys().unwrap());

        db.load_chunk_to_read_buffer("1970-01-01T00", 0)
            .await
            .unwrap();

        print!("Partitions2: {:?}", db.partition_keys().unwrap());

        db.rollover_partition("1970-01-05T15").await.unwrap();
        write_lp(&db, "cpu bar=1,baz=3,blargh=3 400000000000000");

        fn to_arc(s: &str) -> Arc<String> {
            Arc::new(s.to_string())
        }

        let chunk_summaries = db.chunk_summaries().expect("expected summary to return");
        let chunk_summaries = normalize_summaries(chunk_summaries);

        let expected = vec![
            ChunkSummary::new_without_timestamps(
                to_arc("1970-01-01T00"),
                0,
                ChunkStorage::ReadBuffer,
                1285,
            ),
            ChunkSummary::new_without_timestamps(
                to_arc("1970-01-01T00"),
                1,
                ChunkStorage::OpenMutableBuffer,
                101,
            ),
            ChunkSummary::new_without_timestamps(
                to_arc("1970-01-05T15"),
                0,
                ChunkStorage::ClosedMutableBuffer,
                133,
            ),
            ChunkSummary::new_without_timestamps(
                to_arc("1970-01-05T15"),
                1,
                ChunkStorage::OpenMutableBuffer,
                135,
            ),
        ];

        assert_eq!(db.memory_registries.mutable_buffer.bytes(), 101 + 133 + 135);
        assert_eq!(db.memory_registries.read_buffer.bytes(), 1285);

        assert_eq!(
            expected, chunk_summaries,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, chunk_summaries
        );
    }

    #[tokio::test]
    async fn partition_summaries() {
        // Test that chunk id listing is hooked up
        let db = make_db();

        write_lp(&db, "cpu bar=1 1");
        let chunk_id = db.rollover_partition("1970-01-01T00").await.unwrap().id();
        write_lp(&db, "cpu bar=2,baz=3.0 2");
        write_lp(&db, "mem foo=1 1");

        // load a chunk to the read buffer
        db.load_chunk_to_read_buffer("1970-01-01T00", chunk_id)
            .await
            .unwrap();

        // write into a separate partitiion
        write_lp(&db, "cpu bar=1 400000000000000");
        write_lp(&db, "mem frob=3 400000000000001");

        print!("Partitions: {:?}", db.partition_keys().unwrap());

        let partition_summaries = vec![
            db.partition_summary("1970-01-01T00"),
            db.partition_summary("1970-01-05T15"),
        ];

        let expected = vec![
            PartitionSummary {
                key: "1970-01-01T00".into(),
                tables: vec![
                    TableSummary {
                        name: "cpu".into(),
                        columns: vec![
                            ColumnSummary {
                                name: "bar".into(),
                                stats: Statistics::F64(StatValues {
                                    min: 1.0,
                                    max: 2.0,
                                    count: 2,
                                }),
                            },
                            ColumnSummary {
                                name: "time".into(),
                                stats: Statistics::I64(StatValues {
                                    min: 1,
                                    max: 2,
                                    count: 2,
                                }),
                            },
                            ColumnSummary {
                                name: "baz".into(),
                                stats: Statistics::F64(StatValues {
                                    min: 3.0,
                                    max: 3.0,
                                    count: 1,
                                }),
                            },
                        ],
                    },
                    TableSummary {
                        name: "mem".into(),
                        columns: vec![
                            ColumnSummary {
                                name: "time".into(),
                                stats: Statistics::I64(StatValues {
                                    min: 1,
                                    max: 1,
                                    count: 1,
                                }),
                            },
                            ColumnSummary {
                                name: "foo".into(),
                                stats: Statistics::F64(StatValues {
                                    min: 1.0,
                                    max: 1.0,
                                    count: 1,
                                }),
                            },
                        ],
                    },
                ],
            },
            PartitionSummary {
                key: "1970-01-05T15".into(),
                tables: vec![
                    TableSummary {
                        name: "cpu".into(),
                        columns: vec![
                            ColumnSummary {
                                name: "bar".into(),
                                stats: Statistics::F64(StatValues {
                                    min: 1.0,
                                    max: 1.0,
                                    count: 1,
                                }),
                            },
                            ColumnSummary {
                                name: "time".into(),
                                stats: Statistics::I64(StatValues {
                                    min: 400000000000000,
                                    max: 400000000000000,
                                    count: 1,
                                }),
                            },
                        ],
                    },
                    TableSummary {
                        name: "mem".into(),
                        columns: vec![
                            ColumnSummary {
                                name: "time".into(),
                                stats: Statistics::I64(StatValues {
                                    min: 400000000000001,
                                    max: 400000000000001,
                                    count: 1,
                                }),
                            },
                            ColumnSummary {
                                name: "frob".into(),
                                stats: Statistics::F64(StatValues {
                                    min: 3.0,
                                    max: 3.0,
                                    count: 1,
                                }),
                            },
                        ],
                    },
                ],
            },
        ];

        assert_eq!(
            expected, partition_summaries,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, partition_summaries
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
            .into_iter()
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
            .into_iter()
            .filter_map(|chunk| match chunk.storage {
                ChunkStorage::ReadBuffer => Some(chunk.id),
                ChunkStorage::ReadBufferAndObjectStore => Some(chunk.id),
                _ => None,
            })
            .collect();
        chunk_ids.sort_unstable();
        chunk_ids
    }

    fn read_parquet_file_chunk_ids(db: &Db, partition_key: &str) -> Vec<u32> {
        let mut chunk_ids: Vec<u32> = db
            .partition_chunk_summaries(partition_key)
            .into_iter()
            .filter_map(|chunk| match chunk.storage {
                ChunkStorage::ReadBufferAndObjectStore => Some(chunk.id),
                ChunkStorage::ObjectStoreOnly => Some(chunk.id),
                _ => None,
            })
            .collect();
        chunk_ids.sort_unstable();
        chunk_ids
    }
}
