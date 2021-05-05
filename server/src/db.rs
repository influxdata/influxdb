//! This module contains the main IOx Database object which has the
//! instances of the mutable buffer, read buffer, and object store

use super::{
    buffer::{self, Buffer},
    JobRegistry,
};
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use async_trait::async_trait;
use catalog::{
    chunk::{Chunk as CatalogChunk, ChunkState},
    Catalog,
};
use chrono::Utc;
pub(crate) use chunk::DbChunk;
use data_types::{
    chunk::ChunkSummary,
    database_rules::DatabaseRules,
    job::Job,
    partition_metadata::{PartitionSummary, TableSummary},
    server_id::ServerId,
    timestamp::TimestampRange,
};
use datafusion::{
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    physical_plan::SendableRecordBatchStream,
};
use entry::{ClockValue, Entry, OwnedSequencedEntry, SequencedEntry};
use internal_types::{arrow::sort::sort_record_batch, selection::Selection};
use lifecycle::LifecycleManager;
use metrics::{KeyValue, MetricObserver, MetricObserverBuilder, MetricRegistry};
use object_store::ObjectStore;
use observability_deps::tracing::{debug, info};
use parking_lot::{Mutex, RwLock};
use parquet_file::{chunk::Chunk, storage::Storage};
use query::predicate::{Predicate, PredicateBuilder};
use query::{exec::Executor, Database, DEFAULT_SCHEMA};
use read_buffer::Chunk as ReadBufferChunk;
use snafu::{ensure, ResultExt, Snafu};
use std::{
    any::Any,
    convert::TryInto,
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use system_tables::{SystemSchemaProvider, SYSTEM_SCHEMA};
use tracker::{MemRegistry, TaskTracker, TrackedFutureExt};

pub mod catalog;
mod chunk;
mod lifecycle;
pub mod pred;
mod streams;
mod system_tables;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Can not drop chunk {}:{}:{} from catalog: {}",
        partition_key,
        table_name,
        chunk_id,
        source
    ))]
    DroppingChunk {
        partition_key: String,
        table_name: String,
        chunk_id: u32,
        source: catalog::Error,
    },

    #[snafu(display(
        "Can not rollover partition {}:{} : {}",
        partition_key,
        table_name,
        source
    ))]
    RollingOverPartition {
        partition_key: String,
        table_name: String,
        source: catalog::Error,
    },

    #[snafu(display(
        "Can not drop chunk {}:{}:{} which is {:?}. Wait for the movement to complete",
        partition_key,
        table_name,
        chunk_id,
        chunk_state
    ))]
    DropMovingChunk {
        partition_key: String,
        table_name: String,
        chunk_id: u32,
        chunk_state: String,
    },

    #[snafu(display(
        "Can not load partition chunk {}:{}:{} to read buffer: {}",
        partition_key,
        table_name,
        chunk_id,
        source
    ))]
    LoadingChunk {
        partition_key: String,
        table_name: String,
        chunk_id: u32,
        source: catalog::Error,
    },

    #[snafu(display(
        "Can not load partition chunk {}:{},{} to parquet format in memory: {}",
        partition_key,
        table_name,
        chunk_id,
        source
    ))]
    LoadingChunkToParquet {
        partition_key: String,
        table_name: String,
        chunk_id: u32,
        source: catalog::Error,
    },

    UnloadingChunkFromReadBuffer {
        partition_key: String,
        table_name: String,
        chunk_id: u32,
        source: catalog::Error,
    },

    #[snafu(display("Read Buffer Error in chunk {}{} : {}", chunk_id, table_name, source))]
    ReadBufferChunkError {
        source: read_buffer::Error,
        table_name: String,
        chunk_id: u32,
    },

    #[snafu(display(
        "Read Buffer Schema Error in chunk {}:{} : {}",
        chunk_id,
        table_name,
        source
    ))]
    ReadBufferChunkSchemaError {
        source: read_buffer::Error,
        table_name: String,
        chunk_id: u32,
    },

    #[snafu(display(
        "Read Buffer Timestamp Error in chunk {}:{} : {}",
        chunk_id,
        table_name,
        source
    ))]
    ReadBufferChunkTimestampError {
        source: read_buffer::Error,
        table_name: String,
        chunk_id: u32,
    },

    #[snafu(display("Error writing to object store: {}", source))]
    WritingToObjectStore {
        source: parquet_file::storage::Error,
    },

    #[snafu(display("Unknown Mutable Buffer Chunk {}", chunk_id))]
    UnknownMutableBufferChunk { chunk_id: u32 },

    #[snafu(display("Cannot write to this database: no mutable buffer configured"))]
    DatabaseNotWriteable {},

    #[snafu(display("Hard buffer size limit reached"))]
    HardLimitReached {},

    #[snafu(display("Can not write entry {}:{} : {}", partition_key, chunk_id, source))]
    WriteEntry {
        partition_key: String,
        chunk_id: u32,
        source: mutable_buffer::chunk::Error,
    },

    #[snafu(display("Can not create open entry {} : {}", partition_key, source))]
    OpenEntry {
        partition_key: String,
        source: catalog::Error,
    },

    #[snafu(display("Error building sequenced entry: {}", source))]
    SequencedEntryError { source: entry::Error },

    #[snafu(display("Error building sequenced entry: {}", source))]
    SchemaConversion {
        source: internal_types::schema::Error,
    },

    #[snafu(display("Error sending Sequenced Entry to Write Buffer: {}", source))]
    WriteBufferError { source: buffer::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

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
/// │             │  multiple Tables (measurements) │
/// │             ▼                                 │
/// │    ┌────────────────┐                         │
/// │    │     Table      │                         │
/// │    └────────────────┘                         │
/// │             │  one open Chunk                 │
/// │             │  zero or more closed            │
/// │             ▼  Chunks                         │
/// │    ┌────────────────┐                         │
/// │    │     Chunk      │                         │
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

    pub server_id: ServerId, // this is also the Query Server ID

    /// Interface to use for persistence
    pub store: Arc<ObjectStore>,

    /// Executor for running queries
    exec: Arc<Executor>,

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

    /// The Write Buffer holds sequenced entries in an append in-memory
    /// buffer. This buffer is used for sending data to subscribers
    /// and to persist segments in object storage for recovery.
    pub write_buffer: Option<Mutex<Buffer>>,

    /// A handle to the global jobs registry for long running tasks
    jobs: Arc<JobRegistry>,

    /// All of the metrics for this Db.
    metrics: DbMetrics,

    // The metrics registry to inject into created components in the Db.
    metrics_registry: Arc<metrics::MetricRegistry>,

    /// Memory registries used for tracking memory usage by this Db
    memory_registries: MemoryRegistries,

    /// The system schema provider
    system_tables: Arc<SystemSchemaProvider>,

    /// Process clock used in establishing a partial ordering of operations via a Lamport Clock.
    process_clock: Arc<Mutex<NonZeroU64>>,

    /// Number of iterations of the worker loop for this Db
    worker_iterations: AtomicUsize,
}

#[derive(Debug, Default)]
struct MemoryRegistries {
    mutable_buffer: Arc<MemRegistry>,
    read_buffer: Arc<MemRegistry>,
    parquet: Arc<MemRegistry>,
}

impl MemoryRegistries {
    /// Total bytes over all registries.
    pub fn bytes(&self) -> usize {
        self.mutable_buffer.bytes() + self.read_buffer.bytes() + self.parquet.bytes()
    }
}

impl MetricObserver for &MemoryRegistries {
    fn register(self, builder: MetricObserverBuilder<'_>) {
        let mutable_buffer = Arc::clone(&self.mutable_buffer);
        let read_buffer = Arc::clone(&self.read_buffer);
        let parquet = Arc::clone(&self.parquet);

        builder.register_gauge_u64(
            "chunks_mem_usage",
            Some("bytes"),
            "Memory usage by catalog chunks",
            move |x| {
                x.observe(
                    mutable_buffer.bytes() as u64,
                    &[KeyValue::new("source", "mutable_buffer")],
                );
                x.observe(
                    read_buffer.bytes() as u64,
                    &[KeyValue::new("source", "read_buffer")],
                );
                x.observe(
                    parquet.bytes() as u64,
                    &[KeyValue::new("source", "parquet")],
                );
            },
        );
    }
}

// The set of metrics for `Db`, exposed via our /metrics endpoint.
#[derive(Debug)]
struct DbMetrics {
    // The total number of chunks created moved through various stages of the
    // catalog data lifecycle.
    catalog_chunks: metrics::Counter,

    // Tracks a distribution of sizes in bytes of chunks as they're moved into
    // various immutable stages in IOx: closed/moving in the MUB, in the RB,
    // and in Parquet.
    catalog_immutable_chunk_bytes: metrics::Histogram,

    // Tracks the current total size in bytes of all chunks in the catalog.
    // sizes are segmented by database and chunk location.
    catalog_chunk_bytes: metrics::Gauge,

    // Metrics associated with Read Buffer chunks. Due to the behaviour of the
    // open telemetry observers, we allocate a single source of metrics and push
    // them into each new read buffer chunk.
    read_buffer_chunk_metrics: Arc<read_buffer::ChunkMetrics>,
}

impl DbMetrics {
    // updates the catalog_chunks metric with a new chunk state and increases the
    // size metric associated with the new state.
    fn update_chunk_state(
        &self,
        prev_state_size: Option<(&'static str, usize)>,
        next_state_size: Option<(&'static str, usize)>,
    ) {
        debug!(
            ?prev_state_size,
            ?next_state_size,
            "updating chunk state metrics"
        );

        // Reduce bytes tracked metric for previous state
        if let Some((state, size)) = prev_state_size {
            let labels = vec![metrics::KeyValue::new("state", state)];
            self.catalog_chunk_bytes
                .sub_with_labels(size as f64, labels.as_slice());
            debug!(?size, ?labels, "called catalog_chunk_bytes.sub_with_labels");
        }

        // Increase next metric for next chunk state
        if let Some((state, size)) = next_state_size {
            let labels = vec![metrics::KeyValue::new("state", state)];
            self.catalog_chunk_bytes
                .add_with_labels(size as f64, labels.as_slice());
            debug!(size, ?labels, "called catalog_chunk_bytes.add_with_labels");

            // New chunk in new state
            self.catalog_chunks.inc_with_labels(labels.as_slice());
            debug!(?labels, "called catalog_chunks.inc_with_labels");
        }
    }
}

impl Db {
    pub fn new(
        rules: DatabaseRules,
        server_id: ServerId,
        object_store: Arc<ObjectStore>,
        exec: Arc<Executor>,
        write_buffer: Option<Buffer>,
        jobs: Arc<JobRegistry>,
        metrics: Arc<MetricRegistry>,
    ) -> Self {
        let db_name = rules.name.clone();
        let domain = metrics.register_domain_with_labels(
            "catalog",
            vec![
                metrics::KeyValue::new("db_name", db_name.to_string()),
                metrics::KeyValue::new("svr_id", format!("{}", server_id)),
            ],
        );

        let memory_registries = Default::default();
        domain.register_observer(None, &[], &memory_registries);

        let db_metrics = DbMetrics {
            catalog_chunks: domain.register_counter_metric_with_labels(
                "chunks",
                None,
                "In-memory chunks created in various life-cycle stages",
                vec![],
            ),
            catalog_immutable_chunk_bytes: domain
                .register_histogram_metric(
                    "chunk_creation",
                    "size",
                    "bytes",
                    "The new size of an immutable chunk",
                )
                .init(),
            catalog_chunk_bytes: domain.register_gauge_metric_with_labels(
                "chunk_size",
                Some("bytes"),
                "The size in bytes of all chunks",
                vec![],
            ),
            read_buffer_chunk_metrics: Arc::new(read_buffer::ChunkMetrics::new_with_db(
                &metrics,
                db_name.to_string(),
            )),
        };

        let rules = RwLock::new(rules);
        let server_id = server_id;
        let store = Arc::clone(&object_store);
        let write_buffer = write_buffer.map(Mutex::new);
        let catalog = Arc::new(Catalog::new(domain));
        let system_tables =
            SystemSchemaProvider::new(&db_name, Arc::clone(&catalog), Arc::clone(&jobs));
        let system_tables = Arc::new(system_tables);

        let process_clock = Arc::new(Mutex::new(
            NonZeroU64::new(now_nanos()).expect("current time should not be 0"),
        ));

        Self {
            rules,
            server_id,
            store,
            exec,
            catalog,
            write_buffer,
            jobs,
            metrics: db_metrics,
            metrics_registry: metrics,
            system_tables,
            memory_registries,
            process_clock,
            worker_iterations: AtomicUsize::new(0),
        }
    }

    /// Return a handle to the executor used to run queries
    pub fn executor(&self) -> Arc<Executor> {
        Arc::clone(&self.exec)
    }

    /// Rolls over the active chunk in the database's specified
    /// partition. Returns the previously open (now closed) Chunk if there was any.
    pub async fn rollover_partition(
        &self,
        partition_key: &str,
        table_name: &str,
    ) -> Result<Option<Arc<DbChunk>>> {
        let partition =
            self.catalog
                .valid_partition(partition_key)
                .context(RollingOverPartition {
                    partition_key,
                    table_name,
                })?;

        let partition = partition.write();
        if let Some(chunk) = partition
            .open_chunk(table_name)
            .context(RollingOverPartition {
                partition_key,
                table_name,
            })?
        {
            let mut chunk = chunk.write();
            let prev_chunk_state = Some((chunk.state().metric_label(), chunk.size()));
            chunk.set_closed().context(RollingOverPartition {
                partition_key,
                table_name,
            })?;

            // update metrics reflecting chunk moved to new state
            self.metrics.update_chunk_state(
                prev_chunk_state,
                Some((chunk.state().metric_label(), chunk.size())),
            );

            Ok(Some(DbChunk::snapshot(&chunk)))
        } else {
            Ok(None)
        }
    }

    /// Drops the specified chunk from the catalog and all storage systems
    pub fn drop_chunk(&self, partition_key: &str, table_name: &str, chunk_id: u32) -> Result<()> {
        debug!(%partition_key, %table_name, %chunk_id, "dropping chunk");

        let partition = self
            .catalog
            .valid_partition(partition_key)
            .context(DroppingChunk {
                partition_key,
                table_name,
                chunk_id,
            })?;

        // lock the partition so that no one else can be messing /
        // with it while we drop the chunk
        let mut partition = partition.write();

        let chunk_state;

        let prev_chunk_state = {
            let chunk = partition
                .chunk(table_name, chunk_id)
                .context(DroppingChunk {
                    partition_key,
                    table_name,
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
                    table_name,
                    chunk_id,
                    chunk_state,
                }
            );

            // track previous state before it's dropped
            (chunk.state().metric_label(), chunk.size())
        };

        debug!(%partition_key, %table_name, %chunk_id, %chunk_state, "dropping chunk");

        partition
            .drop_chunk(table_name, chunk_id)
            .context(DroppingChunk {
                partition_key,
                table_name,
                chunk_id,
            })
            .map(|_| {
                // update metrics reflecting chunk has been dropped
                self.metrics
                    .update_chunk_state(Some(prev_chunk_state), None);
            })
    }

    /// Copies a chunk in the Closed state into the ReadBuffer from
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
        table_name: &str,
        chunk_id: u32,
    ) -> Result<Arc<DbChunk>> {
        let chunk = {
            let partition = self
                .catalog
                .valid_partition(partition_key)
                .context(LoadingChunk {
                    partition_key,
                    table_name,
                    chunk_id,
                })?;
            let partition = partition.read();

            partition
                .chunk(table_name, chunk_id)
                .context(LoadingChunk {
                    partition_key,
                    table_name,
                    chunk_id,
                })?
        };

        let prev_chunk_state = {
            let chunk = chunk.read();
            (chunk.state().metric_label(), chunk.size())
        };

        // update the catalog to say we are processing this chunk and
        // then drop the lock while we do the work
        let mb_chunk = {
            let mut chunk = chunk.write();

            chunk.set_moving().context(LoadingChunk {
                partition_key,
                table_name,
                chunk_id,
            })?
        };
        //
        // Track the size of the newly immutable closed MUB chunk.
        self.metrics
            .catalog_immutable_chunk_bytes
            .observe_with_labels(
                mb_chunk.size() as f64,
                &[metrics::KeyValue::new(
                    "state",
                    chunk.read().state().metric_label(),
                )],
            );

        // chunk transitioned from open/closing to moving
        {
            let chunk = chunk.read();
            self.metrics.update_chunk_state(
                Some(prev_chunk_state),
                Some((chunk.state().metric_label(), chunk.size())),
            );
        }

        info!(%partition_key, %table_name, %chunk_id, "chunk marked MOVING, loading tables into read buffer");

        let mut batches = Vec::new();
        let table_summary = mb_chunk.table_summary();

        // create a new read buffer chunk with memory tracking
        let rb_chunk = ReadBufferChunk::new_with_registries(
            chunk_id,
            &self.memory_registries.read_buffer,
            Arc::clone(&self.metrics.read_buffer_chunk_metrics),
        );

        // load table into the new chunk one by one.
        debug!(%partition_key, %table_name, %chunk_id, table=%table_summary.name, "loading table to read buffer");
        mb_chunk
            .table_to_arrow(&mut batches, Selection::All)
            // It is probably reasonable to recover from this error
            // (reset the chunk state to Open) but until that is
            // implemented (and tested) just panic
            .expect("Loading chunk to mutable buffer");

        for batch in batches.drain(..) {
            let sorted = sort_record_batch(batch).expect("failed to sort");
            rb_chunk.upsert_table(&table_summary.name, sorted)
        }

        // Relock the chunk again (nothing else should have been able
        // to modify the chunk state while we were moving it
        let mut chunk = chunk.write();
        let prev_chunk_state = (chunk.state().metric_label(), chunk.size());

        // update the catalog to say we are done processing
        chunk.set_moved(Arc::new(rb_chunk)).context(LoadingChunk {
            partition_key,
            table_name,
            chunk_id,
        })?;

        // Track the size of the newly immutable RB chunk.
        self.metrics
            .catalog_immutable_chunk_bytes
            .observe_with_labels(
                chunk.size() as f64,
                &[metrics::KeyValue::new(
                    "state",
                    chunk.state().metric_label(),
                )],
            );

        // chunk transitioned from moving to moved
        self.metrics.update_chunk_state(
            Some(prev_chunk_state),
            Some((chunk.state().metric_label(), chunk.size())),
        );
        debug!(%partition_key, %table_name, %chunk_id, "chunk marked MOVED. loading complete");

        Ok(DbChunk::snapshot(&chunk))
    }

    /// Write given table of a given chunk to object store.
    /// The writing only happen if that chunk already in read buffer

    pub async fn write_chunk_to_object_store(
        &self,
        partition_key: &str,
        table_name: &str,
        chunk_id: u32,
    ) -> Result<Arc<DbChunk>> {
        // Get the chunk from the catalog
        let chunk = {
            let partition =
                self.catalog
                    .valid_partition(partition_key)
                    .context(LoadingChunkToParquet {
                        partition_key,
                        table_name,
                        chunk_id,
                    })?;
            let partition = partition.read();

            partition
                .chunk(table_name, chunk_id)
                .context(LoadingChunkToParquet {
                    partition_key,
                    table_name,
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
                    table_name,
                    chunk_id,
                })?
        };

        // chunk transitioned from moved to "writing", but the chunk remains in
        // the read buffer so we don't want to decrease that metric.
        {
            let chunk = chunk.read();
            self.metrics
                .update_chunk_state(None, Some((chunk.state().metric_label(), chunk.size())));
        }
        debug!(%partition_key, %table_name, %chunk_id, "chunk marked WRITING , loading tables into object store");

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
            debug!(%partition_key, %table_name, %chunk_id, table=%stats.name, "loading table to object store");

            let predicate = read_buffer::Predicate::default();

            // Get RecordBatchStream of data from the read buffer chunk
            let read_results = rb_chunk
                .read_filter(stats.name.as_str(), predicate, Selection::All)
                .context(ReadBufferChunkError {
                    table_name,
                    chunk_id,
                })?;

            let arrow_schema: ArrowSchemaRef = rb_chunk
                .read_filter_table_schema(stats.name.as_str(), Selection::All)
                .context(ReadBufferChunkSchemaError {
                    table_name,
                    chunk_id,
                })?
                .into();
            let time_range = rb_chunk.table_time_range(stats.name.as_str()).context(
                ReadBufferChunkTimestampError {
                    table_name,
                    chunk_id,
                },
            )?;
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
            let table_time_range = time_range.map(|(start, end)| TimestampRange::new(start, end));
            parquet_chunk.add_table(
                stats,
                path,
                Arc::clone(&self.store),
                schema,
                table_time_range,
            );
        }

        // Relock the chunk again (nothing else should have been able
        // to modify the chunk state while we were moving it
        let mut chunk = chunk.write();
        let prev_chunk_state = (chunk.state().metric_label(), chunk.size());

        // update the catalog to say we are done processing
        let parquet_chunk = Arc::clone(&Arc::new(parquet_chunk));
        chunk
            .set_written_to_object_store(parquet_chunk)
            .context(LoadingChunkToParquet {
                partition_key,
                table_name,
                chunk_id,
            })?;

        // Track the size of the newly written to OS chunk.
        self.metrics
            .catalog_immutable_chunk_bytes
            .observe_with_labels(
                chunk.size() as f64,
                &[metrics::KeyValue::new(
                    "state",
                    chunk.state().metric_label(),
                )],
            );

        // TODO(edd): metric updates here are brittle. Come up with a better
        // solution.
        // Reduce size of "written" chunk bytes
        self.metrics.catalog_chunk_bytes.sub_with_labels(
            prev_chunk_state.1 as f64,
            &[metrics::KeyValue::new("state", prev_chunk_state.0)],
        );

        // Increase size of chunk in "os" state.
        self.metrics.catalog_chunk_bytes.add_with_labels(
            chunk.size() as f64,
            &[metrics::KeyValue::new("state", "os")],
        );

        // Track a new chunk in "rub_and_os" state
        self.metrics
            .catalog_chunks
            .inc_with_labels(&[metrics::KeyValue::new(
                "state",
                chunk.state().metric_label(),
            )]);

        debug!(%partition_key, %table_name, %chunk_id, "chunk marked WRITTEN. Persisting to object store complete");

        // We know this chunk is ParquetFile type
        Ok(DbChunk::parquet_file_snapshot(&chunk))
    }

    /// Unload chunk from read buffer but keep it in object store
    pub async fn unload_read_buffer(
        &self,
        partition_key: &str,
        table_name: &str,
        chunk_id: u32,
    ) -> Result<Arc<DbChunk>> {
        debug!(%partition_key, %table_name, %chunk_id, "unloading chunk from read buffer");

        // Get the chunk from the catalog
        let chunk = {
            let partition = self.catalog.valid_partition(partition_key).context(
                UnloadingChunkFromReadBuffer {
                    partition_key,
                    table_name,
                    chunk_id,
                },
            )?;
            let partition = partition.read();

            partition
                .chunk(table_name, chunk_id)
                .context(UnloadingChunkFromReadBuffer {
                    partition_key,
                    table_name,
                    chunk_id,
                })?
        };

        // update the catalog to no longer use read buffer chunk if any
        let mut chunk = chunk.write();

        let rb_chunk = {
            chunk
                .set_unload_from_read_buffer()
                .context(UnloadingChunkFromReadBuffer {
                    partition_key,
                    table_name,
                    chunk_id,
                })?
        };

        // TODO(edd): metric updates here are brittle. Come up with a better
        // solution.
        // Reduce size of "moved" chunk bytes (in read buffer)
        self.metrics.catalog_chunk_bytes.sub_with_labels(
            rb_chunk.size() as f64,
            &[metrics::KeyValue::new("state", "moved")],
        );

        // Track a new chunk in "os"-only state
        self.metrics
            .catalog_chunks
            .inc_with_labels(&[metrics::KeyValue::new(
                "state",
                chunk.state().metric_label(),
            )]);

        self.metrics
            .catalog_immutable_chunk_bytes
            .observe_with_labels(
                chunk.size() as f64,
                &[metrics::KeyValue::new(
                    "state",
                    chunk.state().metric_label(),
                )],
            );

        debug!(%partition_key, %table_name, %chunk_id, "chunk marked UNLOADED from read buffer");

        Ok(DbChunk::snapshot(&chunk))
    }

    /// Spawns a task to perform
    /// [`load_chunk_to_read_buffer`](Self::load_chunk_to_read_buffer)
    pub fn load_chunk_to_read_buffer_in_background(
        self: &Arc<Self>,
        partition_key: String,
        table_name: String,
        chunk_id: u32,
    ) -> TaskTracker<Job> {
        let name = self.rules.read().name.clone();
        let (tracker, registration) = self.jobs.register(Job::CloseChunk {
            db_name: name.to_string(),
            partition_key: partition_key.clone(),
            table_name: table_name.clone(),
            chunk_id,
        });

        let captured = Arc::clone(&self);
        let task = async move {
            debug!(%name, %partition_key, %table_name, %chunk_id, "background task loading chunk to read buffer");
            let result = captured
                .load_chunk_to_read_buffer(&partition_key, &table_name, chunk_id)
                .await;
            if let Err(e) = result {
                info!(?e, %name, %partition_key, %chunk_id, "background task error loading read buffer chunk");
                return Err(e);
            }

            debug!(%name, %partition_key, %table_name, %chunk_id, "background task completed loading chunk to read buffer");

            Ok(())
        };

        tokio::spawn(task.track(registration));

        tracker
    }

    /// Spawns a task to perform
    /// [`write_chunk_to_object_store`](Self::write_chunk_to_object_store)
    pub fn write_chunk_to_object_store_in_background(
        self: &Arc<Self>,
        partition_key: String,
        table_name: String,
        chunk_id: u32,
    ) -> TaskTracker<Job> {
        let name = self.rules.read().name.clone();
        let (tracker, registration) = self.jobs.register(Job::WriteChunk {
            db_name: name.to_string(),
            partition_key: partition_key.clone(),
            table_name: table_name.clone(),
            chunk_id,
        });

        let captured = Arc::clone(&self);
        let task = async move {
            debug!(%name, %partition_key, %table_name, %chunk_id, "background task loading chunk to object store");
            let result = captured
                .write_chunk_to_object_store(&partition_key, &table_name, chunk_id)
                .await;
            if let Err(e) = result {
                info!(?e, %name, %partition_key, %chunk_id, "background task error loading object store chunk");
                return Err(e);
            }

            debug!(%name, %partition_key, %table_name, %chunk_id, "background task completed writing chunk to object store");

            Ok(())
        };

        tokio::spawn(task.track(registration));

        tracker
    }

    /// Returns the next process clock value, which will be the maximum of the system time in
    /// nanoseconds or the previous process clock value plus 1. Every operation that needs a
    /// process clock value should be incrementing it as well, so there should never be a read of
    /// the process clock without an accompanying increment of at least 1 nanosecond.
    pub fn next_process_clock(&self) -> ClockValue {
        let now = now_nanos();
        let mut current_process_clock = self.process_clock.lock();
        let next_candidate = current_process_clock.get() + 1;

        let next = now.max(next_candidate);
        let next = NonZeroU64::new(next).expect("next process clock must not be 0");
        *current_process_clock = next;
        ClockValue::new(next)
    }

    /// Return chunk summary information for all chunks in the specified
    /// partition across all storage systems
    pub fn partition_chunk_summaries(&self, partition_key: &str) -> Vec<ChunkSummary> {
        self.catalog.filtered_chunks(
            &PredicateBuilder::new().partition_key(partition_key).build(),
            CatalogChunk::summary,
        )
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
    pub fn table_summary(
        &self,
        partition_key: &str,
        table_name: &str,
        chunk_id: u32,
    ) -> Option<TableSummary> {
        if let Some(partition) = self.catalog.partition(partition_key) {
            let partition = partition.read();
            if let Ok(chunk) = partition.chunk(table_name, chunk_id) {
                return Some(chunk.read().table_summary());
            }
        }
        None
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

        let mut lifecycle_manager = LifecycleManager::new(Arc::clone(&self));

        while !shutdown.is_cancelled() {
            self.worker_iterations.fetch_add(1, Ordering::Relaxed);
            tokio::select! {
                _ = lifecycle_manager.check_for_work() => {},
                _ = shutdown.cancelled() => break
            }
        }

        info!("finished background worker");
    }

    /// Stores an entry based on the configuration. The Entry will first be
    /// converted into a `SequencedEntry` with the logical clock assigned
    /// from the database, and then the `SequencedEntry` will be passed to
    /// `store_sequenced_entry`.
    pub fn store_entry(&self, entry: Entry) -> Result<()> {
        let sequenced_entry = Arc::new(
            OwnedSequencedEntry::new_from_entry_bytes(
                self.next_process_clock(),
                self.server_id,
                entry.data(),
            )
            .context(SequencedEntryError)?,
        );

        self.store_sequenced_entry(sequenced_entry)
    }

    /// Given a `SequencedEntry`:
    ///
    /// - If the write buffer is configured, write the `SequencedEntry` into the buffer, which
    ///   will replicate the `SequencedEntry` based on the configured rules.
    /// - If the mutable buffer is configured, the `SequencedEntry` is then written into the
    ///   mutable buffer.
    ///
    /// Note that if the write buffer is configured but there is an error storing the
    /// `SequencedEntry` in the write buffer, the `SequencedEntry` will *not* reach the mutable
    /// buffer.
    pub fn store_sequenced_entry(&self, sequenced_entry: Arc<dyn SequencedEntry>) -> Result<()> {
        // Send to the write buffer, if configured
        if let Some(wb) = &self.write_buffer {
            wb.lock()
                .append(Arc::clone(&sequenced_entry))
                .context(WriteBufferError)?;
        }

        // Send to the mutable buffer

        let rules = self.rules.read();
        let mutable_size_threshold = rules.lifecycle_rules.mutable_size_threshold;
        if rules.lifecycle_rules.immutable {
            return DatabaseNotWriteable {}.fail();
        }
        if let Some(hard_limit) = rules.lifecycle_rules.buffer_size_hard {
            if self.memory_registries.bytes() > hard_limit.get() {
                return HardLimitReached {}.fail();
            }
        }
        std::mem::drop(rules);

        // TODO: Direct writes to closing chunks

        if let Some(partitioned_writes) = sequenced_entry.partition_writes() {
            for write in partitioned_writes {
                let partition_key = write.key();
                let partition = self.catalog.get_or_create_partition(partition_key);
                let mut partition = partition.write();
                partition.update_last_write_at();

                for table_batch in write.table_batches() {
                    match partition.open_chunk(table_batch.name()).ok().flatten() {
                        Some(chunk) => {
                            let mut chunk = chunk.write();
                            chunk.record_write();
                            let chunk_id = chunk.id();

                            let mb_chunk =
                                chunk.mutable_buffer().expect("cannot mutate open chunk");

                            mb_chunk
                                .write_table_batch(
                                    sequenced_entry.clock_value(),
                                    sequenced_entry.server_id(),
                                    table_batch,
                                )
                                .context(WriteEntry {
                                    partition_key,
                                    chunk_id,
                                })?;

                            // set new size of chunk
                            self.metrics.catalog_chunk_bytes.set_with_labels(
                                chunk.size() as f64,
                                &[metrics::KeyValue::new(
                                    "state",
                                    chunk.state().metric_label(),
                                )],
                            );

                            check_chunk_closed(&mut *chunk, mutable_size_threshold, &self.metrics);
                        }
                        None => {
                            let new_chunk = partition
                                .create_open_chunk(
                                    table_batch,
                                    sequenced_entry.clock_value(),
                                    sequenced_entry.server_id(),
                                    self.memory_registries.mutable_buffer.as_ref(),
                                )
                                .context(OpenEntry { partition_key })?;

                            {
                                // track new open chunk
                                let chunk = new_chunk.read();
                                self.metrics.update_chunk_state(
                                    None,
                                    Some((chunk.state().metric_label(), chunk.size())),
                                );
                            }

                            check_chunk_closed(
                                &mut *new_chunk.write(),
                                mutable_size_threshold,
                                &self.metrics,
                            );
                        }
                    };
                }
            }
        }

        Ok(())
    }
}

/// Check if the given chunk should be closed based on the the MutableBuffer size threshold.
fn check_chunk_closed(
    chunk: &mut CatalogChunk,
    mutable_size_threshold: Option<NonZeroUsize>,
    metrics: &DbMetrics,
) {
    let prev_chunk_state = (chunk.state().metric_label(), chunk.size());

    if let Some(threshold) = mutable_size_threshold {
        if let Ok(mb_chunk) = chunk.mutable_buffer() {
            let size = mb_chunk.size();

            if size > threshold.get() {
                chunk.set_closed().expect("cannot close open chunk");
                metrics.update_chunk_state(
                    Some(prev_chunk_state),
                    Some((chunk.state().metric_label(), chunk.size())),
                );
            }
        }
    }
}

#[async_trait]
impl Database for Db {
    type Error = Error;
    type Chunk = DbChunk;

    /// Return a covering set of chunks for a particular partition
    ///
    /// Note there could/should be an error here (if the partition
    /// doesn't exist... but the trait doesn't have an error)
    fn chunks(&self, predicate: &Predicate) -> Vec<Arc<Self::Chunk>> {
        self.catalog.filtered_chunks(predicate, DbChunk::snapshot)
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

// Convenience function for getting the current time in a `u64` represented as nanoseconds since
// the epoch
fn now_nanos() -> u64 {
    Utc::now()
        .timestamp_nanos()
        .try_into()
        .expect("current time since the epoch should be positive")
}

pub mod test_helpers {
    use super::*;
    use entry::test_helpers::lp_to_entries;
    use std::collections::HashSet;

    /// Try to write lineprotocol data and return all tables that where written.
    pub fn try_write_lp(db: &Db, lp: &str) -> Result<Vec<String>> {
        let entries = lp_to_entries(lp);

        let mut tables = HashSet::new();
        for entry in &entries {
            if let Some(writes) = entry.partition_writes() {
                for write in writes {
                    for batch in write.table_batches() {
                        tables.insert(batch.name().to_string());
                    }
                }
            }
        }

        entries
            .into_iter()
            .try_for_each(|entry| db.store_entry(entry))?;

        let mut tables: Vec<_> = tables.into_iter().collect();
        tables.sort();
        Ok(tables)
    }

    /// Same was [`try_write_lp`](try_write_lp) but will panic on failure.
    pub fn write_lp(db: &Db, lp: &str) -> Vec<String> {
        try_write_lp(db, lp).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        test_helpers::{try_write_lp, write_lp},
        *,
    };
    use crate::query_tests::utils::{make_db, TestDb};
    use ::test_helpers::assert_contains;
    use arrow::record_batch::RecordBatch;
    use arrow_util::{assert_batches_eq, assert_batches_sorted_eq};
    use chrono::Utc;
    use data_types::{
        chunk::ChunkStorage,
        database_rules::{Order, Sort, SortOrder},
        partition_metadata::{ColumnSummary, InfluxDbType, StatValues, Statistics, TableSummary},
    };
    use datafusion::execution::context;
    use entry::test_helpers::lp_to_entry;
    use futures::{stream, StreamExt, TryStreamExt};
    use object_store::{disk::File, path::Path, ObjectStore, ObjectStoreApi};
    use query::{frontend::sql::SqlQueryPlanner, PartitionChunk};
    use std::{convert::TryFrom, iter::Iterator, num::NonZeroUsize, str};
    use tempfile::TempDir;

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    #[test]
    fn write_no_mutable_buffer() {
        // Validate that writes are rejected if there is no mutable buffer
        let db = make_db().db;
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
        let db = Arc::new(make_db().db);
        write_lp(&db, "cpu bar=1 10");

        let batches = run_query(db, "select * from cpu").await;

        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "+-----+-------------------------------+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    fn catalog_chunk_size_bytes_metric_eq(
        reg: &metrics::TestMetricRegistry,
        state: &'static str,
        v: u64,
    ) -> Result<(), metrics::Error> {
        reg.has_metric_family("catalog_chunk_size_bytes")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", state),
                ("svr_id", "1"),
            ])
            .gauge()
            .eq(v as f64)
    }

    #[tokio::test]
    async fn metrics_during_rollover() {
        let test_db = make_db();
        let db = Arc::new(test_db.db);

        write_lp(db.as_ref(), "cpu bar=1 10");

        // A chunk has been opened
        test_db
            .metric_registry
            .has_metric_family("catalog_chunks_total")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "open"),
                ("svr_id", "1"),
            ])
            .counter()
            .eq(1.0)
            .unwrap();

        // verify chunk size updated
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "open", 72).unwrap();

        // write into same chunk again.
        write_lp(db.as_ref(), "cpu bar=2 10");

        // verify chunk size updated
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "open", 88).unwrap();

        // Still only one chunk open
        test_db
            .metric_registry
            .has_metric_family("catalog_chunks_total")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "open"),
                ("svr_id", "1"),
            ])
            .counter()
            .eq(1.0)
            .unwrap();

        db.rollover_partition("1970-01-01T00", "cpu").await.unwrap();

        // A chunk is now closed
        test_db
            .metric_registry
            .has_metric_family("catalog_chunks_total")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "closed"),
                ("svr_id", "1"),
            ])
            .counter()
            .eq(1.0)
            .unwrap();

        // verify chunk size updated (chunk moved from open to closed)
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "open", 0).unwrap();
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "closed", 88).unwrap();

        db.load_chunk_to_read_buffer("1970-01-01T00", "cpu", 0)
            .await
            .unwrap();

        // A chunk is now in the read buffer
        test_db
            .metric_registry
            .has_metric_family("catalog_chunks_total")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "closed"),
                ("svr_id", "1"),
            ])
            .counter()
            .eq(1.0)
            .unwrap();

        // verify chunk size updated (chunk moved from closing to moving to moved)
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "closed", 0).unwrap();
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "moving", 0).unwrap();
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "moved", 1230).unwrap();

        db.write_chunk_to_object_store("1970-01-01T00", "cpu", 0)
            .await
            .unwrap();

        // A chunk is now in the object store and still in read buffer
        test_db
            .metric_registry
            .has_metric_family("catalog_chunks_total")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "rub_and_os"),
                ("svr_id", "1"),
            ])
            .counter()
            .eq(1.0)
            .unwrap();

        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "moved", 1230).unwrap();
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "os", 1921).unwrap(); // now also in OS

        db.unload_read_buffer("1970-01-01T00", "cpu", 0)
            .await
            .unwrap();

        // A chunk is now now in the "os-only" state.
        test_db
            .metric_registry
            .has_metric_family("catalog_chunks_total")
            .with_labels(&[("db_name", "placeholder"), ("state", "os"), ("svr_id", "1")])
            .counter()
            .eq(1.0)
            .unwrap();

        // verify chunk size not increased for OS (it was in OS before unload)
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "os", 1921).unwrap();
        // verify chunk size for RB has decreased
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "moved", 0).unwrap();
    }

    #[tokio::test]
    async fn write_with_rollover() {
        let db = Arc::new(make_db().db);
        write_lp(db.as_ref(), "cpu bar=1 10");
        assert_eq!(vec!["1970-01-01T00"], db.partition_keys().unwrap());

        let mb_chunk = db
            .rollover_partition("1970-01-01T00", "cpu")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mb_chunk.id(), 0);

        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "+-----+-------------------------------+",
        ];
        let batches = run_query(Arc::clone(&db), "select * from cpu").await;
        assert_batches_sorted_eq!(expected, &batches);

        // add new data
        write_lp(db.as_ref(), "cpu bar=2 20");
        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "| 2   | 1970-01-01 00:00:00.000000020 |",
            "+-----+-------------------------------+",
        ];
        let batches = run_query(Arc::clone(&db), "select * from cpu").await;
        assert_batches_sorted_eq!(&expected, &batches);

        // And expect that we still get the same thing when data is rolled over again
        let chunk = db
            .rollover_partition("1970-01-01T00", "cpu")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(chunk.id(), 1);

        let batches = run_query(db, "select * from cpu").await;
        assert_batches_sorted_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn write_with_missing_tags_are_null() {
        let db = Arc::new(make_db().db);
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

        let mb_chunk = db
            .rollover_partition("1970-01-01T00", "cpu")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mb_chunk.id(), 0);

        let expected = vec![
            "+------+--------+-------------------------------+------+",
            "| core | region | time                          | user |",
            "+------+--------+-------------------------------+------+",
            "|      | west   | 1970-01-01 00:00:00.000000010 | 23.2 |",
            "|      |        | 1970-01-01 00:00:00.000000011 | 10   |",
            "| one  |        | 1970-01-01 00:00:00.000000011 | 10   |",
            "+------+--------+-------------------------------+------+",
        ];
        let batches = run_query(Arc::clone(&db), "select * from cpu").await;
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn read_from_read_buffer() {
        // Test that data can be loaded into the ReadBuffer
        let test_db = make_db();
        let db = Arc::new(test_db.db);

        write_lp(db.as_ref(), "cpu bar=1 10");
        write_lp(db.as_ref(), "cpu bar=2 20");

        let partition_key = "1970-01-01T00";
        let mb_chunk = db
            .rollover_partition(partition_key, "cpu")
            .await
            .unwrap()
            .unwrap();
        let rb_chunk = db
            .load_chunk_to_read_buffer(partition_key, "cpu", mb_chunk.id())
            .await
            .unwrap();

        // it should be the same chunk!
        assert_eq!(mb_chunk.id(), rb_chunk.id());

        // we should have chunks in both the read buffer only
        assert!(mutable_chunk_ids(&db, partition_key).is_empty());
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![0]);

        // data should be readable
        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "| 2   | 1970-01-01 00:00:00.000000020 |",
            "+-----+-------------------------------+",
        ];
        let batches = run_query(Arc::clone(&db), "select * from cpu").await;
        assert_batches_eq!(&expected, &batches);

        // A chunk is now in the object store
        test_db
            .metric_registry
            .has_metric_family("catalog_chunks_total")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "moved"),
                ("svr_id", "1"),
            ])
            .counter()
            .eq(1.0)
            .unwrap();

        // verify chunk size updated (chunk moved from moved to writing to written)
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "moved", 1230).unwrap();

        // drop, the chunk from the read buffer
        db.drop_chunk(partition_key, "cpu", mb_chunk.id()).unwrap();
        assert_eq!(
            read_buffer_chunk_ids(db.as_ref(), partition_key),
            vec![] as Vec<u32>
        );

        // verify chunk size updated (chunk dropped from moved state)
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "moved", 0).unwrap();

        // Currently this doesn't work (as we need to teach the stores how to
        // purge tables after data bas been dropped println!("running
        // query after all data dropped!"); let expected = vec![] as
        // Vec<&str>; let batches = run_query(&db, "select * from
        // cpu").await; assert_batches_eq!(expected, &batches);
    }

    async fn collect_read_filter(chunk: &DbChunk, table_name: &str) -> Vec<RecordBatch> {
        chunk
            .read_filter(table_name, &Default::default(), Selection::All)
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect()
    }

    #[tokio::test]
    async fn load_to_read_buffer_sorted() {
        let test_db = make_db();
        let db = Arc::new(test_db.db);

        write_lp(db.as_ref(), "cpu,tag1=cupcakes bar=1 10");
        write_lp(db.as_ref(), "cpu,tag1=asfd,tag2=foo bar=2 20");
        write_lp(db.as_ref(), "cpu,tag1=bingo,tag2=foo bar=2 10");
        write_lp(db.as_ref(), "cpu,tag1=bongo,tag2=a bar=2 20");
        write_lp(db.as_ref(), "cpu,tag1=bongo,tag2=a bar=2 10");
        write_lp(db.as_ref(), "cpu,tag2=a bar=3 5");

        let partition_key = "1970-01-01T00";
        let mb_chunk = db
            .rollover_partition(partition_key, "cpu")
            .await
            .unwrap()
            .unwrap();

        let mb = collect_read_filter(&mb_chunk, "cpu").await;

        let rb_chunk = db
            .load_chunk_to_read_buffer(partition_key, "cpu", mb_chunk.id())
            .await
            .unwrap();

        // MUB chunk size
        test_db
            .metric_registry
            .has_metric_family("catalog_chunk_creation_size_bytes")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "moving"),
                ("svr_id", "1"),
            ])
            .histogram()
            .sample_sum_eq(316.0)
            .unwrap();

        // RB chunk size
        test_db
            .metric_registry
            .has_metric_family("catalog_chunk_creation_size_bytes")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "moved"),
                ("svr_id", "1"),
            ])
            .histogram()
            .sample_sum_eq(3547.0)
            .unwrap();

        let rb = collect_read_filter(&rb_chunk, "cpu").await;

        // Test that data on load into the read buffer is sorted

        assert_batches_eq!(
            &[
                "+-----+----------+------+-------------------------------+",
                "| bar | tag1     | tag2 | time                          |",
                "+-----+----------+------+-------------------------------+",
                "| 1   | cupcakes |      | 1970-01-01 00:00:00.000000010 |",
                "| 2   | asfd     | foo  | 1970-01-01 00:00:00.000000020 |",
                "| 2   | bingo    | foo  | 1970-01-01 00:00:00.000000010 |",
                "| 2   | bongo    | a    | 1970-01-01 00:00:00.000000020 |",
                "| 2   | bongo    | a    | 1970-01-01 00:00:00.000000010 |",
                "| 3   |          | a    | 1970-01-01 00:00:00.000000005 |",
                "+-----+----------+------+-------------------------------+",
            ],
            &mb
        );

        assert_batches_eq!(
            &[
                "+-----+----------+------+-------------------------------+",
                "| bar | tag1     | tag2 | time                          |",
                "+-----+----------+------+-------------------------------+",
                "| 1   | cupcakes |      | 1970-01-01 00:00:00.000000010 |",
                "| 3   |          | a    | 1970-01-01 00:00:00.000000005 |",
                "| 2   | bongo    | a    | 1970-01-01 00:00:00.000000010 |",
                "| 2   | bongo    | a    | 1970-01-01 00:00:00.000000020 |",
                "| 2   | asfd     | foo  | 1970-01-01 00:00:00.000000020 |",
                "| 2   | bingo    | foo  | 1970-01-01 00:00:00.000000010 |",
                "+-----+----------+------+-------------------------------+",
            ],
            &rb
        );
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
    async fn write_one_chunk_to_parquet_file() {
        // Test that data can be written into parquet files

        // Create an object store with a specified location in a local disk
        let root = TempDir::new().unwrap();
        let object_store = Arc::new(ObjectStore::new_file(File::new(root.path())));

        // Create a DB given a server id, an object store and a db name
        let server_id = ServerId::try_from(10).unwrap();
        let db_name = "parquet_test_db";
        let test_db = TestDb::builder()
            .server_id(server_id)
            .object_store(Arc::clone(&object_store))
            .db_name(db_name)
            .build();

        let db = Arc::new(test_db.db);

        // Write some line protocols in Mutable buffer of the DB
        write_lp(db.as_ref(), "cpu bar=1 10");
        write_lp(db.as_ref(), "cpu bar=2 20");

        //Now mark the MB chunk close
        let partition_key = "1970-01-01T00";
        let mb_chunk = db
            .rollover_partition("1970-01-01T00", "cpu")
            .await
            .unwrap()
            .unwrap();
        // Move that MB chunk to RB chunk and drop it from MB
        let rb_chunk = db
            .load_chunk_to_read_buffer(partition_key, "cpu", mb_chunk.id())
            .await
            .unwrap();
        // Write the RB chunk to Object Store but keep it in RB
        let pq_chunk = db
            .write_chunk_to_object_store(partition_key, "cpu", mb_chunk.id())
            .await
            .unwrap();

        // Read buffer + Parquet chunk size
        test_db
            .metric_registry
            .has_metric_family("catalog_chunk_creation_size_bytes")
            .with_labels(&[
                ("db_name", "parquet_test_db"),
                ("state", "rub_and_os"),
                ("svr_id", "10"),
            ])
            .histogram()
            .sample_sum_eq(1921.0)
            .unwrap();

        // it should be the same chunk!
        assert_eq!(mb_chunk.id(), rb_chunk.id());
        assert_eq!(mb_chunk.id(), pq_chunk.id());

        // we should have chunks in both the read buffer only
        assert!(mutable_chunk_ids(&db, partition_key).is_empty());
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
        let path0 = match &paths[0] {
            Path::File(file_path) => file_path.to_raw(),
            other => panic!("expected `Path::File`, got: {:?}", other),
        };

        let mut path = root.path().to_path_buf();
        path.push(&path0);
        println!("path: {}", path.display());

        // Create External table of this parquet file to get its content in a human
        // readable form
        // Note: We do not care about escaping quotes here because it is just a test
        let sql = format!(
            "CREATE EXTERNAL TABLE parquet_table STORED AS PARQUET LOCATION '{}'",
            path.display()
        );

        let mut ctx = context::ExecutionContext::new();
        let df = ctx.sql(&sql).unwrap();
        df.collect().await.unwrap();

        // Select data from that table
        let sql = "SELECT * FROM parquet_table";
        let content = ctx.sql(&sql).unwrap().collect().await.unwrap();
        println!("Content: {:?}", content);
        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "| 2   | 1970-01-01 00:00:00.000000020 |",
            "+-----+-------------------------------+",
        ];
        assert_batches_eq!(expected, &content);
    }

    #[tokio::test]
    async fn unload_chunk_from_read_buffer() {
        // Test that data can be written into parquet files and then
        // remove it from read buffer and make sure we are still
        // be able to read data from object store

        // Create an object store with a specified location in a local disk
        let root = TempDir::new().unwrap();
        let object_store = Arc::new(ObjectStore::new_file(File::new(root.path())));

        // Create a DB given a server id, an object store and a db name
        let server_id = ServerId::try_from(10).unwrap();
        let db_name = "unload_read_buffer_test_db";
        let test_db = TestDb::builder()
            .server_id(server_id)
            .object_store(Arc::clone(&object_store))
            .db_name(db_name)
            .build();

        let db = Arc::new(test_db.db);

        // Write some line protocols in Mutable buffer of the DB
        write_lp(db.as_ref(), "cpu bar=1 10");
        write_lp(db.as_ref(), "cpu bar=2 20");

        // Now mark the MB chunk close
        let partition_key = "1970-01-01T00";
        let mb_chunk = db
            .rollover_partition("1970-01-01T00", "cpu")
            .await
            .unwrap()
            .unwrap();
        // Move that MB chunk to RB chunk and drop it from MB
        let rb_chunk = db
            .load_chunk_to_read_buffer(partition_key, "cpu", mb_chunk.id())
            .await
            .unwrap();
        // Write the RB chunk to Object Store but keep it in RB
        let pq_chunk = db
            .write_chunk_to_object_store(partition_key, "cpu", mb_chunk.id())
            .await
            .unwrap();

        // it should be the same chunk!
        assert_eq!(mb_chunk.id(), rb_chunk.id());
        assert_eq!(mb_chunk.id(), pq_chunk.id());

        // we should have chunks in both the read buffer only
        assert!(mutable_chunk_ids(&db, partition_key).is_empty());
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![0]);
        assert_eq!(read_parquet_file_chunk_ids(&db, partition_key), vec![0]);

        // Read buffer + Parquet chunk size
        test_db
            .metric_registry
            .has_metric_family("catalog_chunk_creation_size_bytes")
            .with_labels(&[
                ("db_name", "unload_read_buffer_test_db"),
                ("state", "rub_and_os"),
                ("svr_id", "10"),
            ])
            .histogram()
            .sample_sum_eq(1921.0)
            .unwrap();

        // Unload RB chunk but keep it in OS
        let pq_chunk = db
            .unload_read_buffer(partition_key, "cpu", mb_chunk.id())
            .await
            .unwrap();

        // still should be the same chunk!
        assert_eq!(mb_chunk.id(), rb_chunk.id());
        assert_eq!(mb_chunk.id(), pq_chunk.id());

        // we should only have chunk in os
        assert!(mutable_chunk_ids(&db, partition_key).is_empty());
        assert!(read_buffer_chunk_ids(&db, partition_key).is_empty());
        assert_eq!(read_parquet_file_chunk_ids(&db, partition_key), vec![0]);

        // Parquet chunk size only
        test_db
            .metric_registry
            .has_metric_family("catalog_chunk_creation_size_bytes")
            .with_labels(&[
                ("db_name", "unload_read_buffer_test_db"),
                ("state", "os"),
                ("svr_id", "10"),
            ])
            .histogram()
            .sample_sum_eq(691.0)
            .unwrap();

        // Verify data written to the parquet file in object store
        //
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
        let path0 = match &paths[0] {
            Path::File(file_path) => file_path.to_raw(),
            other => panic!("expected `Path::File`, got: {:?}", other),
        };

        let mut path = root.path().to_path_buf();
        path.push(&path0);
        println!("path: {}", path.display());

        // Read Metadata

        // Get data back using SQL
        // Create External table of this parquet file to get its content in a human
        // readable form
        // Note: We do not care about escaping quotes here because it is just a test
        let sql = format!(
            "CREATE EXTERNAL TABLE parquet_table STORED AS PARQUET LOCATION '{}'",
            path.display()
        );

        let mut ctx = context::ExecutionContext::new();
        let df = ctx.sql(&sql).unwrap();
        df.collect().await.unwrap();

        // Select data from that table
        let sql = "SELECT * FROM parquet_table";
        let content = ctx.sql(&sql).unwrap().collect().await.unwrap();
        println!("Content: {:?}", content);
        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "| 2   | 1970-01-01 00:00:00.000000020 |",
            "+-----+-------------------------------+",
        ];
        assert_batches_eq!(expected, &content);
    }

    #[test]
    fn write_updates_last_write_at() {
        let db = Arc::new(make_db().db);
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
        let db = Arc::new(make_db().db);

        // Given data loaded into two chunks
        write_lp(&db, "cpu bar=1 10");
        let after_data_load = Utc::now();

        // When the chunk is rolled over
        let partition_key = "1970-01-01T00";
        let chunk_id = db
            .rollover_partition("1970-01-01T00", "cpu")
            .await
            .unwrap()
            .unwrap()
            .id();
        let after_rollover = Utc::now();

        let partition = db.catalog.valid_partition(partition_key).unwrap();
        let partition = partition.read();
        let chunk = partition.chunk("cpu", chunk_id).unwrap();
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
        assert!(after_data_load < chunk.time_closed().unwrap());
        assert!(chunk.time_closed().unwrap() < after_rollover);
    }

    #[test]
    fn test_chunk_closing() {
        let db = Arc::new(make_db().db);
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
        assert!(matches!(chunks[0].read().state(), ChunkState::Closed(_)));
        assert!(matches!(chunks[1].read().state(), ChunkState::Closed(_)));
    }

    #[test]
    fn chunks_sorted_by_times() {
        let db = Arc::new(make_db().db);
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
        let db = Arc::new(make_db().db);
        let partition_key = "1970-01-01T00";

        write_lp(&db, "cpu bar=1 10");
        write_lp(&db, "cpu bar=1 20");

        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![0]);
        assert_eq!(
            read_buffer_chunk_ids(&db, partition_key),
            vec![] as Vec<u32>
        );

        let partition_key = "1970-01-01T00";
        let mb_chunk = db
            .rollover_partition("1970-01-01T00", "cpu")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(mb_chunk.id(), 0);

        // add a new chunk in mutable buffer, and move chunk1 (but
        // not chunk 0) to read buffer
        write_lp(&db, "cpu bar=1 30");
        let mb_chunk = db
            .rollover_partition("1970-01-01T00", "cpu")
            .await
            .unwrap()
            .unwrap();
        db.load_chunk_to_read_buffer(partition_key, "cpu", mb_chunk.id())
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
                    table_name,
                    id,
                    storage,
                    estimated_bytes,
                    row_count,
                    ..
                } = summary;
                ChunkSummary::new_without_timestamps(
                    partition_key,
                    table_name,
                    id,
                    storage,
                    estimated_bytes,
                    row_count,
                )
            })
            .collect::<Vec<_>>();
        summaries.sort_unstable();
        summaries
    }

    #[tokio::test]
    async fn partition_chunk_summaries() {
        // Test that chunk id listing is hooked up
        let db = Arc::new(make_db().db);

        write_lp(&db, "cpu bar=1 1");
        db.rollover_partition("1970-01-01T00", "cpu").await.unwrap();

        // write into a separate partitiion
        write_lp(&db, "cpu bar=1,baz2,frob=3 400000000000000");

        print!("Partitions: {:?}", db.partition_keys().unwrap());

        let chunk_summaries = db.partition_chunk_summaries("1970-01-05T15");
        let chunk_summaries = normalize_summaries(chunk_summaries);

        let expected = vec![ChunkSummary::new_without_timestamps(
            Arc::from("1970-01-05T15"),
            Arc::from("cpu"),
            0,
            ChunkStorage::OpenMutableBuffer,
            106,
            1,
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
        let db = Arc::new(make_db().db);
        let start = Utc::now();
        write_lp(&db, "cpu bar=1 1");
        let after_first_write = Utc::now();
        write_lp(&db, "cpu bar=2 2");
        db.rollover_partition("1970-01-01T00", "cpu").await.unwrap();
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
            summary.time_closed.unwrap() > after_first_write,
            "summary; {:#?}",
            summary
        );
        assert!(
            summary.time_closed.unwrap() < after_close,
            "summary; {:#?}",
            summary
        );
    }

    #[tokio::test]
    async fn chunk_summaries() {
        // Test that chunk id listing is hooked up
        let db = Arc::new(make_db().db);

        // get three chunks: one open, one closed in mb and one close in rb
        write_lp(&db, "cpu bar=1 1");
        db.rollover_partition("1970-01-01T00", "cpu").await.unwrap();

        write_lp(&db, "cpu bar=1,baz=2 2");
        write_lp(&db, "cpu bar=1,baz=2,frob=3 400000000000000");

        print!("Partitions: {:?}", db.partition_keys().unwrap());

        db.load_chunk_to_read_buffer("1970-01-01T00", "cpu", 0)
            .await
            .unwrap();

        db.write_chunk_to_object_store("1970-01-01T00", "cpu", 0)
            .await
            .unwrap();

        print!("Partitions2: {:?}", db.partition_keys().unwrap());

        db.rollover_partition("1970-01-05T15", "cpu").await.unwrap();
        write_lp(&db, "cpu bar=1,baz=3,blargh=3 400000000000000");

        let chunk_summaries = db.chunk_summaries().expect("expected summary to return");
        let chunk_summaries = normalize_summaries(chunk_summaries);

        let expected = vec![
            ChunkSummary::new_without_timestamps(
                Arc::from("1970-01-01T00"),
                Arc::from("cpu"),
                0,
                ChunkStorage::ReadBufferAndObjectStore,
                1912, // size of RB and OS chunks
                1,
            ),
            ChunkSummary::new_without_timestamps(
                Arc::from("1970-01-01T00"),
                Arc::from("cpu"),
                1,
                ChunkStorage::OpenMutableBuffer,
                100,
                1,
            ),
            ChunkSummary::new_without_timestamps(
                Arc::from("1970-01-05T15"),
                Arc::from("cpu"),
                0,
                ChunkStorage::ClosedMutableBuffer,
                129,
                1,
            ),
            ChunkSummary::new_without_timestamps(
                Arc::from("1970-01-05T15"),
                Arc::from("cpu"),
                1,
                ChunkStorage::OpenMutableBuffer,
                131,
                1,
            ),
        ];

        assert_eq!(
            expected, chunk_summaries,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, chunk_summaries
        );

        assert_eq!(db.memory_registries.mutable_buffer.bytes(), 100 + 129 + 131);
        assert_eq!(db.memory_registries.read_buffer.bytes(), 1221);
        assert_eq!(db.memory_registries.parquet.bytes(), 89); // TODO: This 89 must be replaced with 675. Ticket #1311
    }

    #[tokio::test]
    async fn partition_summaries() {
        // Test that chunk id listing is hooked up
        let db = Arc::new(make_db().db);

        write_lp(&db, "cpu bar=1 1");
        let chunk_id = db
            .rollover_partition("1970-01-01T00", "cpu")
            .await
            .unwrap()
            .unwrap()
            .id();
        write_lp(&db, "cpu bar=2,baz=3.0 2");
        write_lp(&db, "mem foo=1 1");

        // load a chunk to the read buffer
        db.load_chunk_to_read_buffer("1970-01-01T00", "cpu", chunk_id)
            .await
            .unwrap();

        // write the read buffer chunk to object store
        db.write_chunk_to_object_store("1970-01-01T00", "cpu", chunk_id)
            .await
            .unwrap();

        // write into a separate partition
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
                                influxdb_type: Some(InfluxDbType::Field),
                                stats: Statistics::F64(StatValues {
                                    min: Some(1.0),
                                    max: Some(2.0),
                                    count: 2,
                                }),
                            },
                            ColumnSummary {
                                name: "time".into(),
                                influxdb_type: Some(InfluxDbType::Timestamp),
                                stats: Statistics::I64(StatValues {
                                    min: Some(1),
                                    max: Some(2),
                                    count: 2,
                                }),
                            },
                            ColumnSummary {
                                name: "baz".into(),
                                influxdb_type: Some(InfluxDbType::Field),
                                stats: Statistics::F64(StatValues {
                                    min: Some(3.0),
                                    max: Some(3.0),
                                    count: 1,
                                }),
                            },
                        ],
                    },
                    TableSummary {
                        name: "mem".into(),
                        columns: vec![
                            ColumnSummary {
                                name: "foo".into(),
                                influxdb_type: Some(InfluxDbType::Field),
                                stats: Statistics::F64(StatValues {
                                    min: Some(1.0),
                                    max: Some(1.0),
                                    count: 1,
                                }),
                            },
                            ColumnSummary {
                                name: "time".into(),
                                influxdb_type: Some(InfluxDbType::Timestamp),
                                stats: Statistics::I64(StatValues {
                                    min: Some(1),
                                    max: Some(1),
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
                                influxdb_type: Some(InfluxDbType::Field),
                                stats: Statistics::F64(StatValues {
                                    min: Some(1.0),
                                    max: Some(1.0),
                                    count: 1,
                                }),
                            },
                            ColumnSummary {
                                name: "time".into(),
                                influxdb_type: Some(InfluxDbType::Timestamp),
                                stats: Statistics::I64(StatValues {
                                    min: Some(400000000000000),
                                    max: Some(400000000000000),
                                    count: 1,
                                }),
                            },
                        ],
                    },
                    TableSummary {
                        name: "mem".into(),
                        columns: vec![
                            ColumnSummary {
                                name: "frob".into(),
                                influxdb_type: Some(InfluxDbType::Field),
                                stats: Statistics::F64(StatValues {
                                    min: Some(3.0),
                                    max: Some(3.0),
                                    count: 1,
                                }),
                            },
                            ColumnSummary {
                                name: "time".into(),
                                influxdb_type: Some(InfluxDbType::Timestamp),
                                stats: Statistics::I64(StatValues {
                                    min: Some(400000000000001),
                                    max: Some(400000000000001),
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
        let planner = SqlQueryPlanner::default();
        let executor = db.executor();

        let physical_plan = planner.query(db, query, &executor).unwrap();

        executor.collect(physical_plan).await.unwrap()
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

    #[tokio::test]
    async fn write_chunk_to_object_store_in_background() {
        // Test that data can be written to object store using a background task
        let db = Arc::new(make_db().db);

        // create MB partition
        write_lp(db.as_ref(), "cpu bar=1 10");
        write_lp(db.as_ref(), "cpu bar=2 20");

        // MB => RB
        let partition_key = "1970-01-01T00";
        let table_name = "cpu";
        let mb_chunk = db
            .rollover_partition(partition_key, table_name)
            .await
            .unwrap()
            .unwrap();
        let rb_chunk = db
            .load_chunk_to_read_buffer(partition_key, table_name, mb_chunk.id())
            .await
            .unwrap();
        assert_eq!(mb_chunk.id(), rb_chunk.id());

        // RB => OS
        let task = db.write_chunk_to_object_store_in_background(
            partition_key.to_string(),
            table_name.to_string(),
            rb_chunk.id(),
        );
        let t_start = std::time::Instant::now();
        while !task.is_complete() {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            assert!(
                std::time::Instant::now() - t_start < std::time::Duration::from_secs(10),
                "task deadline exceeded"
            );
        }

        // we should have chunks in both the read buffer only
        assert!(mutable_chunk_ids(&db, partition_key).is_empty());
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![0]);
        assert_eq!(read_parquet_file_chunk_ids(&db, partition_key), vec![0]);
    }

    #[test]
    fn write_hard_limit() {
        let db = Arc::new(make_db().db);
        db.rules.write().lifecycle_rules.buffer_size_hard = Some(NonZeroUsize::new(10).unwrap());

        // inserting first line does not trigger hard buffer limit
        write_lp(db.as_ref(), "cpu bar=1 10");

        // but second line will
        assert!(matches!(
            try_write_lp(db.as_ref(), "cpu bar=2 20"),
            Err(super::Error::HardLimitReached {})
        ));
    }

    #[test]
    fn write_goes_to_write_buffer_if_configured() {
        let db = Arc::new(TestDb::builder().write_buffer(true).build().db);

        assert_eq!(db.write_buffer.as_ref().unwrap().lock().size(), 0);
        write_lp(db.as_ref(), "cpu bar=1 10");
        assert_ne!(db.write_buffer.as_ref().unwrap().lock().size(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lock_tracker_metrics() {
        // Test that data can be written into parquet files and then
        // remove it from read buffer and make sure we are still
        // be able to read data from object store

        // Create an object store with a specified location in a local disk
        let root = TempDir::new().unwrap();
        let object_store = Arc::new(ObjectStore::new_file(File::new(root.path())));

        // Create a DB given a server id, an object store and a db name
        let server_id = ServerId::try_from(10).unwrap();
        let db_name = "lock_tracker";
        let test_db = TestDb::builder()
            .server_id(server_id)
            .object_store(Arc::clone(&object_store))
            .db_name(db_name)
            .build();

        let db = Arc::new(test_db.db);

        // Expect partition lock registry to have been created
        test_db
            .metric_registry
            .has_metric_family("catalog_lock_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "partition"),
                ("svr_id", "10"),
                ("access", "exclusive"),
            ])
            .counter()
            .eq(0.)
            .unwrap();

        test_db
            .metric_registry
            .has_metric_family("catalog_lock_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "partition"),
                ("svr_id", "10"),
                ("access", "shared"),
            ])
            .counter()
            .eq(0.)
            .unwrap();

        write_lp(db.as_ref(), "cpu bar=1 10");

        test_db
            .metric_registry
            .has_metric_family("catalog_lock_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "partition"),
                ("svr_id", "10"),
                ("access", "exclusive"),
            ])
            .counter()
            .eq(1.)
            .unwrap();

        test_db
            .metric_registry
            .has_metric_family("catalog_lock_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "partition"),
                ("svr_id", "10"),
                ("access", "shared"),
            ])
            .counter()
            .eq(0.)
            .unwrap();

        let partition_key = db
            .catalog
            .partitions()
            .next()
            .unwrap()
            .read()
            .key()
            .to_string();

        let chunks = db.catalog.chunks();
        assert_eq!(chunks.len(), 1);

        let chunk_a = Arc::clone(&chunks[0]);
        let chunk_b = Arc::clone(&chunks[0]);

        let chunk_b = chunk_b.write();

        let task = tokio::spawn(async move {
            let _ = chunk_a.read();
        });

        // Hold lock for 100 seconds blocking background task
        std::thread::sleep(std::time::Duration::from_millis(100));

        std::mem::drop(chunk_b);
        task.await.unwrap();

        test_db
            .metric_registry
            .has_metric_family("catalog_lock_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "partition"),
                ("svr_id", "10"),
                ("access", "exclusive"),
            ])
            .counter()
            .eq(1.)
            .unwrap();

        test_db
            .metric_registry
            .has_metric_family("catalog_lock_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "partition"),
                ("svr_id", "10"),
                ("access", "shared"),
            ])
            .counter()
            .eq(2.)
            .unwrap();

        test_db
            .metric_registry
            .has_metric_family("catalog_lock_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "chunk"),
                ("partition", &partition_key),
                ("svr_id", "10"),
                ("access", "exclusive"),
            ])
            .counter()
            .eq(2.)
            .unwrap();

        test_db
            .metric_registry
            .has_metric_family("catalog_lock_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "chunk"),
                ("partition", &partition_key),
                ("svr_id", "10"),
                ("access", "shared"),
            ])
            .counter()
            .eq(2.)
            .unwrap();

        test_db
            .metric_registry
            .has_metric_family("catalog_lock_wait_seconds_total")
            .with_labels(&[
                ("db_name", "lock_tracker"),
                ("lock", "chunk"),
                ("svr_id", "10"),
                ("partition", &partition_key),
                ("access", "shared"),
            ])
            .counter()
            .gt(0.07)
            .unwrap();
    }

    #[test]
    fn process_clock_defaults_to_current_time_in_ns() {
        let before: u64 = Utc::now().timestamp_nanos().try_into().unwrap();

        let db = Arc::new(TestDb::builder().build().db);
        let db_process_clock = db.process_clock.lock();

        let after: u64 = Utc::now().timestamp_nanos().try_into().unwrap();

        assert!(
            before < db_process_clock.get(),
            "expected {} to be less than {}",
            before,
            db_process_clock
        );
        assert!(
            db_process_clock.get() < after,
            "expected {} to be less than {}",
            db_process_clock,
            after
        );
    }

    #[test]
    fn process_clock_incremented_and_set_on_sequenced_entry() {
        let before: u64 = Utc::now().timestamp_nanos().try_into().unwrap();
        let before = ClockValue::try_from(before).unwrap();

        let db = Arc::new(TestDb::builder().write_buffer(true).build().db);

        let entry = lp_to_entry("cpu bar=1 10");
        db.store_entry(entry).unwrap();

        let between: u64 = Utc::now().timestamp_nanos().try_into().unwrap();
        let between = ClockValue::try_from(between).unwrap();

        let entry = lp_to_entry("cpu foo=2 10");
        db.store_entry(entry).unwrap();

        let after: u64 = Utc::now().timestamp_nanos().try_into().unwrap();
        let after = ClockValue::try_from(after).unwrap();

        let sequenced_entries = db
            .write_buffer
            .as_ref()
            .unwrap()
            .lock()
            .writes_since(before);
        assert_eq!(sequenced_entries.len(), 2);

        assert!(
            sequenced_entries[0].clock_value() < between,
            "expected {:?} to be before {:?}",
            sequenced_entries[0].clock_value(),
            between
        );

        assert!(
            between < sequenced_entries[1].clock_value(),
            "expected {:?} to be before {:?}",
            between,
            sequenced_entries[1].clock_value(),
        );

        assert!(
            sequenced_entries[1].clock_value() < after,
            "expected {:?} to be before {:?}",
            sequenced_entries[1].clock_value(),
            after
        );
    }

    #[test]
    fn next_process_clock_always_increments() {
        // Process clock defaults to the current time
        let db = Arc::new(TestDb::builder().write_buffer(true).build().db);

        // Set the process clock value to a time in the future, so that when compared to the
        // current time, the process clock value will be greater
        let later: u64 = (Utc::now() + chrono::Duration::weeks(4))
            .timestamp_nanos()
            .try_into()
            .unwrap();
        {
            let mut db_process_clock = db.process_clock.lock();
            *db_process_clock = NonZeroU64::new(later).unwrap();
        }

        // Every call to next_process_clock should increment at least 1, even in this case
        // where the system time will be less than the process clock
        assert_eq!(
            db.next_process_clock(),
            ClockValue::try_from(later + 1).unwrap()
        );
        assert_eq!(
            db.next_process_clock(),
            ClockValue::try_from(later + 2).unwrap()
        );
    }
}
