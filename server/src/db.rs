//! This module contains the main IOx Database object which has the
//! instances of the mutable buffer, read buffer, and object store

use super::{
    buffer::{self, Buffer},
    JobRegistry,
};
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use async_trait::async_trait;
use catalog::{chunk::Chunk as CatalogChunk, Catalog};
pub(crate) use chunk::DbChunk;
use data_types::{
    chunk_metadata::ChunkSummary,
    database_rules::DatabaseRules,
    job::Job,
    partition_metadata::{PartitionSummary, TableSummary},
    server_id::ServerId,
};
use datafusion::{
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    physical_plan::SendableRecordBatchStream,
};
use entry::{Entry, OwnedSequencedEntry, SequencedEntry};
use internal_types::{arrow::sort::sort_record_batch, selection::Selection};
use lifecycle::LifecycleManager;
use metrics::{KeyValue, MetricRegistry};
use mutable_buffer::chunk::{
    Chunk as MutableBufferChunk, ChunkMetrics as MutableBufferChunkMetrics,
};
use object_store::{path::parsed::DirsAndFileName, ObjectStore};
use observability_deps::tracing::{debug, error, info};
use parking_lot::{Mutex, RwLock};
use parquet_file::{
    catalog::{CatalogParquetInfo, CatalogState, PreservedCatalog},
    chunk::{Chunk as ParquetChunk, ChunkMetrics as ParquetChunkMetrics},
    cleanup::cleanup_unreferenced_parquet_files,
    metadata::{
        read_schema_from_parquet_metadata, read_statistics_from_parquet_metadata, IoxMetadata,
    },
    storage::Storage,
};
use query::predicate::{Predicate, PredicateBuilder};
use query::{exec::Executor, Database, DEFAULT_SCHEMA};
use read_buffer::{Chunk as ReadBufferChunk, ChunkMetrics as ReadBufferChunkMetrics};
use snafu::{ResultExt, Snafu};
use std::{
    any::Any,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use system_tables::{SystemSchemaProvider, SYSTEM_SCHEMA};
use tracker::{TaskRegistration, TaskTracker, TrackedFutureExt};

pub mod catalog;
mod chunk;
mod lifecycle;
pub mod pred;
mod process_clock;
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
        "Can not drop chunk {}:{}:{} which has an in-progress lifecycle action {}. Wait for this to complete",
        partition_key,
        table_name,
        chunk_id,
        action
    ))]
    DropMovingChunk {
        partition_key: String,
        table_name: String,
        chunk_id: u32,
        action: String,
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

    #[snafu(display("Can not write entry to new MUB chunk {} : {}", partition_key, source))]
    WriteEntryInitial {
        partition_key: String,
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

    #[snafu(display("Error while handling transaction on preserved catalog: {}", source))]
    TransactionError {
        source: parquet_file::catalog::Error,
    },
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
    catalog: PreservedCatalog<Catalog>,

    /// The Write Buffer holds sequenced entries in an append in-memory
    /// buffer. This buffer is used for sending data to subscribers
    /// and to persist segments in object storage for recovery.
    pub write_buffer: Option<Mutex<Buffer>>,

    /// A handle to the global jobs registry for long running tasks
    jobs: Arc<JobRegistry>,

    // The metrics registry to inject into created components in the Db.
    metrics_registry: Arc<metrics::MetricRegistry>,

    /// The system schema provider
    system_tables: Arc<SystemSchemaProvider>,

    /// Process clock used in establishing a partial ordering of operations via a Lamport Clock.
    ///
    /// Value is nanoseconds since the Unix Epoch.
    process_clock: process_clock::ProcessClock,

    /// Number of iterations of the worker lifecycle loop for this Db
    worker_iterations_lifecycle: AtomicUsize,

    /// Number of iterations of the worker cleanup loop for this Db
    worker_iterations_cleanup: AtomicUsize,

    /// Metric labels
    metric_labels: Vec<KeyValue>,
}

/// Load preserved catalog state from store.
///
/// If no catalog exists yet, a new one will be created.
///
/// **For now, if the catalog is broken, it will be wiped! (https://github.com/influxdata/influxdb_iox/issues/1522)**
pub async fn load_or_create_preserved_catalog(
    db_name: &str,
    object_store: Arc<ObjectStore>,
    server_id: ServerId,
    metrics_registry: Arc<MetricRegistry>,
) -> std::result::Result<PreservedCatalog<Catalog>, parquet_file::catalog::Error> {
    let metric_labels = vec![
        KeyValue::new("db_name", db_name.to_string()),
        KeyValue::new("svr_id", format!("{}", server_id)),
    ];

    // first try to load existing catalogs
    let metrics_domain =
        metrics_registry.register_domain_with_labels("catalog", metric_labels.clone());

    match PreservedCatalog::load(
        Arc::clone(&object_store),
        server_id,
        db_name.to_string(),
        CatalogEmptyInput {
            domain: metrics_domain,
            metrics_registry: Arc::clone(&metrics_registry),
            metric_labels: metric_labels.clone(),
        },
    )
    .await
    {
        Ok(Some(catalog)) => {
            // successfull load
            info!("Found existing catalog for DB {}", db_name);
            Ok(catalog)
        }
        Ok(None) => {
            // no catalog yet => create one
            info!(
                "Found NO existing catalog for DB {}, creating new one",
                db_name
            );
            let metrics_domain =
                metrics_registry.register_domain_with_labels("catalog", metric_labels.clone());

            PreservedCatalog::new_empty(
                Arc::clone(&object_store),
                server_id,
                db_name.to_string(),
                CatalogEmptyInput {
                    domain: metrics_domain,
                    metrics_registry: Arc::clone(&metrics_registry),
                    metric_labels: metric_labels.clone(),
                },
            )
            .await
        }
        Err(e) => {
            // https://github.com/influxdata/influxdb_iox/issues/1522)
            // broken => wipe for now (at least during early iterations)
            error!("cannot load catalog, so wipe it: {}", e);
            PreservedCatalog::<Catalog>::wipe(&object_store, server_id, db_name).await?;

            let metrics_domain =
                metrics_registry.register_domain_with_labels("catalog", metric_labels.clone());

            PreservedCatalog::new_empty(
                Arc::clone(&object_store),
                server_id,
                db_name.to_string(),
                CatalogEmptyInput {
                    domain: metrics_domain,
                    metrics_registry: Arc::clone(&metrics_registry),
                    metric_labels: metric_labels.clone(),
                },
            )
            .await
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
        preserved_catalog: PreservedCatalog<Catalog>,
    ) -> Self {
        let db_name = rules.name.clone();

        let rules = RwLock::new(rules);
        let server_id = server_id;
        let store = Arc::clone(&object_store);
        let write_buffer = write_buffer.map(Mutex::new);
        let system_tables =
            SystemSchemaProvider::new(&db_name, preserved_catalog.state(), Arc::clone(&jobs));
        let system_tables = Arc::new(system_tables);
        let metrics_registry = Arc::clone(&preserved_catalog.state().metrics_registry);
        let metric_labels = preserved_catalog.state().metric_labels.clone();

        let process_clock = process_clock::ProcessClock::new();

        Self {
            rules,
            server_id,
            store,
            exec,
            catalog: preserved_catalog,
            write_buffer,
            jobs,
            metrics_registry,
            system_tables,
            process_clock,
            worker_iterations_lifecycle: AtomicUsize::new(0),
            worker_iterations_cleanup: AtomicUsize::new(0),
            metric_labels,
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
        let partition = self
            .catalog
            .state()
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
            chunk.set_closed().context(RollingOverPartition {
                partition_key,
                table_name,
            })?;

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
            .state()
            .valid_partition(partition_key)
            .context(DroppingChunk {
                partition_key,
                table_name,
                chunk_id,
            })?;

        // lock the partition so that no one else can be messing /
        // with it while we drop the chunk
        let mut partition = partition.write();

        {
            let chunk = partition
                .chunk(table_name, chunk_id)
                .context(DroppingChunk {
                    partition_key,
                    table_name,
                    chunk_id,
                })?;
            let chunk = chunk.read();
            let chunk_state = chunk.stage().name();

            // prevent chunks that are actively being moved. TODO it
            // would be nicer to allow this to happen have the chunk
            // migration logic cleanup afterwards so that users
            // weren't prevented from dropping chunks due to
            // background tasks
            if let Some(lifecycle_action) = chunk.lifecycle_action() {
                return DropMovingChunk {
                    partition_key,
                    table_name,
                    chunk_id,
                    action: lifecycle_action.name(),
                }
                .fail();
            }

            debug!(%partition_key, %table_name, %chunk_id, %chunk_state, "dropping chunk");
        }

        partition
            .drop_chunk(table_name, chunk_id)
            .context(DroppingChunk {
                partition_key,
                table_name,
                chunk_id,
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
        tracker: &TaskRegistration,
    ) -> Result<Arc<DbChunk>> {
        let chunk = {
            let partition = self
                .catalog
                .state()
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

        // update the catalog to say we are processing this chunk and
        // then drop the lock while we do the work
        let (mb_chunk, table_summary) = {
            let mut chunk = chunk.write();

            let mb_chunk = chunk.set_moving(tracker).context(LoadingChunk {
                partition_key,
                table_name,
                chunk_id,
            })?;
            (mb_chunk, chunk.table_summary())
        };

        info!(%partition_key, %table_name, %chunk_id, "chunk marked MOVING, loading tables into read buffer");

        // create a new read buffer chunk with memory tracking
        let metrics = self
            .metrics_registry
            .register_domain_with_labels("read_buffer", self.metric_labels.clone());
        let mut rb_chunk = ReadBufferChunk::new(ReadBufferChunkMetrics::new(
            &metrics,
            self.catalog.state().metrics().memory().read_buffer(),
        ));

        // load table into the new chunk one by one.
        debug!(%partition_key, %table_name, %chunk_id, table=%table_summary.name, "loading table to read buffer");
        let batch = mb_chunk
            .read_filter(table_name, Selection::All)
            // It is probably reasonable to recover from this error
            // (reset the chunk state to Open) but until that is
            // implemented (and tested) just panic
            .expect("Loading chunk to mutable buffer");

        let sorted = sort_record_batch(batch).expect("failed to sort");
        rb_chunk.upsert_table(&table_summary.name, sorted);

        // Relock the chunk again (nothing else should have been able
        // to modify the chunk state while we were moving it
        let mut chunk = chunk.write();

        // update the catalog to say we are done processing
        chunk.set_moved(Arc::new(rb_chunk)).context(LoadingChunk {
            partition_key,
            table_name,
            chunk_id,
        })?;

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
        tracker: &TaskRegistration,
    ) -> Result<Arc<DbChunk>> {
        // Get the chunk from the catalog
        let chunk = {
            let partition = self
                .catalog
                .state()
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
        let (rb_chunk, table_summary) = {
            let mut chunk = chunk.write();

            let rb_chunk =
                chunk
                    .set_writing_to_object_store(tracker)
                    .context(LoadingChunkToParquet {
                        partition_key,
                        table_name,
                        chunk_id,
                    })?;

            (rb_chunk, chunk.table_summary())
        };

        debug!(%partition_key, %table_name, %chunk_id, "chunk marked WRITING , loading tables into object store");

        // Create a storage to save data of this chunk
        let storage = Storage::new(
            Arc::clone(&self.store),
            self.server_id,
            self.rules.read().name.to_string(),
        );

        let table_name = table_summary.name.as_str();
        debug!(%partition_key, %table_name, %chunk_id, table=table_name, "loading table to object store");

        let predicate = read_buffer::Predicate::default();

        // Get RecordBatchStream of data from the read buffer chunk
        let read_results = rb_chunk
            .read_filter(table_name, predicate, Selection::All)
            .context(ReadBufferChunkError {
                table_name,
                chunk_id,
            })?;

        let arrow_schema: ArrowSchemaRef = rb_chunk
            .read_filter_table_schema(table_name, Selection::All)
            .context(ReadBufferChunkSchemaError {
                table_name,
                chunk_id,
            })?
            .into();
        let stream: SendableRecordBatchStream = Box::pin(streams::ReadFilterResultsStream::new(
            read_results,
            Arc::clone(&arrow_schema),
        ));

        // catalog-level transaction for preseveration layer
        {
            let mut transaction = self.catalog.open_transaction().await;

            // Write this table data into the object store
            let metadata = IoxMetadata {
                transaction_revision_counter: transaction.revision_counter(),
                transaction_uuid: transaction.uuid(),
            };
            let (path, parquet_metadata) = storage
                .write_to_object_store(
                    partition_key.to_string(),
                    chunk_id,
                    table_name.to_string(),
                    stream,
                    metadata,
                )
                .await
                .context(WritingToObjectStore)?;

            transaction
                .add_parquet(&path.into(), &parquet_metadata)
                .context(TransactionError)?;
            transaction.commit().await.context(TransactionError)?;
        }

        // We know this chunk is ParquetFile type
        let chunk = chunk.read();
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
            let partition = self
                .catalog
                .state()
                .valid_partition(partition_key)
                .context(UnloadingChunkFromReadBuffer {
                    partition_key,
                    table_name,
                    chunk_id,
                })?;
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

        chunk
            .set_unload_from_read_buffer()
            .context(UnloadingChunkFromReadBuffer {
                partition_key,
                table_name,
                chunk_id,
            })?;

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

        let captured_registration = registration.clone();
        let captured_db = Arc::clone(&self);
        let task = async move {
            debug!(%name, %partition_key, %table_name, %chunk_id, "background task loading chunk to read buffer");
            let result = captured_db
                .load_chunk_to_read_buffer(
                    &partition_key,
                    &table_name,
                    chunk_id,
                    &captured_registration,
                )
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

        let captured_registration = registration.clone();
        let captured_db = Arc::clone(&self);
        let task = async move {
            debug!(%name, %partition_key, %table_name, %chunk_id, "background task loading chunk to object store");
            let result = captured_db
                .write_chunk_to_object_store(
                    &partition_key,
                    &table_name,
                    chunk_id,
                    &captured_registration,
                )
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

    /// Return chunk summary information for all chunks in the specified
    /// partition across all storage systems
    pub fn partition_chunk_summaries(&self, partition_key: &str) -> Vec<ChunkSummary> {
        self.catalog.state().filtered_chunks(
            &PredicateBuilder::new().partition_key(partition_key).build(),
            CatalogChunk::summary,
        )
    }

    /// Return Summary information for all columns in all chunks in the
    /// partition across all storage systems
    pub fn partition_summary(&self, partition_key: &str) -> PartitionSummary {
        self.catalog
            .state()
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
    ) -> Option<Arc<TableSummary>> {
        if let Some(partition) = self.catalog.state().partition(partition_key) {
            let partition = partition.read();
            if let Ok(chunk) = partition.chunk(table_name, chunk_id) {
                return Some(chunk.read().table_summary());
            }
        }
        None
    }

    /// Returns the number of iterations of the background worker lifecycle loop
    pub fn worker_iterations_lifecycle(&self) -> usize {
        self.worker_iterations_lifecycle.load(Ordering::Relaxed)
    }

    /// Returns the number of iterations of the background worker lifecycle loop
    pub fn worker_iterations_cleanup(&self) -> usize {
        self.worker_iterations_cleanup.load(Ordering::Relaxed)
    }

    /// Background worker function
    pub async fn background_worker(
        self: &Arc<Self>,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        info!("started background worker");

        tokio::join!(
            // lifecycle manager loop
            async {
                let mut lifecycle_manager = LifecycleManager::new(Arc::clone(&self));

                while !shutdown.is_cancelled() {
                    self.worker_iterations_lifecycle
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::select! {
                        _ = lifecycle_manager.check_for_work() => {},
                        _ = shutdown.cancelled() => break,
                    }
                }
            },
            // object store cleanup loop
            async {
                while !shutdown.is_cancelled() {
                    self.worker_iterations_cleanup
                        .fetch_add(1, Ordering::Relaxed);
                    tokio::select! {
                        _ = async {
                            if let Err(e) = cleanup_unreferenced_parquet_files(&self.catalog).await {
                                error!("error in background cleanup task: {:?}", e);
                            }
                            tokio::time::sleep(Duration::from_secs(500)).await;
                        } => {},
                        _ = shutdown.cancelled() => break,
                    }
                }
            },
        );

        info!("finished background worker");
    }

    /// Stores an entry based on the configuration. The Entry will first be
    /// converted into a `SequencedEntry` with the logical clock assigned
    /// from the database, and then the `SequencedEntry` will be passed to
    /// `store_sequenced_entry`.
    pub fn store_entry(&self, entry: Entry) -> Result<()> {
        let sequenced_entry = Arc::new(
            OwnedSequencedEntry::new_from_entry_bytes(
                self.process_clock.next(),
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
            if self.catalog.state().metrics().memory().total() > hard_limit.get() {
                return HardLimitReached {}.fail();
            }
        }
        std::mem::drop(rules);

        // TODO: Direct writes to closing chunks

        if let Some(partitioned_writes) = sequenced_entry.partition_writes() {
            for write in partitioned_writes {
                let partition_key = write.key();
                let partition = self.catalog.state().get_or_create_partition(partition_key);
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

                            check_chunk_closed(&mut *chunk, mutable_size_threshold);
                        }
                        None => {
                            let metrics = self.metrics_registry.register_domain_with_labels(
                                "mutable_buffer",
                                self.metric_labels.clone(),
                            );
                            let mut mb_chunk = MutableBufferChunk::new(
                                table_batch.name(),
                                MutableBufferChunkMetrics::new(
                                    &metrics,
                                    self.catalog.state().metrics().memory().mutable_buffer(),
                                ),
                            );

                            mb_chunk
                                .write_table_batch(
                                    sequenced_entry.clock_value(),
                                    sequenced_entry.server_id(),
                                    table_batch,
                                )
                                .context(WriteEntryInitial { partition_key })?;

                            let new_chunk = partition
                                .create_open_chunk(mb_chunk)
                                .context(OpenEntry { partition_key })?;

                            check_chunk_closed(&mut *new_chunk.write(), mutable_size_threshold);
                        }
                    };
                }
            }
        }

        Ok(())
    }
}

/// Check if the given chunk should be closed based on the the MutableBuffer size threshold.
fn check_chunk_closed(chunk: &mut CatalogChunk, mutable_size_threshold: Option<NonZeroUsize>) {
    if let Some(threshold) = mutable_size_threshold {
        if let Ok(mb_chunk) = chunk.mutable_buffer() {
            let size = mb_chunk.size();

            if size > threshold.get() {
                chunk.set_closed().expect("cannot close open chunk");
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
        self.catalog
            .state()
            .filtered_chunks(predicate, DbChunk::snapshot)
    }

    fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        Ok(self.catalog.state().partition_keys())
    }

    fn chunk_summaries(&self) -> Result<Vec<ChunkSummary>> {
        Ok(self.catalog.state().chunk_summaries())
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
            DEFAULT_SCHEMA => Some(self.catalog.state()),
            SYSTEM_SCHEMA => Some(Arc::<SystemSchemaProvider>::clone(&self.system_tables)),
            _ => None,
        }
    }
}

/// All input required to create an empty [`Catalog`](crate::db::catalog::Catalog).
#[derive(Debug)]
pub struct CatalogEmptyInput {
    domain: ::metrics::Domain,
    metrics_registry: Arc<::metrics::MetricRegistry>,
    metric_labels: Vec<KeyValue>,
}

impl CatalogState for Catalog {
    type EmptyInput = CatalogEmptyInput;

    fn new_empty(data: Self::EmptyInput) -> Self {
        Self::new(data.domain, data.metrics_registry, data.metric_labels)
    }

    fn clone_or_keep(origin: &Arc<Self>) -> Arc<Self> {
        // no copy semantics
        Arc::clone(origin)
    }

    fn add(
        &self,
        object_store: Arc<ObjectStore>,
        server_id: ServerId,
        db_name: &str,
        info: CatalogParquetInfo,
    ) -> parquet_file::catalog::Result<()> {
        use parquet_file::catalog::{
            CatalogStateFailure, PathParseFailed, SchemaReadFailed, StatisticsReadFailed,
        };

        // extract all relevant bits for the in-memory catalog
        let storage = Storage::new(Arc::clone(&object_store), server_id, db_name.to_string());
        // this is temporary until https://github.com/influxdata/influxdb_iox/issues/1506 is fixed.
        let (partition_key, chunk_id, table_name) = storage
            .parse_location(info.path.clone())
            .context(PathParseFailed {
                path: info.path.clone(),
            })?;
        let schema =
            read_schema_from_parquet_metadata(&info.metadata).context(SchemaReadFailed {
                path: info.path.clone(),
            })?;
        let table_summary =
            read_statistics_from_parquet_metadata(&info.metadata, &schema, &table_name).context(
                StatisticsReadFailed {
                    path: info.path.clone(),
                },
            )?;

        // Create a parquet chunk for this chunk
        let metrics = self
            .metrics_registry
            .register_domain_with_labels("parquet", self.metric_labels.clone());

        let metrics = ParquetChunkMetrics::new(&metrics, self.metrics().memory().parquet());
        let parquet_chunk = ParquetChunk::new(
            &partition_key,
            table_summary,
            object_store.path_from_dirs_and_filename(info.path.clone()),
            object_store,
            schema,
            metrics,
        );
        let parquet_chunk = Arc::new(parquet_chunk);

        // Get partition from the catalog
        // Note that the partition might not exist yet if the chunk is loaded from an existing preserved catalog.
        let partition = self.get_or_create_partition(&partition_key);
        let partition_guard = partition.read();

        // Get the chunk from the catalog
        match partition_guard.chunk(table_name.clone(), chunk_id) {
            Ok(chunk) => {
                // Chunk exists => should be in frozen stage and will transition from there

                // Relock the chunk again (nothing else should have been able
                // to modify the chunk state while we were moving it
                let mut chunk = chunk.write();

                // update the catalog to say we are done processing
                chunk
                    .set_written_to_object_store(parquet_chunk)
                    .map_err(|e| Box::new(e) as _)
                    .context(CatalogStateFailure { path: info.path })?;
                debug!(%partition_key, %table_name, %chunk_id, "chunk marked WRITTEN. Persisting to object store complete");
            }
            Err(catalog::Error::UnknownTable { .. }) | Err(catalog::Error::UnknownChunk { .. }) => {
                // table unknown => that's ok, create chunk in "object store only" stage which will also create the table
                // table chunk, but table already known => that's ok, create chunk in "object store only" stage
                drop(partition_guard);
                let mut partition_guard = partition.write();
                partition_guard
                    .create_object_store_only_chunk(chunk_id, parquet_chunk)
                    .map_err(|e| Box::new(e) as _)
                    .context(CatalogStateFailure { path: info.path })?;
                debug!(%partition_key, %table_name, %chunk_id, "recovered chunk from persisted catalog");
            }
            Err(e) => {
                // Other unknown error => bail
                return Err(parquet_file::catalog::Error::CatalogStateFailure {
                    source: Box::new(e),
                    path: info.path,
                });
            }
        }

        Ok(())
    }

    fn remove(&self, _path: DirsAndFileName) -> parquet_file::catalog::Result<()> {
        unimplemented!("parquet files cannot be removed from the catalog for now")
    }
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
    use crate::db::catalog::chunk::{
        ChunkStage, ChunkStageFrozen, ChunkStageFrozenRepr, ChunkStagePersisted,
    };
    use crate::query_tests::utils::{make_db, TestDb};
    use ::test_helpers::assert_contains;
    use arrow::record_batch::RecordBatch;
    use arrow_util::{assert_batches_eq, assert_batches_sorted_eq};
    use bytes::Bytes;
    use chrono::Utc;
    use data_types::{
        chunk_metadata::ChunkStorage,
        database_rules::{Order, Sort, SortOrder},
        partition_metadata::{ColumnSummary, InfluxDbType, StatValues, Statistics, TableSummary},
    };
    use entry::test_helpers::lp_to_entry;
    use futures::{stream, StreamExt, TryStreamExt};
    use object_store::{
        memory::InMemory,
        path::{parts::PathPart, ObjectStorePath, Path},
        ObjectStore, ObjectStoreApi,
    };
    use parquet_file::{
        metadata::{read_parquet_metadata_from_file, read_schema_from_parquet_metadata},
        test_utils::{load_parquet_from_store_for_path, read_data_from_parquet_data},
    };
    use query::{frontend::sql::SqlQueryPlanner, PartitionChunk};
    use std::{
        collections::HashSet,
        convert::TryFrom,
        iter::Iterator,
        num::NonZeroUsize,
        str,
        time::{Duration, Instant},
    };
    use tokio_util::sync::CancellationToken;

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    #[tokio::test]
    async fn write_no_mutable_buffer() {
        // Validate that writes are rejected if there is no mutable buffer
        let db = make_db().await.db;
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
        let db = Arc::new(make_db().await.db);
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
        source: &'static str,
        v: u64,
    ) -> Result<(), metrics::Error> {
        reg.has_metric_family("catalog_chunks_mem_usage_bytes")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("source", source),
                ("svr_id", "1"),
            ])
            .gauge()
            .eq(v as f64)
    }

    #[tokio::test]
    async fn metrics_during_rollover() {
        let test_db = make_db().await;
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
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "mutable_buffer", 44).unwrap();

        // write into same chunk again.
        write_lp(db.as_ref(), "cpu bar=2 10");

        // verify chunk size updated
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "mutable_buffer", 60).unwrap();

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

        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "mutable_buffer", 1143)
            .unwrap();

        db.load_chunk_to_read_buffer("1970-01-01T00", "cpu", 0, &Default::default())
            .await
            .unwrap();

        // A chunk is now in the read buffer
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

        // verify chunk size updated (chunk moved from closing to moving to moved)
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "mutable_buffer", 0).unwrap();
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "read_buffer", 1630).unwrap();

        db.write_chunk_to_object_store("1970-01-01T00", "cpu", 0, &Default::default())
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

        let expected_parquet_size = 759;
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "read_buffer", 1630).unwrap();
        // now also in OS
        catalog_chunk_size_bytes_metric_eq(
            &test_db.metric_registry,
            "parquet",
            expected_parquet_size,
        )
        .unwrap(); // TODO: #1311

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
        catalog_chunk_size_bytes_metric_eq(
            &test_db.metric_registry,
            "parquet",
            expected_parquet_size,
        )
        .unwrap();
        // verify chunk size for RB has decreased
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "read_buffer", 0).unwrap();
    }

    #[tokio::test]
    async fn write_with_rollover() {
        let db = Arc::new(make_db().await.db);
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
        let db = Arc::new(make_db().await.db);
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
        let test_db = make_db().await;
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
            .load_chunk_to_read_buffer(partition_key, "cpu", mb_chunk.id(), &Default::default())
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
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "read_buffer", 1630).unwrap();

        // drop, the chunk from the read buffer
        db.drop_chunk(partition_key, "cpu", mb_chunk.id()).unwrap();
        assert_eq!(
            read_buffer_chunk_ids(db.as_ref(), partition_key),
            vec![] as Vec<u32>
        );

        // verify size is reported until chunk dropped
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "read_buffer", 1630).unwrap();
        std::mem::drop(rb_chunk);

        // verify chunk size updated (chunk dropped from moved state)
        catalog_chunk_size_bytes_metric_eq(&test_db.metric_registry, "read_buffer", 0).unwrap();

        // Currently this doesn't work (as we need to teach the stores how to
        // purge tables after data bas been dropped println!("running
        // query after all data dropped!"); let expected = vec![] as
        // Vec<&str>; let batches = run_query(&db, "select * from
        // cpu").await; assert_batches_eq!(expected, &batches);
    }

    async fn collect_read_filter(chunk: &DbChunk) -> Vec<RecordBatch> {
        chunk
            .read_filter(&Default::default(), Selection::All)
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect()
    }

    #[tokio::test]
    async fn load_to_read_buffer_sorted() {
        let test_db = make_db().await;
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

        let mb = collect_read_filter(&mb_chunk).await;

        let rb_chunk = db
            .load_chunk_to_read_buffer(partition_key, "cpu", mb_chunk.id(), &Default::default())
            .await
            .unwrap();

        // MUB chunk size
        test_db
            .metric_registry
            .has_metric_family("catalog_chunk_creation_size_bytes")
            .with_labels(&[
                ("db_name", "placeholder"),
                ("state", "closed"),
                ("svr_id", "1"),
            ])
            .histogram()
            .sample_sum_eq(280.0)
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
            .sample_sum_eq(3231.0)
            .unwrap();

        let rb = collect_read_filter(&rb_chunk).await;

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
        let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));

        // Create a DB given a server id, an object store and a db name
        let server_id = ServerId::try_from(10).unwrap();
        let db_name = "parquet_test_db";
        let test_db = TestDb::builder()
            .server_id(server_id)
            .object_store(Arc::clone(&object_store))
            .db_name(db_name)
            .build()
            .await;

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
            .load_chunk_to_read_buffer(partition_key, "cpu", mb_chunk.id(), &Default::default())
            .await
            .unwrap();
        // Write the RB chunk to Object Store but keep it in RB
        let pq_chunk = db
            .write_chunk_to_object_store(partition_key, "cpu", mb_chunk.id(), &Default::default())
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
            .sample_sum_eq(2389.0)
            .unwrap();

        // it should be the same chunk!
        assert_eq!(mb_chunk.id(), rb_chunk.id());
        assert_eq!(mb_chunk.id(), pq_chunk.id());

        // we should have chunks in both the read buffer only
        assert!(mutable_chunk_ids(&db, partition_key).is_empty());
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![0]);
        assert_eq!(read_parquet_file_chunk_ids(&db, partition_key), vec![0]);

        // Verify data written to the parquet file in object store
        //
        // First, there must be one path of object store in the catalog
        let path = pq_chunk.object_store_path().unwrap();

        // Check that the path must exist in the object store
        let path_list = flatten_list_stream(Arc::clone(&object_store), Some(&path))
            .await
            .unwrap();
        assert_eq!(path_list.len(), 1);
        assert_eq!(path_list[0], path);

        // Now read data from that path
        let parquet_data = load_parquet_from_store_for_path(&path_list[0], object_store)
            .await
            .unwrap();
        let parquet_metadata = read_parquet_metadata_from_file(parquet_data.clone()).unwrap();
        // Read metadata at file level
        let schema = read_schema_from_parquet_metadata(&parquet_metadata).unwrap();
        // Read data
        let record_batches =
            read_data_from_parquet_data(Arc::clone(&schema.as_arrow()), parquet_data);

        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "| 2   | 1970-01-01 00:00:00.000000020 |",
            "+-----+-------------------------------+",
        ];
        assert_batches_eq!(expected, &record_batches);
    }

    #[tokio::test]
    async fn unload_chunk_from_read_buffer() {
        // Test that data can be written into parquet files and then
        // remove it from read buffer and make sure we are still
        // be able to read data from object store

        // Create an object store in memory
        let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));

        // Create a DB given a server id, an object store and a db name
        let server_id = ServerId::try_from(10).unwrap();
        let db_name = "unload_read_buffer_test_db";
        let test_db = TestDb::builder()
            .server_id(server_id)
            .object_store(Arc::clone(&object_store))
            .db_name(db_name)
            .build()
            .await;

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
            .load_chunk_to_read_buffer(partition_key, "cpu", mb_chunk.id(), &Default::default())
            .await
            .unwrap();
        // Write the RB chunk to Object Store but keep it in RB
        let pq_chunk = db
            .write_chunk_to_object_store(partition_key, "cpu", mb_chunk.id(), &Default::default())
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
            .sample_sum_eq(2389.0)
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
            .sample_sum_eq(759.0)
            .unwrap();

        // Verify data written to the parquet file in object store
        //
        // First, there must be one path of object store in the catalog
        let path = pq_chunk.object_store_path().unwrap();

        // Check that the path must exist in the object store
        let path_list = flatten_list_stream(Arc::clone(&object_store), Some(&path))
            .await
            .unwrap();
        println!("path_list: {:#?}", path_list);
        assert_eq!(path_list.len(), 1);
        assert_eq!(path_list[0], path);

        // Now read data from that path
        let parquet_data = load_parquet_from_store_for_path(&path_list[0], object_store)
            .await
            .unwrap();
        let parquet_metadata = read_parquet_metadata_from_file(parquet_data.clone()).unwrap();
        // Read metadata at file level
        let schema = read_schema_from_parquet_metadata(&parquet_metadata).unwrap();
        // Read data
        let record_batches =
            read_data_from_parquet_data(Arc::clone(&schema.as_arrow()), parquet_data);

        let expected = vec![
            "+-----+-------------------------------+",
            "| bar | time                          |",
            "+-----+-------------------------------+",
            "| 1   | 1970-01-01 00:00:00.000000010 |",
            "| 2   | 1970-01-01 00:00:00.000000020 |",
            "+-----+-------------------------------+",
        ];
        assert_batches_eq!(expected, &record_batches);
    }

    #[tokio::test]
    async fn write_updates_last_write_at() {
        let db = Arc::new(make_db().await.db);
        let before_create = Utc::now();

        let partition_key = "1970-01-01T00";
        write_lp(&db, "cpu bar=1 10");
        let after_write = Utc::now();

        let last_write_prev = {
            let partition = db.catalog.state().valid_partition(partition_key).unwrap();
            let partition = partition.read();

            assert_ne!(partition.created_at(), partition.last_write_at());
            assert!(before_create < partition.last_write_at());
            assert!(after_write > partition.last_write_at());
            partition.last_write_at()
        };

        write_lp(&db, "cpu bar=1 20");
        {
            let partition = db.catalog.state().valid_partition(partition_key).unwrap();
            let partition = partition.read();
            assert!(last_write_prev < partition.last_write_at());
        }
    }

    #[tokio::test]
    async fn test_chunk_timestamps() {
        let start = Utc::now();
        let db = Arc::new(make_db().await.db);

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

        let partition = db.catalog.state().valid_partition(partition_key).unwrap();
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

    #[tokio::test]
    async fn test_chunk_closing() {
        let db = Arc::new(make_db().await.db);
        db.rules.write().lifecycle_rules.mutable_size_threshold =
            Some(NonZeroUsize::new(2).unwrap());

        write_lp(&db, "cpu bar=1 10");
        write_lp(&db, "cpu bar=1 20");

        let partitions = db.catalog.state().partition_keys();
        assert_eq!(partitions.len(), 1);

        let partition = db.catalog.state().partition(&partitions[0]).unwrap();
        let partition = partition.read();

        let chunks: Vec<_> = partition.chunks().collect();
        assert_eq!(chunks.len(), 2);
        assert!(matches!(
            chunks[0].read().stage(),
            ChunkStage::Frozen(ChunkStageFrozen {
                meta: _,
                representation: ChunkStageFrozenRepr::MutableBufferSnapshot(_)
            })
        ));
        assert!(matches!(
            chunks[1].read().stage(),
            ChunkStage::Frozen(ChunkStageFrozen {
                meta: _,
                representation: ChunkStageFrozenRepr::MutableBufferSnapshot(_)
            })
        ));
    }

    #[tokio::test]
    async fn chunks_sorted_by_times() {
        let db = Arc::new(make_db().await.db);
        write_lp(&db, "cpu val=1 1");
        write_lp(&db, "mem val=2 400000000000001");
        write_lp(&db, "cpu val=1 2");
        write_lp(&db, "mem val=2 400000000000002");

        let sort_rules = SortOrder {
            order: Order::Desc,
            sort: Sort::LastWriteTime,
        };
        let chunks = db.catalog.state().chunks_sorted_by(&sort_rules);
        let partitions: Vec<_> = chunks
            .into_iter()
            .map(|x| x.read().key().to_string())
            .collect();

        assert_eq!(partitions, vec!["1970-01-05T15", "1970-01-01T00"]);

        let sort_rules = SortOrder {
            order: Order::Asc,
            sort: Sort::CreatedAtTime,
        };
        let chunks = db.catalog.state().chunks_sorted_by(&sort_rules);
        let partitions: Vec<_> = chunks
            .into_iter()
            .map(|x| x.read().key().to_string())
            .collect();
        assert_eq!(partitions, vec!["1970-01-01T00", "1970-01-05T15"]);
    }

    #[tokio::test]
    async fn chunk_id_listing() {
        // Test that chunk id listing is hooked up
        let db = Arc::new(make_db().await.db);
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
        db.load_chunk_to_read_buffer(partition_key, "cpu", mb_chunk.id(), &Default::default())
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
        let db = Arc::new(make_db().await.db);

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
            70,
            1,
        )];

        let size: usize = db
            .chunk_summaries()
            .unwrap()
            .into_iter()
            .map(|x| x.estimated_bytes)
            .sum();

        assert_eq!(
            db.catalog
                .state()
                .metrics()
                .memory()
                .mutable_buffer()
                .get_total(),
            size
        );

        assert_eq!(
            expected, chunk_summaries,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, chunk_summaries
        );
    }

    #[tokio::test]
    async fn partition_chunk_summaries_timestamp() {
        let db = Arc::new(make_db().await.db);
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
        let db = Arc::new(make_db().await.db);

        // get three chunks: one open, one closed in mb and one close in rb
        write_lp(&db, "cpu bar=1 1");
        db.rollover_partition("1970-01-01T00", "cpu").await.unwrap();

        write_lp(&db, "cpu bar=1,baz=2 2");
        write_lp(&db, "cpu bar=1,baz=2,frob=3 400000000000000");

        print!("Partitions: {:?}", db.partition_keys().unwrap());

        db.load_chunk_to_read_buffer("1970-01-01T00", "cpu", 0, &Default::default())
            .await
            .unwrap();

        db.write_chunk_to_object_store("1970-01-01T00", "cpu", 0, &Default::default())
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
                2380, // size of RB and OS chunks
                1,
            ),
            ChunkSummary::new_without_timestamps(
                Arc::from("1970-01-01T00"),
                Arc::from("cpu"),
                1,
                ChunkStorage::OpenMutableBuffer,
                64,
                1,
            ),
            ChunkSummary::new_without_timestamps(
                Arc::from("1970-01-05T15"),
                Arc::from("cpu"),
                0,
                ChunkStorage::ClosedMutableBuffer,
                2190,
                1,
            ),
            ChunkSummary::new_without_timestamps(
                Arc::from("1970-01-05T15"),
                Arc::from("cpu"),
                1,
                ChunkStorage::OpenMutableBuffer,
                87,
                1,
            ),
        ];

        assert_eq!(
            expected, chunk_summaries,
            "expected:\n{:#?}\n\nactual:{:#?}\n\n",
            expected, chunk_summaries
        );

        assert_eq!(
            db.catalog
                .state()
                .metrics()
                .memory()
                .mutable_buffer()
                .get_total(),
            64 + 2190 + 87
        );
        assert_eq!(
            db.catalog
                .state()
                .metrics()
                .memory()
                .read_buffer()
                .get_total(),
            1621
        );
        assert_eq!(
            db.catalog.state().metrics().memory().parquet().get_total(),
            759
        );
    }

    #[tokio::test]
    async fn partition_summaries() {
        // Test that chunk id listing is hooked up
        let db = Arc::new(make_db().await.db);

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
        db.load_chunk_to_read_buffer("1970-01-01T00", "cpu", chunk_id, &Default::default())
            .await
            .unwrap();

        // write the read buffer chunk to object store
        db.write_chunk_to_object_store("1970-01-01T00", "cpu", chunk_id, &Default::default())
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
                                stats: Statistics::F64(StatValues::new(Some(1.0), Some(2.0), 2)),
                            },
                            ColumnSummary {
                                name: "time".into(),
                                influxdb_type: Some(InfluxDbType::Timestamp),
                                stats: Statistics::I64(StatValues::new(Some(1), Some(2), 2)),
                            },
                            ColumnSummary {
                                name: "baz".into(),
                                influxdb_type: Some(InfluxDbType::Field),
                                stats: Statistics::F64(StatValues::new(Some(3.0), Some(3.0), 1)),
                            },
                        ],
                    },
                    TableSummary {
                        name: "mem".into(),
                        columns: vec![
                            ColumnSummary {
                                name: "foo".into(),
                                influxdb_type: Some(InfluxDbType::Field),
                                stats: Statistics::F64(StatValues::new(Some(1.0), Some(1.0), 1)),
                            },
                            ColumnSummary {
                                name: "time".into(),
                                influxdb_type: Some(InfluxDbType::Timestamp),
                                stats: Statistics::I64(StatValues::new(Some(1), Some(1), 1)),
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
                                stats: Statistics::F64(StatValues::new(Some(1.0), Some(1.0), 1)),
                            },
                            ColumnSummary {
                                name: "time".into(),
                                influxdb_type: Some(InfluxDbType::Timestamp),
                                stats: Statistics::I64(StatValues::new(
                                    Some(400000000000000),
                                    Some(400000000000000),
                                    1,
                                )),
                            },
                        ],
                    },
                    TableSummary {
                        name: "mem".into(),
                        columns: vec![
                            ColumnSummary {
                                name: "frob".into(),
                                influxdb_type: Some(InfluxDbType::Field),
                                stats: Statistics::F64(StatValues::new(Some(3.0), Some(3.0), 1)),
                            },
                            ColumnSummary {
                                name: "time".into(),
                                influxdb_type: Some(InfluxDbType::Timestamp),
                                stats: Statistics::I64(StatValues::new(
                                    Some(400000000000001),
                                    Some(400000000000001),
                                    1,
                                )),
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
        let db = Arc::new(make_db().await.db);

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
            .load_chunk_to_read_buffer(
                partition_key,
                table_name,
                mb_chunk.id(),
                &Default::default(),
            )
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

    #[tokio::test]
    async fn write_hard_limit() {
        let db = Arc::new(make_db().await.db);
        db.rules.write().lifecycle_rules.buffer_size_hard = Some(NonZeroUsize::new(10).unwrap());

        // inserting first line does not trigger hard buffer limit
        write_lp(db.as_ref(), "cpu bar=1 10");

        // but second line will
        assert!(matches!(
            try_write_lp(db.as_ref(), "cpu bar=2 20"),
            Err(super::Error::HardLimitReached {})
        ));
    }

    #[tokio::test]
    async fn write_goes_to_write_buffer_if_configured() {
        let db = Arc::new(TestDb::builder().write_buffer(true).build().await.db);

        assert_eq!(db.write_buffer.as_ref().unwrap().lock().size(), 0);
        write_lp(db.as_ref(), "cpu bar=1 10");
        assert_ne!(db.write_buffer.as_ref().unwrap().lock().size(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lock_tracker_metrics() {
        let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));

        // Create a DB given a server id, an object store and a db name
        let server_id = ServerId::try_from(10).unwrap();
        let db_name = "lock_tracker";
        let test_db = TestDb::builder()
            .server_id(server_id)
            .object_store(Arc::clone(&object_store))
            .db_name(db_name)
            .build()
            .await;

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
            .state()
            .partitions()
            .next()
            .unwrap()
            .read()
            .key()
            .to_string();

        let chunks = db.catalog.state().chunks();
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
            .eq(1.)
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

    #[tokio::test]
    async fn load_or_create_preserved_catalog_recovers_from_error() {
        let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let server_id = ServerId::try_from(1).unwrap();
        let db_name = "preserved_catalog_test";

        let preserved_catalog =
            PreservedCatalog::<parquet_file::catalog::test_helpers::TestCatalogState>::new_empty(
                Arc::clone(&object_store),
                server_id,
                db_name.to_string(),
                (),
            )
            .await
            .unwrap();
        parquet_file::catalog::test_helpers::break_catalog_with_weird_version(&preserved_catalog)
            .await;

        let metrics_registry = Arc::new(metrics::MetricRegistry::new());
        load_or_create_preserved_catalog(db_name, object_store, server_id, metrics_registry)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn write_one_chunk_to_preserved_catalog() {
        // Test that parquet data is commited to preserved catalog

        // ==================== setup ====================
        let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let server_id = ServerId::try_from(1).unwrap();
        let db_name = "preserved_catalog_test";

        // ==================== do: create DB ====================
        // Create a DB given a server id, an object store and a db name
        let test_db = TestDb::builder()
            .object_store(Arc::clone(&object_store))
            .server_id(server_id)
            .db_name(db_name)
            .build()
            .await;
        let db = Arc::new(test_db.db);

        // ==================== check: empty catalog created ====================
        // at this point, an empty preserved catalog exists
        let maybe_preserved_catalog =
            PreservedCatalog::<parquet_file::catalog::test_helpers::TestCatalogState>::load(
                Arc::clone(&object_store),
                server_id,
                db_name.to_string(),
                (),
            )
            .await
            .unwrap();
        assert!(maybe_preserved_catalog.is_some());

        // ==================== do: write data to parquet ====================
        // create two chunks within the same table (to better test "new chunk ID" and "new table" during transaction
        // replay)
        let mut chunks = vec![];
        for _ in 0..2 {
            chunks.push(create_parquet_chunk(db.as_ref()).await);
        }

        // ==================== check: catalog state ====================
        // the preserved catalog should now register a single file
        let mut paths_expected = vec![];
        for (partition_key, table_name, chunk_id) in &chunks {
            let chunk = {
                let partition = db.catalog.state().valid_partition(&partition_key).unwrap();
                let partition = partition.read();

                partition.chunk(table_name, *chunk_id).unwrap()
            };
            let chunk = chunk.read();
            if let ChunkStage::Persisted(stage) = chunk.stage() {
                paths_expected.push(stage.parquet.table_path().display());
            } else {
                panic!("Wrong chunk state.");
            }
        }
        paths_expected.sort();
        let preserved_catalog =
            PreservedCatalog::<parquet_file::catalog::test_helpers::TestCatalogState>::load(
                Arc::clone(&object_store),
                server_id,
                db_name.to_string(),
                (),
            )
            .await
            .unwrap()
            .unwrap();
        let mut paths_actual: Vec<String> = preserved_catalog
            .state()
            .inner
            .borrow()
            .parquet_files
            .keys()
            .map(|p| p.display())
            .collect();
        paths_actual.sort();
        assert_eq!(paths_actual, paths_expected);

        // ==================== do: re-load DB ====================
        // Re-create database with same store, serverID, and DB name
        drop(db);
        let test_db = TestDb::builder()
            .object_store(Arc::clone(&object_store))
            .server_id(server_id)
            .db_name(db_name)
            .build()
            .await;
        let db = Arc::new(test_db.db);

        // ==================== check: DB state ====================
        // Re-created DB should have an "object store only"-chunk
        for (partition_key, table_name, chunk_id) in &chunks {
            let chunk = {
                let partition = db.catalog.state().valid_partition(&partition_key).unwrap();
                let partition = partition.read();

                partition.chunk(table_name, *chunk_id).unwrap()
            };
            let chunk = chunk.read();
            assert!(matches!(
                chunk.stage(),
                ChunkStage::Persisted(ChunkStagePersisted {
                    parquet: _,
                    read_buffer: None,
                    meta: _,
                })
            ));
        }

        // ==================== check: DB still writable ====================
        write_lp(db.as_ref(), "cpu bar=1 10");
    }

    #[tokio::test]
    async fn object_store_cleanup() {
        // Test that stale parquet files are removed from object store

        // ==================== setup ====================
        let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));

        // ==================== do: create DB ====================
        // Create a DB given a server id, an object store and a db name
        let test_db = TestDb::builder()
            .object_store(Arc::clone(&object_store))
            .build()
            .await;
        let db = Arc::new(test_db.db);

        // ==================== do: write data to parquet ====================
        // create the following chunks:
        //   0: ReadBuffer + Parquet
        //   1: only Parquet
        //   2: dropped (not in current catalog but parquet file still present for time travel)
        let mut paths_keep = vec![];
        for i in 0..3i8 {
            let (partition_key, table_name, chunk_id) = create_parquet_chunk(db.as_ref()).await;
            let chunk = {
                let partition = db.catalog.state().valid_partition(&partition_key).unwrap();
                let partition = partition.read();

                partition.chunk(table_name.clone(), chunk_id).unwrap()
            };
            let chunk = chunk.read();
            if let ChunkStage::Persisted(stage) = chunk.stage() {
                paths_keep.push(stage.parquet.table_path());
            } else {
                panic!("Wrong chunk state.");
            }

            // drop lock
            drop(chunk);

            if i == 1 {
                db.unload_read_buffer(&partition_key, &table_name, chunk_id)
                    .await
                    .unwrap();
            }
            if i == 2 {
                db.drop_chunk(&partition_key, &table_name, chunk_id)
                    .unwrap();
            }
        }

        // ==================== do: create garbage ====================
        let mut path: DirsAndFileName = paths_keep[0].clone().into();
        path.file_name = Some(PathPart::from(
            format!("prefix_{}", path.file_name.unwrap().encoded()).as_ref(),
        ));
        let path_delete = object_store.path_from_dirs_and_filename(path);
        create_empty_file(&object_store, &path_delete).await;
        let path_delete = path_delete.display();

        // ==================== check: all files are there ====================
        let all_files = get_object_store_files(&object_store).await;
        for path in &paths_keep {
            assert!(all_files.contains(&path.display()));
        }

        // ==================== do: start background task loop ====================
        let shutdown: CancellationToken = Default::default();
        let shutdown_captured = shutdown.clone();
        let db_captured = Arc::clone(&db);
        let join_handle =
            tokio::spawn(async move { db_captured.background_worker(shutdown_captured).await });

        // ==================== check: after a while the dropped file should be gone ====================
        let t_0 = Instant::now();
        loop {
            let all_files = get_object_store_files(&object_store).await;
            if !all_files.contains(&path_delete) {
                break;
            }
            assert!(t_0.elapsed() < Duration::from_secs(10));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // ==================== do: stop background task loop ====================
        shutdown.cancel();
        join_handle.await.unwrap();

        // ==================== check: some files are there ====================
        let all_files = get_object_store_files(&object_store).await;
        assert!(!all_files.contains(&path_delete));
        for path in &paths_keep {
            assert!(all_files.contains(&path.display()));
        }
    }

    async fn create_parquet_chunk(db: &Db) -> (String, String, u32) {
        write_lp(db, "cpu bar=1 10");
        let partition_key = "1970-01-01T00";
        let table_name = "cpu";

        //Now mark the MB chunk close
        let chunk_id = {
            let mb_chunk = db
                .rollover_partition(partition_key, table_name)
                .await
                .unwrap()
                .unwrap();
            mb_chunk.id()
        };
        // Move that MB chunk to RB chunk and drop it from MB
        db.load_chunk_to_read_buffer(partition_key, table_name, chunk_id, &Default::default())
            .await
            .unwrap();

        // Write the RB chunk to Object Store but keep it in RB
        db.write_chunk_to_object_store(partition_key, table_name, chunk_id, &Default::default())
            .await
            .unwrap();

        (partition_key.to_string(), table_name.to_string(), chunk_id)
    }

    async fn get_object_store_files(object_store: &ObjectStore) -> HashSet<String> {
        object_store
            .list(None)
            .await
            .unwrap()
            .try_concat()
            .await
            .unwrap()
            .iter()
            .map(|p| p.display())
            .collect()
    }

    async fn create_empty_file(object_store: &ObjectStore, path: &Path) {
        let data = Bytes::default();
        let len = data.len();

        object_store
            .put(
                &path,
                futures::stream::once(async move { Ok(data) }),
                Some(len),
            )
            .await
            .unwrap();
    }
}
