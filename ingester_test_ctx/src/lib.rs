#![doc = include_str!("../README.md")]
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    clippy::clone_on_ref_ptr,
    clippy::dbg_macro,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::use_self,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::{collections::HashMap, sync::Arc, time::Duration};

use arrow::record_batch::RecordBatch;
use arrow_flight::{decode::FlightRecordBatchStream, flight_service_server::FlightService, Ticket};
use data_types::{
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    Namespace, NamespaceId, NamespaceSchema, ParquetFile, PartitionKey, SequenceNumber, TableId,
};
use dml::{DmlMeta, DmlWrite};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt, TryStreamExt};
use generated_types::influxdata::iox::ingester::v1::{
    write_service_server::WriteService, WriteRequest,
};
use ingester::{GossipConfig, IngesterGuard, IngesterRpcInterface};
use ingester_query_grpc::influxdata::iox::ingester::v1::IngesterQueryRequest;
use iox_catalog::{
    interface::{Catalog, SoftDeletedRows},
    test_helpers::arbitrary_namespace,
    validate_or_insert_schema,
};
use iox_time::TimeProvider;
use metric::{Attributes, Metric, MetricObserver};
use mutable_batch_lp::lines_to_batches;
use mutable_batch_pb::encode::encode_write;
use object_store::ObjectStore;
use observability_deps::tracing::*;
use parquet_file::storage::ParquetStorage;
use tempfile::TempDir;
use test_helpers::timeout::FutureTimeout;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use trace::ctx::SpanContext;

/// The default max persist queue depth - configurable with
/// [`TestContextBuilder::with_max_persist_queue_depth()`].
pub const DEFAULT_MAX_PERSIST_QUEUE_DEPTH: usize = 5;
/// The default partition hot persist cost - configurable with
/// [`TestContextBuilder::with_persist_hot_partition_cost()`].
pub const DEFAULT_PERSIST_HOT_PARTITION_COST: usize = 20_000_000;
/// The default write-ahead log rotation period - configurable with
/// [`TestContextBuilder::with_wal_rotation_period()`].
/// This value is high to effectively stop the test ingester from
/// performing WAL rotations and the associated time-based persistence.
pub const DEFAULT_WAL_ROTATION_PERIOD: Duration = Duration::from_secs(1_000_000);
/// Construct a new [`TestContextBuilder`] to make a [`TestContext`] for an [`ingester`] instance.
pub fn test_context() -> TestContextBuilder {
    TestContextBuilder::default()
}

/// Configure and construct a [`TestContext`] containing an [`ingester`] instance.
#[derive(Debug)]
pub struct TestContextBuilder {
    wal_dir: Option<Arc<TempDir>>,
    catalog: Option<Arc<dyn Catalog>>,

    max_persist_queue_depth: usize,
    persist_hot_partition_cost: usize,
    wal_rotation_period: Duration,
}

impl Default for TestContextBuilder {
    fn default() -> Self {
        Self {
            wal_dir: None,
            catalog: None,
            max_persist_queue_depth: DEFAULT_MAX_PERSIST_QUEUE_DEPTH,
            persist_hot_partition_cost: DEFAULT_PERSIST_HOT_PARTITION_COST,
            wal_rotation_period: DEFAULT_WAL_ROTATION_PERIOD,
        }
    }
}

impl TestContextBuilder {
    /// Set the WAL file directory, defaulting to a random temporary directory
    /// if not specified.
    pub fn with_wal_dir(mut self, wal_dir: Arc<TempDir>) -> Self {
        self.wal_dir = Some(wal_dir);
        self
    }

    /// Set the catalog implementation, defaulting to an in-memory
    /// implementation if not specified.
    pub fn with_catalog(mut self, catalog: Arc<dyn Catalog>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Configure the ingester to reject write requests after this many persist
    /// jobs are queued for persistence. Defaults to
    /// [`DEFAULT_MAX_PERSIST_QUEUE_DEPTH`].
    pub fn with_max_persist_queue_depth(mut self, max: usize) -> Self {
        self.max_persist_queue_depth = max;
        self
    }

    /// Configure the ingester to persist partitions after their abstract "cost"
    /// exceeds this value. Defaults to [`DEFAULT_PERSIST_HOT_PARTITION_COST`].
    pub fn with_persist_hot_partition_cost(mut self, cost: usize) -> Self {
        self.persist_hot_partition_cost = cost;
        self
    }

    /// Configure the ingester to rotate the write-ahead log at the regular
    /// interval specified by [`Duration`]. Defaults to
    /// [`DEFAULT_WAL_ROTATION_PERIOD`].
    pub fn with_wal_rotation_period(mut self, period: Duration) -> Self {
        self.wal_rotation_period = period;
        self
    }

    /// Initialise the [`ingester`] instance and return a [`TestContext`] for it.
    pub async fn build(self) -> TestContext<impl IngesterRpcInterface> {
        let Self {
            wal_dir,
            catalog,
            max_persist_queue_depth,
            persist_hot_partition_cost,
            wal_rotation_period,
        } = self;

        test_helpers::maybe_start_logging();

        let metrics: Arc<metric::Registry> = Default::default();

        let dir = wal_dir.unwrap_or_else(|| Arc::new(test_helpers::tmp_dir().unwrap()));
        let catalog = catalog
            .unwrap_or_else(|| Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics))));

        let object_store: Arc<object_store::DynObjectStore> =
            Arc::new(object_store::memory::InMemory::default());
        let storage =
            ParquetStorage::new(object_store, parquet_file::storage::StorageId::from("iox"));

        let persist_background_fetch_time = Duration::from_secs(10);
        let persist_executor = Arc::new(iox_query::exec::Executor::new_testing());
        let persist_workers = 5;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let ingester = ingester::new(
            Arc::clone(&catalog),
            Arc::clone(&metrics),
            persist_background_fetch_time,
            dir.path().to_owned(),
            wal_rotation_period,
            persist_executor,
            persist_workers,
            max_persist_queue_depth,
            persist_hot_partition_cost,
            storage.clone(),
            GossipConfig::default(),
            shutdown_rx.map(|v| v.expect("shutdown sender dropped without calling shutdown")),
        )
        .await
        .expect("failed to initialise ingester instance");

        TestContext {
            ingester,
            shutdown_tx,
            _dir: dir,
            catalog,
            storage,
            metrics,
            namespaces: Default::default(),
        }
    }
}

/// A command interface to the underlying [`ingester`] instance.
///
/// When the [`TestContext`] is dropped, the underlying [`ingester`] instance
/// it controls is (ungracefully) stopped.
#[derive(Debug)]
pub struct TestContext<T> {
    ingester: IngesterGuard<T>,
    shutdown_tx: oneshot::Sender<CancellationToken>,
    catalog: Arc<dyn Catalog>,
    storage: ParquetStorage,
    metrics: Arc<metric::Registry>,

    /// Once the last [`TempDir`] reference is dropped, the directory it
    /// references is deleted.
    ///
    /// The [`TempDir`] instance must be held for the lifetime of the ingester
    /// so the temporary WAL directory is not deleted while in use.
    _dir: Arc<TempDir>,

    /// A map of namespace IDs to schemas, also serving as the set of known
    /// namespaces.
    namespaces: HashMap<NamespaceId, NamespaceSchema>,
}

impl<T> TestContext<T>
where
    T: IngesterRpcInterface,
{
    /// Create a namespace in the catalog for the ingester to discover.
    ///
    /// # Panics
    ///
    /// Must not be called twice with the same `name`.
    pub async fn ensure_namespace(
        &mut self,
        name: &str,
        retention_period_ns: Option<i64>,
        partition_template: Option<NamespacePartitionTemplateOverride>,
    ) -> Namespace {
        let mut repos = self.catalog.repositories().await;
        let ns = arbitrary_namespace(&mut *repos, name).await;

        assert!(
            self.namespaces
                .insert(
                    ns.id,
                    NamespaceSchema {
                        id: ns.id,
                        tables: Default::default(),
                        max_columns_per_table: iox_catalog::DEFAULT_MAX_COLUMNS_PER_TABLE as usize,
                        max_tables: iox_catalog::DEFAULT_MAX_TABLES as usize,
                        retention_period_ns,
                        partition_template: partition_template.unwrap_or_default(),
                    },
                )
                .is_none(),
            "namespace must not be duplicated"
        );

        debug!(?ns, "test namespace created");

        ns
    }

    /// Construct and submit a RPC request to write the given line protocol to
    /// the specified namespace & partition.
    pub async fn write_lp(
        &mut self,
        namespace: &str,
        lp: &str,
        partition_key: PartitionKey,
        sequence_number: u64,
        span_ctx: Option<SpanContext>,
    ) {
        // Resolve the namespace ID needed to construct the DML op
        let namespace_id = self.namespace_id(namespace).await;

        let schema = self
            .namespaces
            .get_mut(&namespace_id)
            .expect("namespace does not exist");
        let partition_template =
            TablePartitionTemplateOverride::try_new(None, &schema.partition_template).unwrap();

        let batches = lines_to_batches(lp, 0).unwrap();

        validate_or_insert_schema(
            batches
                .iter()
                .map(|(table_name, batch)| (table_name.as_str(), batch)),
            schema,
            self.catalog.repositories().await.as_mut(),
        )
        .await
        .expect("failed schema validation for enqueuing write");

        // Build the TableId -> Batch map, upserting the tables into the catalog in the
        // process.
        let batches_by_ids = batches
            .into_iter()
            .map(|(table_name, batch)| {
                let catalog = Arc::clone(&self.catalog);
                let partition_template = partition_template.clone();
                async move {
                    match catalog
                        .repositories()
                        .await
                        .tables()
                        .get_by_namespace_and_name(namespace_id, table_name.as_str())
                        .await
                        .unwrap()
                    {
                        Some(table) => (table.id, batch),
                        None => {
                            let id = catalog
                                .repositories()
                                .await
                                .tables()
                                .create(table_name.as_str(), partition_template, namespace_id)
                                .await
                                .expect("table should create OK")
                                .id;
                            (id, batch)
                        }
                    }
                }
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<hashbrown::HashMap<_, _>>()
            .await;

        let op = DmlWrite::new(
            namespace_id,
            batches_by_ids,
            partition_key,
            DmlMeta::sequenced(
                SequenceNumber::new(sequence_number),
                iox_time::SystemProvider::new().now(),
                None,
                50,
            ),
        );

        let mut req = tonic::Request::new(WriteRequest {
            payload: Some(encode_write(namespace_id.get(), &op)),
        });

        // Mock out the trace extraction middleware by inserting the given
        // span context straight into the requests extensions
        span_ctx.map(|span_ctx| req.extensions_mut().insert(span_ctx));

        self.ingester
            .rpc()
            .write_service()
            .write(req)
            .await
            .unwrap();

        debug!("pushed dml op");
    }

    /// Return the [`NamespaceId`] in the catalog for the specified namespace
    /// name, or panic.
    pub async fn namespace_id(&self, namespace: &str) -> NamespaceId {
        self.catalog
            .repositories()
            .await
            .namespaces()
            .get_by_name(namespace, SoftDeletedRows::AllRows)
            .await
            .expect("should be able to get namespace by name")
            .expect("namespace does not exist")
            .id
    }

    /// Return the [`TableId`] in the catalog for `name`, or panic.
    pub async fn table_id(&self, namespace: &str, name: &str) -> TableId {
        let namespace_id = self.namespace_id(namespace).await;

        self.catalog
            .repositories()
            .await
            .tables()
            .get_by_namespace_and_name(namespace_id, name)
            .await
            .expect("query failed")
            .expect("no table entry for the specified namespace/table name pair")
            .id
    }

    /// Submit a query to the ingester's public query interface.
    pub async fn query(
        &self,
        request: IngesterQueryRequest,
    ) -> Result<Vec<RecordBatch>, influxdb_iox_client::flight::Error> {
        let mut bytes = bytes::BytesMut::new();
        prost::Message::encode(&request, &mut bytes)?;
        let t = Ticket {
            ticket: bytes.into(),
        };

        let flight_data_stream = self
            .ingester
            .rpc()
            .query_service(5)
            .do_get(tonic::Request::new(t))
            .await?
            .into_inner();

        let record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
            // convert tonic::Status to FlightError
            flight_data_stream.map_err(|e| e.into()),
        );

        let iox_record_batch_stream =
            influxdb_iox_client::flight::IOxRecordBatchStream::new(record_batch_stream);

        let record_batches = iox_record_batch_stream.try_collect().await?;

        Ok(record_batches)
    }

    /// Request `namespace` be persisted and block for its completion.
    ///
    /// Be aware of the unreliable caveats of this interface (see proto
    /// definition).
    pub async fn persist(&self, namespace: impl Into<String> + Send) {
        use generated_types::influxdata::iox::ingester::v1::{
            self as proto, persist_service_server::PersistService,
        };

        let namespace = namespace.into();
        self.ingester
            .rpc()
            .persist_service()
            .persist(Request::new(proto::PersistRequest { namespace }))
            .await
            .expect("failed to invoke persist");
    }

    /// Gracefully stop the ingester, blocking until completion.
    pub async fn shutdown(self) {
        self.shutdown_tx
            .send(CancellationToken::new())
            .expect("shutdown channel dead");
        self.ingester
            .join()
            .with_timeout_panic(Duration::from_secs(10))
            .await;
    }

    /// Retrieve the specified metric value.
    pub fn get_metric<U, A>(&self, name: &'static str, attrs: A) -> U::Recorder
    where
        U: MetricObserver,
        A: Into<Attributes>,
    {
        let attrs = attrs.into();

        self.metrics
            .get_instrument::<Metric<U>>(name)
            .unwrap_or_else(|| panic!("failed to find metric {name}"))
            .get_observer(&attrs)
            .unwrap_or_else(|| {
                panic!(
                    "failed to find metric {} with attributes {:?}",
                    name, &attrs
                )
            })
            .recorder()
    }

    /// Return the metric recorder for the [`TestContext`].
    pub fn metrics(&self) -> &metric::Registry {
        &self.metrics
    }

    /// Retrieve the Parquet files in the catalog for the specified namespace.
    pub async fn catalog_parquet_file_records(&self, namespace: &str) -> Vec<ParquetFile> {
        let namespace_id = self.namespace_id(namespace).await;
        self.catalog
            .repositories()
            .await
            .parquet_files()
            .list_by_namespace_not_to_delete(namespace_id)
            .await
            .unwrap()
    }

    /// Return the [`Catalog`] for this [`TestContext`].
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        Arc::clone(&self.catalog)
    }

    /// Return the [`ObjectStore`] for this [`TestContext`].
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(self.storage.object_store())
    }

    /// Return the [`IngesterRpcInterface`] for this [`TestContext`].
    ///
    /// Calls duration made through this interface measures the cost of the
    /// request deserialisation and processing, and not any network I/O (as if
    /// the I/O had just finished and was being handed off to invoke the request
    /// handler with the fully read request).
    pub fn rpc(&self) -> &T {
        self.ingester.rpc()
    }
}
