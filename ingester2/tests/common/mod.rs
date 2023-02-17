use arrow::record_batch::RecordBatch;
use arrow_flight::{decode::FlightRecordBatchStream, flight_service_server::FlightService, Ticket};
use data_types::{
    Namespace, NamespaceId, NamespaceSchema, ParquetFile, PartitionKey, QueryPoolId, Sequence,
    SequenceNumber, ShardIndex, TableId, TopicId,
};
use dml::{DmlMeta, DmlWrite};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt, TryStreamExt};
use generated_types::influxdata::iox::ingester::v1::{
    write_service_server::WriteService, WriteRequest,
};
use influxdb_iox_client::flight;
use ingester2::{IngesterGuard, IngesterRpcInterface};
use iox_catalog::{
    interface::{Catalog, SoftDeletedRows},
    validate_or_insert_schema,
};
use iox_time::TimeProvider;
use metric::{Attributes, Metric, MetricObserver};
use mutable_batch_lp::lines_to_batches;
use mutable_batch_pb::encode::encode_write;
use observability_deps::tracing::*;
use parquet_file::storage::ParquetStorage;
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub const TEST_TOPIC_NAME: &str = "banana-topics";

/// Construct a new [`TestContextBuilder`] to make a [`TestContext`] for an [`ingester2`] instance.
pub fn test_context() -> TestContextBuilder {
    TestContextBuilder::default()
}

#[derive(Debug, Default)]
pub struct TestContextBuilder {
    wal_dir: Option<Arc<TempDir>>,
    catalog: Option<Arc<dyn Catalog>>,
}

impl TestContextBuilder {
    pub fn wal_dir(mut self, wal_dir: Arc<TempDir>) -> Self {
        self.wal_dir = Some(wal_dir);
        self
    }

    pub fn catalog(mut self, catalog: Arc<dyn Catalog>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    pub async fn build(self) -> TestContext<impl IngesterRpcInterface> {
        let Self { wal_dir, catalog } = self;

        test_helpers::maybe_start_logging();

        let metrics: Arc<metric::Registry> = Default::default();

        let dir = wal_dir.unwrap_or_else(|| Arc::new(test_helpers::tmp_dir().unwrap()));
        let catalog = catalog
            .unwrap_or_else(|| Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics))));

        let object_store: Arc<object_store::DynObjectStore> =
            Arc::new(object_store::memory::InMemory::default());
        let storage =
            ParquetStorage::new(object_store, parquet_file::storage::StorageId::from("iox"));

        // Initialise a topic and query pool.
        //
        // Note that tests should set up their own namespace via
        // ensure_namespace()
        let mut txn = catalog.start_transaction().await.unwrap();
        let topic = txn
            .topics()
            .create_or_get(crate::common::TEST_TOPIC_NAME)
            .await
            .unwrap();
        let query_id = txn
            .query_pools()
            .create_or_get("banana-query-pool")
            .await
            .unwrap()
            .id;
        txn.commit().await.unwrap();

        let (ingester, shutdown_tx) = new_ingester(
            Arc::clone(&catalog),
            Arc::clone(&metrics),
            dir.path().to_path_buf(),
            storage.clone(),
        )
        .await;

        TestContext::new(
            ingester,
            shutdown_tx,
            dir,
            catalog,
            storage,
            query_id,
            topic.id,
            metrics,
        )
        .await
    }
}

async fn new_ingester(
    catalog: Arc<dyn Catalog>,
    metrics: Arc<metric::Registry>,
    wal_directory: PathBuf,
    storage: ParquetStorage,
) -> (
    IngesterGuard<impl IngesterRpcInterface>,
    oneshot::Sender<CancellationToken>,
) {
    // Settings so that the ingester will effectively never persist by itself, only on demand
    let wal_rotation_period = Duration::from_secs(1_000_000);
    let persist_hot_partition_cost = 20_000_000;

    let persist_background_fetch_time = Duration::from_secs(10);
    let persist_executor = Arc::new(iox_query::exec::Executor::new_testing());
    let persist_workers = 5;
    let persist_queue_depth = 5;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let ingester = ingester2::new(
        catalog,
        metrics,
        persist_background_fetch_time,
        wal_directory,
        wal_rotation_period,
        persist_executor,
        persist_workers,
        persist_queue_depth,
        persist_hot_partition_cost,
        storage,
        shutdown_rx.map(|v| v.expect("shutdown sender dropped without calling shutdown")),
    )
    .await
    .expect("failed to initialise ingester instance");
    (ingester, shutdown_tx)
}

const SHARD_INDEX: ShardIndex = ShardIndex::new(42);

#[allow(dead_code)]
pub struct TestContext<T> {
    ingester: IngesterGuard<T>,
    shutdown_tx: oneshot::Sender<CancellationToken>,
    dir: Arc<TempDir>,
    catalog: Arc<dyn Catalog>,
    storage: ParquetStorage,
    query_id: QueryPoolId,
    topic_id: TopicId,
    metrics: Arc<metric::Registry>,
    /// A map of namespace IDs to schemas, also serving as the set of known
    /// namespaces.
    namespaces: HashMap<NamespaceId, NamespaceSchema>,
}

impl<T> TestContext<T>
where
    T: IngesterRpcInterface,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        ingester: IngesterGuard<T>,
        shutdown_tx: oneshot::Sender<CancellationToken>,
        dir: Arc<TempDir>,
        catalog: Arc<dyn Catalog>,
        storage: ParquetStorage,
        query_id: QueryPoolId,
        topic_id: TopicId,
        metrics: Arc<metric::Registry>,
    ) -> Self {
        TestContext {
            ingester,
            shutdown_tx,
            dir,
            catalog,
            storage,
            query_id,
            topic_id,
            metrics,
            namespaces: Default::default(),
        }
    }

    /// Create a namespace in the catalog for the ingester to discover.
    ///
    /// # Panics
    ///
    /// Must not be called twice with the same `name`.
    pub async fn ensure_namespace(
        &mut self,
        name: &str,
        retention_period_ns: Option<i64>,
    ) -> Namespace {
        let ns = self
            .catalog
            .repositories()
            .await
            .namespaces()
            .create(name, None, self.topic_id, self.query_id)
            .await
            .expect("failed to create test namespace");

        assert!(
            self.namespaces
                .insert(
                    ns.id,
                    NamespaceSchema::new(
                        ns.id,
                        self.topic_id,
                        self.query_id,
                        iox_catalog::DEFAULT_MAX_COLUMNS_PER_TABLE,
                        iox_catalog::DEFAULT_MAX_TABLES,
                        retention_period_ns,
                    ),
                )
                .is_none(),
            "namespace must not be duplicated"
        );

        debug!(?ns, "test namespace created");

        ns
    }

    /// A helper wrapper over [`Self::enqueue_write()`] for line-protocol.
    pub async fn write_lp(
        &mut self,
        namespace: &str,
        lp: &str,
        partition_key: PartitionKey,
        sequence_number: i64,
    ) {
        // Resolve the namespace ID needed to construct the DML op
        let namespace_id = self.namespace_id(namespace).await;

        let schema = self
            .namespaces
            .get_mut(&namespace_id)
            .expect("namespace does not exist");

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
                async move {
                    let id = catalog
                        .repositories()
                        .await
                        .tables()
                        .create_or_get(&table_name, namespace_id)
                        .await
                        .expect("table should create OK")
                        .id;

                    (id, batch)
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
                Sequence::new(SHARD_INDEX, SequenceNumber::new(sequence_number)),
                iox_time::SystemProvider::new().now(),
                None,
                50,
            ),
        );

        self.ingester
            .rpc()
            .write_service()
            .write(tonic::Request::new(WriteRequest {
                payload: Some(encode_write(namespace_id.get(), &op)),
            }))
            .await
            .unwrap();

        debug!("pushed dml op");
    }

    /// Return the [`NamespaceId`] in the catalog for the specified namespace name, or panic.
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
        request: flight::generated_types::IngesterQueryRequest,
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
}
