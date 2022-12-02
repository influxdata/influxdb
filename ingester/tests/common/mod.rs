use std::{collections::HashMap, num::NonZeroU32, sync::Arc, time::Duration};

use data_types::{
    Namespace, NamespaceId, NamespaceSchema, PartitionKey, QueryPoolId, Sequence, SequenceNumber,
    ShardId, ShardIndex, TableId, TopicId,
};
use dml::{DmlMeta, DmlWrite};
use futures::{stream::FuturesUnordered, StreamExt};
use generated_types::ingester::IngesterQueryRequest;
use ingester::{
    handler::{IngestHandler, IngestHandlerImpl},
    lifecycle::LifecycleConfig,
    querier_handler::IngesterQueryResponse,
};
use iox_catalog::{interface::Catalog, mem::MemCatalog, validate_or_insert_schema};
use iox_query::exec::Executor;
use iox_time::TimeProvider;
use metric::{Attributes, Metric, MetricObserver};
use mutable_batch_lp::lines_to_batches;
use object_store::DynObjectStore;
use observability_deps::tracing::*;
use test_helpers::{maybe_start_logging, timeout::FutureTimeout};
use write_buffer::{
    core::WriteBufferReading,
    mock::{MockBufferForReading, MockBufferSharedState},
};
use write_summary::ShardProgress;

/// The byte size of 1 MiB.
const ONE_MIB: usize = 1024 * 1024;

/// The shard index used for the [`TestContext`].
pub const TEST_SHARD_INDEX: ShardIndex = ShardIndex::new(0);

/// The topic name used for tests.
pub const TEST_TOPIC_NAME: &str = "banana-topics";

/// The lifecycle configuration used for tests.
pub const TEST_LIFECYCLE_CONFIG: LifecycleConfig = LifecycleConfig::new(
    ONE_MIB,
    ONE_MIB / 10,
    ONE_MIB / 10,
    Duration::from_secs(10),
    Duration::from_secs(10),
    1_000,
);

pub struct TestContext {
    ingester: IngestHandlerImpl,

    // Catalog data initialised at construction time for later reuse.
    query_id: QueryPoolId,
    topic_id: TopicId,
    shard_id: ShardId,

    // A map of namespace IDs to schemas, also serving as the set of known
    // namespaces.
    namespaces: HashMap<NamespaceId, NamespaceSchema>,

    catalog: Arc<dyn Catalog>,
    object_store: Arc<DynObjectStore>,
    write_buffer_state: MockBufferSharedState,
    metrics: Arc<metric::Registry>,
}

impl TestContext {
    pub async fn new() -> Self {
        maybe_start_logging();

        let metrics: Arc<metric::Registry> = Default::default();
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));

        // Initialise a topic, query pool and shard.
        //
        // Note that tests should set up their own namespace via
        // ensure_namespace()
        let mut txn = catalog.start_transaction().await.unwrap();
        let topic = txn.topics().create_or_get(TEST_TOPIC_NAME).await.unwrap();
        let query_id = txn
            .query_pools()
            .create_or_get("banana-query-pool")
            .await
            .unwrap()
            .id;
        let shard = txn
            .shards()
            .create_or_get(&topic, TEST_SHARD_INDEX)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Mock in-memory write buffer.
        let write_buffer_state =
            MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(1).unwrap());
        let write_buffer_read: Arc<dyn WriteBufferReading> =
            Arc::new(MockBufferForReading::new(write_buffer_state.clone(), None).unwrap());

        // Mock object store that persists in memory.
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::new());

        let ingester = IngestHandlerImpl::new(
            TEST_LIFECYCLE_CONFIG,
            topic.clone(),
            [(TEST_SHARD_INDEX, shard)].into_iter().collect(),
            Arc::clone(&catalog),
            Arc::clone(&object_store),
            write_buffer_read,
            Arc::new(Executor::new_testing()),
            Arc::clone(&metrics),
            true,
            1,
        )
        .await
        .unwrap();

        Self {
            ingester,
            query_id,
            topic_id: topic.id,
            shard_id: shard.id,
            catalog,
            object_store,
            write_buffer_state,
            metrics,
            namespaces: Default::default(),
        }
    }

    /// Restart the Ingester, driving initialisation again.
    ///
    /// NOTE: metric contents are not reset.
    pub async fn restart(&mut self) {
        info!("restarting test context ingester");

        let write_buffer_read: Arc<dyn WriteBufferReading> =
            Arc::new(MockBufferForReading::new(self.write_buffer_state.clone(), None).unwrap());

        let topic = self
            .catalog
            .repositories()
            .await
            .topics()
            .create_or_get(TEST_TOPIC_NAME)
            .await
            .unwrap();

        let shard = self
            .catalog
            .repositories()
            .await
            .shards()
            .create_or_get(&topic, TEST_SHARD_INDEX)
            .await
            .unwrap();

        self.ingester = IngestHandlerImpl::new(
            TEST_LIFECYCLE_CONFIG,
            topic,
            [(TEST_SHARD_INDEX, shard)].into_iter().collect(),
            Arc::clone(&self.catalog),
            Arc::clone(&self.object_store),
            write_buffer_read,
            Arc::new(Executor::new_testing()),
            Arc::clone(&self.metrics),
            true,
            1,
        )
        .await
        .unwrap();
    }

    /// Create a namespace in the catalog for the ingester to discover.
    ///
    /// # Panics
    ///
    /// Must not be called twice with the same `name`.
    #[track_caller]
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
    #[track_caller]
    pub async fn write_lp(
        &mut self,
        namespace: &str,
        lp: &str,
        partition_key: PartitionKey,
        sequence_number: i64,
    ) -> SequenceNumber {
        // Resolve the namespace ID needed to construct the DML op
        let namespace_id = self
            .catalog
            .repositories()
            .await
            .namespaces()
            .get_by_name(namespace)
            .await
            .expect("should be able to get namespace by name")
            .expect("namespace does not exist")
            .id;

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
                Sequence::new(TEST_SHARD_INDEX, SequenceNumber::new(sequence_number)),
                iox_time::SystemProvider::new().now(),
                None,
                50,
            ),
        );

        // Pull the sequence number out of the op to return it back to the user
        // for simplicity.
        let offset = op
            .meta()
            .sequence()
            .expect("write must be sequenced")
            .sequence_number;

        // Push the write into the write buffer.
        self.write_buffer_state.push_write(op);

        debug!(?offset, "enqueued write in write buffer");
        offset
    }

    /// Return the [`TableId`] in the catalog for `name`, or panic.
    pub async fn table_id(&self, namespace: &str, name: &str) -> TableId {
        let namespace_id = self
            .catalog
            .repositories()
            .await
            .namespaces()
            .get_by_name(namespace)
            .await
            .expect("should be able to get namespace by name")
            .expect("namespace does not exist")
            .id;

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

    /// Utilise the progress API to query for the current state of the test
    /// shard.
    pub async fn progress(&self) -> ShardProgress {
        self.ingester
            .progresses(vec![TEST_SHARD_INDEX])
            .await
            .get(&TEST_SHARD_INDEX)
            .unwrap()
            .clone()
    }

    /// Wait for the specified `offset` to be readable according to the external
    /// progress API.
    ///
    /// # Panics
    ///
    /// This method panics if `offset` is not readable within 10 seconds.
    pub async fn wait_for_readable(&self, offset: SequenceNumber) {
        async {
            loop {
                let is_readable = self.progress().await.readable(offset);
                if is_readable {
                    debug!(?offset, "offset reported as readable");
                    return;
                }

                trace!(?offset, "offset reported as not yet readable");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        .with_timeout_panic(Duration::from_secs(10))
        .await;
    }

    /// Submit a query to the ingester's public query interface.
    pub async fn query(
        &self,
        req: IngesterQueryRequest,
    ) -> Result<IngesterQueryResponse, ingester::querier_handler::Error> {
        self.ingester.query(req, None).await
    }

    /// Retrieve the specified metric value.
    pub fn get_metric<T, A>(&self, name: &'static str, attrs: A) -> T::Recorder
    where
        T: MetricObserver,
        A: Into<Attributes>,
    {
        let attrs = attrs.into();

        self.metrics
            .get_instrument::<Metric<T>>(name)
            .unwrap_or_else(|| panic!("failed to find metric {}", name))
            .get_observer(&attrs)
            .unwrap_or_else(|| {
                panic!(
                    "failed to find metric {} with attributes {:?}",
                    name, &attrs
                )
            })
            .recorder()
    }

    /// Return a reference to the catalog.
    pub fn catalog(&self) -> &dyn Catalog {
        self.catalog.as_ref()
    }

    /// Return the [`ShardId`] of the test shard.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }
}
