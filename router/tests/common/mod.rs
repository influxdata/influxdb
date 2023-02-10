use data_types::{PartitionTemplate, QueryPoolId, TableId, TemplatePart, TopicId};
use generated_types::influxdata::iox::ingester::v1::WriteRequest;
use hashbrown::HashMap;
use hyper::{Body, Request, Response};
use iox_catalog::{
    interface::{Catalog, SoftDeletedRows},
    mem::MemCatalog,
};
use metric::Registry;
use mutable_batch::MutableBatch;
use object_store::memory::InMemory;
use router::{
    dml_handlers::{
        client::mock::MockWriteClient, Chain, DmlHandlerChainExt, FanOutAdaptor,
        InstrumentationDecorator, Partitioned, Partitioner, RetentionValidator, RpcWrite,
        SchemaValidator, WriteSummaryAdapter,
    },
    namespace_cache::{MemoryNamespaceCache, ShardedCache},
    namespace_resolver::{MissingNamespaceAction, NamespaceAutocreation, NamespaceSchemaResolver},
    server::{grpc::RpcWriteGrpcDelegate, http::HttpDelegate},
};
use std::{iter, string::String, sync::Arc};

/// The topic catalog ID assigned by the namespace auto-creator in the
/// handler stack for namespaces it has not yet observed.
pub const TEST_TOPIC_ID: i64 = 1;

/// The query pool catalog ID assigned by the namespace auto-creator in the
/// handler stack for namespaces it has not yet observed.
pub const TEST_QUERY_POOL_ID: i64 = 1;

/// Common retention period value we'll use in tests
pub const TEST_RETENTION_PERIOD_NS: Option<i64> = Some(3_600 * 1_000_000_000);

#[derive(Debug)]
pub struct TestContext {
    client: Arc<MockWriteClient>,
    http_delegate: HttpDelegateStack,
    grpc_delegate: RpcWriteGrpcDelegate,
    catalog: Arc<dyn Catalog>,
    metrics: Arc<Registry>,

    autocreate_ns: bool,
    ns_autocreate_retention_period_ns: Option<i64>,
}

// This mass of words is certainly a downside of chained handlers.
//
// Fortunately the compiler errors are very descriptive and updating this is
// relatively easy when something changes!
type HttpDelegateStack = HttpDelegate<
    InstrumentationDecorator<
        Chain<
            Chain<
                Chain<
                    RetentionValidator<Arc<ShardedCache<Arc<MemoryNamespaceCache>>>>,
                    SchemaValidator<Arc<ShardedCache<Arc<MemoryNamespaceCache>>>>,
                >,
                Partitioner,
            >,
            WriteSummaryAdapter<
                FanOutAdaptor<
                    RpcWrite<Arc<MockWriteClient>>,
                    Vec<Partitioned<HashMap<TableId, (String, MutableBatch)>>>,
                >,
            >,
        >,
    >,
    NamespaceAutocreation<
        Arc<ShardedCache<Arc<MemoryNamespaceCache>>>,
        NamespaceSchemaResolver<Arc<ShardedCache<Arc<MemoryNamespaceCache>>>>,
    >,
>;

/// A [`router`] stack configured with the various DML handlers using mock catalog backends.
impl TestContext {
    pub async fn new(autocreate_ns: bool, ns_autocreate_retention_period_ns: Option<i64>) -> Self {
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));

        Self::init_with_catalog(
            autocreate_ns,
            ns_autocreate_retention_period_ns,
            catalog,
            metrics,
        )
    }

    fn init_with_catalog(
        autocreate_ns: bool,
        ns_autocreate_retention_period_ns: Option<i64>,
        catalog: Arc<dyn Catalog>,
        metrics: Arc<metric::Registry>,
    ) -> Self {
        let client = Arc::new(MockWriteClient::default());
        let rpc_writer = RpcWrite::new([(Arc::clone(&client), "mock client")], &metrics);

        let ns_cache = Arc::new(ShardedCache::new(
            iter::repeat_with(|| Arc::new(MemoryNamespaceCache::default())).take(10),
        ));

        let schema_validator =
            SchemaValidator::new(Arc::clone(&catalog), Arc::clone(&ns_cache), &metrics);

        let retention_validator =
            RetentionValidator::new(Arc::clone(&catalog), Arc::clone(&ns_cache));

        let partitioner = Partitioner::new(PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat("%Y-%m-%d".to_owned())],
        });

        let namespace_resolver =
            NamespaceSchemaResolver::new(Arc::clone(&catalog), Arc::clone(&ns_cache));
        let namespace_resolver = NamespaceAutocreation::new(
            namespace_resolver,
            Arc::clone(&ns_cache),
            Arc::clone(&catalog),
            TopicId::new(TEST_TOPIC_ID),
            QueryPoolId::new(TEST_QUERY_POOL_ID),
            {
                if autocreate_ns {
                    MissingNamespaceAction::AutoCreate(ns_autocreate_retention_period_ns)
                } else {
                    MissingNamespaceAction::Reject
                }
            },
        );

        let parallel_write = WriteSummaryAdapter::new(FanOutAdaptor::new(rpc_writer));

        let handler_stack = retention_validator
            .and_then(schema_validator)
            .and_then(partitioner)
            .and_then(parallel_write);

        let handler_stack = InstrumentationDecorator::new("request", &metrics, handler_stack);

        let http_delegate =
            HttpDelegate::new(1024, 100, namespace_resolver, handler_stack, &metrics);

        let grpc_delegate = RpcWriteGrpcDelegate::new(
            Arc::clone(&catalog),
            Arc::new(InMemory::default()),
            TopicId::new(TEST_TOPIC_ID),
            QueryPoolId::new(TEST_QUERY_POOL_ID),
        );

        Self {
            client,
            http_delegate,
            grpc_delegate,
            catalog,
            metrics,
            autocreate_ns,
            ns_autocreate_retention_period_ns,
        }
    }

    // Restart the server, rebuilding all state from the catalog (preserves
    // metrics).
    pub fn restart(self) -> Self {
        let catalog = self.catalog();
        let metrics = Arc::clone(&self.metrics);
        Self::init_with_catalog(
            self.autocreate_ns,
            self.ns_autocreate_retention_period_ns,
            catalog,
            metrics,
        )
    }

    /// Get a reference to the test context's http delegate.
    pub fn http_delegate(&self) -> &HttpDelegateStack {
        &self.http_delegate
    }

    /// Get a reference to the test context's grpc delegate.
    pub fn grpc_delegate(&self) -> &RpcWriteGrpcDelegate {
        &self.grpc_delegate
    }

    /// Get a reference to the test context's catalog.
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        Arc::clone(&self.catalog)
    }

    pub fn write_calls(&self) -> Vec<WriteRequest> {
        self.client.calls()
    }

    /// Get a reference to the test context's metrics.
    pub fn metrics(&self) -> &Registry {
        self.metrics.as_ref()
    }

    /// Return the [`TableId`] in the catalog for `name` in `namespace`, or panic.
    pub async fn table_id(&self, namespace: &str, name: &str) -> TableId {
        let mut repos = self.catalog.repositories().await;
        let namespace_id = repos
            .namespaces()
            .get_by_name(namespace, SoftDeletedRows::AllRows)
            .await
            .expect("query failed")
            .expect("namespace does not exist")
            .id;

        repos
            .tables()
            .get_by_namespace_and_name(namespace_id, name)
            .await
            .expect("query failed")
            .expect("no table entry for the specified namespace/table name pair")
            .id
    }

    /// A helper method to write LP to this [`TestContext`].
    pub async fn write_lp(
        &self,
        org: &str,
        bucket: &str,
        lp: impl Into<String>,
    ) -> Result<Response<Body>, router::server::http::Error> {
        let request = Request::builder()
            .uri(format!(
                "https://bananas.example/api/v2/write?org={org}&bucket={bucket}"
            ))
            .method("POST")
            .body(Body::from(lp.into()))
            .expect("failed to construct HTTP request");

        self.http_delegate().route(request).await
    }
}
