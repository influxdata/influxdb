use std::{iter, string::String, sync::Arc, time::Duration};

use data_types::TableId;
use generated_types::influxdata::iox::ingester::v1::WriteRequest;
use hashbrown::HashMap;
use hyper::{Body, Request, Response};
use iox_catalog::{
    interface::{Catalog, SoftDeletedRows},
    mem::MemCatalog,
};
use mutable_batch::MutableBatch;
use object_store::memory::InMemory;
use router::{
    dml_handlers::{
        client::mock::MockWriteClient, Chain, DmlHandlerChainExt, FanOutAdaptor,
        InstrumentationDecorator, Partitioned, Partitioner, RetentionValidator, RpcWrite,
        SchemaValidator,
    },
    namespace_cache::{MemoryNamespaceCache, ReadThroughCache, ShardedCache},
    namespace_resolver::{MissingNamespaceAction, NamespaceAutocreation, NamespaceSchemaResolver},
    server::{
        grpc::RpcWriteGrpcDelegate,
        http::{write::multi_tenant::MultiTenantRequestUnifier, HttpDelegate},
    },
};

/// Common retention period value we'll use in tests
pub const TEST_RETENTION_PERIOD: Duration = Duration::from_secs(3600);

/// A [`TestContextBuilder`] can be used to build up a [`TestContext`] from a set
/// of sensible defaults. The default context produced will reject writes for a
/// missing namespace.
#[derive(Debug)]
pub struct TestContextBuilder {
    namespace_autocreation: MissingNamespaceAction,
    single_tenancy: bool,
    rpc_write_error_window: Duration,
    rpc_write_num_probes: u64,
}

impl Default for TestContextBuilder {
    fn default() -> Self {
        Self {
            namespace_autocreation: MissingNamespaceAction::Reject,
            single_tenancy: false,
            rpc_write_error_window: Duration::from_secs(5),
            rpc_write_num_probes: 10,
        }
    }
}

impl TestContextBuilder {
    /// Enable implicit namespace creation, with an optional retention duration,
    /// for all subsequently built [`TestContext`]s.
    pub fn with_autocreate_namespace(mut self, retention_duration: Option<Duration>) -> Self {
        self.namespace_autocreation =
            MissingNamespaceAction::AutoCreate(retention_duration.map(|d| {
                i64::try_from(d.as_nanos()).expect("retention period outside of i64 range")
            }));
        self
    }

    /// Disable implicit namespace creation, for all subsequently built
    /// [`TestContext`]s.
    pub fn without_autocreate_namespace(mut self) -> Self {
        self.namespace_autocreation = MissingNamespaceAction::Reject;
        self
    }

    pub fn with_single_tenancy(mut self) -> Self {
        self.single_tenancy = true;
        self
    }

    pub async fn build(self) -> TestContext {
        test_helpers::maybe_start_logging();

        let metrics: Arc<metric::Registry> = Default::default();
        let catalog = Arc::new(MemCatalog::new(Arc::clone(&metrics)));

        TestContext::new(
            self.namespace_autocreation,
            self.single_tenancy,
            catalog,
            metrics,
            self.rpc_write_error_window,
            self.rpc_write_num_probes,
        )
        .await
    }
}

#[derive(Debug)]
pub struct TestContext {
    client: Arc<MockWriteClient>,
    http_delegate: HttpDelegateStack,
    grpc_delegate: RpcWriteGrpcDelegate,
    catalog: Arc<dyn Catalog>,
    metrics: Arc<metric::Registry>,

    namespace_autocreation: MissingNamespaceAction,
    single_tenancy: bool,
    rpc_write_error_window: Duration,
    rpc_write_num_probes: u64,
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
                    RetentionValidator,
                    SchemaValidator<Arc<ReadThroughCache<Arc<ShardedCache<MemoryNamespaceCache>>>>>,
                >,
                Partitioner,
            >,
            FanOutAdaptor<
                RpcWrite<Arc<MockWriteClient>>,
                Vec<Partitioned<HashMap<TableId, (String, MutableBatch)>>>,
            >,
        >,
    >,
    NamespaceAutocreation<
        Arc<ReadThroughCache<Arc<ShardedCache<MemoryNamespaceCache>>>>,
        NamespaceSchemaResolver<Arc<ReadThroughCache<Arc<ShardedCache<MemoryNamespaceCache>>>>>,
    >,
>;

/// A [`router`] stack configured with the various DML handlers using mock catalog backends.
impl TestContext {
    async fn new(
        namespace_autocreation: MissingNamespaceAction,
        single_tenancy: bool,
        catalog: Arc<dyn Catalog>,
        metrics: Arc<metric::Registry>,
        rpc_write_error_window: Duration,
        rpc_write_num_probes: u64,
    ) -> Self {
        let client = Arc::new(MockWriteClient::default());
        let rpc_writer = RpcWrite::new(
            [(Arc::clone(&client), "mock client")],
            1.try_into().unwrap(),
            &metrics,
            rpc_write_num_probes,
        );

        let ns_cache = Arc::new(ReadThroughCache::new(
            Arc::new(ShardedCache::new(
                iter::repeat_with(MemoryNamespaceCache::default).take(10),
            )),
            Arc::clone(&catalog),
        ));

        let schema_validator =
            SchemaValidator::new(Arc::clone(&catalog), Arc::clone(&ns_cache), &metrics);

        let retention_validator = RetentionValidator::new();

        let partitioner = Partitioner::default();

        let namespace_resolver = NamespaceSchemaResolver::new(Arc::clone(&ns_cache));
        let namespace_resolver = NamespaceAutocreation::new(
            namespace_resolver,
            Arc::clone(&ns_cache),
            Arc::clone(&catalog),
            namespace_autocreation,
        );

        let parallel_write = FanOutAdaptor::new(rpc_writer);

        let handler_stack = retention_validator
            .and_then(schema_validator)
            .and_then(partitioner)
            .and_then(parallel_write);

        let handler_stack = InstrumentationDecorator::new("request", &metrics, handler_stack);

        let write_request_unifier = Box::<MultiTenantRequestUnifier>::default();

        let http_delegate = HttpDelegate::new(
            1024,
            100,
            namespace_resolver,
            handler_stack,
            &metrics,
            write_request_unifier,
        );

        let grpc_delegate =
            RpcWriteGrpcDelegate::new(Arc::clone(&catalog), Arc::new(InMemory::default()));

        Self {
            client,
            http_delegate,
            grpc_delegate,
            catalog,
            metrics,

            namespace_autocreation,
            single_tenancy,
            rpc_write_error_window,
            rpc_write_num_probes,
        }
    }

    // Restart the server, rebuilding all state from the catalog (preserves
    // metrics).
    pub async fn restart(self) -> Self {
        let catalog = self.catalog();
        let metrics = Arc::clone(&self.metrics);
        Self::new(
            self.namespace_autocreation,
            self.single_tenancy,
            catalog,
            metrics,
            self.rpc_write_error_window,
            self.rpc_write_num_probes,
        )
        .await
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
    pub fn metrics(&self) -> &metric::Registry {
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
