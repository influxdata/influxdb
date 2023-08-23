#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    clippy::clone_on_ref_ptr,
    clippy::dbg_macro,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::use_self,
    missing_debug_implementations,
    unused_crate_dependencies
)]
#![allow(clippy::default_constructed_unit_structs)]

use gossip::TopicInterests;
use gossip_schema::{dispatcher::SchemaRx, handle::SchemaTx};
// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use authz::{Authorizer, AuthorizerInstrumentation, IoxAuthorizer};
use clap_blocks::{gossip::GossipConfig, router::RouterConfig};
use data_types::NamespaceName;
use hashbrown::HashMap;
use hyper::{Body, Request, Response};
use iox_catalog::interface::Catalog;
use ioxd_common::{
    add_service,
    http::error::{HttpApiError, HttpApiErrorSource},
    reexport::{
        generated_types::influxdata::iox::{
            catalog::v1::catalog_service_server, gossip::Topic,
            namespace::v1::namespace_service_server, object_store::v1::object_store_service_server,
            schema::v1::schema_service_server, table::v1::table_service_server,
        },
        tonic::transport::Endpoint,
    },
    rpc::RpcBuilderInput,
    serve_builder,
    server_type::{CommonServerState, RpcError, ServerType},
    setup_builder,
};
use metric::Registry;
use mutable_batch::MutableBatch;
use object_store::DynObjectStore;
use router::{
    dml_handlers::{
        lazy_connector::LazyConnector, DmlHandler, DmlHandlerChainExt, FanOutAdaptor,
        InstrumentationDecorator, Partitioner, RetentionValidator, RpcWrite, SchemaValidator,
    },
    gossip::{
        namespace_cache::NamespaceSchemaGossip, schema_change_observer::SchemaChangeObserver,
    },
    namespace_cache::{
        metrics::InstrumentedCache, MaybeLayer, MemoryNamespaceCache, NamespaceCache,
        ReadThroughCache, ShardedCache,
    },
    namespace_resolver::{
        MissingNamespaceAction, NamespaceAutocreation, NamespaceResolver, NamespaceSchemaResolver,
    },
    server::{
        grpc::RpcWriteGrpcDelegate,
        http::{
            write::{
                multi_tenant::MultiTenantRequestUnifier, single_tenant::SingleTenantRequestUnifier,
                WriteRequestUnifier,
            },
            HttpDelegate,
        },
        RpcWriteRouterServer,
    },
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("Catalog DSN error: {0}")]
    CatalogDsn(#[from] clap_blocks::catalog_dsn::Error),

    #[error("authz configuration error for '{addr}': '{source}'")]
    AuthzConfig {
        source: Box<dyn std::error::Error>,
        addr: String,
    },

    /// An error binding the UDP socket for gossip communication.
    #[error("failed to bind udp gossip socket: {0}")]
    GossipBind(std::io::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct RpcWriteRouterServerType<D, N> {
    server: RpcWriteRouterServer<D, N>,
    shutdown: CancellationToken,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

impl<D, N> RpcWriteRouterServerType<D, N> {
    pub fn new(server: RpcWriteRouterServer<D, N>, common_state: &CommonServerState) -> Self {
        Self {
            server,
            shutdown: CancellationToken::new(),
            trace_collector: common_state.trace_collector(),
        }
    }
}

impl<D, N> std::fmt::Debug for RpcWriteRouterServerType<D, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Router")
    }
}

#[async_trait]
impl<D, N> ServerType for RpcWriteRouterServerType<D, N>
where
    D: DmlHandler<WriteInput = HashMap<String, MutableBatch>, WriteOutput = ()> + 'static,
    N: NamespaceResolver + 'static,
{
    fn name(&self) -> &str {
        "rpc_write_router"
    }

    /// Return the [`metric::Registry`] used by the router.
    fn metric_registry(&self) -> Arc<Registry> {
        self.server.metric_registry()
    }

    /// Returns the trace collector for router traces.
    fn trace_collector(&self) -> Option<Arc<dyn TraceCollector>> {
        self.trace_collector.as_ref().map(Arc::clone)
    }

    /// Dispatches `req` to the router [`HttpDelegate`] delegate.
    ///
    /// [`HttpDelegate`]: router::server::http::HttpDelegate
    async fn route_http_request(
        &self,
        req: Request<Body>,
    ) -> Result<Response<Body>, Box<dyn HttpApiErrorSource>> {
        self.server
            .http()
            .route(req)
            .await
            .map_err(IoxHttpErrorAdaptor)
            .map_err(|e| Box::new(e) as _)
    }

    /// Registers the services exposed by the router [`RpcWriteGrpcDelegate`] delegate.
    ///
    /// [`RpcWriteGrpcDelegate`]: router::server::grpc::RpcWriteGrpcDelegate
    async fn server_grpc(self: Arc<Self>, builder_input: RpcBuilderInput) -> Result<(), RpcError> {
        let builder = setup_builder!(builder_input, self);
        add_service!(
            builder,
            schema_service_server::SchemaServiceServer::new(self.server.grpc().schema_service())
        );
        add_service!(
            builder,
            catalog_service_server::CatalogServiceServer::new(self.server.grpc().catalog_service())
        );
        add_service!(
            builder,
            object_store_service_server::ObjectStoreServiceServer::new(
                self.server.grpc().object_store_service()
            )
        );
        add_service!(
            builder,
            namespace_service_server::NamespaceServiceServer::new(
                self.server.grpc().namespace_service()
            )
        );
        add_service!(
            builder,
            table_service_server::TableServiceServer::new(self.server.grpc().table_service())
        );
        serve_builder!(builder);

        Ok(())
    }

    async fn join(self: Arc<Self>) {
        self.shutdown.cancelled().await;
    }

    fn shutdown(&self, frontend: CancellationToken) {
        frontend.cancel();
        self.shutdown.cancel();
    }
}

/// This adaptor converts the `router` http error type into a type that
/// satisfies the requirements of ioxd's runner framework, keeping the
/// two decoupled.
#[derive(Debug)]
pub struct IoxHttpErrorAdaptor(router::server::http::Error);

impl Display for IoxHttpErrorAdaptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl std::error::Error for IoxHttpErrorAdaptor {}

impl HttpApiErrorSource for IoxHttpErrorAdaptor {
    fn to_http_api_error(&self) -> HttpApiError {
        HttpApiError::new(self.0.as_status_code(), self.to_string())
    }
}

/// Instantiate a router server that uses the RPC write path
pub async fn create_router_server_type(
    common_state: &CommonServerState,
    metrics: Arc<metric::Registry>,
    catalog: Arc<dyn Catalog>,
    object_store: Arc<DynObjectStore>,
    router_config: &RouterConfig,
    gossip_config: &GossipConfig,
    trace_context_header_name: String,
) -> Result<Arc<dyn ServerType>> {
    let ingester_connections = router_config.ingester_addresses.iter().map(|addr| {
        let addr = addr.to_string();
        let endpoint = Endpoint::from_shared(hyper::body::Bytes::from(addr.clone()))
            .expect("invalid ingester connection address");
        (
            LazyConnector::new(
                endpoint,
                router_config.rpc_write_timeout_seconds,
                router_config.rpc_write_max_outgoing_bytes,
                trace_context_header_name.clone(),
            ),
            addr,
        )
    });

    // Initialise the DML handler that sends writes to the ingester using the RPC write path.
    let rpc_writer = RpcWrite::new(
        ingester_connections,
        router_config.rpc_write_replicas,
        &metrics,
        router_config.rpc_write_health_num_probes,
    );
    let rpc_writer = InstrumentationDecorator::new("rpc_writer", &metrics, rpc_writer);

    // # Namespace cache
    //
    // Initialise an instrumented namespace cache to be shared with the schema
    // validator, and namespace auto-creator that reports cache hit/miss/update
    // metrics.
    let ns_cache = Arc::new(InstrumentedCache::new(
        Arc::new(ShardedCache::new(
            std::iter::repeat_with(|| Arc::new(MemoryNamespaceCache::default())).take(10),
        )),
        &metrics,
    ));

    // Pre-warm the cache before adding the gossip layer to avoid broadcasting
    // the full cache content at startup.
    pre_warm_schema_cache(&ns_cache, &*catalog)
        .await
        .expect("namespace cache pre-warming failed");

    // Optionally initialise the schema gossip subsystem.
    //
    // The schema gossip primitives sit in the stack of NamespaceCache layers:
    //
    //                   ┌───────────────────────────┐
    //                   │     ReadThroughCache      │
    //                   └───────────────────────────┘
    //                                 │
    //                                 ▼
    //                   ┌───────────────────────────┐
    //                   │   SchemaChangeObserver    │◀ ─ ─ ─ ─
    //                   └───────────────────────────┘         │
    //                                 │                     peers
    //                                 ▼                       ▲
    //                   ┌───────────────────────────┐         │
    //                   │   Incoming Gossip Apply   │─ ─ ─ ─ ─
    //                   └───────────────────────────┘
    //                                 │
    //                                 ▼
    //                   ┌───────────────────────────┐
    //                   │      Underlying Impl      │
    //                   └───────────────────────────┘
    //
    //
    //   - SchemaChangeObserver: sends outgoing gossip schema diffs
    //   - NamespaceSchemaGossip: applies incoming gossip diffs locally
    //
    // The SchemaChangeObserver is responsible for gossiping any diffs that pass
    // through it - it MUST sit above the NamespaceSchemaGossip layer
    // responsible for applying gossip diffs from peers (otherwise the local
    // node will receive a gossip diff and apply it, which passes through the
    // diff layer, and causes the local node to gossip it again).
    //
    // These gossip layers sit below the catalog lookups, so that they do not
    // drive catalog queries themselves (defeating the point of the gossiping!).
    // If a local node has to perform a catalog lookup, it gossips the result to
    // other peers, helping converge them.
    let ns_cache = match gossip_config.gossip_bind_address {
        Some(bind_addr) => {
            // Initialise the NamespaceSchemaGossip responsible for applying the
            // incoming gossip schema diffs.
            let gossip_reader = Arc::new(NamespaceSchemaGossip::new(Arc::clone(&ns_cache)));
            // Adapt it to the gossip subsystem via the "Dispatcher" trait
            let dispatcher = SchemaRx::new(Arc::clone(&gossip_reader), 100);

            // Initialise the gossip subsystem, delegating message processing to
            // the above dispatcher.
            let handle = gossip::Builder::<_, Topic>::new(
                gossip_config.seed_list.clone(),
                dispatcher,
                Arc::clone(&metrics),
            )
            // Configure the router to listen to SchemaChange messages.
            .with_topic_filter(TopicInterests::default().with_topic(Topic::SchemaChanges))
            .bind(*bind_addr)
            .await
            .map_err(Error::GossipBind)?;

            // Initialise the local diff observer responsible for gossiping any
            // local changes made to the cache content.
            //
            // This sits above / wraps the NamespaceSchemaGossip layer.
            let ns_cache = SchemaChangeObserver::new(ns_cache, SchemaTx::new(handle));

            MaybeLayer::With(ns_cache)
        }
        None => MaybeLayer::Without(ns_cache),
    };

    // Wrap the NamespaceCache in a read-through layer that queries the catalog
    // for cache misses, and populates the local cache with the result.
    let ns_cache = Arc::new(ReadThroughCache::new(ns_cache, Arc::clone(&catalog)));

    // # Schema validator
    //
    // Initialise and instrument the schema validator
    let schema_validator =
        SchemaValidator::new(Arc::clone(&catalog), Arc::clone(&ns_cache), &metrics);
    let schema_validator =
        InstrumentationDecorator::new("schema_validator", &metrics, schema_validator);

    // # Retention validator
    //
    // Add a retention validator into handler stack to reject data outside the retention period
    let retention_validator = RetentionValidator::new();
    let retention_validator =
        InstrumentationDecorator::new("retention_validator", &metrics, retention_validator);

    // # Write partitioner
    //
    // Add a write partitioner into the handler stack that splits by the date
    // portion of the write's timestamp (the default table partition template)
    let partitioner = Partitioner::default();
    let partitioner = InstrumentationDecorator::new("partitioner", &metrics, partitioner);

    // # Namespace resolver
    //
    // Initialise the Namespace ID lookup + cache
    let namespace_resolver = NamespaceSchemaResolver::new(Arc::clone(&ns_cache));

    let namespace_resolver = NamespaceAutocreation::new(
        namespace_resolver,
        Arc::clone(&ns_cache),
        Arc::clone(&catalog),
        {
            if router_config.namespace_autocreation_enabled {
                MissingNamespaceAction::AutoCreate(
                    router_config
                        .new_namespace_retention_hours
                        .map(|hours| hours as i64 * 60 * 60 * 1_000_000_000),
                )
            } else {
                MissingNamespaceAction::Reject
            }
        },
    );
    //
    ////////////////////////////////////////////////////////////////////////////

    // # Parallel writer
    let parallel_write = FanOutAdaptor::new(rpc_writer);

    // # Handler stack
    //
    // Build the chain of DML handlers that forms the request processing pipeline
    let handler_stack = retention_validator
        .and_then(schema_validator)
        .and_then(partitioner)
        // Once writes have been partitioned, they are processed in parallel.
        //
        // This block initialises a fan-out adaptor that parallelises partitioned
        // writes into the handler chain it decorates (schema validation, and then
        // into the ingester RPC), and instruments the parallelised
        // operation.
        .and_then(InstrumentationDecorator::new(
            "parallel_write",
            &metrics,
            parallel_write,
        ));

    // Record the overall request handling latency
    let handler_stack = InstrumentationDecorator::new("request", &metrics, handler_stack);

    // Initialize the HTTP API delegate
    let write_request_unifier: Result<Box<dyn WriteRequestUnifier>> = match (
        router_config.single_tenant_deployment,
        &router_config.authz_address,
    ) {
        (true, Some(addr)) => {
            let authz = IoxAuthorizer::connect_lazy(addr.clone())
                .map(|c| {
                    Arc::new(AuthorizerInstrumentation::new(&metrics, c)) as Arc<dyn Authorizer>
                })
                .map_err(|source| Error::AuthzConfig {
                    source,
                    addr: addr.clone(),
                })?;
            authz.probe().await.expect("Authz connection test failed.");

            Ok(Box::new(SingleTenantRequestUnifier::new(authz)))
        }
        (true, None) => {
            // Single tenancy was requested, but no auth was provided - the
            // router's clap flag parse configuration should not allow this
            // combination to be accepted and therefore execution should
            // never reach here.
            unreachable!("INFLUXDB_IOX_SINGLE_TENANCY is set, but could not create an authz service. Check the INFLUXDB_IOX_AUTHZ_ADDR")
        }
        (false, None) => Ok(Box::<MultiTenantRequestUnifier>::default()),
        (false, Some(_)) => {
            // As above, this combination should be prevented by the
            // router's clap flag parse configuration.
            unreachable!("INFLUXDB_IOX_AUTHZ_ADDR is set, but authz only exists for single_tenancy. Check the INFLUXDB_IOX_SINGLE_TENANCY")
        }
    };
    let http = HttpDelegate::new(
        common_state.run_config().max_http_request_size,
        router_config.http_request_limit,
        namespace_resolver,
        handler_stack,
        &metrics,
        write_request_unifier?,
    );

    // Initialize the gRPC API delegate that creates the services relevant to the RPC
    // write router path and use it to create the relevant `RpcWriteRouterServer` and
    // `RpcWriteRouterServerType`.
    let grpc = RpcWriteGrpcDelegate::new(catalog, object_store);

    let router_server =
        RpcWriteRouterServer::new(http, grpc, metrics, common_state.trace_collector());
    let server_type = Arc::new(RpcWriteRouterServerType::new(router_server, common_state));
    Ok(server_type)
}

/// Pre-populate `cache` with the all existing schemas in `catalog`.
async fn pre_warm_schema_cache<T>(
    cache: &T,
    catalog: &dyn Catalog,
) -> Result<(), iox_catalog::interface::Error>
where
    T: NamespaceCache,
{
    iox_catalog::interface::list_schemas(catalog)
        .await?
        .for_each(|(ns, schema)| {
            let name = NamespaceName::try_from(ns.name)
                .expect("cannot convert existing namespace string to a `NamespaceName` instance");

            cache.put_schema(name, schema);
        });

    Ok(())
}

#[cfg(test)]
mod tests {
    use data_types::ColumnType;
    use iox_catalog::{
        mem::MemCatalog,
        test_helpers::{arbitrary_namespace, arbitrary_table},
    };

    use super::*;

    #[tokio::test]
    async fn test_pre_warm_cache() {
        let catalog = Arc::new(MemCatalog::new(Default::default()));

        let mut repos = catalog.repositories().await;
        let namespace = arbitrary_namespace(&mut *repos, "test_ns").await;
        let table = arbitrary_table(&mut *repos, "name", &namespace).await;
        let _column = repos
            .columns()
            .create_or_get("name", table.id, ColumnType::U64)
            .await
            .unwrap();

        drop(repos); // Or it'll deadlock.

        let cache = Arc::new(MemoryNamespaceCache::default());
        pre_warm_schema_cache(&cache, &*catalog)
            .await
            .expect("pre-warming failed");

        let name = NamespaceName::new("test_ns").unwrap();
        let got = cache
            .get_schema(&name)
            .await
            .expect("should contain a schema");

        assert!(got.tables.get("name").is_some());
    }
}
