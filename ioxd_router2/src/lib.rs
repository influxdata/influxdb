use std::{
    collections::BTreeSet,
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use clap_blocks::write_buffer::WriteBufferConfig;
use data_types2::{PartitionTemplate, TemplatePart};
use hashbrown::HashMap;
use hyper::{Body, Request, Response};
use iox_catalog::interface::Catalog;
use metric::Registry;
use mutable_batch::MutableBatch;
use observability_deps::tracing::info;
use router2::{
    dml_handlers::{
        DmlHandler, DmlHandlerChainExt, FanOutAdaptor, InstrumentationDecorator,
        NamespaceAutocreation, Partitioner, SchemaValidator, ShardedWriteBuffer,
        WriteSummaryAdapter,
    },
    namespace_cache::{metrics::InstrumentedCache, MemoryNamespaceCache, ShardedCache},
    sequencer::Sequencer,
    server::{grpc::GrpcDelegate, http::HttpDelegate, RouterServer},
    sharder::JumpHash,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use trace::TraceCollector;
use write_summary::WriteSummary;

use ioxd_common::{
    add_service,
    http::error::{HttpApiError, HttpApiErrorSource},
    rpc::RpcBuilderInput,
    serve_builder,
    server_type::{CommonServerState, RpcError, ServerType},
    setup_builder,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to initialise write buffer connection: {0}")]
    WriteBuffer(#[from] write_buffer::core::WriteBufferError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] iox_catalog::interface::Error),

    #[error("Catalog DSN error: {0}")]
    CatalogDsn(#[from] clap_blocks::catalog_dsn::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct RouterServerType<D> {
    server: RouterServer<D>,
    shutdown: CancellationToken,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

impl<D> RouterServerType<D> {
    pub fn new(server: RouterServer<D>, common_state: &CommonServerState) -> Self {
        Self {
            server,
            shutdown: CancellationToken::new(),
            trace_collector: common_state.trace_collector(),
        }
    }
}

#[async_trait]
impl<D> ServerType for RouterServerType<D>
where
    D: DmlHandler<WriteInput = HashMap<String, MutableBatch>, WriteOutput = WriteSummary> + 'static,
{
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
    /// [`HttpDelegate`]: router2::server::http::HttpDelegate
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

    /// Registers the services exposed by the router [`GrpcDelegate`] delegate.
    ///
    /// [`GrpcDelegate`]: router2::server::grpc::GrpcDelegate
    async fn server_grpc(self: Arc<Self>, builder_input: RpcBuilderInput) -> Result<(), RpcError> {
        let builder = setup_builder!(builder_input, self);
        add_service!(builder, self.server.grpc().write_service());
        add_service!(builder, self.server.grpc().schema_service());
        serve_builder!(builder);

        Ok(())
    }

    async fn join(self: Arc<Self>) {
        self.shutdown.cancelled().await;
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

/// This adaptor converts the `router2` http error type into a type that
/// satisfies the requirements of ioxd's runner framework, keeping the
/// two decoupled.
#[derive(Debug)]
pub struct IoxHttpErrorAdaptor(router2::server::http::Error);

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

/// Instantiate a router2 server
pub async fn create_router2_server_type(
    common_state: &CommonServerState,
    metrics: Arc<metric::Registry>,
    catalog: Arc<dyn Catalog>,
    write_buffer_config: &WriteBufferConfig,
    query_pool_name: &str,
) -> Result<Arc<dyn ServerType>> {
    // Initialise the sharded write buffer and instrument it with DML handler
    // metrics.
    let write_buffer = init_write_buffer(
        write_buffer_config,
        Arc::clone(&metrics),
        common_state.trace_collector(),
    )
    .await?;
    let write_buffer =
        InstrumentationDecorator::new("sharded_write_buffer", &*metrics, write_buffer);

    // Initialise an instrumented namespace cache to be shared with the schema
    // validator, and namespace auto-creator that reports cache hit/miss/update
    // metrics.
    let ns_cache = Arc::new(InstrumentedCache::new(
        Arc::new(ShardedCache::new(
            std::iter::repeat_with(|| Arc::new(MemoryNamespaceCache::default())).take(10),
        )),
        &*metrics,
    ));

    // Initialise and instrument the schema validator
    let schema_validator =
        SchemaValidator::new(Arc::clone(&catalog), Arc::clone(&ns_cache), &*metrics);
    let schema_validator =
        InstrumentationDecorator::new("schema_validator", &*metrics, schema_validator);

    // Add a write partitioner into the handler stack that splits by the date
    // portion of the write's timestamp.
    let partitioner = Partitioner::new(PartitionTemplate {
        parts: vec![TemplatePart::TimeFormat("%Y-%m-%d".to_owned())],
    });
    let partitioner = InstrumentationDecorator::new("partitioner", &*metrics, partitioner);

    ////////////////////////////////////////////////////////////////////////////
    //
    // THIS CODE IS FOR TESTING ONLY.
    //
    // The source of truth for the kafka topics & query pools will be read from
    // the DB, rather than CLI args for a prod deployment.
    //
    ////////////////////////////////////////////////////////////////////////////
    //
    // Look up the kafka topic ID needed to populate namespace creation
    // requests.
    //
    // This code / auto-creation is for architecture testing purposes only - a
    // prod deployment would expect namespaces to be explicitly created and this
    // layer would be removed.
    let schema_catalog = Arc::clone(&catalog);
    let mut txn = catalog.start_transaction().await?;
    let topic_id = txn
        .kafka_topics()
        .get_by_name(write_buffer_config.topic())
        .await?
        .map(|v| v.id)
        .unwrap_or_else(|| {
            panic!(
                "no kafka topic named {} in catalog",
                write_buffer_config.topic()
            )
        });
    let query_id = txn
        .query_pools()
        .create_or_get(query_pool_name)
        .await
        .map(|v| v.id)
        .unwrap_or_else(|e| {
            panic!(
                "failed to upsert query pool {} in catalog: {}",
                write_buffer_config.topic(),
                e
            )
        });
    txn.commit().await?;

    let ns_creator = NamespaceAutocreation::new(
        catalog,
        ns_cache,
        topic_id,
        query_id,
        iox_catalog::INFINITE_RETENTION_POLICY.to_owned(),
    );
    //
    ////////////////////////////////////////////////////////////////////////////

    let parallel_write = WriteSummaryAdapter::new(FanOutAdaptor::new(write_buffer));

    // Build the chain of DML handlers that forms the request processing
    // pipeline, starting with the namespace creator (for testing purposes) and
    // write partitioner that yields a set of partitioned batches.
    let handler_stack = ns_creator
        .and_then(schema_validator)
        .and_then(partitioner)
        // Once writes have been partitioned, they are processed in parallel.
        //
        // This block initialises a fan-out adaptor that parallelises partitioned
        // writes into the handler chain it decorates (schema validation, and then
        // into the sharded write buffer), and instruments the parallelised
        // operation.
        .and_then(InstrumentationDecorator::new(
            "parallel_write",
            &*metrics,
            parallel_write,
        ));

    // Record the overall request handling latency
    let handler_stack = InstrumentationDecorator::new("request", &*metrics, handler_stack);

    // Initialise the API delegates, sharing the handler stack between them.
    let handler_stack = Arc::new(handler_stack);
    let http = HttpDelegate::new(
        common_state.run_config().max_http_request_size,
        Arc::clone(&handler_stack),
        &metrics,
    );
    let grpc = GrpcDelegate::new(handler_stack, schema_catalog, Arc::clone(&metrics));

    let router_server = RouterServer::new(http, grpc, metrics, common_state.trace_collector());
    let server_type = Arc::new(RouterServerType::new(router_server, common_state));
    Ok(server_type)
}

/// Initialise the [`ShardedWriteBuffer`] with one shard per Kafka partition,
/// using [`JumpHash`] to shard operations by their destination namespace &
/// table name.
async fn init_write_buffer(
    write_buffer_config: &WriteBufferConfig,
    metrics: Arc<metric::Registry>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
) -> Result<ShardedWriteBuffer<JumpHash<Arc<Sequencer>>>> {
    let write_buffer = Arc::new(
        write_buffer_config
            .writing(Arc::clone(&metrics), trace_collector)
            .await?,
    );

    // Construct the (ordered) set of sequencers.
    //
    // The sort order must be deterministic in order for all nodes to shard to
    // the same sequencers, therefore we type assert the returned set is of the
    // ordered variety.
    let shards: BTreeSet<_> = write_buffer.sequencer_ids();
    //          ^ don't change this to an unordered set

    info!(
        topic = write_buffer_config.topic(),
        shards = shards.len(),
        "connected to write buffer topic",
    );

    Ok(ShardedWriteBuffer::new(
        shards
            .into_iter()
            .map(|id| Sequencer::new(id as _, Arc::clone(&write_buffer), &metrics))
            .map(Arc::new)
            .collect::<JumpHash<_>>(),
    ))
}
