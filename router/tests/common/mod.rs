use std::{collections::BTreeSet, iter, string::String, sync::Arc};

use data_types::{PartitionTemplate, QueryPoolId, TableId, TemplatePart, TopicId};
use hashbrown::HashMap;
use hyper::{Body, Request, Response};
use iox_catalog::{interface::Catalog, mem::MemCatalog};
use metric::Registry;
use mutable_batch::MutableBatch;
use object_store::memory::InMemory;
use router::{
    dml_handlers::{
        Chain, DmlHandlerChainExt, FanOutAdaptor, InstrumentationDecorator, Partitioned,
        Partitioner, RetentionValidator, SchemaValidator, ShardedWriteBuffer, WriteSummaryAdapter,
    },
    namespace_cache::{MemoryNamespaceCache, ShardedCache},
    namespace_resolver::{MissingNamespaceAction, NamespaceAutocreation, NamespaceSchemaResolver},
    server::{grpc::RpcWriteGrpcDelegate, http::HttpDelegate},
    shard::Shard,
};
use sharder::JumpHash;
use write_buffer::{
    core::WriteBufferWriting,
    mock::{MockBufferForWriting, MockBufferSharedState},
};

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
    http_delegate: HttpDelegateStack,
    grpc_delegate: RpcWriteGrpcDelegate,
    catalog: Arc<dyn Catalog>,
    write_buffer_state: Arc<MockBufferSharedState>,
    metrics: Arc<Registry>,
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
                    ShardedWriteBuffer<JumpHash<Arc<Shard>>>,
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

/// A [`router`] stack configured with the various DML handlers using mock
/// catalog / write buffer backends.
impl TestContext {
    pub async fn new(autocreate_ns: bool, ns_autocreate_retention_period_ns: Option<i64>) -> Self {
        let metrics = Arc::new(metric::Registry::default());
        let time = iox_time::MockProvider::new(
            iox_time::Time::from_timestamp_millis(668563200000).unwrap(),
        );

        let write_buffer = MockBufferForWriting::new(
            MockBufferSharedState::empty_with_n_shards(1.try_into().unwrap()),
            None,
            Arc::new(time),
        )
        .expect("failed to init mock write buffer");
        let write_buffer_state = write_buffer.state();
        let write_buffer: Arc<dyn WriteBufferWriting> = Arc::new(write_buffer);

        let shards: BTreeSet<_> = write_buffer.shard_indexes();
        let sharded_write_buffer = ShardedWriteBuffer::new(JumpHash::new(
            shards
                .into_iter()
                .map(|shard_index| Shard::new(shard_index, Arc::clone(&write_buffer), &metrics))
                .map(Arc::new),
        ));

        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let ns_cache = Arc::new(ShardedCache::new(
            iter::repeat_with(|| Arc::new(MemoryNamespaceCache::default())).take(10),
        ));

        let retention_validator =
            RetentionValidator::new(Arc::clone(&catalog), Arc::clone(&ns_cache));
        let schema_validator =
            SchemaValidator::new(Arc::clone(&catalog), Arc::clone(&ns_cache), &metrics);
        let partitioner = Partitioner::new(PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat("%Y-%m-%d".to_owned())],
        });

        let handler_stack = retention_validator
            .and_then(schema_validator)
            .and_then(partitioner)
            .and_then(WriteSummaryAdapter::new(FanOutAdaptor::new(
                sharded_write_buffer,
            )));

        let handler_stack = InstrumentationDecorator::new("request", &metrics, handler_stack);

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

        let http_delegate =
            HttpDelegate::new(1024, 100, namespace_resolver, handler_stack, &metrics);

        let grpc_delegate = RpcWriteGrpcDelegate::new(
            Arc::clone(&catalog),
            Arc::new(InMemory::default()),
            TopicId::new(TEST_TOPIC_ID),
            QueryPoolId::new(TEST_QUERY_POOL_ID),
        );

        Self {
            http_delegate,
            grpc_delegate,
            catalog,
            write_buffer_state,
            metrics,
        }
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

    /// Get a reference to the test context's write buffer state.
    pub fn write_buffer_state(&self) -> &Arc<MockBufferSharedState> {
        &self.write_buffer_state
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
            .get_by_name(namespace)
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
