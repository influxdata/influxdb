use std::{collections::BTreeSet, iter, string::String, sync::Arc};

use assert_matches::assert_matches;
use data_types::database_rules::{PartitionTemplate, TemplatePart};
use dml::DmlOperation;
use hashbrown::HashMap;
use hyper::{Body, Request, StatusCode};
use iox_catalog::{
    interface::{Catalog, KafkaTopicId, QueryPoolId},
    mem::MemCatalog,
};
use metric::{Attributes, Metric, Registry, U64Counter, U64Histogram};
use mutable_batch::MutableBatch;
use router2::{
    dml_handlers::{
        Chain, DmlHandlerChainExt, FanOutAdaptor, InstrumentationDecorator, NamespaceAutocreation,
        Partitioned, Partitioner, SchemaValidator, ShardedWriteBuffer,
    },
    namespace_cache::{MemoryNamespaceCache, ShardedCache},
    sequencer::Sequencer,
    server::http::HttpDelegate,
    sharder::JumpHash,
};
use write_buffer::core::WriteBufferWriting;
use write_buffer::mock::{MockBufferForWriting, MockBufferSharedState};

/// The kafka topic catalog ID assigned by the namespace auto-creator in the
/// handler stack for namespaces it has not yet observed.
const TEST_KAFKA_TOPIC_ID: i32 = 1;

/// The query pool catalog ID assigned by the namespace auto-creator in the
/// handler stack for namespaces it has not yet observed.
const TEST_QUERY_POOL_ID: i16 = 1;

pub struct TestContext {
    delegate: HttpDelegateStack,
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
                    NamespaceAutocreation<
                        Arc<ShardedCache<Arc<MemoryNamespaceCache>>>,
                        HashMap<String, MutableBatch>,
                    >,
                    SchemaValidator<Arc<ShardedCache<Arc<MemoryNamespaceCache>>>>,
                >,
                Partitioner,
            >,
            FanOutAdaptor<
                ShardedWriteBuffer<JumpHash<Arc<Sequencer>>>,
                Vec<Partitioned<HashMap<String, MutableBatch>>>,
            >,
        >,
    >,
>;

/// A [`router2`] stack configured with the various DML handlers using mock
/// catalog / write buffer backends.
impl TestContext {
    pub fn new() -> Self {
        let metrics = Arc::new(metric::Registry::default());
        let time = time::MockProvider::new(time::Time::from_timestamp_millis(668563200000));

        let write_buffer = MockBufferForWriting::new(
            MockBufferSharedState::empty_with_n_sequencers(1.try_into().unwrap()),
            None,
            Arc::new(time),
        )
        .expect("failed to init mock write buffer");
        let write_buffer_state = write_buffer.state();
        let write_buffer: Arc<dyn WriteBufferWriting> = Arc::new(write_buffer);

        let shards: BTreeSet<_> = write_buffer.sequencer_ids();
        let sharded_write_buffer = ShardedWriteBuffer::new(
            shards
                .into_iter()
                .map(|id| Sequencer::new(id as _, Arc::clone(&write_buffer)))
                .map(Arc::new)
                .collect::<JumpHash<_>>(),
        );

        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));
        let ns_cache = Arc::new(ShardedCache::new(
            iter::repeat_with(|| Arc::new(MemoryNamespaceCache::default())).take(10),
        ));

        let ns_creator = NamespaceAutocreation::new(
            Arc::clone(&catalog),
            Arc::clone(&ns_cache),
            KafkaTopicId::new(TEST_KAFKA_TOPIC_ID),
            QueryPoolId::new(TEST_QUERY_POOL_ID),
            iox_catalog::INFINITE_RETENTION_POLICY.to_owned(),
        );

        let schema_validator = SchemaValidator::new(Arc::clone(&catalog), ns_cache);
        let partitioner = Partitioner::new(PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat("%Y-%m-%d".to_owned())],
        });

        let handler_stack = ns_creator
            .and_then(schema_validator)
            .and_then(partitioner)
            .and_then(FanOutAdaptor::new(sharded_write_buffer));

        let handler_stack =
            InstrumentationDecorator::new("request", Arc::clone(&metrics), handler_stack);

        let delegate = HttpDelegate::new(1024, handler_stack, &metrics);

        Self {
            delegate,
            catalog,
            write_buffer_state,
            metrics,
        }
    }

    /// Get a reference to the test context's delegate.
    pub fn delegate(&self) -> &HttpDelegateStack {
        &self.delegate
    }

    /// Get a reference to the test context's catalog.
    pub fn catalog(&self) -> &dyn Catalog {
        self.catalog.as_ref()
    }

    /// Get a reference to the test context's write buffer state.
    pub fn write_buffer_state(&self) -> &Arc<MockBufferSharedState> {
        &self.write_buffer_state
    }

    /// Get a reference to the test context's metrics.
    pub fn metrics(&self) -> &Registry {
        self.metrics.as_ref()
    }
}

impl Default for TestContext {
    fn default() -> Self {
        Self::new()
    }
}

#[tokio::test]
async fn test_write_ok() {
    let ctx = TestContext::new();

    let request = Request::builder()
        .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
        .method("POST")
        .body(Body::from("platanos,tag1=A,tag2=B val=42i 123456"))
        .expect("failed to construct HTTP request");

    let response = ctx
        .delegate()
        .route(request)
        .await
        .expect("LP write request failed");

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Check the write buffer observed the correct write.
    let writes = ctx.write_buffer_state().get_messages(0);
    assert_eq!(writes.len(), 1);
    assert_matches!(writes.as_slice(), [Ok(DmlOperation::Write(w))] => {
        assert_eq!(w.namespace(), "bananas_test");
        assert!(w.table("platanos").is_some());
    });

    // Ensure the catalog saw the namespace creation
    let ns = ctx
        .catalog()
        .repositories()
        .await
        .namespaces()
        .get_by_name("bananas_test")
        .await
        .expect("query should succeed")
        .expect("namespace not found");
    assert_eq!(ns.name, "bananas_test");
    assert_eq!(
        ns.retention_duration.as_deref(),
        Some(iox_catalog::INFINITE_RETENTION_POLICY)
    );
    assert_eq!(ns.kafka_topic_id, KafkaTopicId::new(TEST_KAFKA_TOPIC_ID));
    assert_eq!(ns.query_pool_id, QueryPoolId::new(TEST_QUERY_POOL_ID));

    // Ensure the metric instrumentation was hit
    let histogram = ctx
        .metrics()
        .get_instrument::<Metric<U64Histogram>>("dml_handler_write_duration_ms")
        .expect("failed to read metric")
        .get_observer(&Attributes::from(&[
            ("handler", "request"),
            ("result", "success"),
        ]))
        .expect("failed to get observer")
        .fetch();
    let hit_count = histogram.buckets.iter().fold(0, |acc, v| acc + v.count);
    assert_eq!(hit_count, 1);

    assert_eq!(
        ctx.metrics()
            .get_instrument::<Metric<U64Counter>>("http_write_lines_total")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[]))
            .expect("failed to get observer")
            .fetch(),
        1
    );
}
