use std::{collections::BTreeSet, iter, string::String, sync::Arc};

use assert_matches::assert_matches;
use data_types::{
    ColumnType, PartitionTemplate, QueryPoolId, ShardIndex, TableId, TemplatePart, TopicId,
};
use dml::DmlOperation;
use futures::{stream::FuturesUnordered, StreamExt};
use hashbrown::HashMap;
use hyper::{Body, Request, StatusCode};
use iox_catalog::{interface::Catalog, mem::MemCatalog};
use iox_time::{SystemProvider, TimeProvider};
use metric::{Attributes, DurationHistogram, Metric, Registry, U64Counter};
use mutable_batch::MutableBatch;
use router::{
    dml_handlers::{
        Chain, DmlError, DmlHandlerChainExt, FanOutAdaptor, InstrumentationDecorator, Partitioned,
        Partitioner, RetentionError, RetentionValidator, SchemaError, SchemaValidator,
        ShardedWriteBuffer, WriteSummaryAdapter,
    },
    namespace_cache::{MemoryNamespaceCache, ShardedCache},
    namespace_resolver::{MissingNamespaceAction, NamespaceAutocreation, NamespaceSchemaResolver},
    server::http::HttpDelegate,
    shard::Shard,
};
use sharder::JumpHash;
use write_buffer::{
    core::WriteBufferWriting,
    mock::{MockBufferForWriting, MockBufferSharedState},
};

/// The topic catalog ID assigned by the namespace auto-creator in the
/// handler stack for namespaces it has not yet observed.
const TEST_TOPIC_ID: i64 = 1;

/// The query pool catalog ID assigned by the namespace auto-creator in the
/// handler stack for namespaces it has not yet observed.
const TEST_QUERY_POOL_ID: i64 = 1;

/// Common retention period value we'll use in tests
const TEST_RETENTION_PERIOD_NS: Option<i64> = Some(3_600 * 1_000_000_000);

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
    pub fn new(autocreate_ns: bool, ns_autocreate_retention_period_ns: Option<i64>) -> Self {
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

        let delegate = HttpDelegate::new(1024, 100, namespace_resolver, handler_stack, &metrics);

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
}

impl Default for TestContext {
    fn default() -> Self {
        Self::new(true, None)
    }
}

#[tokio::test]
async fn test_write_ok() {
    let ctx = TestContext::new(true, None);

    // Write data inside retention period
    let now = SystemProvider::default()
        .now()
        .timestamp_nanos()
        .to_string();
    let lp = "platanos,tag1=A,tag2=B val=42i ".to_string() + &now;

    let request = Request::builder()
        .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
        .method("POST")
        .body(Body::from(lp))
        .expect("failed to construct HTTP request");

    let response = ctx
        .delegate()
        .route(request)
        .await
        .expect("LP write request failed");

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Check the write buffer observed the correct write.
    let writes = ctx.write_buffer_state().get_messages(ShardIndex::new(0));
    assert_eq!(writes.len(), 1);
    assert_matches!(writes.as_slice(), [Ok(DmlOperation::Write(w))] => {
        let table_id = ctx.table_id("bananas_test", "platanos").await;
        assert!(w.table(&table_id).is_some());
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
    assert_eq!(ns.topic_id, TopicId::new(TEST_TOPIC_ID));
    assert_eq!(ns.query_pool_id, QueryPoolId::new(TEST_QUERY_POOL_ID));
    assert_eq!(ns.retention_period_ns, None);

    // Ensure the metric instrumentation was hit
    let histogram = ctx
        .metrics()
        .get_instrument::<Metric<DurationHistogram>>("dml_handler_write_duration")
        .expect("failed to read metric")
        .get_observer(&Attributes::from(&[
            ("handler", "request"),
            ("result", "success"),
        ]))
        .expect("failed to get observer")
        .fetch();
    let hit_count = histogram.sample_count();
    assert_eq!(hit_count, 1);

    assert_eq!(
        ctx.metrics()
            .get_instrument::<Metric<U64Counter>>("http_write_lines")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[]))
            .expect("failed to get observer")
            .fetch(),
        1
    );

    let histogram = ctx
        .metrics()
        .get_instrument::<Metric<DurationHistogram>>("shard_enqueue_duration")
        .expect("failed to read metric")
        .get_observer(&Attributes::from(&[
            ("kafka_partition", "0"),
            ("result", "success"),
        ]))
        .expect("failed to get observer")
        .fetch();
    let hit_count = histogram.sample_count();
    assert_eq!(hit_count, 1);
}

#[tokio::test]
async fn test_write_outside_retention_period() {
    let ctx = TestContext::new(true, TEST_RETENTION_PERIOD_NS);

    // Write data outside retention period into a new table
    let two_hours_ago =
        (SystemProvider::default().now().timestamp_nanos() - 2 * 3_600 * 1_000_000_000).to_string();
    let lp = "apple,tag1=AAA,tag2=BBB val=422i ".to_string() + &two_hours_ago;

    let request = Request::builder()
        .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
        .method("POST")
        .body(Body::from(lp))
        .expect("failed to construct HTTP request");

    let err = ctx
        .delegate()
        .route(request)
        .await
        .expect_err("LP write request should fail");

    assert_matches!(
        &err,
        router::server::http::Error::DmlHandler(
            DmlError::Retention(
                RetentionError::OutsideRetention(e))
        ) => {
            assert_eq!(e, "apple");
        }
    );
    assert_eq!(err.as_status_code(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_schema_conflict() {
    let ctx = TestContext::new(true, None);

    // data inside the retention period
    let now = SystemProvider::default()
        .now()
        .timestamp_nanos()
        .to_string();
    let lp = "platanos,tag1=A,tag2=B val=42i ".to_string() + &now;

    let request = Request::builder()
        .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
        .method("POST")
        .body(Body::from(lp))
        .expect("failed to construct HTTP request");

    let response = ctx
        .delegate()
        .route(request)
        .await
        .expect("LP write request failed");

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let now = SystemProvider::default()
        .now()
        .timestamp_nanos()
        .to_string();
    let lp = "platanos,tag1=A,tag2=B val=42.0 ".to_string() + &now;

    let request = Request::builder()
        .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
        .method("POST")
        .body(Body::from(lp))
        .expect("failed to construct HTTP request");

    let err = ctx
        .delegate()
        .route(request)
        .await
        .expect_err("LP write request should fail");

    assert_matches!(
        &err,
        router::server::http::Error::DmlHandler(
            DmlError::Schema(
                SchemaError::Conflict(
                    e
                )
            )
        ) => {
            assert_matches!(e.err(), iox_catalog::interface::Error::ColumnTypeMismatch {
                name,
                existing,
                new,
            } => {
                assert_eq!(name, "val");
                assert_eq!(*existing, ColumnType::I64);
                assert_eq!(*new, ColumnType::F64);
            });
        }
    );
    assert_eq!(err.as_status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_rejected_ns() {
    let ctx = TestContext::new(false, None);

    let now = SystemProvider::default()
        .now()
        .timestamp_nanos()
        .to_string();
    let lp = "platanos,tag1=A,tag2=B val=42i ".to_string() + &now;

    let request = Request::builder()
        .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
        .method("POST")
        .body(Body::from(lp))
        .expect("failed to construct HTTP request");

    let err = ctx
        .delegate()
        .route(request)
        .await
        .expect_err("should error");
    assert_matches!(
        err,
        router::server::http::Error::NamespaceResolver(
            // can't check the type of the create error without making ns_autocreation public, but
            // not worth it just for this test, as the correct error is asserted in unit tests in
            // that module. here it's just important that the write fails.
            router::namespace_resolver::Error::Create(_)
        )
    );
    assert_eq!(err.as_status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_schema_limit() {
    let ctx = TestContext::new(true, None);

    let now = SystemProvider::default()
        .now()
        .timestamp_nanos()
        .to_string();
    let lp = "platanos,tag1=A,tag2=B val=42i ".to_string() + &now;

    // Drive the creation of the namespace
    let request = Request::builder()
        .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
        .method("POST")
        .body(Body::from(lp))
        .expect("failed to construct HTTP request");
    let response = ctx
        .delegate()
        .route(request)
        .await
        .expect("LP write request failed");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Update the table limit
    ctx.catalog()
        .repositories()
        .await
        .namespaces()
        .update_table_limit("bananas_test", 1)
        .await
        .expect("failed to update table limit");

    // Attempt to create another table
    let now = SystemProvider::default()
        .now()
        .timestamp_nanos()
        .to_string();
    let lp = "platanos2,tag1=A,tag2=B val=42i ".to_string() + &now;

    let request = Request::builder()
        .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
        .method("POST")
        .body(Body::from(lp))
        .expect("failed to construct HTTP request");
    let err = ctx
        .delegate()
        .route(request)
        .await
        .expect_err("LP write request should fail");

    assert_matches!(
        &err,
        router::server::http::Error::DmlHandler(
            DmlError::Schema(
                SchemaError::ServiceLimit(e)
            )
        ) => {
            assert_eq!(
                e.to_string(),
                "couldn't create table platanos2; limit reached on namespace 1"
            );
        }
    );
    assert_eq!(err.as_status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_write_propagate_ids() {
    let ctx = TestContext::new(true, None);

    // Create the namespace and a set of tables.
    let ns = ctx
        .catalog()
        .repositories()
        .await
        .namespaces()
        .create(
            "bananas_test",
            None,
            TopicId::new(TEST_TOPIC_ID),
            QueryPoolId::new(TEST_QUERY_POOL_ID),
        )
        .await
        .expect("failed to update table limit");

    let catalog = ctx.catalog();
    let ids = ["another", "test", "table", "platanos"]
        .iter()
        .map(|t| {
            let catalog = Arc::clone(&catalog);
            async move {
                let table = catalog
                    .repositories()
                    .await
                    .tables()
                    .create_or_get(t, ns.id)
                    .await
                    .unwrap();
                (*t, table.id)
            }
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<HashMap<_, _>>()
        .await;

    // data inside the retention period
    let now = SystemProvider::default()
        .now()
        .timestamp_nanos()
        .to_string();
    let lp = format! {
        "
            platanos,tag1=A,tag2=B val=42i {}\n\
            another,tag1=A,tag2=B val=42i {}\n\
            test,tag1=A,tag2=B val=42i {}\n\
            platanos,tag1=A,tag2=B val=42i {}\n\
            table,tag1=A,tag2=B val=42i {}\n\
        ", now, now, now, now, now

    };

    let request = Request::builder()
        .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
        .method("POST")
        .body(Body::from(lp))
        .expect("failed to construct HTTP request");

    let response = ctx
        .delegate()
        .route(request)
        .await
        .expect("LP write request failed");

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Check the write buffer observed the correct write.
    let writes = ctx.write_buffer_state().get_messages(ShardIndex::new(0));
    assert_eq!(writes.len(), 1);
    assert_matches!(writes.as_slice(), [Ok(DmlOperation::Write(w))] => {
        assert_eq!(w.namespace_id(), ns.id);

        for id in ids.values() {
            assert!(w.table(id).is_some());
        }
    });
}

#[tokio::test]
async fn test_delete_propagate_ids() {
    let ctx = TestContext::new(true, None);

    // Create the namespace and a set of tables.
    let ns = ctx
        .catalog()
        .repositories()
        .await
        .namespaces()
        .create(
            "bananas_test",
            None,
            TopicId::new(TEST_TOPIC_ID),
            QueryPoolId::new(TEST_QUERY_POOL_ID),
        )
        .await
        .expect("failed to update table limit");

    let request = Request::builder()
        .uri("https://bananas.example/api/v2/delete?org=bananas&bucket=test")
        .method("POST")
        .body(Body::from(
            r#"{
                "predicate": "_measurement=bananas",
                "start": "1970-01-01T00:00:00Z",
                "stop": "2070-01-02T00:00:00Z"
            }"#,
        ))
        .expect("failed to construct HTTP request");

    let response = ctx
        .delegate()
        .route(request)
        .await
        .expect("delete request failed");

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Check the write buffer observed the correct write.
    let writes = ctx.write_buffer_state().get_messages(ShardIndex::new(0));
    assert_eq!(writes.len(), 1);
    assert_matches!(writes.as_slice(), [Ok(DmlOperation::Delete(w))] => {
        assert_eq!(w.namespace_id(), ns.id);
    });
}
