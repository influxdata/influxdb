use std::sync::Arc;

use assert_matches::assert_matches;
use data_types::{ColumnType, QueryPoolId, ShardIndex, TopicId};
use dml::DmlOperation;
use futures::{stream::FuturesUnordered, StreamExt};
use hashbrown::HashMap;
use hyper::{Body, Request, StatusCode};
use iox_time::{SystemProvider, TimeProvider};
use metric::{Attributes, DurationHistogram, Metric, U64Counter};
use router::dml_handlers::{DmlError, RetentionError, SchemaError};

use crate::common::{TestContext, TEST_QUERY_POOL_ID, TEST_RETENTION_PERIOD_NS, TEST_TOPIC_ID};

pub mod common;

#[tokio::test]
async fn test_write_ok() {
    let ctx = TestContext::new(true, None).await;

    // Write data inside retention period
    let now = SystemProvider::default()
        .now()
        .timestamp_nanos()
        .to_string();
    let lp = "platanos,tag1=A,tag2=B val=42i ".to_string() + &now;
    let response = ctx
        .write_lp("bananas", "test", lp)
        .await
        .expect("write failed");
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
    let ctx = TestContext::new(true, TEST_RETENTION_PERIOD_NS).await;

    // Write data outside retention period into a new table
    let two_hours_ago =
        (SystemProvider::default().now().timestamp_nanos() - 2 * 3_600 * 1_000_000_000).to_string();
    let lp = "apple,tag1=AAA,tag2=BBB val=422i ".to_string() + &two_hours_ago;

    let response = ctx
        .write_lp("bananas", "test", lp)
        .await
        .expect_err("write should fail");

    assert_matches!(
        &response,
        router::server::http::Error::DmlHandler(
            DmlError::Retention(
                RetentionError::OutsideRetention(e))
        ) => {
            assert_eq!(e, "apple");
        }
    );
    assert_eq!(response.as_status_code(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_schema_conflict() {
    let ctx = TestContext::new(true, None).await;

    // data inside the retention period
    let now = SystemProvider::default()
        .now()
        .timestamp_nanos()
        .to_string();
    let lp = "platanos,tag1=A,tag2=B val=42i ".to_string() + &now;

    let response = ctx
        .write_lp("bananas", "test", lp)
        .await
        .expect("write should succeed");

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let now = SystemProvider::default()
        .now()
        .timestamp_nanos()
        .to_string();
    let lp = "platanos,tag1=A,tag2=B val=42.0 ".to_string() + &now;

    let err = ctx
        .write_lp("bananas", "test", lp)
        .await
        .expect_err("write should fail");

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
    let ctx = TestContext::new(false, None).await;

    let now = SystemProvider::default()
        .now()
        .timestamp_nanos()
        .to_string();
    let lp = "platanos,tag1=A,tag2=B val=42i ".to_string() + &now;

    let err = ctx
        .write_lp("bananas", "test", lp)
        .await
        .expect_err("write should fail");

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
    let ctx = TestContext::new(true, None).await;

    let now = SystemProvider::default()
        .now()
        .timestamp_nanos()
        .to_string();
    let lp = "platanos,tag1=A,tag2=B val=42i ".to_string() + &now;

    // Drive the creation of the namespace
    let response = ctx
        .write_lp("bananas", "test", lp)
        .await
        .expect("write should succeed");
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

    let err = ctx
        .write_lp("bananas", "test", lp)
        .await
        .expect_err("write should fail");

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
    let ctx = TestContext::new(true, None).await;

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

    let response = ctx
        .write_lp("bananas", "test", lp)
        .await
        .expect("write should succeed");

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
    let ctx = TestContext::new(true, None).await;

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
        .http_delegate()
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
