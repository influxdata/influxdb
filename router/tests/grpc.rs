use std::time::Duration;

use assert_matches::assert_matches;
use generated_types::influxdata::iox::namespace::v1::{
    namespace_service_server::NamespaceService, *,
};
use hyper::StatusCode;
use iox_time::{SystemProvider, TimeProvider};
use router::{
    namespace_resolver::{self, NamespaceCreationError},
    server::http::Error,
};
use tonic::Request;

use crate::common::TestContext;

pub mod common;

/// Ensure invoking the gRPC NamespaceService to create a namespace populates
/// the catalog.
#[tokio::test]
async fn test_namespace_create() {
    // Initialise a TestContext requiring explicit namespace creation.
    let ctx = TestContext::new(false, None).await;

    // Try writing to the non-existant namespace, which should return an error.
    let now = SystemProvider::default()
        .now()
        .timestamp_nanos()
        .to_string();
    let lp = "platanos,tag1=A,tag2=B val=42i ".to_string() + &now;

    let response = ctx
        .write_lp("bananas", "test", &lp)
        .await
        .expect_err("write failed");

    assert_matches!(
        response,
        Error::NamespaceResolver(namespace_resolver::Error::Create(
            NamespaceCreationError::Reject(_)
        ))
    );
    assert_eq!(
        response.to_string(),
        "rejecting write due to non-existing namespace: bananas_test"
    );

    // The failed write MUST NOT populate the catalog.
    {
        let current = ctx
            .catalog()
            .repositories()
            .await
            .namespaces()
            .list()
            .await
            .expect("failed to query for existing namespaces");
        assert!(current.is_empty());
    }

    // The RPC endpoint must know nothing about the namespace either.
    {
        let current = ctx
            .grpc_delegate()
            .namespace_service()
            .get_namespaces(Request::new(Default::default()))
            .await
            .expect("must return namespaces")
            .into_inner();
        assert!(current.namespaces.is_empty());
    }

    const RETENTION: i64 = Duration::from_secs(42 * 60 * 60).as_nanos() as _;

    // Explicitly create the namespace.
    let req = CreateNamespaceRequest {
        name: "bananas_test".to_string(),
        retention_period_ns: Some(RETENTION),
    };
    let got = ctx
        .grpc_delegate()
        .namespace_service()
        .create_namespace(Request::new(req))
        .await
        .expect("failed to create namesapce")
        .into_inner()
        .namespace
        .expect("no namespace in response");

    assert_eq!(got.name, "bananas_test");
    assert_eq!(got.id, 1);
    assert_eq!(got.retention_period_ns, Some(RETENTION));

    // The list namespace RPC should show the new namespace
    {
        let list = ctx
            .grpc_delegate()
            .namespace_service()
            .get_namespaces(Request::new(Default::default()))
            .await
            .expect("must return namespaces")
            .into_inner();
        assert_matches!(list.namespaces.as_slice(), [ns] => {
            assert_eq!(*ns, got);
        });
    }

    // The catalog should contain the namespace.
    {
        let db_list = ctx
            .catalog()
            .repositories()
            .await
            .namespaces()
            .list()
            .await
            .expect("query failure");
        assert_matches!(db_list.as_slice(), [ns] => {
            assert_eq!(ns.id.get(), got.id);
            assert_eq!(ns.name, got.name);
            assert_eq!(ns.retention_period_ns, got.retention_period_ns);
        });
    }

    // And writing should succeed
    let response = ctx
        .write_lp("bananas", "test", lp)
        .await
        .expect("write failed");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}
