use std::time::Duration;

use assert_matches::assert_matches;
use generated_types::influxdata::iox::namespace::v1::{
    namespace_service_server::NamespaceService, *,
};
use hyper::StatusCode;
use iox_catalog::interface::SoftDeletedRows;
use iox_time::{SystemProvider, TimeProvider};
use router::{
    dml_handlers::{DmlError, RetentionError},
    namespace_resolver::{self, NamespaceCreationError},
    server::http::Error,
};
use tonic::{Code, Request};

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
            .list(SoftDeletedRows::AllRows)
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
        .expect("failed to create namespace")
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
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
            .expect("query failure");
        assert_matches!(db_list.as_slice(), [ns] => {
            assert_eq!(ns.id.get(), got.id);
            assert_eq!(ns.name, got.name);
            assert_eq!(ns.retention_period_ns, got.retention_period_ns);
            assert!(ns.deleted_at.is_none());
        });
    }

    // And writing should succeed
    let response = ctx
        .write_lp("bananas", "test", lp)
        .await
        .expect("write failed");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

/// Ensure invoking the gRPC NamespaceService to delete a namespace propagates
/// the catalog and denies writes after the cache has converged / router
/// restarted.
#[tokio::test]
async fn test_namespace_delete() {
    // Initialise a TestContext requiring explicit namespace creation.
    let ctx = TestContext::new(true, None).await;

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
        .expect("failed to create namespace")
        .into_inner()
        .namespace
        .expect("no namespace in response");

    assert_eq!(got.name, "bananas_test");
    assert_eq!(got.id, 1);
    assert_eq!(got.retention_period_ns, Some(RETENTION));

    // The namespace is usable.
    let now = SystemProvider::default()
        .now()
        .timestamp_nanos()
        .to_string();
    let lp = "platanos,tag1=A,tag2=B val=42i ".to_string() + &now;
    let response = ctx
        .write_lp("bananas", "test", &lp)
        .await
        .expect("write failed");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // The RPC endpoint must return a namespace.
    {
        let current = ctx
            .grpc_delegate()
            .namespace_service()
            .get_namespaces(Request::new(Default::default()))
            .await
            .expect("must return namespaces")
            .into_inner();
        assert!(!current.namespaces.is_empty());
    }

    // Delete the namespace
    {
        let _resp = ctx
            .grpc_delegate()
            .namespace_service()
            .delete_namespace(Request::new(DeleteNamespaceRequest {
                name: "bananas_test".to_string(),
            }))
            .await
            .expect("must delete");
    }

    // The RPC endpoint must not return the namespace.
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

    // The catalog should contain the namespace, but "soft-deleted".
    {
        let db_list = ctx
            .catalog()
            .repositories()
            .await
            .namespaces()
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
            .expect("query failure");
        assert!(db_list.is_empty());

        let db_list = ctx
            .catalog()
            .repositories()
            .await
            .namespaces()
            .list(SoftDeletedRows::OnlyDeleted)
            .await
            .expect("query failure");
        assert_matches!(db_list.as_slice(), [ns] => {
            assert_eq!(ns.id.get(), got.id);
            assert_eq!(ns.name, got.name);
            assert_eq!(ns.retention_period_ns, got.retention_period_ns);
            assert!(ns.deleted_at.is_some());
        });
    }

    // The cached entry is not affected, and writes continue to be validated
    // against cached entry.
    //
    // https://github.com/influxdata/influxdb_iox/issues/6175

    let response = ctx
        .write_lp("bananas", "test", &lp)
        .await
        .expect("write failed");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // The router restarts, and writes are no longer accepted for the
    // soft-deleted bucket.
    let ctx = ctx.restart();

    let err = ctx
        .write_lp("bananas", "test", lp)
        .await
        .expect_err("write should fail");
    assert_matches!(
        err,
        router::server::http::Error::NamespaceResolver(router::namespace_resolver::Error::Lookup(
            iox_catalog::interface::Error::NamespaceNotFoundByName { .. }
        ))
    );
}

/// Ensure creating a namespace with a retention period of 0 maps to "infinite"
/// and not "none".
#[tokio::test]
async fn test_create_namespace_0_retention_period() {
    // Initialise a TestContext requiring explicit namespace creation.
    let ctx = TestContext::new(false, None).await;

    // Explicitly create the namespace.
    let req = CreateNamespaceRequest {
        name: "bananas_test".to_string(),
        retention_period_ns: Some(0), // A zero!
    };
    let got = ctx
        .grpc_delegate()
        .namespace_service()
        .create_namespace(Request::new(req))
        .await
        .expect("failed to create namespace")
        .into_inner()
        .namespace
        .expect("no namespace in response");

    assert_eq!(got.name, "bananas_test");
    assert_eq!(got.id, 1);
    assert_eq!(got.retention_period_ns, None);

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
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
            .expect("query failure");
        assert_matches!(db_list.as_slice(), [ns] => {
            assert_eq!(ns.id.get(), got.id);
            assert_eq!(ns.name, got.name);
            assert_eq!(ns.retention_period_ns, got.retention_period_ns);
            assert!(ns.deleted_at.is_none());
        });
    }

    // And writing should succeed
    let response = ctx
        .write_lp("bananas", "test", "platanos,tag1=A,tag2=B val=42i 42424242")
        .await
        .expect("write failed");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

/// Ensure creating a namespace with a negative retention period is rejected.
#[tokio::test]
async fn test_create_namespace_negative_retention_period() {
    // Initialise a TestContext requiring explicit namespace creation.
    let ctx = TestContext::new(false, None).await;

    // Explicitly create the namespace.
    let req = CreateNamespaceRequest {
        name: "bananas_test".to_string(),
        retention_period_ns: Some(-42),
    };
    let err = ctx
        .grpc_delegate()
        .namespace_service()
        .create_namespace(Request::new(req))
        .await
        .expect_err("negative retention period should fail to create namespace");

    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(err.message(), "invalid negative retention period");

    // The list namespace RPC should not show a new namespace
    {
        let list = ctx
            .grpc_delegate()
            .namespace_service()
            .get_namespaces(Request::new(Default::default()))
            .await
            .expect("must return namespaces")
            .into_inner();
        assert!(list.namespaces.is_empty());
    }

    // The catalog should not contain the namespace.
    {
        let db_list = ctx
            .catalog()
            .repositories()
            .await
            .namespaces()
            .list(SoftDeletedRows::AllRows)
            .await
            .expect("query failure");
        assert!(db_list.is_empty());
    }

    // And writing should not succeed
    let response = ctx
        .write_lp("bananas", "test", "platanos,tag1=A,tag2=B val=42i 42424242")
        .await
        .expect_err("write should fail");
    assert_matches!(
        response,
        Error::NamespaceResolver(namespace_resolver::Error::Create(
            NamespaceCreationError::Reject(_)
        ))
    );
}

/// Ensure updating a namespace with a retention period of 0 maps to "infinite"
/// and not "none".
#[tokio::test]
async fn test_update_namespace_0_retention_period() {
    // Initialise a TestContext requiring explicit namespace creation.
    let ctx = TestContext::new(false, None).await;

    // Explicitly create the namespace.
    let create = ctx
        .grpc_delegate()
        .namespace_service()
        .create_namespace(Request::new(CreateNamespaceRequest {
            name: "bananas_test".to_string(),
            retention_period_ns: Some(42),
        }))
        .await
        .expect("failed to create namespace")
        .into_inner()
        .namespace
        .expect("no namespace in response");

    // And writing in the past should fail
    ctx.write_lp("bananas", "test", "platanos,tag1=A,tag2=B val=42i 42424242")
        .await
        .expect_err("write outside retention period should fail");

    let got = ctx
        .grpc_delegate()
        .namespace_service()
        .update_namespace_retention(Request::new(UpdateNamespaceRetentionRequest {
            name: "bananas_test".to_string(),
            retention_period_ns: Some(0), // A zero!
        }))
        .await
        .expect("failed to create namespace")
        .into_inner()
        .namespace
        .expect("no namespace in response");

    assert_eq!(got.name, create.name);
    assert_eq!(got.id, create.id);
    assert_eq!(create.retention_period_ns, Some(42));
    assert_eq!(got.retention_period_ns, None);

    // The list namespace RPC should show the updated namespace
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
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
            .expect("query failure");
        assert_matches!(db_list.as_slice(), [ns] => {
            assert_eq!(ns.id.get(), got.id);
            assert_eq!(ns.name, got.name);
            assert_eq!(ns.retention_period_ns, got.retention_period_ns);
            assert!(ns.deleted_at.is_none());
        });
    }

    // The cached entry is not affected, and writes continue to be validated
    // against the old value.
    //
    // https://github.com/influxdata/influxdb_iox/issues/6175

    let err = ctx
        .write_lp("bananas", "test", "platanos,tag1=A,tag2=B val=42i 42424242")
        .await
        .expect_err("cached entry rejects write");

    assert_matches!(
        err,
        router::server::http::Error::DmlHandler(DmlError::Retention(
            RetentionError::OutsideRetention(name)
        )) => {
            assert_eq!(name, "platanos");
        }
    );

    // The router restarts, and writes are then accepted.
    let ctx = ctx.restart();

    let response = ctx
        .write_lp("bananas", "test", "platanos,tag1=A,tag2=B val=42i 42424242")
        .await
        .expect("cached entry should be removed");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

/// Ensure updating a namespace with a negative retention period fails.
#[tokio::test]
async fn test_update_namespace_negative_retention_period() {
    // Initialise a TestContext requiring explicit namespace creation.
    let ctx = TestContext::new(false, None).await;

    // Explicitly create the namespace.
    let create = ctx
        .grpc_delegate()
        .namespace_service()
        .create_namespace(Request::new(CreateNamespaceRequest {
            name: "bananas_test".to_string(),
            retention_period_ns: Some(42),
        }))
        .await
        .expect("failed to create namespace")
        .into_inner()
        .namespace
        .expect("no namespace in response");

    let err = ctx
        .grpc_delegate()
        .namespace_service()
        .update_namespace_retention(Request::new(UpdateNamespaceRetentionRequest {
            name: "bananas_test".to_string(),
            retention_period_ns: Some(-42),
        }))
        .await
        .expect_err("negative retention period should fail to create namespace");

    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(err.message(), "invalid negative retention period");

    // The list namespace RPC should show the original namespace
    {
        let list = ctx
            .grpc_delegate()
            .namespace_service()
            .get_namespaces(Request::new(Default::default()))
            .await
            .expect("must return namespaces")
            .into_inner();
        assert_matches!(list.namespaces.as_slice(), [ns] => {
            assert_eq!(*ns, create);
        });
    }

    // The catalog should contain the original namespace.
    {
        let db_list = ctx
            .catalog()
            .repositories()
            .await
            .namespaces()
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
            .expect("query failure");
        assert_matches!(db_list.as_slice(), [ns] => {
            assert_eq!(ns.id.get(), create.id);
            assert_eq!(ns.name, create.name);
            assert_eq!(ns.retention_period_ns, create.retention_period_ns);
        });
    }
}
