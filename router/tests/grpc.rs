use std::time::Duration;

use assert_matches::assert_matches;
use data_types::NamespaceId;
use generated_types::influxdata::{
    iox::{
        ingester::v1::WriteRequest,
        namespace::v1::{namespace_service_server::NamespaceService, *},
        partition_template::v1::*,
        table::v1::{table_service_server::TableService, *},
    },
    pbdata::v1::DatabaseBatch,
};
use hyper::StatusCode;
use iox_catalog::interface::{Error as CatalogError, SoftDeletedRows};
use iox_time::{SystemProvider, TimeProvider};
use router::{
    dml_handlers::{CachedServiceProtectionLimit, DmlError, RetentionError, SchemaError},
    namespace_resolver::{self, NamespaceCreationError},
    server::http::Error,
};
use test_helpers::assert_error;
use tonic::{Code, Request};

use crate::common::TestContextBuilder;

pub mod common;

/// Ensure invoking the gRPC NamespaceService to create a namespace populates
/// the catalog.
#[tokio::test]
async fn test_namespace_create() {
    // Initialise a TestContext without a namespace autocreation policy.
    let ctx = TestContextBuilder::default().build().await;

    // Try writing to the non-existent namespace, which should return an error.
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
        partition_template: None,
        service_protection_limits: None,
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
    // Initialise a TestContext with implicit namespace creation.
    let ctx = TestContextBuilder::default()
        .with_autocreate_namespace(None)
        .build()
        .await;

    const RETENTION: i64 = Duration::from_secs(42 * 60 * 60).as_nanos() as _;

    // Explicitly create the namespace.
    let req = CreateNamespaceRequest {
        name: "bananas_test".to_string(),
        retention_period_ns: Some(RETENTION),
        partition_template: None,
        service_protection_limits: None,
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
    let ctx = ctx.restart().await;

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
    // Initialise a test context without implicit namespace creation policy.
    let ctx = TestContextBuilder::default().build().await;

    // Explicitly create the namespace.
    let req = CreateNamespaceRequest {
        name: "bananas_test".to_string(),
        retention_period_ns: Some(0), // A zero!
        partition_template: None,
        service_protection_limits: None,
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
    let ctx = TestContextBuilder::default().build().await;

    // Explicitly create the namespace.
    let req = CreateNamespaceRequest {
        name: "bananas_test".to_string(),
        retention_period_ns: Some(-42),
        partition_template: None,
        service_protection_limits: None,
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
    let ctx = TestContextBuilder::default().build().await;

    // Explicitly create the namespace.
    let create = ctx
        .grpc_delegate()
        .namespace_service()
        .create_namespace(Request::new(CreateNamespaceRequest {
            name: "bananas_test".to_string(),
            retention_period_ns: Some(42),
            partition_template: None,
            service_protection_limits: None,
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
            RetentionError::OutsideRetention{table_name, min_acceptable_ts, observed_ts}
        )) => {
            assert_eq!(table_name, "platanos");
            assert!(observed_ts < min_acceptable_ts);
        }
    );

    // The router restarts, and writes are then accepted.
    let ctx = ctx.restart().await;

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
    let ctx = TestContextBuilder::default().build().await;

    // Explicitly create the namespace.
    let create = ctx
        .grpc_delegate()
        .namespace_service()
        .create_namespace(Request::new(CreateNamespaceRequest {
            name: "bananas_test".to_string(),
            retention_period_ns: Some(42),
            partition_template: None,
            service_protection_limits: None,
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

#[tokio::test]
async fn test_update_namespace_limit_max_tables() {
    // Initialise a TestContext with namespace autocreation.
    let ctx = TestContextBuilder::default()
        .with_autocreate_namespace(None)
        .build()
        .await;

    // Writing to two initial tables should succeed
    ctx.write_lp("bananas", "test", "ananas,tag1=A,tag2=B val=42i 42424242")
        .await
        .expect("write should succeed");
    ctx.write_lp("bananas", "test", "platanos,tag3=C,tag4=D val=99i 42424243")
        .await
        .expect("write should succeed");

    // Limit the maximum number of tables to prevent a write adding another table
    let got = ctx
        .grpc_delegate()
        .namespace_service()
        .update_namespace_service_protection_limit(Request::new(
            UpdateNamespaceServiceProtectionLimitRequest {
                name: "bananas_test".to_string(),
                limit_update: Some(
                    update_namespace_service_protection_limit_request::LimitUpdate::MaxTables(1),
                ),
            },
        ))
        .await
        .expect("failed to update namespace max table limit")
        .into_inner()
        .namespace
        .expect("no namespace in response");

    assert_eq!(got.name, "bananas_test");
    assert_eq!(got.id, 1);
    assert_eq!(got.max_tables, 1);
    assert_eq!(
        got.max_columns_per_table,
        iox_catalog::DEFAULT_MAX_COLUMNS_PER_TABLE
    );

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
            assert_eq!(ns.max_tables, got.max_tables);
            assert_eq!(ns.max_columns_per_table, got.max_columns_per_table);
            assert!(ns.deleted_at.is_none());
        });
    }

    // New table should fail to be created by the catalog.
    let err = ctx
        .write_lp(
            "bananas",
            "test",
            "arán_banana,tag1=A,tag2=B val=42i 42424244",
        )
        .await
        .expect_err("cached entry should be removed");
    assert_matches!(err, router::server::http::Error::DmlHandler(DmlError::Schema(SchemaError::ServiceLimit(e))) => {
        let e: CatalogError = *e.downcast::<CatalogError>().expect("error returned should be a table create limit error");
        assert_matches!(&e, CatalogError::TableCreateLimitError { table_name, .. } => {
            assert_eq!(table_name, "arán_banana");
            assert_eq!(e.to_string(), "couldn't create table arán_banana; limit reached on namespace 1")
        });
    });
}
#[tokio::test]
async fn test_update_namespace_limit_max_columns_per_table() {
    // Initialise a TestContext with namespace autocreation.
    let ctx = TestContextBuilder::default()
        .with_autocreate_namespace(None)
        .build()
        .await;

    // Initial write within limit should succeed
    ctx.write_lp("bananas", "test", "ananas,tag1=A,tag2=B val=42i 42424242")
        .await
        .expect("write should succeed");

    // Limit the maximum number of columns per table so an extra column is rejected
    let got = ctx
        .grpc_delegate()
        .namespace_service()
        .update_namespace_service_protection_limit(Request::new(
            UpdateNamespaceServiceProtectionLimitRequest {
                name: "bananas_test".to_string(),
                limit_update: Some(
                    update_namespace_service_protection_limit_request::LimitUpdate::MaxColumnsPerTable(1),
                ),
            },
        ))
        .await
        .expect("failed to update namespace max table limit")
        .into_inner()
        .namespace
        .expect("no namespace in response");

    assert_eq!(got.name, "bananas_test");
    assert_eq!(got.id, 1);
    assert_eq!(got.max_tables, iox_catalog::DEFAULT_MAX_TABLES);
    assert_eq!(got.max_columns_per_table, 1);

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
            assert_eq!(ns.max_tables, got.max_tables);
            assert_eq!(ns.max_columns_per_table, got.max_columns_per_table);
            assert!(ns.deleted_at.is_none());
        });
    }

    // The cached entry is not affected, and writes continue to be validated
    // against the old value.
    //
    // https://github.com/influxdata/influxdb_iox/issues/6175

    // Writing to second table should succeed while using the cached entry
    ctx.write_lp(
        "bananas",
        "test",
        "platanos,tag1=A,tag2=B val=1337i 42424243",
    )
    .await
    .expect("write should succeed");

    // The router restarts, and writes with too many columns are then rejected.
    let ctx = ctx.restart().await;

    let err = ctx
        .write_lp(
            "bananas",
            "test",
            "arán_banana,tag1=A,tag2=B val=76i 42424243",
        )
        .await
        .expect_err("cached entry should be removed and write should be blocked");
    assert_matches!(
        err, router::server::http::Error::DmlHandler(DmlError::Schema(
            SchemaError::ServiceLimit(e)
        )) => {
            let e: CachedServiceProtectionLimit = *e.downcast::<CachedServiceProtectionLimit>().expect("error returned should be a cached service protection limit");
            assert_matches!(e, CachedServiceProtectionLimit::Column {
                table_name,
                max_columns_per_table,
                ..
            } => {
            assert_eq!(table_name, "arán_banana");
                assert_eq!(max_columns_per_table, 1);
            });
        }
    )
}

#[tokio::test]
async fn test_update_namespace_limit_0_max_tables_max_columns() {
    // Initialise a TestContext requiring explicit namespace creation.
    let ctx = TestContextBuilder::default().build().await;

    // Explicitly create the namespace.
    let create = ctx
        .grpc_delegate()
        .namespace_service()
        .create_namespace(Request::new(CreateNamespaceRequest {
            name: "bananas_test".to_string(),
            retention_period_ns: Some(0),
            partition_template: None,
            service_protection_limits: None,
        }))
        .await
        .expect("failed to create namespace")
        .into_inner()
        .namespace
        .expect("no namespace in response");

    // Attempt to use an invalid table limit
    let err = ctx
        .grpc_delegate()
        .namespace_service()
        .update_namespace_service_protection_limit(Request::new(
            UpdateNamespaceServiceProtectionLimitRequest {
                name: "bananas_test".to_string(),
                limit_update: Some(
                    update_namespace_service_protection_limit_request::LimitUpdate::MaxTables(0),
                ),
            },
        ))
        .await
        .expect_err("should not have been able to update the table limit to 0");
    assert_eq!(err.code(), Code::InvalidArgument);

    // Attempt to use an invalid column limit
    let err = ctx
        .grpc_delegate()
        .namespace_service()
        .update_namespace_service_protection_limit(Request::new(
            UpdateNamespaceServiceProtectionLimitRequest {
                name: "bananas_test".to_string(),
                limit_update: Some(
                    update_namespace_service_protection_limit_request::LimitUpdate::MaxColumnsPerTable(0),
                ),
            },
        ))
        .await
        .expect_err("should not have been able to update the column per table limit to 0");
    assert_eq!(err.code(), Code::InvalidArgument);

    // The catalog should contain the namespace unchanged.
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
            assert_eq!(ns.max_tables, create.max_tables);
            assert_eq!(ns.max_columns_per_table, create.max_columns_per_table);
            assert!(ns.deleted_at.is_none());
        });
    }
}

/// Ensure invoking the gRPC TableService to create a table populates
/// the catalog.
#[tokio::test]
async fn test_table_create() {
    // Initialise a TestContext without a namespace autocreation policy.
    let ctx = TestContextBuilder::default().build().await;

    // Explicitly create the namespace.
    let req = CreateNamespaceRequest {
        name: "bananas_test".to_string(),
        retention_period_ns: None,
        partition_template: None,
        service_protection_limits: None,
    };
    let namespace = ctx
        .grpc_delegate()
        .namespace_service()
        .create_namespace(Request::new(req))
        .await
        .unwrap()
        .into_inner()
        .namespace
        .unwrap();

    // Explicitly create the table.
    let req = CreateTableRequest {
        name: "plantains".to_string(),
        namespace: "bananas_test".to_string(),
        partition_template: None,
    };
    let got = ctx
        .grpc_delegate()
        .table_service()
        .create_table(Request::new(req))
        .await
        .unwrap()
        .into_inner()
        .table
        .unwrap();

    assert_eq!(got.name, "plantains");
    assert_eq!(got.id, 1);

    // The catalog should contain the table.
    {
        let db_list = ctx
            .catalog()
            .repositories()
            .await
            .tables()
            .list_by_namespace_id(NamespaceId::new(namespace.id))
            .await
            .unwrap();
        assert_matches!(db_list.as_slice(), [table] => {
            assert_eq!(table.id.get(), got.id);
            assert_eq!(table.name, got.name);
        });
    }

    let lp = "plantains,tag1=A,tag2=B val=42i 1685026200000000000".to_string();

    // Writing should succeed and should use the default partition template because no partition
    // template was set on either the namespace or the table.
    let response = ctx.write_lp("bananas", "test", lp).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let writes = ctx.write_calls();
    assert_eq!(writes.len(), 1);
    assert_matches!(
        writes.as_slice(),
        [
            WriteRequest {
                payload: Some(DatabaseBatch {
                    table_batches,
                    partition_key,
                    ..
                }),
            },
        ] => {
        let table_id = ctx.table_id("bananas_test", "plantains").await.get();
        assert_eq!(table_batches.len(), 1);
        assert_eq!(table_batches[0].table_id, table_id);
        assert_eq!(partition_key, "2023-05-25");
    })
}

#[tokio::test]
async fn test_invalid_strftime_partition_template() {
    // Initialise a TestContext without a namespace autocreation policy.
    let ctx = TestContextBuilder::default().build().await;

    // Explicitly create a namespace with a custom partition template.
    let req = CreateNamespaceRequest {
        name: "bananas_test".to_string(),
        retention_period_ns: None,
        partition_template: Some(PartitionTemplate {
            parts: vec![TemplatePart {
                part: Some(template_part::Part::TimeFormat("%3F".into())),
            }],
        }),
        service_protection_limits: None,
    };

    // Check namespace creation returned an error
    let got = ctx
        .grpc_delegate()
        .namespace_service()
        .create_namespace(Request::new(req))
        .await;

    assert_error!(
        got,
        ref status
            if status.code() == Code::InvalidArgument
                && status.message() == "invalid strftime format in partition template: %3F"
    );
}

#[tokio::test]
async fn test_invalid_tag_value_partition_template() {
    // Initialise a TestContext without a namespace autocreation policy.
    let ctx = TestContextBuilder::default().build().await;

    // Explicitly create a namespace with a custom partition template.
    let req = CreateNamespaceRequest {
        name: "bananas_test".to_string(),
        retention_period_ns: None,
        partition_template: Some(PartitionTemplate {
            parts: vec![TemplatePart {
                part: Some(template_part::Part::TagValue("time".into())),
            }],
        }),
        service_protection_limits: None,
    };

    // Check namespace creation returned an error
    let got = ctx
        .grpc_delegate()
        .namespace_service()
        .create_namespace(Request::new(req))
        .await;

    assert_error!(
        got,
        ref status
            if status.code() == Code::InvalidArgument
                && status.message() == "invalid tag value in partition template: time cannot be used"
    );
}

#[tokio::test]
async fn test_namespace_partition_template_implicit_table_creation() {
    // Initialise a TestContext without a namespace autocreation policy.
    let ctx = TestContextBuilder::default().build().await;

    // Explicitly create a namespace with a custom partition template.
    let req = CreateNamespaceRequest {
        name: "bananas_test".to_string(),
        retention_period_ns: None,
        partition_template: Some(PartitionTemplate {
            parts: vec![TemplatePart {
                part: Some(template_part::Part::TagValue("tag1".into())),
            }],
        }),
        service_protection_limits: None,
    };
    ctx.grpc_delegate()
        .namespace_service()
        .create_namespace(Request::new(req))
        .await
        .unwrap()
        .into_inner()
        .namespace
        .unwrap();

    // Write, which implicitly creates the table with the namespace's custom partition template
    let lp = "plantains,tag1=A,tag2=B val=42i".to_string();
    let response = ctx.write_lp("bananas", "test", lp).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Check the ingester observed the correct write that uses the namespace's template.
    let writes = ctx.write_calls();
    assert_eq!(writes.len(), 1);
    assert_matches!(
        writes.as_slice(),
        [
            WriteRequest {
                payload: Some(DatabaseBatch {
                    table_batches,
                    partition_key,
                    ..
                }),
            },
        ] => {
        let table_id = ctx.table_id("bananas_test", "plantains").await.get();
        assert_eq!(table_batches.len(), 1);
        assert_eq!(table_batches[0].table_id, table_id);
        assert_eq!(partition_key, "A");
    });
}

#[tokio::test]
async fn test_namespace_partition_template_explicit_table_creation_without_partition_template() {
    // Initialise a TestContext without a namespace autocreation policy.
    let ctx = TestContextBuilder::default().build().await;

    // Explicitly create a namespace with a custom partition template.
    let req = CreateNamespaceRequest {
        name: "bananas_test".to_string(),
        retention_period_ns: None,
        partition_template: Some(PartitionTemplate {
            parts: vec![TemplatePart {
                part: Some(template_part::Part::TagValue("tag1".into())),
            }],
        }),
        service_protection_limits: None,
    };
    ctx.grpc_delegate()
        .namespace_service()
        .create_namespace(Request::new(req))
        .await
        .unwrap()
        .into_inner()
        .namespace
        .unwrap();

    // Explicitly create a table *without* a custom partition template.
    let req = CreateTableRequest {
        name: "plantains".to_string(),
        namespace: "bananas_test".to_string(),
        partition_template: None,
    };
    ctx.grpc_delegate()
        .table_service()
        .create_table(Request::new(req))
        .await
        .unwrap()
        .into_inner()
        .table
        .unwrap();

    // Write to the just-created table
    let lp = "plantains,tag1=A,tag2=B val=42i".to_string();
    let response = ctx.write_lp("bananas", "test", lp).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Check the ingester observed the correct write that uses the namespace's template.
    let writes = ctx.write_calls();
    assert_eq!(writes.len(), 1);
    assert_matches!(
        writes.as_slice(),
        [
            WriteRequest {
                payload: Some(DatabaseBatch {
                    table_batches,
                    partition_key,
                    ..
                }),
            },
        ] => {
        let table_id = ctx.table_id("bananas_test", "plantains").await.get();
        assert_eq!(table_batches.len(), 1);
        assert_eq!(table_batches[0].table_id, table_id);
        assert_eq!(partition_key, "A");
    });
}

#[tokio::test]
async fn test_namespace_partition_template_explicit_table_creation_with_partition_template() {
    // Initialise a TestContext without a namespace autocreation policy.
    let ctx = TestContextBuilder::default().build().await;

    // Explicitly create a namespace with a custom partition template.
    let req = CreateNamespaceRequest {
        name: "bananas_test".to_string(),
        retention_period_ns: None,
        partition_template: Some(PartitionTemplate {
            parts: vec![TemplatePart {
                part: Some(template_part::Part::TagValue("tag1".into())),
            }],
        }),
        service_protection_limits: None,
    };
    ctx.grpc_delegate()
        .namespace_service()
        .create_namespace(Request::new(req))
        .await
        .unwrap()
        .into_inner()
        .namespace
        .unwrap();

    // Explicitly create a table with a *different* custom partition template.
    let req = CreateTableRequest {
        name: "plantains".to_string(),
        namespace: "bananas_test".to_string(),
        partition_template: Some(PartitionTemplate {
            parts: vec![TemplatePart {
                part: Some(template_part::Part::TagValue("tag2".into())),
            }],
        }),
    };
    ctx.grpc_delegate()
        .table_service()
        .create_table(Request::new(req))
        .await
        .unwrap()
        .into_inner()
        .table
        .unwrap();

    // Write to the just-created table
    let lp = "plantains,tag1=A,tag2=B val=42i".to_string();
    let response = ctx.write_lp("bananas", "test", lp).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Check the ingester observed the correct write that uses the table's template.
    let writes = ctx.write_calls();
    assert_eq!(writes.len(), 1);
    assert_matches!(
        writes.as_slice(),
        [
            WriteRequest {
                payload: Some(DatabaseBatch {
                    table_batches,
                    partition_key,
                    ..
                }),
            },
        ] => {
        let table_id = ctx.table_id("bananas_test", "plantains").await.get();
        assert_eq!(table_batches.len(), 1);
        assert_eq!(table_batches[0].table_id, table_id);
        assert_eq!(partition_key, "B");
    });
}

#[tokio::test]
async fn test_namespace_without_partition_template_table_with_partition_template() {
    // Initialise a TestContext without a namespace autocreation policy.
    let ctx = TestContextBuilder::default().build().await;

    // Explicitly create a namespace _without_ a custom partition template.
    let req = CreateNamespaceRequest {
        name: "bananas_test".to_string(),
        retention_period_ns: None,
        partition_template: None,
        service_protection_limits: None,
    };
    ctx.grpc_delegate()
        .namespace_service()
        .create_namespace(Request::new(req))
        .await
        .unwrap()
        .into_inner()
        .namespace
        .unwrap();

    // Explicitly create a table _with_ a custom partition template.
    let req = CreateTableRequest {
        name: "plantains".to_string(),
        namespace: "bananas_test".to_string(),
        partition_template: Some(PartitionTemplate {
            parts: vec![TemplatePart {
                part: Some(template_part::Part::TagValue("tag2".into())),
            }],
        }),
    };
    ctx.grpc_delegate()
        .table_service()
        .create_table(Request::new(req))
        .await
        .unwrap()
        .into_inner()
        .table
        .unwrap();

    // Write to the just-created table
    let lp = "plantains,tag1=A,tag2=B val=42i".to_string();
    let response = ctx.write_lp("bananas", "test", lp).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Check the ingester observed the correct write that uses the table's template.
    let writes = ctx.write_calls();
    assert_eq!(writes.len(), 1);
    assert_matches!(
        writes.as_slice(),
        [
            WriteRequest {
                payload: Some(DatabaseBatch {
                    table_batches,
                    partition_key,
                    ..
                }),
            },
        ] => {
        let table_id = ctx.table_id("bananas_test", "plantains").await.get();
        assert_eq!(table_batches.len(), 1);
        assert_eq!(table_batches[0].table_id, table_id);
        assert_eq!(partition_key, "B");
    });
}
