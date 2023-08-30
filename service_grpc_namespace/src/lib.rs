//! Implementation of the namespace gRPC service

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

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::sync::Arc;

use data_types::{
    partition_template::NamespacePartitionTemplateOverride, Namespace as CatalogNamespace,
    NamespaceName, NamespaceServiceProtectionLimitsOverride,
};
use generated_types::influxdata::iox::namespace::v1::{
    update_namespace_service_protection_limit_request::LimitUpdate, *,
};
use iox_catalog::interface::{Catalog, SoftDeletedRows};
use observability_deps::tracing::{debug, info, warn};
use tonic::{Request, Response, Status};

/// Implementation of the gRPC namespace service
#[derive(Debug)]
pub struct NamespaceService {
    /// Catalog.
    catalog: Arc<dyn Catalog>,
}

impl NamespaceService {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

#[tonic::async_trait]
impl namespace_service_server::NamespaceService for NamespaceService {
    async fn get_namespaces(
        &self,
        _request: Request<GetNamespacesRequest>,
    ) -> Result<Response<GetNamespacesResponse>, Status> {
        let mut repos = self.catalog.repositories().await;

        let namespaces = repos
            .namespaces()
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
            .map_err(|e| {
                warn!(error=%e, "failed to retrieve namespaces from catalog");
                Status::not_found(e.to_string())
            })?;
        Ok(Response::new(GetNamespacesResponse {
            namespaces: namespaces.into_iter().map(namespace_to_proto).collect(),
        }))
    }

    // create a namespace
    async fn create_namespace(
        &self,
        request: Request<CreateNamespaceRequest>,
    ) -> Result<Response<CreateNamespaceResponse>, Status> {
        let mut repos = self.catalog.repositories().await;

        let CreateNamespaceRequest {
            name: namespace_name,
            retention_period_ns,
            partition_template,
            service_protection_limits,
        } = request.into_inner();

        // Ensure the namespace name is consistently processed within IOx - this
        // is handled by the NamespaceName type.
        let namespace_name = NamespaceName::try_from(namespace_name)
            .map_err(|v| Status::invalid_argument(v.to_string()))?;

        let retention_period_ns = map_retention_period(retention_period_ns)?;

        debug!(%namespace_name, ?retention_period_ns, "Creating namespace");

        let namespace = repos
            .namespaces()
            .create(
                &namespace_name,
                partition_template
                    .map(NamespacePartitionTemplateOverride::try_from)
                    .transpose()
                    .map_err(|v| Status::invalid_argument(v.to_string()))?,
                retention_period_ns,
                service_protection_limits.map(NamespaceServiceProtectionLimitsOverride::from),
            )
            .await
            .map_err(|e| {
                warn!(error=%e, %namespace_name, "failed to create namespace");
                match e {
                    iox_catalog::interface::Error::NameExists { name } => Status::already_exists(
                        format!("A namespace with the name `{name}` already exists"),
                    ),
                    other => Status::internal(other.to_string()),
                }
            })?;

        info!(
            %namespace_name,
            namespace_id = %namespace.id,
            "created namespace"
        );

        Ok(Response::new(namespace_to_create_response_proto(namespace)))
    }

    async fn delete_namespace(
        &self,
        request: Request<DeleteNamespaceRequest>,
    ) -> Result<Response<DeleteNamespaceResponse>, Status> {
        let namespace_name = request.into_inner().name;

        self.catalog
            .repositories()
            .await
            .namespaces()
            .soft_delete(&namespace_name)
            .await
            .map_err(|e| {
                warn!(error=%e, %namespace_name, "failed to soft-delete namespace");
                Status::internal(e.to_string())
            })?;

        info!(namespace_name, "soft-deleted namespace");

        Ok(Response::new(Default::default()))
    }

    async fn update_namespace_retention(
        &self,
        request: Request<UpdateNamespaceRetentionRequest>,
    ) -> Result<Response<UpdateNamespaceRetentionResponse>, Status> {
        let mut repos = self.catalog.repositories().await;

        let UpdateNamespaceRetentionRequest {
            name: namespace_name,
            retention_period_ns,
        } = request.into_inner();

        let retention_period_ns = map_retention_period(retention_period_ns)?;

        debug!(
            %namespace_name,
            ?retention_period_ns,
            "Updating namespace retention",
        );

        let namespace = repos
            .namespaces()
            .update_retention_period(&namespace_name, retention_period_ns)
            .await
            .map_err(|e| {
                warn!(error=%e, %namespace_name, "failed to update namespace retention");
                Status::not_found(e.to_string())
            })?;

        info!(
            %namespace_name,
            retention_period_ns,
            namespace_id = %namespace.id,
            "updated namespace retention"
        );

        Ok(Response::new(UpdateNamespaceRetentionResponse {
            namespace: Some(namespace_to_proto(namespace)),
        }))
    }

    async fn update_namespace_service_protection_limit(
        &self,
        request: Request<UpdateNamespaceServiceProtectionLimitRequest>,
    ) -> Result<Response<UpdateNamespaceServiceProtectionLimitResponse>, Status> {
        let mut repos = self.catalog.repositories().await;

        let UpdateNamespaceServiceProtectionLimitRequest {
            name: namespace_name,
            limit_update,
        } = request.into_inner();

        debug!(
            %namespace_name,
            ?limit_update,
            "updating namespace service protection limit",
        );

        let namespace = match limit_update {
            Some(LimitUpdate::MaxTables(n)) => {
                if n == 0 {
                    return Err(Status::invalid_argument(
                        "max table limit for namespace must be greater than 0",
                    ));
                }
                repos
                    .namespaces()
                    .update_table_limit(&namespace_name, n)
                    .await
                    .map_err(|e| {
                        warn!(
                            error = %e,
                            %namespace_name,
                            table_limit = %n,
                            "failed to update table limit for namespace",
                        );
                        status_from_catalog_namespace_error(e)
                    })
            }
            Some(LimitUpdate::MaxColumnsPerTable(n)) => {
                if n == 0 {
                    return Err(Status::invalid_argument(
                        "max columns per table limit for namespace must be greater than 0",
                    ));
                }
                repos
                    .namespaces()
                    .update_column_limit(&namespace_name, n)
                    .await
                    .map_err(|e| {
                        warn!(
                            error = %e,
                            %namespace_name,
                            per_table_column_limit = %n,
                            "failed to update per table column limit for namespace",
                        );
                        status_from_catalog_namespace_error(e)
                    })
            }
            None => Err(Status::invalid_argument(
                "unsupported service protection limit change requested",
            )),
        }?;

        info!(
            %namespace_name,
            namespace_id = %namespace.id,
            max_tables = %namespace.max_tables,
            max_columns_per_table = %namespace.max_columns_per_table,
            "updated namespace service protection limits",
        );

        Ok(Response::new(
            UpdateNamespaceServiceProtectionLimitResponse {
                namespace: Some(namespace_to_proto(namespace)),
            },
        ))
    }
}

fn namespace_to_proto(namespace: CatalogNamespace) -> Namespace {
    Namespace {
        id: namespace.id.get(),
        name: namespace.name.clone(),
        retention_period_ns: namespace.retention_period_ns,
        max_tables: namespace.max_tables,
        max_columns_per_table: namespace.max_columns_per_table,
        partition_template: namespace.partition_template.as_proto().cloned(),
    }
}

fn namespace_to_create_response_proto(namespace: CatalogNamespace) -> CreateNamespaceResponse {
    CreateNamespaceResponse {
        namespace: Some(Namespace {
            id: namespace.id.get(),
            name: namespace.name.clone(),
            retention_period_ns: namespace.retention_period_ns,
            max_tables: namespace.max_tables,
            max_columns_per_table: namespace.max_columns_per_table,
            partition_template: namespace.partition_template.as_proto().cloned(),
        }),
    }
}

/// Map a user-submitted retention period value to the correct internal
/// encoding.
///
/// 0 is always mapped to [`None`], indicating infinite retention.
///
/// Negative retention periods are rejected with an error.
fn map_retention_period(v: Option<i64>) -> Result<Option<i64>, Status> {
    match v {
        Some(0) => Ok(None),
        Some(v @ 1..) => Ok(Some(v)),
        Some(_v @ ..=0) => Err(Status::invalid_argument(
            "invalid negative retention period",
        )),
        None => Ok(None),
    }
}

fn status_from_catalog_namespace_error(err: iox_catalog::interface::Error) -> Status {
    match err {
        iox_catalog::interface::Error::NamespaceNotFoundByName { .. } => {
            Status::not_found(err.to_string())
        }
        _ => Status::internal(err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert_matches::assert_matches;
    use data_types::partition_template::PARTITION_BY_DAY_PROTO;
    use generated_types::influxdata::iox::{
        namespace::v1::namespace_service_server::NamespaceService as _,
        partition_template::v1::PartitionTemplate,
    };
    use iox_catalog::mem::MemCatalog;
    use tonic::Code;

    use super::*;

    const RETENTION: i64 = Duration::from_secs(42 * 60 * 60).as_nanos() as _;
    const NS_NAME: &str = "bananas";

    #[test]
    fn test_retention_mapping() {
        assert_matches!(map_retention_period(None), Ok(None));
        assert_matches!(map_retention_period(Some(0)), Ok(None));
        assert_matches!(map_retention_period(Some(1)), Ok(Some(1)));
        assert_matches!(map_retention_period(Some(42)), Ok(Some(42)));
        assert_matches!(map_retention_period(Some(-1)), Err(e) => {
            assert_eq!(e.code(), Code::InvalidArgument)
        });
        assert_matches!(map_retention_period(Some(-42)), Err(e) => {
            assert_eq!(e.code(), Code::InvalidArgument)
        });
    }

    #[test]
    fn test_namespace_error_mapping() {
        let not_found_err = iox_catalog::interface::Error::NamespaceNotFoundByName {
            name: String::from("bananas_namespace"),
        };
        let not_found_msg = not_found_err.to_string();
        assert_matches!(
            status_from_catalog_namespace_error(not_found_err),
        s => {
            assert_eq!(s.code(), Code::NotFound);
            assert_eq!(s.message(), not_found_msg);
        });

        let other_err = iox_catalog::interface::Error::ColumnCreateLimitError {
            column_name: String::from("quantity"),
            table_id: data_types::TableId::new(42),
        };
        let other_err_msg = other_err.to_string();
        assert_matches!(
            status_from_catalog_namespace_error(other_err),
        s => {
            assert_eq!(s.code(), Code::Internal);
            assert_eq!(s.message(), other_err_msg);
        });
    }

    #[tokio::test]
    async fn test_crud() {
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));

        let handler = NamespaceService::new(catalog);

        // There should be no namespaces to start with.
        {
            let current = handler
                .get_namespaces(Request::new(Default::default()))
                .await
                .expect("must return namespaces")
                .into_inner()
                .namespaces;
            assert!(current.is_empty());
        }

        let req = CreateNamespaceRequest {
            name: NS_NAME.to_string(),
            retention_period_ns: Some(RETENTION),
            partition_template: None,
            service_protection_limits: None,
        };
        let created_ns = handler
            .create_namespace(Request::new(req))
            .await
            .expect("failed to create namespace")
            .into_inner()
            .namespace
            .expect("no namespace in response");
        assert_eq!(created_ns.name, NS_NAME);
        assert_eq!(created_ns.retention_period_ns, Some(RETENTION));
        assert_eq!(created_ns.partition_template, None);

        // There should now be one namespace
        {
            let current = handler
                .get_namespaces(Request::new(Default::default()))
                .await
                .expect("must return namespaces")
                .into_inner()
                .namespaces;
            assert_matches!(current.as_slice(), [ns] => {
                assert_eq!(ns, &created_ns);
            })
        }

        // Update the retention period
        let updated_ns = handler
            .update_namespace_retention(Request::new(UpdateNamespaceRetentionRequest {
                name: NS_NAME.to_string(),
                retention_period_ns: Some(0), // A zero!
            }))
            .await
            .expect("failed to update namespace")
            .into_inner()
            .namespace
            .expect("no namespace in response");
        assert_eq!(updated_ns.name, created_ns.name);
        assert_eq!(updated_ns.id, created_ns.id);
        assert_eq!(created_ns.retention_period_ns, Some(RETENTION));
        assert_eq!(updated_ns.retention_period_ns, None);
        assert_eq!(created_ns.partition_template, updated_ns.partition_template);

        // Listing the namespaces should return the updated namespace
        {
            let current = handler
                .get_namespaces(Request::new(Default::default()))
                .await
                .expect("must return namespaces")
                .into_inner()
                .namespaces;
            assert_matches!(current.as_slice(), [ns] => {
                assert_eq!(ns, &updated_ns);
            })
        }

        // Update the max allowed tables
        let want_max_tables = created_ns.max_tables + 42;
        let updated_ns = handler
            .update_namespace_service_protection_limit(Request::new(
                UpdateNamespaceServiceProtectionLimitRequest {
                    name: NS_NAME.to_string(),
                    limit_update: Some(LimitUpdate::MaxTables(want_max_tables)),
                },
            ))
            .await
            .expect("failed to update namespace")
            .into_inner()
            .namespace
            .expect("no namespace in response");
        assert_eq!(updated_ns.name, created_ns.name);
        assert_eq!(updated_ns.id, created_ns.id);
        assert_eq!(updated_ns.max_tables, want_max_tables);
        assert_eq!(
            updated_ns.max_columns_per_table,
            created_ns.max_columns_per_table
        );

        // Update the max allowed columns per table
        let want_max_columns_per_table = created_ns.max_columns_per_table + 7;
        let updated_ns = handler
            .update_namespace_service_protection_limit(Request::new(
                UpdateNamespaceServiceProtectionLimitRequest {
                    name: NS_NAME.to_string(),
                    limit_update: Some(LimitUpdate::MaxColumnsPerTable(want_max_columns_per_table)),
                },
            ))
            .await
            .expect("failed to update namespace")
            .into_inner()
            .namespace
            .expect("no namespace in response");
        assert_eq!(updated_ns.name, created_ns.name);
        assert_eq!(updated_ns.id, created_ns.id);
        assert_eq!(updated_ns.max_tables, want_max_tables);
        assert_eq!(updated_ns.max_columns_per_table, want_max_columns_per_table);

        // Deleting the namespace should cause it to disappear
        handler
            .delete_namespace(Request::new(DeleteNamespaceRequest {
                name: NS_NAME.to_string(),
            }))
            .await
            .expect("must delete");

        // Listing the namespaces should now return nothing.
        {
            let current = handler
                .get_namespaces(Request::new(Default::default()))
                .await
                .expect("must return namespaces")
                .into_inner()
                .namespaces;
            assert_matches!(current.as_slice(), []);
        }
    }

    #[tokio::test]
    async fn creating_same_namespace_twice_fails() {
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));
        let handler = NamespaceService::new(Arc::clone(&catalog));

        let req = CreateNamespaceRequest {
            name: NS_NAME.to_string(),
            retention_period_ns: Some(RETENTION),
            partition_template: None,
            service_protection_limits: None,
        };

        let created_ns = handler
            .create_namespace(Request::new(req.clone()))
            .await
            .unwrap()
            .into_inner()
            .namespace
            .unwrap();

        // First creation attempt succeeds
        assert_eq!(created_ns.name, NS_NAME);
        assert_eq!(created_ns.retention_period_ns, Some(RETENTION));

        // Trying to create a namespace with the same name fails with an "already exists" error
        let error = handler
            .create_namespace(Request::new(req))
            .await
            .unwrap_err();

        assert_eq!(error.code(), Code::AlreadyExists);
        assert_eq!(
            error.message(),
            "A namespace with the name `bananas` already exists"
        );

        let all_namespaces = catalog
            .repositories()
            .await
            .namespaces()
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap();
        assert_eq!(all_namespaces.len(), 1);
    }

    #[tokio::test]
    async fn custom_namespace_template_returned_in_responses() {
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));
        let handler = NamespaceService::new(Arc::clone(&catalog));

        // Ensure the create reponse feeds back the partition template
        let req = CreateNamespaceRequest {
            name: NS_NAME.to_string(),
            retention_period_ns: None,
            partition_template: Some(PARTITION_BY_DAY_PROTO.as_ref().clone()),
            service_protection_limits: None,
        };
        let created_ns = handler
            .create_namespace(Request::new(req))
            .await
            .expect("failed to create namespace")
            .into_inner()
            .namespace
            .expect("no namespace in response");
        assert_eq!(created_ns.name, NS_NAME);
        assert_eq!(created_ns.retention_period_ns, None);
        assert_eq!(
            created_ns.partition_template,
            Some(PARTITION_BY_DAY_PROTO.as_ref().clone())
        );

        // And then make sure that a list call will include the details.
        let listed_ns = handler
            .get_namespaces(Request::new(Default::default()))
            .await
            .expect("must return namespaces")
            .into_inner()
            .namespaces;
        assert_matches!(listed_ns.as_slice(), [listed_ns] => {
            assert_eq!(listed_ns.partition_template, Some(PARTITION_BY_DAY_PROTO.as_ref().clone()));
        })
    }

    #[tokio::test]
    async fn invalid_custom_namespace_template_returns_error() {
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));
        let handler = NamespaceService::new(Arc::clone(&catalog));

        let req = CreateNamespaceRequest {
            name: NS_NAME.to_string(),
            retention_period_ns: None,
            partition_template: Some(PartitionTemplate { parts: vec![] }),
            service_protection_limits: None,
        };

        let error = handler
            .create_namespace(Request::new(req))
            .await
            .unwrap_err();

        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(
            error.message(),
            "Custom partition template must have at least one part"
        );

        let all_namespaces = catalog
            .repositories()
            .await
            .namespaces()
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
            .unwrap();
        assert_eq!(all_namespaces.len(), 0);
    }

    #[tokio::test]
    async fn test_reject_invalid_service_protection_limits() {
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));

        let handler = NamespaceService::new(catalog);
        let req = CreateNamespaceRequest {
            name: NS_NAME.to_string(),
            retention_period_ns: Some(RETENTION),
            partition_template: None,
            service_protection_limits: None,
        };
        let created_ns = handler
            .create_namespace(Request::new(req))
            .await
            .expect("failed to create namespace")
            .into_inner()
            .namespace
            .expect("no namespace in response");
        assert_eq!(created_ns.name, NS_NAME);
        assert_eq!(created_ns.retention_period_ns, Some(RETENTION));
        assert_ne!(created_ns.max_tables, 0);
        assert_ne!(created_ns.max_columns_per_table, 0);

        // The handler should reject any attempt to set the table limit to zero.
        let status = handler
            .update_namespace_service_protection_limit(Request::new(
                UpdateNamespaceServiceProtectionLimitRequest {
                    name: NS_NAME.to_string(),
                    limit_update: Some(LimitUpdate::MaxTables(0)),
                },
            ))
            .await
            .expect_err("invalid namespace update request for max table limit should fail");
        assert_eq!(status.code(), Code::InvalidArgument);

        // ...and likewise should reject any attempt to set the column per table limit to zero.
        let status = handler
            .update_namespace_service_protection_limit(Request::new(
                UpdateNamespaceServiceProtectionLimitRequest {
                    name: NS_NAME.to_string(),
                    limit_update: Some(LimitUpdate::MaxColumnsPerTable(0)),
                },
            ))
            .await
            .expect_err(
                "invalid namespace update request for max columns per table limit should fail",
            );
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_create_with_service_protection_limits() {
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));

        let max_tables = 123;
        let max_columns_per_table = 321;

        let handler = NamespaceService::new(catalog);
        let req = CreateNamespaceRequest {
            name: NS_NAME.to_string(),
            retention_period_ns: Some(RETENTION),
            partition_template: None,
            service_protection_limits: Some(ServiceProtectionLimits {
                max_tables: Some(max_tables),
                max_columns_per_table: Some(max_columns_per_table),
            }),
        };
        let created_ns = handler
            .create_namespace(Request::new(req))
            .await
            .expect("failed to create namespace")
            .into_inner()
            .namespace
            .expect("no namespace in response");
        assert_eq!(created_ns.name, NS_NAME);
        assert_eq!(created_ns.retention_period_ns, Some(RETENTION));
        assert_eq!(created_ns.max_tables, max_tables);
        assert_eq!(created_ns.max_columns_per_table, max_columns_per_table);
    }

    macro_rules! test_create_namespace_name {
        (
            $test_name:ident,
            name = $name:expr,  // The input namespace name string
            want = $($want:tt)+ // A pattern match against Result<str, tonic::Status>
                                //    where the Ok variant contains the actual namespace
                                //    name used on the server side (potentially encoded)
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_create_namespace_name_ $test_name>]() {
                    let catalog: Arc<dyn Catalog> =
                        Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));

                    let handler = NamespaceService::new(catalog);

                    let req = CreateNamespaceRequest {
                        name: String::from($name),
                        retention_period_ns: Some(RETENTION),
                        partition_template: None,
                        service_protection_limits: None,
                    };

                    let got = handler.create_namespace(Request::new(req)).await;

                    // Map the result into just the name so it's easier for the
                    // test to assert the correct namespace name was used.
                    let actual_name_res = got.as_ref().map(|v| {
                        v.get_ref()
                            .namespace
                            .as_ref()
                            .expect("no namespace in response")
                            .name
                            .as_str()
                    });
                    assert_matches!(actual_name_res, $($want)+);

                    // Attempt to list the namespaces
                    let list = handler
                        .get_namespaces(Request::new(Default::default()))
                        .await
                        .expect("must return namespaces")
                        .into_inner()
                        .namespaces;

                    // Validate the expected state - if the request succeeded, then the
                    // namespace MUST exist.
                    match got {
                        Ok(got) => {
                            assert_matches!(list.as_slice(), [ns] => {
                                assert_eq!(ns, got.get_ref().namespace.as_ref().unwrap());
                            })
                        }
                        Err(_) => assert!(list.is_empty()),
                    }
                }
            }
        }
    }

    test_create_namespace_name!(ok, name = "bananas", want = Ok("bananas"));

    test_create_namespace_name!(multi_byte, name = "🍌", want = Err(e) => {
        assert_eq!(e.code(), Code::InvalidArgument);
        assert_eq!(e.message(), "namespace name '🍌' contains invalid character, character number 0 is not whitelisted");
    });

    test_create_namespace_name!(
        tab,
        name = "it\tis\ttabtasitc",
        want = Err(e) => {
            assert_eq!(e.code(), Code::InvalidArgument);
            assert_eq!(e.message(), "namespace name 'it\tis\ttabtasitc' contains invalid character, character number 2 is not whitelisted");
        }
    );

    test_create_namespace_name!(
        null,
        name = "bad\0bananas",
        want = Err(e) => {
            assert_eq!(e.code(), Code::InvalidArgument);
            assert_eq!(e.message(), "namespace name 'bad\0bananas' contains invalid character, character number 3 is not whitelisted");
        }
    );

    test_create_namespace_name!(
        length_lower,
        name = "",
        want = Err(e) => {
            assert_eq!(e.code(), Code::InvalidArgument);
            assert_eq!(e.message(), r#"namespace name  length must be between 1 and 64 characters"#);
        }
    );

    test_create_namespace_name!(
        length_upper_inclusive,
        name = "A".repeat(64),
        want = Ok(v) if v == "A".repeat(64)
    );

    test_create_namespace_name!(
        length_upper_exclusive,
        name = "A".repeat(65),
        want = Err(e) => {
            assert_eq!(e.code(), Code::InvalidArgument);
            assert_eq!(e.message(), r#"namespace name AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA length must be between 1 and 64 characters"#);
        }
    );
}
