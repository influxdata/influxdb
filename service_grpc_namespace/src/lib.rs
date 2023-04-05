//! Implementation of the namespace gRPC service
use std::sync::Arc;

use data_types::{Namespace as CatalogNamespace, QueryPoolId, TopicId};
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
    topic_id: Option<TopicId>,
    query_id: Option<QueryPoolId>,
}

impl NamespaceService {
    pub fn new(
        catalog: Arc<dyn Catalog>,
        topic_id: Option<TopicId>,
        query_id: Option<QueryPoolId>,
    ) -> Self {
        Self {
            catalog,
            topic_id,
            query_id,
        }
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
        if self.topic_id.is_none() || self.query_id.is_none() {
            return Err(Status::invalid_argument("topic_id or query_id not set"));
        }

        let mut repos = self.catalog.repositories().await;

        let CreateNamespaceRequest {
            name: namespace_name,
            retention_period_ns,
        } = request.into_inner();

        let retention_period_ns = map_retention_period(retention_period_ns)?;

        debug!(%namespace_name, ?retention_period_ns, "Creating namespace");

        let namespace = repos
            .namespaces()
            .create(
                &namespace_name,
                retention_period_ns,
                self.topic_id.unwrap(),
                self.query_id.unwrap(),
            )
            .await
            .map_err(|e| {
                warn!(error=%e, %namespace_name, "failed to create namespace");
                Status::internal(e.to_string())
            })?;

        info!(
            namespace_name,
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
    use generated_types::influxdata::iox::namespace::v1::namespace_service_server::NamespaceService as _;
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

        let topic = catalog
            .repositories()
            .await
            .topics()
            .create_or_get("kafka-topic")
            .await
            .unwrap();
        let query_pool = catalog
            .repositories()
            .await
            .query_pools()
            .create_or_get("query-pool")
            .await
            .unwrap();

        let handler = NamespaceService::new(catalog, Some(topic.id), Some(query_pool.id));

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
    async fn test_reject_invalid_service_protection_limits() {
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));

        let topic = catalog
            .repositories()
            .await
            .topics()
            .create_or_get("kafka-topic")
            .await
            .unwrap();
        let query_pool = catalog
            .repositories()
            .await
            .query_pools()
            .create_or_get("query-pool")
            .await
            .unwrap();

        let handler = NamespaceService::new(catalog, Some(topic.id), Some(query_pool.id));
        let req = CreateNamespaceRequest {
            name: NS_NAME.to_string(),
            retention_period_ns: Some(RETENTION),
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
}
