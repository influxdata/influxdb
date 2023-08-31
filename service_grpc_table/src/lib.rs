//! Implementation of the table gRPC service

#![deny(
    rustdoc::broken_intra_doc_links,
    rustdoc::bare_urls,
    rust_2018_idioms,
    missing_debug_implementations,
    unreachable_pub
)]
#![warn(
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]
#![allow(clippy::missing_docs_in_private_items)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::sync::Arc;

use data_types::{partition_template::TablePartitionTemplateOverride, NamespaceName};
use generated_types::influxdata::iox::table::v1::*;
use iox_catalog::interface::{Catalog, SoftDeletedRows};
use observability_deps::tracing::{debug, error, info, warn};
use tonic::{Request, Response, Status};

/// Implementation of the table gRPC service
#[derive(Debug)]
pub struct TableService {
    /// Catalog.
    catalog: Arc<dyn Catalog>,
}

impl TableService {
    /// Create a new `TableService` instance
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

#[tonic::async_trait]
impl table_service_server::TableService for TableService {
    // List tables for a namespace
    async fn get_tables(
        &self,
        request: Request<GetTablesRequest>,
    ) -> Result<Response<GetTablesResponse>, Status> {
        let mut repos = self.catalog.repositories().await;

        let namespace_name = NamespaceName::try_from(request.into_inner().namespace_name)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        debug!(%namespace_name, "listing tables for namespace");

        let namespace = repos
            .namespaces()
            .get_by_name(&namespace_name, SoftDeletedRows::ExcludeDeleted)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "Could not find a namespace with name {namespace_name}"
                ))
            })?;

        let tables = repos.tables().list_by_namespace_id(namespace.id).await.map_err(|e| {
            error!(error=%e, namespace_id=%namespace.id, %namespace_name, "failed to list tables for namespace");
            Status::internal(e.to_string())
        })?.into_iter().map(Table::from).collect::<Vec<_>>();

        Ok(Response::new(GetTablesResponse { tables }))
    }

    // create a table
    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        let mut repos = self.catalog.repositories().await;

        let CreateTableRequest {
            name,
            namespace,
            partition_template,
        } = request.into_inner();

        let namespace_name = NamespaceName::try_from(namespace)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        debug!(%name, %namespace_name, "Creating table");

        let namespace = repos
            .namespaces()
            .get_by_name(&namespace_name, SoftDeletedRows::ExcludeDeleted)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "Could not find a namespace with name {namespace_name}"
                ))
            })?;

        let table = repos
            .tables()
            .create(
                &name,
                TablePartitionTemplateOverride::try_new(
                    partition_template,
                    &namespace.partition_template,
                )
                .map_err(|v| Status::invalid_argument(v.to_string()))?,
                namespace.id,
            )
            .await
            .map_err(|e| {
                warn!(error=%e, %name, "failed to create table");
                match e {
                    iox_catalog::interface::Error::TableNameExists { name, .. } => {
                        Status::already_exists(format!(
                            "A table with the name `{name}` already exists \
                                in the namespace `{}`",
                            namespace.name
                        ))
                    }
                    other => Status::internal(other.to_string()),
                }
            })?;

        info!(
            %name,
            table_id = %table.id,
            partition_template = ?table.partition_template,
            "created table"
        );

        Ok(Response::new(CreateTableResponse {
            table: Some(table.into()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use data_types::{partition_template::NamespacePartitionTemplateOverride, TableId};
    use generated_types::influxdata::iox::{
        partition_template::v1::{template_part, PartitionTemplate, TemplatePart},
        table::v1::table_service_server::TableService as _,
    };
    use iox_catalog::{mem::MemCatalog, test_helpers::arbitrary_namespace};
    use tonic::Code;

    use super::*;

    #[tokio::test]
    async fn test_get_tables() {
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));

        let handler = TableService::new(Arc::clone(&catalog));

        // Set up the tables to check
        let namespace = arbitrary_namespace(&mut *catalog.repositories().await, "hops").await;
        let table_names = ["simcoe", "mosaic", "east kent golding"];
        let mut expect_tables = Vec::default();
        for table_name in table_names {
            expect_tables.push(
                handler
                    .create_table(Request::new(CreateTableRequest {
                        name: table_name.to_string(),
                        namespace: namespace.name.clone(),
                        partition_template: None,
                    }))
                    .await
                    .expect("failed to create table")
                    .into_inner()
                    .table
                    .unwrap(),
            );
        }
        expect_tables.sort_by_key(|t| t.id);

        let mut got_tables = handler
            .get_tables(Request::new(GetTablesRequest {
                namespace_name: namespace.name,
            }))
            .await
            .expect("list request failed unexpectedly")
            .into_inner()
            .tables;
        got_tables.sort_by_key(|t| t.id);

        assert_eq!(got_tables, expect_tables);
    }

    #[tokio::test]
    async fn test_basic_happy_path() {
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));
        let handler = TableService::new(Arc::clone(&catalog));

        let namespace = arbitrary_namespace(&mut *catalog.repositories().await, "grapes").await;
        let table_name = "varietals";

        let request = CreateTableRequest {
            name: table_name.into(),
            namespace: namespace.name.clone(),
            partition_template: None,
        };

        let created_table = handler
            .create_table(Request::new(request))
            .await
            .unwrap()
            .into_inner()
            .table
            .unwrap();

        assert!(created_table.id > 0);
        assert_eq!(created_table.name, table_name);
        assert_eq!(created_table.namespace_id, namespace.id.get());
        assert_eq!(created_table.partition_template, None);

        // The default template doesn't use any tag values, so no columns need to be created.
        let table_columns = catalog
            .repositories()
            .await
            .columns()
            .list_by_table_id(TableId::new(created_table.id))
            .await
            .unwrap();
        assert!(table_columns.is_empty());
    }

    #[tokio::test]
    async fn creating_same_table_twice_fails() {
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));
        let handler = TableService::new(Arc::clone(&catalog));

        let namespace = arbitrary_namespace(&mut *catalog.repositories().await, "grapes").await;
        let table_name = "varietals";

        let request = CreateTableRequest {
            name: table_name.into(),
            namespace: namespace.name.clone(),
            partition_template: None,
        };

        let created_table = handler
            .create_table(Request::new(request.clone()))
            .await
            .unwrap()
            .into_inner()
            .table
            .unwrap();

        // First creation attempt succeeds
        assert!(created_table.id > 0);
        assert_eq!(created_table.name, table_name);
        assert_eq!(created_table.namespace_id, namespace.id.get());
        assert_eq!(created_table.partition_template, None);

        // Trying to create a table in the same namespace with the same name fails with an "already
        // exists" error
        let error = handler
            .create_table(Request::new(request))
            .await
            .unwrap_err();

        assert_eq!(error.code(), Code::AlreadyExists);
        assert_eq!(
            error.message(),
            "A table with the name `varietals` already exists in the namespace `grapes`"
        );

        let all_tables = catalog.repositories().await.tables().list().await.unwrap();
        assert_eq!(all_tables.len(), 1);
    }

    #[tokio::test]
    async fn nonexistent_namespace_errors() {
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));
        let handler = TableService::new(Arc::clone(&catalog));
        let table_name = "varietals";

        let request = CreateTableRequest {
            name: table_name.into(),
            namespace: "does_not_exist".into(),
            partition_template: None,
        };

        let error = handler
            .create_table(Request::new(request))
            .await
            .unwrap_err();

        assert_eq!(error.code(), Code::NotFound);
        assert_eq!(
            error.message(),
            "Could not find a namespace with name does_not_exist"
        );

        let all_tables = catalog.repositories().await.tables().list().await.unwrap();
        assert!(all_tables.is_empty());
    }

    #[tokio::test]
    async fn custom_table_template_using_tags_creates_tag_columns() {
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));
        let handler = TableService::new(Arc::clone(&catalog));

        let namespace = arbitrary_namespace(&mut *catalog.repositories().await, "grapes").await;
        let table_name = "varietals";
        let partition_template = PartitionTemplate {
            parts: vec![
                TemplatePart {
                    part: Some(template_part::Part::TagValue("color".into())),
                },
                TemplatePart {
                    part: Some(template_part::Part::TagValue("tannins".into())),
                },
                TemplatePart {
                    part: Some(template_part::Part::TimeFormat("%Y".into())),
                },
            ],
        };

        let request = CreateTableRequest {
            name: table_name.into(),
            namespace: namespace.name.clone(),
            partition_template: Some(partition_template.clone()),
        };

        let created_table = handler
            .create_table(Request::new(request))
            .await
            .unwrap()
            .into_inner()
            .table
            .unwrap();

        assert!(created_table.id > 0);
        assert_eq!(created_table.name, table_name);
        assert_eq!(created_table.namespace_id, namespace.id.get());
        assert_eq!(created_table.partition_template, Some(partition_template));

        let table_columns = catalog
            .repositories()
            .await
            .columns()
            .list_by_table_id(TableId::new(created_table.id))
            .await
            .unwrap();
        assert_eq!(table_columns.len(), 2);
        assert!(table_columns.iter().all(|c| c.is_tag()));
        let mut column_names: Vec<_> = table_columns.iter().map(|c| &c.name).collect();
        column_names.sort();
        assert_eq!(column_names, &["color", "tannins"])
    }

    #[tokio::test]
    async fn custom_namespace_template_using_tags_creates_tag_columns() {
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));
        let handler = TableService::new(Arc::clone(&catalog));

        let partition_template = PartitionTemplate {
            parts: vec![
                TemplatePart {
                    part: Some(template_part::Part::TagValue("color".into())),
                },
                TemplatePart {
                    part: Some(template_part::Part::TagValue("tannins".into())),
                },
                TemplatePart {
                    part: Some(template_part::Part::TimeFormat("%Y".into())),
                },
            ],
        };

        let namespace_name = NamespaceName::new("grapes").unwrap();
        let namespace = catalog
            .repositories()
            .await
            .namespaces()
            .create(
                &namespace_name,
                Some(
                    NamespacePartitionTemplateOverride::try_from(partition_template.clone())
                        .unwrap(),
                ),
                None,
                None,
            )
            .await
            .unwrap();
        let table_name = "varietals";

        let request = CreateTableRequest {
            name: table_name.into(),
            namespace: namespace.name.clone(),
            partition_template: None,
        };

        let created_table = handler
            .create_table(Request::new(request))
            .await
            .unwrap()
            .into_inner()
            .table
            .unwrap();

        assert!(created_table.id > 0);
        assert_eq!(created_table.name, table_name);
        assert_eq!(created_table.namespace_id, namespace.id.get());
        assert_eq!(created_table.partition_template, Some(partition_template));

        let table_columns = catalog
            .repositories()
            .await
            .columns()
            .list_by_table_id(TableId::new(created_table.id))
            .await
            .unwrap();
        assert_eq!(table_columns.len(), 2);
        assert!(table_columns.iter().all(|c| c.is_tag()));
        let mut column_names: Vec<_> = table_columns.iter().map(|c| &c.name).collect();
        column_names.sort();
        assert_eq!(column_names, &["color", "tannins"])
    }

    #[tokio::test]
    async fn invalid_custom_table_template_returns_error() {
        let catalog: Arc<dyn Catalog> =
            Arc::new(MemCatalog::new(Arc::new(metric::Registry::default())));
        let handler = TableService::new(Arc::clone(&catalog));

        let namespace = arbitrary_namespace(&mut *catalog.repositories().await, "grapes").await;
        let table_name = "varietals";

        let request = CreateTableRequest {
            name: table_name.into(),
            namespace: namespace.name.clone(),
            partition_template: Some(PartitionTemplate { parts: vec![] }),
        };

        let error = handler
            .create_table(Request::new(request))
            .await
            .unwrap_err();

        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(
            error.message(),
            "Custom partition template must have at least one part"
        );

        let all_tables = catalog.repositories().await.tables().list().await.unwrap();
        assert!(all_tables.is_empty());
    }
}
