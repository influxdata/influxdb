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

use data_types::{
    partition_template::TablePartitionTemplateOverride, NamespaceName, Table as CatalogTable,
};
use generated_types::influxdata::iox::table::v1::*;
use iox_catalog::interface::{Catalog, SoftDeletedRows};
use observability_deps::tracing::{debug, info, warn};
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

        Ok(Response::new(table_to_create_response_proto(table)))
    }
}

fn table_to_create_response_proto(table: CatalogTable) -> CreateTableResponse {
    CreateTableResponse {
        table: Some(Table {
            id: table.id.get(),
            name: table.name.clone(),
            namespace_id: table.namespace_id.get(),
        }),
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

        let request = CreateTableRequest {
            name: table_name.into(),
            namespace: namespace.name.clone(),
            partition_template: Some(PartitionTemplate {
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
            }),
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

        let namespace_name = NamespaceName::new("grapes").unwrap();
        let namespace = catalog
            .repositories()
            .await
            .namespaces()
            .create(
                &namespace_name,
                Some(
                    NamespacePartitionTemplateOverride::try_from(PartitionTemplate {
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
                    })
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
