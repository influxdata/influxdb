//! Implementation of the schema gRPC service

use std::{ops::DerefMut, sync::Arc};

use generated_types::influxdata::iox::schema::v1::*;
use iox_catalog::interface::{get_schema_by_name, Catalog, SoftDeletedRows};
use observability_deps::tracing::warn;
use tonic::{Request, Response, Status};

/// Implementation of the gRPC schema service
#[derive(Debug)]
pub struct SchemaService {
    /// Catalog.
    catalog: Arc<dyn Catalog>,
}

impl SchemaService {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

#[tonic::async_trait]
impl schema_service_server::SchemaService for SchemaService {
    async fn get_schema(
        &self,
        request: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let mut repos = self.catalog.repositories().await;

        let req = request.into_inner();
        let schema = get_schema_by_name(
            &req.namespace,
            repos.deref_mut(),
            SoftDeletedRows::ExcludeDeleted,
        )
        .await
        .map_err(|e| {
            warn!(error=%e, %req.namespace, "failed to retrieve namespace schema");
            Status::not_found(e.to_string())
        })
        .map(Arc::new)?;
        Ok(Response::new(schema_to_proto(schema)))
    }
}

fn schema_to_proto(schema: Arc<data_types::NamespaceSchema>) -> GetSchemaResponse {
    let response = GetSchemaResponse {
        schema: Some(NamespaceSchema {
            id: schema.id.get(),
            topic_id: schema.topic_id.get(),
            query_pool_id: schema.query_pool_id.get(),
            tables: schema
                .tables
                .iter()
                .map(|(name, t)| {
                    (
                        name.clone(),
                        TableSchema {
                            id: t.id.get(),
                            columns: t
                                .columns
                                .iter()
                                .map(|(name, c)| {
                                    (
                                        name.clone(),
                                        ColumnSchema {
                                            id: c.id.get(),
                                            column_type: c.column_type as i32,
                                        },
                                    )
                                })
                                .collect(),
                        },
                    )
                })
                .collect(),
        }),
    };
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::ColumnType;
    use generated_types::influxdata::iox::schema::v1::schema_service_server::SchemaService;
    use iox_catalog::mem::MemCatalog;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_schema() {
        // create a catalog and populate it with some test data, then drop the write lock
        let catalog = {
            let metrics = Arc::new(metric::Registry::default());
            let catalog = Arc::new(MemCatalog::new(metrics));
            let mut repos = catalog.repositories().await;
            let topic = repos.topics().create_or_get("franz").await.unwrap();
            let pool = repos.query_pools().create_or_get("franz").await.unwrap();
            let namespace = repos
                .namespaces()
                .create("namespace_schema_test", None, topic.id, pool.id)
                .await
                .unwrap();
            let table = repos
                .tables()
                .create_or_get("schema_test_table", namespace.id)
                .await
                .unwrap();
            repos
                .columns()
                .create_or_get("schema_test_column", table.id, ColumnType::Tag)
                .await
                .unwrap();
            Arc::clone(&catalog)
        };

        // create grpc schema service
        let grpc = super::SchemaService::new(catalog);
        let request = GetSchemaRequest {
            namespace: "namespace_schema_test".to_string(),
        };

        let tonic_response = grpc
            .get_schema(Request::new(request))
            .await
            .expect("rpc request should succeed");
        let response = tonic_response.into_inner();
        let schema = response.schema.expect("schema should be Some()");
        assert_eq!(
            schema.tables.keys().collect::<Vec<&String>>(),
            vec![&"schema_test_table".to_string()]
        );
        assert_eq!(
            schema
                .tables
                .get(&"schema_test_table".to_string())
                .expect("test table should exist")
                .columns
                .keys()
                .collect::<Vec<&String>>(),
            vec![&"schema_test_column".to_string()]
        );
    }
}
