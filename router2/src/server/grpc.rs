//! gRPC service implementations for `router2`.

use std::sync::Arc;

use generated_types::{
    google::FieldViolation,
    influxdata::{iox::schema::v1::*, pbdata::v1::*},
};
use hashbrown::HashMap;
use iox_catalog::interface::{get_schema_by_name, Catalog};
use metric::U64Counter;
use mutable_batch::MutableBatch;
use observability_deps::tracing::*;
use schema::selection::Selection;
use std::ops::DerefMut;
use tonic::{Request, Response, Status};
use trace::ctx::SpanContext;
use write_summary::WriteSummary;

use crate::dml_handlers::{DmlError, DmlHandler, PartitionError};

/// This type is responsible for managing all gRPC services exposed by
/// `router2`.
#[derive(Debug)]
pub struct GrpcDelegate<D> {
    dml_handler: Arc<D>,
    catalog: Arc<dyn Catalog>,
    metrics: Arc<metric::Registry>,
}

impl<D> GrpcDelegate<D> {
    /// Initialise a new gRPC handler, dispatching DML operations to
    /// `dml_handler`.
    pub fn new(
        dml_handler: Arc<D>,
        catalog: Arc<dyn Catalog>,
        metrics: Arc<metric::Registry>,
    ) -> Self {
        Self {
            dml_handler,
            catalog,
            metrics,
        }
    }
}

impl<D> GrpcDelegate<D>
where
    D: DmlHandler<WriteInput = HashMap<String, MutableBatch>, WriteOutput = WriteSummary> + 'static,
{
    /// Acquire a [`WriteService`] gRPC service implementation.
    ///
    /// [`WriteService`]: generated_types::influxdata::pbdata::v1::write_service_server::WriteService.
    pub fn write_service(
        &self,
    ) -> write_service_server::WriteServiceServer<impl write_service_server::WriteService> {
        write_service_server::WriteServiceServer::new(WriteService::new(
            Arc::clone(&self.dml_handler),
            &*self.metrics,
        ))
    }

    /// Acquire a [`SchemaService`] gRPC service implementation.
    ///
    /// [`SchemaService`]: generated_types::influxdata::iox::schema::v1::schema_service_server::SchemaService.
    pub fn schema_service(
        &self,
    ) -> schema_service_server::SchemaServiceServer<impl schema_service_server::SchemaService> {
        schema_service_server::SchemaServiceServer::new(SchemaService::new(Arc::clone(
            &self.catalog,
        )))
    }
}

#[derive(Debug)]
struct WriteService<D> {
    dml_handler: Arc<D>,

    write_metric_rows: U64Counter,
    write_metric_columns: U64Counter,
    write_metric_tables: U64Counter,
}

impl<D> WriteService<D> {
    fn new(dml_handler: Arc<D>, metrics: &metric::Registry) -> Self {
        let write_metric_rows = metrics
            .register_metric::<U64Counter>(
                "grpc_write_rows_total",
                "cumulative number of rows successfully routed",
            )
            .recorder(&[]);
        let write_metric_columns = metrics
            .register_metric::<U64Counter>(
                "grpc_write_fields_total",
                "cumulative number of fields successfully routed",
            )
            .recorder(&[]);
        let write_metric_tables = metrics
            .register_metric::<U64Counter>(
                "grpc_write_tables_total",
                "cumulative number of tables in each write request",
            )
            .recorder(&[]);

        Self {
            dml_handler,
            write_metric_rows,
            write_metric_columns,
            write_metric_tables,
        }
    }
}

#[tonic::async_trait]
impl<D> write_service_server::WriteService for WriteService<D>
where
    D: DmlHandler<WriteInput = HashMap<String, MutableBatch>> + 'static,
{
    /// Receive a gRPC [`WriteRequest`] and dispatch it to the DML handler.
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let span_ctx: Option<SpanContext> = request.extensions().get().cloned();
        let database_batch = request
            .into_inner()
            .database_batch
            .ok_or_else(|| FieldViolation::required("database_batch"))?;

        let tables =
            mutable_batch_pb::decode::decode_database_batch(&database_batch).map_err(|e| {
                FieldViolation {
                    field: "database_batch".into(),
                    description: format!("Invalid DatabaseBatch: {}", e),
                }
            })?;

        let (row_count, column_count) =
            tables.values().fold((0, 0), |(acc_rows, acc_cols), batch| {
                let cols = batch
                    .schema(Selection::All)
                    .expect("failed to get schema")
                    .len();
                let rows = batch.rows();
                (acc_rows + rows, acc_cols + cols)
            });

        let namespace = database_batch
            .database_name
            .try_into()
            .map_err(|e| FieldViolation {
                field: "database_name".into(),
                description: format!("Invalid namespace: {}", e),
            })?;

        let num_tables = tables.len();
        debug!(
            num_tables,
            %namespace,
            "routing grpc write",
        );

        // TODO return the produced WriteSummary to the client
        // https://github.com/influxdata/influxdb_iox/issues/4208

        self.dml_handler
            .write(&namespace, tables, span_ctx)
            .await
            .map_err(|e| match e.into() {
                e @ DmlError::DatabaseNotFound(_) => Status::not_found(e.to_string()),
                e @ DmlError::Schema(_) => Status::aborted(e.to_string()),

                e @ (DmlError::Internal(_)
                | DmlError::WriteBuffer(_)
                | DmlError::NamespaceCreation(_)
                | DmlError::Partition(PartitionError::BatchWrite(_))) => {
                    Status::internal(e.to_string())
                }
            })?;

        self.write_metric_rows.inc(row_count as _);
        self.write_metric_columns.inc(column_count as _);
        self.write_metric_tables.inc(num_tables as _);

        Ok(Response::new(WriteResponse {}))
    }
}

#[derive(Debug)]
struct SchemaService {
    /// Catalog.
    catalog: Arc<dyn Catalog>,
}

impl SchemaService {
    fn new(catalog: Arc<dyn Catalog>) -> Self {
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
        let schema = get_schema_by_name(&req.namespace, repos.deref_mut())
            .await
            .map_err(|e| {
                warn!(error=%e, %req.namespace, "failed to retrieve namespace schema");
                Status::not_found(e.to_string())
            })
            .map(Arc::new)?;
        Ok(Response::new(schema_to_proto(schema)))
    }
}

fn schema_to_proto(schema: Arc<data_types2::NamespaceSchema>) -> GetSchemaResponse {
    let response = GetSchemaResponse {
        schema: Some(NamespaceSchema {
            id: schema.id.get(),
            kafka_topic_id: schema.kafka_topic_id.get(),
            query_pool_id: schema.query_pool_id.get() as i32,
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
    use std::sync::Arc;

    use data_types2::ColumnType;
    use generated_types::influxdata::{
        iox::schema::v1::schema_service_server::SchemaService,
        pbdata::v1::write_service_server::WriteService,
    };
    use iox_catalog::mem::MemCatalog;

    use crate::dml_handlers::{mock::MockDmlHandler, DmlError};

    use super::*;

    fn summary() -> WriteSummary {
        WriteSummary::default()
    }

    #[tokio::test]
    async fn test_write_no_batch() {
        let metrics = Arc::new(metric::Registry::default());
        let handler = Arc::new(MockDmlHandler::default().with_write_return([Ok(summary())]));
        let grpc = super::WriteService::new(Arc::clone(&handler), &metrics);

        let req = WriteRequest::default();

        let err = grpc
            .write(Request::new(req))
            .await
            .expect_err("rpc request should fail");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("database_batch"));
    }

    #[tokio::test]
    async fn test_write_no_namespace() {
        let metrics = Arc::new(metric::Registry::default());
        let handler = Arc::new(MockDmlHandler::default().with_write_return([Ok(summary())]));
        let grpc = super::WriteService::new(Arc::clone(&handler), &metrics);

        let req = WriteRequest {
            database_batch: Some(DatabaseBatch {
                database_name: "".to_owned(),
                table_batches: vec![],
            }),
        };

        let err = grpc
            .write(Request::new(req))
            .await
            .expect_err("rpc request should fail");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("database_name"));
    }

    #[tokio::test]
    async fn test_write_ok() {
        let metrics = Arc::new(metric::Registry::default());
        let handler = Arc::new(MockDmlHandler::default().with_write_return([Ok(summary())]));
        let grpc = super::WriteService::new(Arc::clone(&handler), &metrics);

        let req = WriteRequest {
            database_batch: Some(DatabaseBatch {
                database_name: "bananas".to_owned(),
                table_batches: vec![],
            }),
        };

        grpc.write(Request::new(req))
            .await
            .expect("rpc request should succeed");
    }

    #[tokio::test]
    async fn test_write_dml_handler_error() {
        let metrics = Arc::new(metric::Registry::default());
        let handler = Arc::new(
            MockDmlHandler::default()
                .with_write_return([Err(DmlError::DatabaseNotFound("nope".to_string()))]),
        );
        let grpc = super::WriteService::new(Arc::clone(&handler), &metrics);

        let req = WriteRequest {
            database_batch: Some(DatabaseBatch {
                database_name: "bananas".to_owned(),
                table_batches: vec![],
            }),
        };

        let err = grpc
            .write(Request::new(req))
            .await
            .expect_err("rpc request should fail");

        assert_eq!(err.code(), tonic::Code::NotFound);
        assert!(err.message().contains("nope"));
    }

    #[tokio::test]
    async fn test_schema() {
        // create a catalog and populate it with some test data, then drop the write lock
        let catalog = {
            let metrics = Arc::new(metric::Registry::default());
            let catalog = Arc::new(MemCatalog::new(metrics));
            let mut repos = catalog.repositories().await;
            let kafka = repos.kafka_topics().create_or_get("franz").await.unwrap();
            let pool = repos.query_pools().create_or_get("franz").await.unwrap();
            let namespace = repos
                .namespaces()
                .create("namespace_schema_test", "inf", kafka.id, pool.id)
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
