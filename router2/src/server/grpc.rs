//! gRPC service implementations for `router2`.

use std::sync::Arc;

use generated_types::{google::FieldViolation, influxdata::pbdata::v1::*};
use hashbrown::HashMap;
use metric::U64Counter;
use mutable_batch::MutableBatch;
use observability_deps::tracing::*;
use schema::selection::Selection;
use tonic::{Request, Response, Status};
use trace::ctx::SpanContext;

use crate::dml_handlers::{DmlError, DmlHandler, PartitionError};

/// This type is responsible for managing all gRPC services exposed by
/// `router2`.
#[derive(Debug, Default)]
pub struct GrpcDelegate<D> {
    dml_handler: Arc<D>,
    metrics: Arc<metric::Registry>,
}

impl<D> GrpcDelegate<D> {
    /// Initialise a new gRPC handler, dispatching DML operations to
    /// `dml_handler`.
    pub fn new(dml_handler: Arc<D>, metrics: Arc<metric::Registry>) -> Self {
        Self {
            dml_handler,
            metrics,
        }
    }
}

impl<D> GrpcDelegate<D>
where
    D: DmlHandler<WriteInput = HashMap<String, MutableBatch>> + 'static,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use generated_types::influxdata::pbdata::v1::write_service_server::WriteService;

    use crate::dml_handlers::{mock::MockDmlHandler, DmlError};

    use super::*;

    #[tokio::test]
    async fn test_write_no_batch() {
        let metrics = Arc::new(metric::Registry::default());
        let handler = Arc::new(MockDmlHandler::default().with_write_return([Ok(())]));
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
        let handler = Arc::new(MockDmlHandler::default().with_write_return([Ok(())]));
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
        let handler = Arc::new(MockDmlHandler::default().with_write_return([Ok(())]));
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
}
