use std::sync::Arc;

use async_trait::async_trait;
use data_types::{delete_predicate::DeletePredicate, DatabaseName};
use hashbrown::HashMap;
use iox_catalog::{
    interface::{get_schema_by_name, Catalog},
    validate_or_insert_schema,
};
use mutable_batch::MutableBatch;
use thiserror::Error;
use trace::ctx::SpanContext;

use super::{DmlError, DmlHandler};

/// Errors emitted during schema validation.
#[derive(Debug, Error)]
pub enum SchemaError {
    /// The requested namespace could not be found in the catalog.
    #[error("failed to read namespace schema from catalog: {0}")]
    NamespaceLookup(iox_catalog::interface::Error),

    /// The request failed during schema validation.
    ///
    /// NOTE: this may be due to transient I/O errors while interrogating the
    /// global catalog - the caller should inspect the inner error to determine
    /// the failure reason.
    #[error(transparent)]
    Validate(iox_catalog::interface::Error),

    /// The inner DML handler returned an error.
    #[error(transparent)]
    Inner(Box<DmlError>),
}

/// A [`SchemaValidator`] checks the schema of incoming writes against a
/// centralised schema store.
#[derive(Debug)]
pub struct SchemaValidator<D> {
    inner: D,
    catalog: Arc<dyn Catalog>,
}

impl<D> SchemaValidator<D> {
    /// Initialise a new [`SchemaValidator`] decorator, loading schemas from
    /// `catalog` and passing acceptable requests through to `inner`.
    pub fn new(inner: D, catalog: Arc<dyn Catalog>) -> Self {
        Self { inner, catalog }
    }
}

#[async_trait]
impl<D> DmlHandler for SchemaValidator<D>
where
    D: DmlHandler,
{
    type WriteError = SchemaError;
    type DeleteError = D::DeleteError;

    /// Validate the schema of all the writes in `batches` before passing the
    /// request onto the inner handler.
    ///
    /// # Errors
    ///
    /// If `namespace` does not exist, [`SchemaError::NamespaceLookup`] is
    /// returned.
    ///
    /// If the schema validation fails, [`SchemaError::Validate`] is returned.
    /// Callers should inspect the inner error to determine if the failure was
    /// caused by catalog I/O, or a schema conflict.
    ///
    /// A request that fails validation on one or more tables fails the request
    /// as a whole - calling this method has "all or nothing" semantics.
    ///
    /// If the inner handler returns an error (wrapped in a
    /// [`SchemaError::Inner`]), the semantics of the inner handler write apply.
    async fn write(
        &self,
        namespace: DatabaseName<'static>,
        batches: HashMap<String, MutableBatch>,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::WriteError> {
        let schema = get_schema_by_name(&namespace, &*self.catalog)
            .await
            .map_err(SchemaError::NamespaceLookup)?;

        let _new_schema = validate_or_insert_schema(&batches, &schema, &*self.catalog)
            .await
            .map_err(SchemaError::Validate)?;

        self.inner
            .write(namespace, batches, span_ctx)
            .await
            .map_err(|e| SchemaError::Inner(Box::new(e.into())))?;

        Ok(())
    }

    /// This call is passed through to `D` - no schema validation is performed
    /// on deletes.
    async fn delete<'a>(
        &self,
        namespace: DatabaseName<'static>,
        table_name: impl Into<String> + Send + Sync + 'a,
        predicate: DeletePredicate,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        self.inner
            .delete(namespace, table_name, predicate, span_ctx)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use data_types::timestamp::TimestampRange;
    use iox_catalog::{
        interface::{KafkaTopicId, QueryPoolId},
        mem::MemCatalog,
    };
    use schema::{InfluxColumnType, InfluxFieldType};

    use crate::dml_handlers::mock::{MockDmlHandler, MockDmlHandlerCall};

    use super::*;

    const NAMESPACE: &str = "bananas";

    #[derive(Debug, Error)]
    enum MockError {
        #[error("terrible things")]
        Terrible,
    }

    // Parse `lp` into a table-keyed MutableBatch map.
    fn lp_to_writes(lp: &str) -> HashMap<String, MutableBatch> {
        let (writes, _) = mutable_batch_lp::lines_to_batches_stats(lp, 42)
            .expect("failed to build test writes from LP");
        writes
    }

    /// Initialise an in-memory [`MemCatalog`] and create a single namespace
    /// named [`NAMESPACE`].
    async fn create_catalog() -> Arc<dyn Catalog> {
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new());
        catalog
            .namespaces()
            .create(
                NAMESPACE,
                "inf",
                KafkaTopicId::new(42),
                QueryPoolId::new(24),
            )
            .await
            .expect("failed to create test namespace");
        catalog
    }

    #[tokio::test]
    async fn test_write_ok() {
        let catalog = create_catalog().await;
        let mock = Arc::new(MockDmlHandler::default().with_write_return(vec![Ok(())]));
        let handler = SchemaValidator::new(Arc::clone(&mock), catalog);

        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456");
        handler
            .write(NAMESPACE.try_into().unwrap(), writes, None)
            .await
            .expect("request should succeed");

        // THe mock should observe exactly one write.
        assert_matches!(mock.calls().as_slice(), [MockDmlHandlerCall::Write{namespace, ..}] => {
            assert_eq!(namespace, NAMESPACE);
        });
    }

    #[tokio::test]
    async fn test_write_schema_not_found() {
        let catalog = create_catalog().await;
        let mock = Arc::new(MockDmlHandler::default().with_write_return(vec![Ok(())]));
        let handler = SchemaValidator::new(Arc::clone(&mock), catalog);

        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456");
        let err = handler
            .write("A_DIFFERENT_NAMESPACE".try_into().unwrap(), writes, None)
            .await
            .expect_err("request should fail");

        assert_matches!(err, SchemaError::NamespaceLookup(_));
        assert!(mock.calls().is_empty());
    }

    #[tokio::test]
    async fn test_write_validation_failure() {
        let catalog = create_catalog().await;
        let mock = Arc::new(MockDmlHandler::default().with_write_return(vec![Ok(())]));
        let handler = SchemaValidator::new(Arc::clone(&mock), catalog);

        // First write sets the schema
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456"); // val=i64
        handler
            .write(NAMESPACE.try_into().unwrap(), writes, None)
            .await
            .expect("request should succeed");

        // Second write attempts to violate it causing an error
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42.0 123456"); // val=float
        let err = handler
            .write(NAMESPACE.try_into().unwrap(), writes, None)
            .await
            .expect_err("request should fail");

        assert_matches!(err, SchemaError::Validate(_));

        // THe mock should observe exactly one write from the first call.
        assert_matches!(mock.calls().as_slice(), [MockDmlHandlerCall::Write{namespace, batches}] => {
            assert_eq!(namespace, NAMESPACE);
            let batch = batches.get("bananas").expect("table not found in write");
            assert_eq!(batch.rows(), 1);
            let col = batch.column("val").expect("column not found in write");
            assert_matches!(col.influx_type(), InfluxColumnType::Field(InfluxFieldType::Integer));
        });
    }

    #[tokio::test]
    async fn test_write_inner_handler_error() {
        let catalog = create_catalog().await;
        let mock = Arc::new(
            MockDmlHandler::default()
                .with_write_return(vec![Err(DmlError::Internal(MockError::Terrible.into()))]),
        );
        let handler = SchemaValidator::new(Arc::clone(&mock), catalog);

        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456");
        let err = handler
            .write(NAMESPACE.try_into().unwrap(), writes, None)
            .await
            .expect_err("request should return mock error");

        assert_matches!(err, SchemaError::Inner(e) => {
            assert_matches!(*e, DmlError::Internal(_));
        });

        // The mock should observe exactly one write.
        assert_matches!(mock.calls().as_slice(), [MockDmlHandlerCall::Write { .. }]);
    }

    #[tokio::test]
    async fn test_write_delete_passthrough_ok() {
        const NAMESPACE: &str = "NAMESPACE_IS_NOT_VALIDATED";
        const TABLE: &str = "bananas";

        let catalog = create_catalog().await;
        let mock = Arc::new(MockDmlHandler::default().with_delete_return(vec![Ok(())]));
        let handler = SchemaValidator::new(Arc::clone(&mock), catalog);

        let predicate = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };

        handler
            .delete(
                NAMESPACE.try_into().unwrap(),
                TABLE,
                predicate.clone(),
                None,
            )
            .await
            .expect("request should succeed");

        // The mock should observe exactly one delete.
        assert_matches!(mock.calls().as_slice(), [MockDmlHandlerCall::Delete { namespace, predicate: got, table }] => {
            assert_eq!(namespace, NAMESPACE);
            assert_eq!(predicate, *got);
            assert_eq!(table, TABLE);
        });
    }

    #[tokio::test]
    async fn test_write_delete_passthrough_err() {
        let catalog = create_catalog().await;
        let mock = Arc::new(
            MockDmlHandler::default()
                .with_delete_return(vec![Err(DmlError::Internal(MockError::Terrible.into()))]),
        );
        let handler = SchemaValidator::new(Arc::clone(&mock), catalog);

        let predicate = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };

        let err = handler
            .delete(
                "NAMESPACE_IS_IGNORED".try_into().unwrap(),
                "bananas",
                predicate,
                None,
            )
            .await
            .expect_err("request should return mock error");

        assert_matches!(err, DmlError::Internal(_));

        // The mock should observe exactly one delete.
        assert_matches!(mock.calls().as_slice(), [MockDmlHandlerCall::Delete { .. }]);
    }
}
