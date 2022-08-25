use super::DmlHandler;
use crate::namespace_cache::{metrics::InstrumentedCache, MemoryNamespaceCache, NamespaceCache};
use async_trait::async_trait;
use data_types::{DatabaseName, DeletePredicate};
use hashbrown::HashMap;
use iox_catalog::{
    interface::{get_schema_by_name, Catalog, Error as CatalogError},
    validate_or_insert_schema,
};
use metric::U64Counter;
use mutable_batch::MutableBatch;
use observability_deps::tracing::*;
use std::{ops::DerefMut, sync::Arc};
use thiserror::Error;
use trace::ctx::SpanContext;

/// Errors emitted during schema validation.
#[derive(Debug, Error)]
pub enum SchemaError {
    /// The requested namespace could not be found in the catalog.
    #[error("failed to read namespace schema from catalog: {0}")]
    NamespaceLookup(iox_catalog::interface::Error),

    /// The user has hit their column/table limit.
    #[error("service limit reached: {0}")]
    ServiceLimit(iox_catalog::interface::Error),

    /// The request schema conflicts with the existing namespace schema.
    #[error("schema conflict: {0}")]
    Conflict(iox_catalog::TableScopedError),

    /// A catalog error during schema validation.
    ///
    /// NOTE: this may be due to transient I/O errors while interrogating the
    /// global catalog - the caller should inspect the inner error to determine
    /// the failure reason.
    #[error(transparent)]
    UnexpectedCatalogError(iox_catalog::interface::Error),
}

/// A [`SchemaValidator`] checks the schema of incoming writes against a
/// centralised schema store, maintaining an in-memory cache of all observed
/// schemas.
///
/// # Caching
///
/// This validator attempts to incrementally build an in-memory cache of all
/// table schemas it observes (never evicting the schemas).
///
/// All schema operations are scoped to a single namespace.
///
/// When a request contains columns that do not exist in the cached schema, the
/// catalog is queried for an existing column by the same name and (atomically)
/// the column is created if it does not exist. If the requested column type
/// conflicts with the catalog column, the request is rejected and the cached
/// schema is not updated.
///
/// Any successful write that adds new columns causes the new schema to be
/// cached.
///
/// To minimise locking, this cache is designed to allow (and tolerate) spurious
/// cache "updates" racing with each other and overwriting newer schemas with
/// older schemas. This is acceptable due to the incremental, additive schema
/// creation never allowing a column to change or be removed, therefore columns
/// lost by racy schema cache overwrites are "discovered" in subsequent
/// requests. This overwriting is scoped to the namespace, and is expected to be
/// relatively rare - it results in additional requests being made to the
/// catalog until the cached schema converges to match the catalog schema.
///
/// # Correctness
///
/// The correct functioning of this schema validator relies on the catalog
/// implementation correctly validating new column requests against any existing
/// (but uncached) columns, returning an error if the requested type does not
/// match.
///
/// The catalog must serialize column creation to avoid `set(a=tag)` overwriting
/// a prior `set(a=int)` write to the catalog (returning an error that `a` is an
/// `int` for the second request).
///
/// Because each column is set incrementally and independently of other new
/// columns in the request, racing multiple requests for the same table can
/// produce incorrect schemas ([#3573]).
///
/// [#3573]: https://github.com/influxdata/influxdb_iox/issues/3573
#[derive(Debug)]
pub struct SchemaValidator<C = Arc<InstrumentedCache<MemoryNamespaceCache>>> {
    catalog: Arc<dyn Catalog>,
    cache: C,

    service_limit_hit: U64Counter,
    schema_conflict: U64Counter,
}

impl<C> SchemaValidator<C> {
    /// Initialise a new [`SchemaValidator`] decorator, loading schemas from
    /// `catalog`.
    ///
    /// Schemas are cached in `ns_cache`.
    pub fn new(catalog: Arc<dyn Catalog>, ns_cache: C, metrics: &metric::Registry) -> Self {
        let service_limit_hit = metrics
            .register_metric::<U64Counter>(
                "schema_validation_service_limit_reached",
                "number of requests that have hit the namespace table/column limit",
            )
            .recorder(&[]);
        let schema_conflict = metrics
            .register_metric::<U64Counter>(
                "schema_validation_schema_conflict",
                "number of requests that fail due to a schema conflict",
            )
            .recorder(&[]);

        Self {
            catalog,
            cache: ns_cache,
            service_limit_hit,
            schema_conflict,
        }
    }
}

#[async_trait]
impl<C> DmlHandler for SchemaValidator<C>
where
    C: NamespaceCache,
{
    type WriteError = SchemaError;
    type DeleteError = SchemaError;

    type WriteInput = HashMap<String, MutableBatch>;
    type WriteOutput = Self::WriteInput;

    /// Validate the schema of all the writes in `batches`.
    ///
    /// # Errors
    ///
    /// If `namespace` does not exist, [`SchemaError::NamespaceLookup`] is
    /// returned.
    ///
    /// If the schema validation fails due to a schema conflict in the request,
    /// [`SchemaError::Conflict`] is returned.
    ///
    /// If the schema validation fails due to a service limit being reached,
    /// [`SchemaError::ServiceLimit`] is returned.
    ///
    /// A request that fails validation on one or more tables fails the request
    /// as a whole - calling this method has "all or nothing" semantics.
    async fn write(
        &self,
        namespace: &DatabaseName<'static>,
        batches: Self::WriteInput,
        _span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        let mut repos = self.catalog.repositories().await;

        // Load the namespace schema from the cache, falling back to pulling it
        // from the global catalog (if it exists).
        let schema = self.cache.get_schema(namespace);
        let schema = match schema {
            Some(v) => v,
            None => {
                // Pull the schema from the global catalog or error if it does
                // not exist.
                let schema = get_schema_by_name(namespace, repos.deref_mut())
                    .await
                    .map_err(|e| {
                        warn!(error=%e, %namespace, "failed to retrieve namespace schema");
                        SchemaError::NamespaceLookup(e)
                    })
                    .map(Arc::new)?;

                self.cache
                    .put_schema(namespace.clone(), Arc::clone(&schema));

                trace!(%namespace, "schema cache populated");
                schema
            }
        };

        let maybe_new_schema = validate_or_insert_schema(
            batches.iter().map(|(k, v)| (k.as_str(), v)),
            &schema,
            repos.deref_mut(),
        )
        .await
        .map_err(|e| {
            match e.err() {
                // Schema conflicts
                CatalogError::ColumnTypeMismatch {
                    ref name,
                    ref existing,
                    ref new,
                } => {
                    warn!(
                        %namespace,
                        column_name=%name,
                        existing_column_type=%existing,
                        request_column_type=%new,
                        table_name=%e.table(),
                        "schema conflict"
                    );
                    self.schema_conflict.inc(1);
                    SchemaError::Conflict(e)
                }
                // Service limits
                CatalogError::ColumnCreateLimitError { .. }
                | CatalogError::TableCreateLimitError { .. } => {
                    warn!(%namespace, error=%e, "service protection limit reached");
                    self.service_limit_hit.inc(1);
                    SchemaError::ServiceLimit(e.into_err())
                }
                _ => {
                    error!(%namespace, error=%e, "schema validation failed");
                    SchemaError::UnexpectedCatalogError(e.into_err())
                }
            }
        })?
        .map(Arc::new);

        trace!(%namespace, "schema validation complete");

        // If the schema has been updated, immediately add it to the cache
        // (before passing through the write) in order to allow subsequent,
        // parallel requests to use it while waiting on this request to
        // complete.
        match maybe_new_schema {
            Some(v) => {
                // This call MAY overwrite a more-up-to-date cache entry if
                // racing with another request for the same namespace, but the
                // cache will eventually converge in subsequent requests.
                self.cache.put_schema(namespace.clone(), v);
                trace!(%namespace, "schema cache updated");
            }
            None => {
                trace!(%namespace, "schema unchanged");
            }
        }

        Ok(batches)
    }

    /// This call is passed through to `D` - no schema validation is performed
    /// on deletes.
    async fn delete(
        &self,
        _namespace: &DatabaseName<'static>,
        _table_name: &str,
        _predicate: &DeletePredicate,
        _span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use data_types::{ColumnType, QueryPoolId, TimestampRange, TopicId};
    use iox_catalog::mem::MemCatalog;
    use once_cell::sync::Lazy;
    use std::sync::Arc;

    static NAMESPACE: Lazy<DatabaseName<'static>> = Lazy::new(|| "bananas".try_into().unwrap());

    // Parse `lp` into a table-keyed MutableBatch map.
    fn lp_to_writes(lp: &str) -> HashMap<String, MutableBatch> {
        let (writes, _) = mutable_batch_lp::lines_to_batches_stats(lp, 42)
            .expect("failed to build test writes from LP");
        writes
    }

    /// Initialise an in-memory [`MemCatalog`] and create a single namespace
    /// named [`NAMESPACE`].
    async fn create_catalog() -> Arc<dyn Catalog> {
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));

        let mut repos = catalog.repositories().await;
        repos
            .namespaces()
            .create(
                NAMESPACE.as_str(),
                "inf",
                TopicId::new(42),
                QueryPoolId::new(24),
            )
            .await
            .expect("failed to create test namespace");

        catalog
    }

    fn assert_cache<C>(handler: &SchemaValidator<C>, table: &str, col: &str, want: ColumnType)
    where
        C: NamespaceCache,
    {
        // The cache should be populated.
        let ns = handler
            .cache
            .get_schema(&*NAMESPACE)
            .expect("cache should be populated");
        let table = ns.tables.get(table).expect("table should exist in cache");
        assert_eq!(
            table
                .columns
                .get(col)
                .expect("column not cached")
                .column_type,
            want
        );
    }

    #[tokio::test]
    async fn test_write_ok() {
        let catalog = create_catalog().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(
            catalog,
            Arc::new(MemoryNamespaceCache::default()),
            &*metrics,
        );

        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456");
        handler
            .write(&*NAMESPACE, writes, None)
            .await
            .expect("request should succeed");

        // The cache should be populated.
        assert_cache(&handler, "bananas", "tag1", ColumnType::Tag);
        assert_cache(&handler, "bananas", "tag2", ColumnType::Tag);
        assert_cache(&handler, "bananas", "val", ColumnType::I64);
        assert_cache(&handler, "bananas", "time", ColumnType::Time);
    }

    #[tokio::test]
    async fn test_write_schema_not_found() {
        let catalog = create_catalog().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(
            catalog,
            Arc::new(MemoryNamespaceCache::default()),
            &*metrics,
        );

        let ns = DatabaseName::try_from("A_DIFFERENT_NAMESPACE").unwrap();

        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456");
        let err = handler
            .write(&ns, writes, None)
            .await
            .expect_err("request should fail");

        assert_matches!(err, SchemaError::NamespaceLookup(_));

        // The cache should not have retained the schema.
        assert!(handler.cache.get_schema(&ns).is_none());
    }

    #[tokio::test]
    async fn test_write_validation_failure() {
        let catalog = create_catalog().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(
            catalog,
            Arc::new(MemoryNamespaceCache::default()),
            &*metrics,
        );

        // First write sets the schema
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456"); // val=i64
        let got = handler
            .write(&*NAMESPACE, writes.clone(), None)
            .await
            .expect("request should succeed");
        assert_eq!(writes.len(), got.len());

        // Second write attempts to violate it causing an error
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42.0 123456"); // val=float
        let err = handler
            .write(&*NAMESPACE, writes, None)
            .await
            .expect_err("request should fail");

        assert_matches!(err, SchemaError::Conflict(e) => {
            assert_eq!(e.table(), "bananas");
        });

        // The cache should retain the original schema.
        assert_cache(&handler, "bananas", "tag1", ColumnType::Tag);
        assert_cache(&handler, "bananas", "tag2", ColumnType::Tag);
        assert_cache(&handler, "bananas", "val", ColumnType::I64); // original type
        assert_cache(&handler, "bananas", "time", ColumnType::Time);

        assert_eq!(1, handler.schema_conflict.fetch());
    }

    #[tokio::test]
    async fn test_write_table_service_limit() {
        let catalog = create_catalog().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(
            Arc::clone(&catalog),
            Arc::new(MemoryNamespaceCache::default()),
            &*metrics,
        );

        // First write sets the schema
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456");
        let got = handler
            .write(&*NAMESPACE, writes.clone(), None)
            .await
            .expect("request should succeed");
        assert_eq!(writes.len(), got.len());

        // Configure the service limit to be hit next request
        catalog
            .repositories()
            .await
            .namespaces()
            .update_table_limit(NAMESPACE.as_str(), 1)
            .await
            .expect("failed to set table limit");

        // Second write attempts to violate limits, causing an error
        let writes = lp_to_writes("bananas2,tag1=A,tag2=B val=42i 123456");
        let err = handler
            .write(&*NAMESPACE, writes, None)
            .await
            .expect_err("request should fail");

        assert_matches!(err, SchemaError::ServiceLimit(_));
        assert_eq!(1, handler.service_limit_hit.fetch());
    }

    #[tokio::test]
    async fn test_write_column_service_limit() {
        let catalog = create_catalog().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(
            Arc::clone(&catalog),
            Arc::new(MemoryNamespaceCache::default()),
            &*metrics,
        );

        // First write sets the schema
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456");
        let got = handler
            .write(&*NAMESPACE, writes.clone(), None)
            .await
            .expect("request should succeed");
        assert_eq!(writes.len(), got.len());

        // Configure the service limit to be hit next request
        catalog
            .repositories()
            .await
            .namespaces()
            .update_column_limit(NAMESPACE.as_str(), 1)
            .await
            .expect("failed to set column limit");

        // Second write attempts to violate limits, causing an error
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i,val2=42i 123456");
        let err = handler
            .write(&*NAMESPACE, writes, None)
            .await
            .expect_err("request should fail");

        assert_matches!(err, SchemaError::ServiceLimit(_));
        assert_eq!(1, handler.service_limit_hit.fetch());
    }

    #[tokio::test]
    async fn test_write_delete_passthrough_ok() {
        const NAMESPACE: &str = "NAMESPACE_IS_NOT_VALIDATED";
        const TABLE: &str = "bananas";

        let catalog = create_catalog().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(
            catalog,
            Arc::new(MemoryNamespaceCache::default()),
            &*metrics,
        );

        let predicate = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };

        let ns = DatabaseName::try_from(NAMESPACE).unwrap();

        handler
            .delete(&ns, TABLE, &predicate, None)
            .await
            .expect("request should succeed");

        // Deletes have no effect on the cache.
        assert!(handler.cache.get_schema(&ns).is_none());
    }
}
