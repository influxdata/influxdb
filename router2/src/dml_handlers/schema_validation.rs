use super::DmlHandler;
use crate::namespace_cache::{metrics::InstrumentedCache, MemoryNamespaceCache, NamespaceCache};
use async_trait::async_trait;
use data_types2::{DatabaseName, DeletePredicate};
use hashbrown::HashMap;
use iox_catalog::{
    interface::{get_schema_by_name, Catalog},
    validate_or_insert_schema,
};
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

    /// The request failed during schema validation.
    ///
    /// NOTE: this may be due to transient I/O errors while interrogating the
    /// global catalog - the caller should inspect the inner error to determine
    /// the failure reason.
    #[error(transparent)]
    Validate(iox_catalog::interface::Error),
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
/// The catalog must serialise column creation to avoid `set(a=tag)` overwriting
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
}

impl<C> SchemaValidator<C> {
    /// Initialise a new [`SchemaValidator`] decorator, loading schemas from
    /// `catalog`.
    ///
    /// Schemas are cached in `ns_cache`.
    pub fn new(catalog: Arc<dyn Catalog>, ns_cache: C) -> Self {
        Self {
            catalog,
            cache: ns_cache,
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
    /// If the schema validation fails, [`SchemaError::Validate`] is returned.
    /// Callers should inspect the inner error to determine if the failure was
    /// caused by catalog I/O, or a schema conflict.
    ///
    /// A request that fails validation on one or more tables fails the request
    /// as a whole - calling this method has "all or nothing" semantics.
    async fn write(
        &self,
        namespace: &DatabaseName<'static>,
        batches: Self::WriteInput,
        _span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        // Load the namespace schema from the cache, falling back to pulling it
        // from the global catalog (if it exists).
        let schema = self.cache.get_schema(namespace);
        let schema = match schema {
            Some(v) => v,
            None => {
                // Pull the schema from the global catalog or error if it does
                // not exist.
                let mut repos = self.catalog.repositories().await;
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
            &*self.catalog,
        )
        .await
        .map_err(|e| {
            warn!(error=%e, %namespace, "schema validation failed");
            SchemaError::Validate(e)
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
    use data_types2::{ColumnType, KafkaTopicId, QueryPoolId, TimestampRange};
    use iox_catalog::mem::MemCatalog;
    use std::sync::Arc;

    lazy_static::lazy_static! {
        static ref NAMESPACE: DatabaseName<'static> = "bananas".try_into().unwrap();
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
        let metrics = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(metrics));

        let mut repos = catalog.repositories().await;
        repos
            .namespaces()
            .create(
                NAMESPACE.as_str(),
                "inf",
                KafkaTopicId::new(42),
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
        let handler = SchemaValidator::new(catalog, Arc::new(MemoryNamespaceCache::default()));

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
        let handler = SchemaValidator::new(catalog, Arc::new(MemoryNamespaceCache::default()));

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
        let handler = SchemaValidator::new(catalog, Arc::new(MemoryNamespaceCache::default()));

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

        assert_matches!(err, SchemaError::Validate(_));

        // The cache should retain the original schema.
        assert_cache(&handler, "bananas", "tag1", ColumnType::Tag);
        assert_cache(&handler, "bananas", "tag2", ColumnType::Tag);
        assert_cache(&handler, "bananas", "val", ColumnType::I64); // original type
        assert_cache(&handler, "bananas", "time", ColumnType::Time);
    }

    #[tokio::test]
    async fn test_write_delete_passthrough_ok() {
        const NAMESPACE: &str = "NAMESPACE_IS_NOT_VALIDATED";
        const TABLE: &str = "bananas";

        let catalog = create_catalog().await;
        let handler = SchemaValidator::new(catalog, Arc::new(MemoryNamespaceCache::default()));

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
