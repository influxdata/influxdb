use std::{ops::DerefMut, sync::Arc};

use async_trait::async_trait;
use data_types::{DeletePredicate, NamespaceId, NamespaceName};
use hashbrown::HashMap;
use iox_catalog::interface::{get_schema_by_name, Catalog};
use iox_time::{SystemProvider, TimeProvider};
use mutable_batch::MutableBatch;
use observability_deps::tracing::*;
use thiserror::Error;
use trace::ctx::SpanContext;

use super::DmlHandler;
use crate::namespace_cache::{metrics::InstrumentedCache, MemoryNamespaceCache, NamespaceCache};

/// Errors emitted during retention validation.
#[derive(Debug, Error)]
pub enum RetentionError {
    /// The requested namespace could not be found in the catalog.
    #[error("failed to read namespace schema from catalog: {0}")]
    NamespaceLookup(iox_catalog::interface::Error),

    /// Time is outside the retention period.
    #[error("data in table {0} is outside of the retention period")]
    OutsideRetention(String),
}

/// A [`DmlHandler`] implementation that validates that the write is within the
/// retention period of the  namespace
#[derive(Debug)]
pub struct RetentionValidator<C = Arc<InstrumentedCache<MemoryNamespaceCache>>, P = SystemProvider>
{
    catalog: Arc<dyn Catalog>,
    cache: C,
    time_provider: P,
}

impl<C> RetentionValidator<C> {
    /// Initialise a new [`RetentionValidator`], rejecting time outside retention period
    pub fn new(catalog: Arc<dyn Catalog>, cache: C) -> Self {
        Self {
            catalog,
            cache,
            time_provider: Default::default(),
        }
    }
}

#[async_trait]
impl<C> DmlHandler for RetentionValidator<C>
where
    C: NamespaceCache,
{
    type WriteError = RetentionError;
    type DeleteError = RetentionError;

    type WriteInput = HashMap<String, MutableBatch>;
    type WriteOutput = Self::WriteInput;

    /// Partition the per-table [`MutableBatch`].
    async fn write(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_id: NamespaceId,
        batch: Self::WriteInput,
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
                        warn!(
                            error=%e,
                            %namespace,
                            %namespace_id,
                            "failed to retrieve namespace schema"
                        );
                        RetentionError::NamespaceLookup(e)
                    })
                    .map(Arc::new)?;

                self.cache
                    .put_schema(namespace.clone(), Arc::clone(&schema));

                trace!(%namespace, "schema cache populated");
                schema
            }
        };

        // retention is not infinte, validate all lines of a write are within the retention period
        if let Some(retention_period_ns) = schema.retention_period_ns {
            let min_retention = self.time_provider.now().timestamp_nanos() - retention_period_ns;
            // batch is a HashMap<tring, MutableBatch>
            for (table_name, batch) in &batch {
                if let Some(min) = batch.timestamp_summary().and_then(|v| v.stats.min) {
                    if min < min_retention {
                        return Err(RetentionError::OutsideRetention(table_name.clone()));
                    }
                }
            }
        };

        Ok(batch)
    }

    /// Pass the delete request through unmodified to the next handler.
    async fn delete(
        &self,
        _namespace: &NamespaceName<'static>,
        _namespace_id: NamespaceId,
        _table_name: &str,
        _predicate: &DeletePredicate,
        _span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use iox_tests::util::{TestCatalog, TestNamespace};
    use once_cell::sync::Lazy;
    use std::sync::Arc;

    use super::*;

    static NAMESPACE: Lazy<NamespaceName<'static>> = Lazy::new(|| "bananas".try_into().unwrap());

    #[tokio::test]
    async fn test_time_inside_retention_period() {
        let (catalog, namespace) = test_setup().await;

        // Create the table so that there is a known ID that must be returned.
        let _want_id = namespace.create_table("bananas").await.table.id;

        // Create the validator whse retention period is 1 hour
        let handler =
            RetentionValidator::new(catalog.catalog(), Arc::new(MemoryNamespaceCache::default()));

        // Make time now to be inside the retention period
        let now = SystemProvider::default()
            .now()
            .timestamp_nanos()
            .to_string();
        let line = "bananas,tag1=A,tag2=B val=42i ".to_string() + &now;
        let writes = lp_to_writes(&line);

        let result = handler
            .write(&NAMESPACE, NamespaceId::new(42), writes, None)
            .await;

        // no error means the time is inside the retention period
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_time_outside_retention_period() {
        let (catalog, namespace) = test_setup().await;

        // Create the table so that there is a known ID that must be returned.
        let _want_id = namespace.create_table("bananas").await.table.id;

        // Create the validator whose retention period is 1 hour
        let handler =
            RetentionValidator::new(catalog.catalog(), Arc::new(MemoryNamespaceCache::default()));

        // Make time outside the retention period
        let two_hours_ago = (SystemProvider::default().now().timestamp_nanos()
            - 2 * 3_600 * 1_000_000_000)
            .to_string();
        let line = "bananas,tag1=A,tag2=B val=42i ".to_string() + &two_hours_ago;
        let writes = lp_to_writes(&line);

        let result = handler
            .write(&NAMESPACE, NamespaceId::new(42), writes, None)
            .await;

        // error means the time is outside the retention period
        assert!(result.is_err());
        let message = result.unwrap_err().to_string();
        assert!(message.contains("data in table bananas is outside of the retention period"));
    }

    #[tokio::test]
    async fn test_time_partial_inside_retention_period() {
        let (catalog, namespace) = test_setup().await;

        // Create the table so that there is a known ID that must be returned.
        let _want_id = namespace.create_table("bananas").await.table.id;

        // Create the validator whse retention period is 1 hour
        let handler =
            RetentionValidator::new(catalog.catalog(), Arc::new(MemoryNamespaceCache::default()));

        // Make time now to be inside the retention period
        let now = SystemProvider::default()
            .now()
            .timestamp_nanos()
            .to_string();
        let line1 = "bananas,tag1=A,tag2=B val=42i ".to_string() + &now;
        // Make time outside the retention period
        let two_hours_ago = (SystemProvider::default().now().timestamp_nanos()
            - 2 * 3_600 * 1_000_000_000)
            .to_string();
        let line2 = "bananas,tag1=AA,tag2=BB val=422i ".to_string() + &two_hours_ago;
        // a lp with 2 lines, one inside and one outside retention period
        let lp = format!("{}\n{}", line1, line2);

        let writes = lp_to_writes(&lp);
        let result = handler
            .write(&NAMESPACE, NamespaceId::new(42), writes, None)
            .await;

        // error means the time is outside the retention period
        assert!(result.is_err());
        let message = result.unwrap_err().to_string();
        assert!(message.contains("data in table bananas is outside of the retention period"));
    }

    #[tokio::test]
    async fn test_one_table_inside_one_table_outside_retention_period() {
        let (catalog, namespace) = test_setup().await;

        // Create the table so that there is a known ID that must be returned.
        let _want_id = namespace.create_table("bananas").await.table.id;

        // Create the validator whse retention period is 1 hour
        let handler =
            RetentionValidator::new(catalog.catalog(), Arc::new(MemoryNamespaceCache::default()));

        // Make time now to be inside the retention period
        let now = SystemProvider::default()
            .now()
            .timestamp_nanos()
            .to_string();
        let line1 = "bananas,tag1=A,tag2=B val=42i ".to_string() + &now;
        // Make time outside the retention period
        let two_hours_ago = (SystemProvider::default().now().timestamp_nanos()
            - 2 * 3_600 * 1_000_000_000)
            .to_string();
        let line2 = "apple,tag1=AA,tag2=BB val=422i ".to_string() + &two_hours_ago;
        // a lp with 2 lines, one inside and one outside retention period
        let lp = format!("{}\n{}", line1, line2);

        let writes = lp_to_writes(&lp);
        let result = handler
            .write(&NAMESPACE, NamespaceId::new(42), writes, None)
            .await;

        // error means the time is outside the retention period
        assert!(result.is_err());
        let message = result.unwrap_err().to_string();
        assert!(message.contains("data in table apple is outside of the retention period"));
    }

    // Parse `lp` into a table-keyed MutableBatch map.
    fn lp_to_writes(lp: &str) -> HashMap<String, MutableBatch> {
        let (writes, _) = mutable_batch_lp::lines_to_batches_stats(lp, 42)
            .expect("failed to build test writes from LP");
        writes
    }

    /// Initialise an in-memory [`MemCatalog`] and create a single namespace
    /// named [`NAMESPACE`].
    async fn test_setup() -> (Arc<TestCatalog>, Arc<TestNamespace>) {
        let catalog = TestCatalog::new();
        let namespace = catalog.create_namespace_1hr_retention(&NAMESPACE).await;

        (catalog, namespace)
    }
}
