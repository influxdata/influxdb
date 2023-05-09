use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};
use hashbrown::HashMap;
use iox_time::{SystemProvider, TimeProvider};
use mutable_batch::MutableBatch;
use observability_deps::tracing::*;
use std::sync::Arc;
use thiserror::Error;
use trace::ctx::SpanContext;

use super::DmlHandler;

/// Errors emitted during retention validation.
#[derive(Debug, Error)]
pub enum RetentionError {
    /// Time is outside the retention period.
    #[error("data in table {0} is outside of the retention period")]
    OutsideRetention(String),
}

/// A [`DmlHandler`] implementation that validates that the write is within the
/// retention period of the namespace.
///
/// Each row of data being wrote is inspected, and if any "time" column
/// timestamp lays outside of the configured namespace retention period, the
/// entire write is rejected.
#[derive(Debug, Default)]
pub struct RetentionValidator<P = SystemProvider> {
    time_provider: P,
}

impl RetentionValidator {
    /// Initialise a new [`RetentionValidator`], rejecting time outside retention period
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl DmlHandler for RetentionValidator {
    type WriteError = RetentionError;

    type WriteInput = HashMap<String, MutableBatch>;
    type WriteOutput = Self::WriteInput;

    /// Partition the per-table [`MutableBatch`].
    async fn write(
        &self,
        _namespace: &NamespaceName<'static>,
        namespace_schema: Arc<NamespaceSchema>,
        batch: Self::WriteInput,
        _span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        // retention is not infinte, validate all lines of a write are within the retention period
        if let Some(retention_period_ns) = namespace_schema.retention_period_ns {
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iox_tests::{TestCatalog, TestNamespace};
    use once_cell::sync::Lazy;

    use super::*;

    static NAMESPACE: Lazy<NamespaceName<'static>> = Lazy::new(|| "bananas".try_into().unwrap());

    #[tokio::test]
    async fn test_time_inside_retention_period() {
        let namespace = test_setup().await;

        // Create the table so that there is a known ID that must be returned.
        let _want_id = namespace.create_table("bananas").await.table.id;

        // Create the validator whose retention period is 1 hour
        let handler = RetentionValidator::new();

        // Make time now to be inside the retention period
        let now = SystemProvider::default()
            .now()
            .timestamp_nanos()
            .to_string();
        let line = "bananas,tag1=A,tag2=B val=42i ".to_string() + &now;
        let writes = lp_to_writes(&line);

        let result = handler
            .write(&NAMESPACE, namespace.schema().await.into(), writes, None)
            .await;

        // no error means the time is inside the retention period
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_time_outside_retention_period() {
        let namespace = test_setup().await;

        // Create the table so that there is a known ID that must be returned.
        let _want_id = namespace.create_table("bananas").await.table.id;

        // Create the validator whose retention period is 1 hour
        let handler = RetentionValidator::new();

        // Make time outside the retention period
        let two_hours_ago = (SystemProvider::default().now().timestamp_nanos()
            - 2 * 3_600 * 1_000_000_000)
            .to_string();
        let line = "bananas,tag1=A,tag2=B val=42i ".to_string() + &two_hours_ago;
        let writes = lp_to_writes(&line);

        let result = handler
            .write(&NAMESPACE, namespace.schema().await.into(), writes, None)
            .await;

        // error means the time is outside the retention period
        assert!(result.is_err());
        let message = result.unwrap_err().to_string();
        assert!(message.contains("data in table bananas is outside of the retention period"));
    }

    #[tokio::test]
    async fn test_time_partial_inside_retention_period() {
        let namespace = test_setup().await;

        // Create the table so that there is a known ID that must be returned.
        let _want_id = namespace.create_table("bananas").await.table.id;

        // Create the validator whose retention period is 1 hour
        let handler = RetentionValidator::new();

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
        let lp = format!("{line1}\n{line2}");

        let writes = lp_to_writes(&lp);
        let result = handler
            .write(&NAMESPACE, namespace.schema().await.into(), writes, None)
            .await;

        // error means the time is outside the retention period
        assert!(result.is_err());
        let message = result.unwrap_err().to_string();
        assert!(message.contains("data in table bananas is outside of the retention period"));
    }

    #[tokio::test]
    async fn test_one_table_inside_one_table_outside_retention_period() {
        let namespace = test_setup().await;

        // Create the table so that there is a known ID that must be returned.
        let _want_id = namespace.create_table("bananas").await.table.id;

        // Create the validator whse retention period is 1 hour
        let handler = RetentionValidator::new();

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
        let lp = format!("{line1}\n{line2}");

        let writes = lp_to_writes(&lp);
        let result = handler
            .write(&NAMESPACE, namespace.schema().await.into(), writes, None)
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
    async fn test_setup() -> Arc<TestNamespace> {
        let catalog = TestCatalog::new();

        catalog.create_namespace_1hr_retention(&NAMESPACE).await
    }
}
