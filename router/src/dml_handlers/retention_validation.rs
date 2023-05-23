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
    #[error(
        "data in table {table_name} is outside of the retention period: minimum \
        acceptable timestamp is {min_acceptable_ts}, but observed timestamp \
        {observed_ts} is older."
    )]
    OutsideRetention {
        /// The minimum row timestamp that will be considered within the
        /// retention period.
        min_acceptable_ts: iox_time::Time,
        /// The timestamp in the write that exceeds the retention minimum.
        observed_ts: iox_time::Time,
        /// The table name in which the observed timestamp was found.
        table_name: String,
    },
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
impl<P> DmlHandler for RetentionValidator<P>
where
    P: TimeProvider,
{
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
                        return Err(RetentionError::OutsideRetention {
                            table_name: table_name.clone(),
                            min_acceptable_ts: iox_time::Time::from_timestamp_nanos(min_retention),
                            observed_ts: iox_time::Time::from_timestamp_nanos(min),
                        });
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

    use assert_matches::assert_matches;
    use iox_tests::{TestCatalog, TestNamespace};
    use iox_time::MockProvider;
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

        let _result = handler
            .write(&NAMESPACE, namespace.schema().await.into(), writes, None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_time_outside_retention_period() {
        let namespace = test_setup().await;

        // Create the table so that there is a known ID that must be returned.
        let _want_id = namespace.create_table("bananas").await.table.id;

        let mock_now = iox_time::Time::from_rfc3339("2023-05-23T09:59:06+00:00").unwrap();
        let mock_time = MockProvider::new(mock_now);

        // Create the validator whse retention period is 1 hour
        let handler = RetentionValidator {
            time_provider: mock_time.clone(),
        };

        // Make time outside the retention period
        let two_hours_ago = (mock_now.timestamp_nanos() - 2 * 3_600 * 1_000_000_000).to_string();
        let line = "bananas,tag1=A,tag2=B val=42i ".to_string() + &two_hours_ago;
        let writes = lp_to_writes(&line);

        let result = handler
            .write(&NAMESPACE, namespace.schema().await.into(), writes, None)
            .await;

        // error means the time is outside the retention period
        assert_matches!(result, Err(e) => {
            assert_eq!(
                e.to_string(),
                "data in table bananas is outside of the retention period: \
                minimum acceptable timestamp is 2023-05-23T08:59:06+00:00, but \
                observed timestamp 2023-05-23T07:59:06+00:00 is older."
            )
        });
    }

    #[tokio::test]
    async fn test_time_partial_inside_retention_period() {
        let namespace = test_setup().await;

        // Create the table so that there is a known ID that must be returned.
        let _want_id = namespace.create_table("bananas").await.table.id;

        let mock_now = iox_time::Time::from_rfc3339("2023-05-23T09:59:06+00:00").unwrap();
        let mock_time = MockProvider::new(mock_now);

        // Create the validator whse retention period is 1 hour
        let handler = RetentionValidator {
            time_provider: mock_time.clone(),
        };

        // Make time now to be inside the retention period
        let now = mock_now.timestamp_nanos().to_string();
        let line1 = "bananas,tag1=A,tag2=B val=42i ".to_string() + &now;
        // Make time outside the retention period
        let two_hours_ago = (mock_now.timestamp_nanos() - 2 * 3_600 * 1_000_000_000).to_string();
        let line2 = "bananas,tag1=AA,tag2=BB val=422i ".to_string() + &two_hours_ago;
        // a lp with 2 lines, one inside and one outside retention period
        let lp = format!("{line1}\n{line2}");

        let writes = lp_to_writes(&lp);
        let result = handler
            .write(&NAMESPACE, namespace.schema().await.into(), writes, None)
            .await;

        // error means the time is outside the retention period
        assert_matches!(result, Err(e) => {
            assert_eq!(
                e.to_string(),
                "data in table bananas is outside of the retention period: \
                minimum acceptable timestamp is 2023-05-23T08:59:06+00:00, but \
                observed timestamp 2023-05-23T07:59:06+00:00 is older."
            )
        });
    }

    #[tokio::test]
    async fn test_one_table_inside_one_table_outside_retention_period() {
        let namespace = test_setup().await;

        // Create the table so that there is a known ID that must be returned.
        let _want_id = namespace.create_table("bananas").await.table.id;

        let mock_now = iox_time::Time::from_rfc3339("2023-05-23T09:59:06+00:00").unwrap();
        let mock_time = MockProvider::new(mock_now);

        // Create the validator whse retention period is 1 hour
        let handler = RetentionValidator {
            time_provider: mock_time.clone(),
        };

        // Make time now to be inside the retention period
        let now = mock_now.timestamp_nanos().to_string();
        let line1 = "bananas,tag1=A,tag2=B val=42i ".to_string() + &now;
        // Make time outside the retention period
        let two_hours_ago = (mock_now.timestamp_nanos() - 2 * 3_600 * 1_000_000_000).to_string();
        let line2 = "apple,tag1=AA,tag2=BB val=422i ".to_string() + &two_hours_ago;
        // a lp with 2 lines, one inside and one outside retention period
        let lp = format!("{line1}\n{line2}");

        let writes = lp_to_writes(&lp);
        let result = handler
            .write(&NAMESPACE, namespace.schema().await.into(), writes, None)
            .await;

        // error means the time is outside the retention period
        assert_matches!(result, Err(e) => {
            assert_eq!(
                e.to_string(),
                "data in table apple is outside of the retention period: minimum \
                 acceptable timestamp is 2023-05-23T08:59:06+00:00, but observed \
                 timestamp 2023-05-23T07:59:06+00:00 is older.")
        });
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
