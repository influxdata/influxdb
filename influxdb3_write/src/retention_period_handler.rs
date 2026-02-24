//! Background task for enforcing retention policies by deleting expired data.

use crate::table_index_cache::TableIndexCache;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::{DbId, TableId};
use influxdb3_shutdown::ShutdownToken;
use iox_time::TimeProvider;
use observability_deps::tracing::{debug, error, info};
use std::sync::Arc;
use std::time::Duration;

/// Handles periodic enforcement of retention policies by deleting expired parquet files.
#[derive(Debug)]
pub struct RetentionPeriodHandler {
    table_index_cache: TableIndexCache,
    catalog: Arc<Catalog>,
    time_provider: Arc<dyn TimeProvider>,
    check_interval: Duration,
    node_id: String,
}

impl RetentionPeriodHandler {
    /// Create a new retention period handler.
    pub fn new(
        table_index_cache: TableIndexCache,
        catalog: Arc<Catalog>,
        time_provider: Arc<dyn TimeProvider>,
        check_interval: Duration,
        node_id: String,
    ) -> Self {
        Self {
            table_index_cache,
            catalog,
            time_provider,
            check_interval,
            node_id,
        }
    }

    /// Run the background task that periodically checks and enforces retention policies.
    pub async fn background_task(self: Arc<Self>, shutdown_token: ShutdownToken) {
        info!(
            check_interval_seconds = self.check_interval.as_secs(),
            "Starting retention period handler background task"
        );

        loop {
            let next_check_time = self.time_provider.now() + self.check_interval;

            tokio::select! {
                _ = self.time_provider.sleep_until(next_check_time) => {
                    self.check_and_enforce_retention().await;
                }
                _ = shutdown_token.wait_for_shutdown() => {
                    info!("Retention period handler shutting down");
                    break;
                }
            }
        }
    }

    /// Check all tables for retention policies and enforce them.
    async fn check_and_enforce_retention(&self) {
        debug!("Checking retention policies");

        // Get the retention period cutoff map from the catalog
        let cutoff_map = self.catalog.get_retention_period_cutoff_map();

        if cutoff_map.is_empty() {
            debug!("No retention policies configured");
            return;
        }

        info!(
            num_retention_policies = cutoff_map.len(),
            "Processing retention policies"
        );

        // Process each database and table with a retention policy
        for ((db_id, table_id), cutoff_time_ns) in cutoff_map {
            self.enforce_retention_for_table(db_id, table_id, cutoff_time_ns)
                .await;
        }
    }

    /// Enforce retention for a specific table.
    async fn enforce_retention_for_table(
        &self,
        db_id: DbId,
        table_id: TableId,
        cutoff_time_ns: i64,
    ) {
        debug!(
            ?db_id,
            ?table_id,
            cutoff_time_ns,
            "Enforcing retention policy for table"
        );

        match self
            .table_index_cache
            .purge_expired(&self.node_id, db_id, table_id, cutoff_time_ns)
            .await
        {
            Ok(()) => {
                info!(
                    ?db_id,
                    ?table_id,
                    cutoff_time_ns,
                    "Successfully enforced retention policy"
                );
            }
            Err(e) => {
                error!(
                    ?db_id,
                    ?table_id,
                    cutoff_time_ns,
                    error = %e,
                    "Failed to enforce retention policy"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests;
