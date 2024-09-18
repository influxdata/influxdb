//! Logic for running many compaction tasks in parallel.

use crate::planner::SnapshotAdvancePlan;
use datafusion::execution::object_store::ObjectStoreUrl;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_pro_data_layout::CompactionSummary;
use iox_query::exec::Executor;
use object_store::ObjectStore;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CompactRunnerError {}

/// Run all gen1 to gen2 compactions in the snapshot plan, writing the `CompactionDetail`s as
/// we go and then writing the `CompactionSummary` to object store and returning it at the end.
pub(crate) async fn run_snapshot_plan(
    _snapshot_advance_plan: SnapshotAdvancePlan,
    _compactor_id: Arc<str>,
    _catalog: Arc<Catalog>,
    _object_store: Arc<dyn ObjectStore>,
    _object_store_url: ObjectStoreUrl,
    _exec: Arc<Executor>,
) -> Result<CompactionSummary, CompactRunnerError> {
    unimplemented!("run_snapshot_plan")
}
