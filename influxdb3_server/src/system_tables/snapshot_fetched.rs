use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion::{error::DataFusionError, prelude::Expr};
use influxdb3_sys_events::{events::SnapshotFetchedEvent, SysEventStore, ToRecordBatch};
use iox_system_tables::IoxSystemTable;
use observability_deps::tracing::debug;

#[derive(Debug)]
pub(crate) struct SnapshotFetchedEventSysTable {
    sys_events_store: Arc<SysEventStore>,
    schema: Arc<Schema>,
}

impl SnapshotFetchedEventSysTable {
    pub fn new(sys_events_store: Arc<SysEventStore>) -> Self {
        Self {
            sys_events_store,
            schema: Arc::new(SnapshotFetchedEvent::schema()),
        }
    }
}

#[async_trait::async_trait]
impl IoxSystemTable for SnapshotFetchedEventSysTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let maybe_rec_batch = self
            .sys_events_store
            .as_record_batch::<SnapshotFetchedEvent>();

        debug!(
            ?maybe_rec_batch,
            "System table for snapshot fetched events query"
        );
        maybe_rec_batch
            .unwrap_or_else(|| Ok(RecordBatch::new_empty(Arc::clone(&self.schema))))
            .map_err(|err| DataFusionError::ArrowError(err, None))
    }
}
