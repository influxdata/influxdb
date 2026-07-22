use std::{any::Any, sync::Arc};

use anyhow::{Context, Result};
use influxdb3_catalog::catalog::DatabaseSchema;
use influxdb3_py_api::wal::WalFlushElement;
use influxdb3_wal::{SnapshotDetails, WalContents, WalFileNotifier, WalOp, WriteBatch};
use influxdb3_write::write_buffer::table_buffer::TableBuffer;
use observability_deps::tracing::error;
use tokio::sync::oneshot::Receiver;

use crate::ProcessingEngineManagerImpl;

impl ProcessingEngineManagerImpl {
    async fn process_write_batch(&self, write_batch: &WriteBatch) -> Result<()> {
        let db_name = Arc::clone(&write_batch.database_name);
        let db_schema = self.catalog.db_schema(&db_name).context("DB not found")?;

        let wal_content = write_batch_to_wal_content(write_batch, &db_schema)?;

        let invocations = {
            let trigger_registry = self.trigger_registry.read().await;
            trigger_registry.wal_invocations(db_name, wal_content)
        };
        Self::enqueue_wal_invocations(&self.scheduler, invocations).await;

        Ok(())
    }
}

#[async_trait::async_trait]
impl WalFileNotifier for ProcessingEngineManagerImpl {
    async fn notify(&self, write: Arc<WalContents>) {
        for wal_op in write.ops.iter() {
            match wal_op {
                WalOp::Write(write_batch) => {
                    if let Err(e) = self.process_write_batch(write_batch).await {
                        error!("failed to process write batch: {}", e);
                    }
                }
                WalOp::Noop(_) => {}
            }
        }
    }

    async fn notify_and_snapshot(
        &self,
        write: Arc<WalContents>,
        snapshot_details: SnapshotDetails,
    ) -> Receiver<SnapshotDetails> {
        self.notify(write).await;

        // This method requires us to signal the caller when we took the snapshot. We're done immediately.
        let (tx, rx) = tokio::sync::oneshot::channel();
        tx.send(snapshot_details).ok();
        rx
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(crate) fn write_batch_to_wal_content(
    write_batch: &WriteBatch,
    db_schema: &DatabaseSchema,
) -> Result<Arc<[WalFlushElement]>> {
    let mut wal_content = Vec::with_capacity(write_batch.table_chunks.len());

    for (table_id, table_chunks) in &write_batch.table_chunks {
        let table_def = db_schema
            .table_definition_by_id(table_id)
            .context("Table not found")?;

        let mut table_buffer = TableBuffer::new();
        for (t, chunk) in &table_chunks.chunk_time_to_chunk {
            table_buffer.buffer_chunk(*t, &chunk.rows);
        }

        let chunks = table_buffer.snapshot(Arc::clone(&table_def), i64::MAX);
        wal_content.push(WalFlushElement {
            table_id: *table_id,
            table_name: Arc::clone(&table_def.table_name),
            data: chunks.into_iter().map(|c| c.into_batch()).collect(),
        })
    }

    Ok(Arc::from(wal_content))
}
