//! Compatibility layer providing a [`DmlSink`] impl for [`IngesterData`].

use super::DmlSink;
use crate::{data::IngesterData, lifecycle::LifecycleHandleImpl};
use async_trait::async_trait;
use data_types::SequencerId;
use dml::DmlOperation;
use observability_deps::tracing::warn;
use std::sync::Arc;

/// Provides a [`DmlSink`] implementation for a [`IngesterData`] instance.
#[derive(Debug)]
pub struct IngestSinkAdaptor {
    ingest_data: Arc<IngesterData>,
    lifecycle_handle: LifecycleHandleImpl,
    sequencer_id: SequencerId,
    /// Skip buffering
    skip_buffering: bool,
}

impl IngestSinkAdaptor {
    /// Wrap an [`IngesterData`] in an adaptor layer to provide a [`DmlSink`]
    /// implementation.
    pub fn new(
        ingest_data: Arc<IngesterData>,
        lifecycle_handle: LifecycleHandleImpl,
        sequencer_id: SequencerId,
    ) -> Self {
        // Temporary setting (see
        // https://github.com/influxdata/conductor/issues/1034) that
        // can be used to have the ingesters read from kafka, but
        // ignore (do not buffer or persist)
        let env_name = "INFLUXDB_IOX_INGESTER_SKIP_BUFFER";
        let skip_buffering = match std::env::var(env_name) {
            Ok(s) if s.to_lowercase() == "true" => {
                warn!(name=%env_name, value=%s, "Skipping buffering due to environment request");
                true
            }
            Ok(s) => {
                warn!(name=%env_name, value=%s, "Unknown value for environment request. Expected 'true'");
                false
            }
            Err(_) => false,
        };

        Self {
            ingest_data,
            lifecycle_handle,
            sequencer_id,
            skip_buffering,
        }
    }
}

#[async_trait]
impl DmlSink for IngestSinkAdaptor {
    async fn apply(&self, op: DmlOperation) -> Result<bool, crate::data::Error> {
        if self.skip_buffering {
            // 'false' means do not pause
            Ok(false)
        } else {
            self.ingest_data
                .buffer_operation(self.sequencer_id, op, &self.lifecycle_handle)
                .await
        }
    }
}
