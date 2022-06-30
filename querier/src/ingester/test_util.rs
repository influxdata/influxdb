use super::IngesterConnection;
use async_trait::async_trait;
use data_types::KafkaPartition;
use generated_types::influxdata::iox::ingester::v1::GetWriteInfoResponse;
use parking_lot::Mutex;
use std::{any::Any, sync::Arc};

/// IngesterConnection for testing
#[derive(Debug, Default)]
pub struct MockIngesterConnection {
    next_response: Mutex<Option<super::Result<Vec<super::IngesterPartition>>>>,
}

impl MockIngesterConnection {
    /// Create connection w/ an empty response.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set next response for this connection.
    #[allow(dead_code)]
    pub fn next_response(&self, response: super::Result<Vec<super::IngesterPartition>>) {
        *self.next_response.lock() = Some(response);
    }
}

#[async_trait]
impl IngesterConnection for MockIngesterConnection {
    async fn partitions(
        &self,
        _sequencer_ids: &[KafkaPartition],
        _namespace_name: Arc<str>,
        _table_name: Arc<str>,
        _columns: Vec<String>,
        _predicate: &predicate::Predicate,
        _expected_schema: Arc<schema::Schema>,
    ) -> super::Result<Vec<super::IngesterPartition>> {
        self.next_response
            .lock()
            .take()
            .unwrap_or_else(|| Ok(vec![]))
    }

    async fn get_write_info(&self, _write_token: &str) -> super::Result<GetWriteInfoResponse> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}
