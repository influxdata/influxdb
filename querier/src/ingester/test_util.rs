use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use parking_lot::Mutex;

use super::IngesterConnection;

/// IngesterConnection for testing
#[derive(Debug, Default)]
pub struct MockIngesterConnection {
    next_response: Mutex<Option<super::Result<Vec<Arc<super::IngesterPartition>>>>>,
}

impl MockIngesterConnection {
    /// Create connection w/ an empty response.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set next response for this connection.
    pub fn next_response(&self, response: super::Result<Vec<Arc<super::IngesterPartition>>>) {
        *self.next_response.lock() = Some(response);
    }
}

#[async_trait]
impl IngesterConnection for MockIngesterConnection {
    async fn partitions(
        &self,
        _namespace_name: Arc<str>,
        _table_name: Arc<str>,
        _columns: Vec<String>,
        _predicate: &predicate::Predicate,
        _expected_schema: Arc<schema::Schema>,
    ) -> super::Result<Vec<Arc<super::IngesterPartition>>> {
        self.next_response
            .lock()
            .take()
            .unwrap_or_else(|| Ok(vec![]))
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}
