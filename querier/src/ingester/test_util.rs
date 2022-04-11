use std::sync::Arc;

use async_trait::async_trait;

use super::IngesterConnection;

/// IngesterConnection for testing
#[derive(Debug)]
pub(crate) struct MockIngesterConnection {}

impl MockIngesterConnection {
    pub fn new() -> Self {
        Self {}
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
        _expected_schema: &schema::Schema,
    ) -> super::Result<Vec<Arc<super::IngesterPartition>>> {
        Ok(vec![])
    }
}
